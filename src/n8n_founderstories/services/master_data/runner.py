# =============================================================================
# runner.py (Master ingestion) — UPDATED FOR SHEETS MANAGER (BUFFERED WRITES)
#
# Changes vs your current version:
# - Uses GoogleSheetsManager for buffered writes (append + headers + structural ops batching).
# - Removes any private SheetsClient usage (no _get_sheet_id_by_title).
# - Uses SheetsClient.get_sheet_id() for conditional formatting.
# - Keeps your TEST MODE restart behavior intact.
# =============================================================================

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from ..exports.sheets import SheetsClient, default_sheets_config
from ..exports.sheets_manager import GoogleSheetsManager  # <-- NEW
from ..jobs.logging import job_logger
from ..jobs.sheets_status import ToolStatusWriter
from ..jobs.store import mark_failed, mark_running, mark_succeeded, update_progress
from ..search_plan import SearchPlan

logger = logging.getLogger(__name__)

TAB_MASTER = "Master"
TAB_MASTER_STATE = "Master_State"

# ---------------------------------------------------------------------------
# SINGLE SOURCE OF TRUTH: schema keys + header labels
# ---------------------------------------------------------------------------
MASTER_SCHEMA: list[tuple[str, str]] = [
    ("company", "Company Name"),
    ("domain", "Primary Domain"),
    ("website", "Website URL"),
    ("source_tool", "Source Tool"),
    ("location", "Location"),
    ("lead_query", "Lead Source Query"),
    ("dup_in_run", "Duplicate (This Run)"),
]

MASTER_KEYS = [k for (k, _label) in MASTER_SCHEMA]
MASTER_HEADERS = [label for (_k, label) in MASTER_SCHEMA]

STATE_HEADERS = [
    "Source Tab",
    "Last Processed Row",
    "Updated At (UTC)",
]

DEFAULT_DOMAIN_COL_MAP: dict[str, int] = {
    "HunterIO": 0,
    "GoogleMaps": 6,
    "GoogleSearch": 1,
}

DEFAULT_COLUMN_MAP: dict[str, dict[str, int]] = {
    "HunterIO": {
        "company": 1,
        "domain": 0,
        "website": 0,
        "location": 2,
        "query_source": 6,
    },
    "GoogleMaps": {
        "company": 0,
        "domain": 6,
        "website": 5,
        "location": 1,
        "query_source": 8,
    },
    "GoogleSearch": {
        "company": 0,
        "domain": 1,
        "website": 2,
        "location": -1,
        "query_source": 6,
    },
}

DEFAULT_TEST_CAPS: dict[str, int] = {
    "HunterIO": 10,
    "GoogleMaps": 5,
    "GoogleSearch": 5,
}

STATUS_EVERY_N_PASSES = 1


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_get(row: list[str], idx: int) -> str:
    if idx is None or idx < 0:
        return ""
    if idx >= len(row):
        return ""
    return (row[idx] or "").strip()


def _row_is_empty(row: list[str]) -> bool:
    if not row:
        return True
    return all(not (c or "").strip() for c in row)


def _block_effective_rows(block: list[list[str]]) -> int:
    """
    Returns count of effective (non-empty) rows until the first fully-empty row.
    This makes window reads safe without knowing last_row.
    """
    n = 0
    for r in block:
        if _row_is_empty(r):
            break
        n += 1
    return n


def _col_index_to_a1(col_index: int) -> str:
    if col_index < 0:
        raise ValueError("col_index must be >= 0")
    n = col_index + 1
    letters: list[str] = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(ord("A") + rem))
    return "".join(reversed(letters))


def _load_watermarks(sheets_mgr: GoogleSheetsManager) -> dict[str, int]:
    rows = sheets_mgr.read_range(tab_name=TAB_MASTER_STATE, a1_range="A2:B")
    out: dict[str, int] = {}
    for r in rows:
        if not r:
            continue
        tab = (r[0] or "").strip()
        if not tab:
            continue
        try:
            last_row = int((r[1] or "1").strip())
        except Exception:
            last_row = 1
        out[tab] = max(1, last_row)
    return out


def _write_watermark(sheets: SheetsClient, *, source_tab: str, last_row: int) -> None:
    # NOTE: This still does a bounded scan. That is acceptable in production
    # because it runs once per "window" processed, not per row.
    sheets.upsert_row_by_key(
        tab_name=TAB_MASTER_STATE,
        key=source_tab,
        row_values=[source_tab, str(int(last_row)), _utc_now_iso()],
        key_col_letter="A",
        start_row=2,
        max_scan_rows=2000,
    )


@dataclass(frozen=True)
class MasterMaps:
    company_col: int
    domain_col: int
    website_col: int
    location_col: int
    query_source_col: int


def _resolve_maps_for_tab(
    tab_name: str,
    *,
    domain_col_map: dict[str, int],
    column_map: dict[str, dict[str, int]],
) -> MasterMaps:
    t = (tab_name or "").strip()
    cmap = column_map.get(t) or {}

    domain_col = cmap.get("domain")
    if domain_col is None:
        domain_col = domain_col_map.get(t, 0)

    return MasterMaps(
        company_col=int(cmap.get("company", -1)),
        domain_col=int(domain_col),
        website_col=int(cmap.get("website", -1)),
        location_col=int(cmap.get("location", -1)),
        query_source_col=int(cmap.get("query_source", -1)),
    )


def run_master_job(
    *,
    job_id: str,
    plan: SearchPlan,
    spreadsheet_id: str,
    source_tabs: list[str],
    domain_col_map: Optional[dict[str, int]] = None,
    column_map: Optional[dict[str, dict[str, int]]] = None,
    apply_formatting: bool = True,
    hide_state_tab: bool = True,
    hide_audit_tabs: bool = True,
    reorder_tabs: bool = True,
    linger_seconds: float = 3.0,
    max_empty_passes: int = 10,
    window_rows: int = 500,
    max_total_appends: int = 5000,
    # TEST MODE / CAPS
    test_mode: bool = True,
    reset_master_in_test_mode: bool = True,
    max_rows_per_source: dict[str, int] | None = None,
    max_passes: int | None = None,
) -> None:
    rid = (getattr(plan, "request_id", None) or "").strip()
    log = job_logger(__name__, tool="master", request_id=rid, job_id=job_id)

    sheets: SheetsClient | None = None
    sheets_mgr: GoogleSheetsManager | None = None
    status: ToolStatusWriter | None = None

    domain_col_map_eff = domain_col_map or DEFAULT_DOMAIN_COL_MAP
    column_map_eff = column_map or DEFAULT_COLUMN_MAP

    try:
        mark_running(job_id)

        sid = (spreadsheet_id or "").strip()
        if not sid:
            raise ValueError("spreadsheet_id must not be empty.")

        sheets = SheetsClient(config=default_sheets_config(spreadsheet_id=sid))
        sheets_mgr = GoogleSheetsManager(client=sheets)
        status = ToolStatusWriter(sheets=sheets, spreadsheet_id=sid)

        # ---------------------------------------------------------------------
        # Tabs + headers (test reset uses structural delete)
        # ---------------------------------------------------------------------
        if test_mode and reset_master_in_test_mode:
            try:
                sheets.delete_tab(tab_name=TAB_MASTER)
            except Exception:
                logger.exception("MASTER_TEST_RESET_DELETE_FAILED")

        # Use manager setup for owned tabs: less read quota, more batching
        # (We intentionally keep overwrite_headers_for_owned_tabs=True to avoid header reads.)
        sheets_mgr.setup_tabs(
            headers=[
                (TAB_MASTER, MASTER_HEADERS),
                (TAB_MASTER_STATE, STATE_HEADERS),
            ],
            hide_tabs=[TAB_MASTER_STATE] if hide_state_tab else [],
            tab_order=[],
            overwrite_headers_for_owned_tabs=True,
            # Use a lightweight init marker to prevent repeated setup churn
            # (In test mode, Master tab may be deleted each run; marker is still safe.)
            init_marker_tab="Tool_Status" if not test_mode else "Master_State",
            init_marker_cell="A1",
        )

        status.ensure_ready()

        # Optional: reorder/hide (kept from your logic)
        if reorder_tabs:
            try:
                order = ["Tool_Status", "HunterIO", "GoogleMaps", "GoogleSearch", TAB_MASTER]
                for idx, name in enumerate(order):
                    sheets.move_tab(tab_name=name, index=idx)

                if hide_audit_tabs:
                    for t in ["HunterIO_Audit", "GoogleMaps_Audit", "GoogleSearch_Audit"]:
                        sheets.hide_tab(tab_name=t)

                if hide_state_tab:
                    sheets.hide_tab(tab_name=TAB_MASTER_STATE)
            except Exception:
                logger.exception("MASTER_TAB_REORDER_FAILED")

        cleaned_sources = [t.strip() for t in (source_tabs or []) if (t or "").strip()]
        total_sources = len(cleaned_sources)

        # Root-cause fix for "same N each run":
        if test_mode and max_rows_per_source is None:
            max_rows_per_source = dict(DEFAULT_TEST_CAPS)

        if test_mode:
            watermarks = {t: 1 for t in cleaned_sources}
        else:
            watermarks = _load_watermarks(sheets_mgr)

        linger_s = 0.0 if test_mode else max(0.0, float(linger_seconds))
        empty_cap = 1 if test_mode else max(0, int(max_empty_passes))

        win = max(10, int(window_rows))
        max_append_cap = max(1, int(max_total_appends))
        caps_raw = {k.strip(): int(v) for (k, v) in (max_rows_per_source or {}).items() if (k or "").strip()}
        caps = {t: caps_raw[t] for t in cleaned_sources if t in caps_raw}

        update_progress(
            job_id,
            phase="master",
            current=0,
            total=total_sources,
            message="Starting Master ingestion.",
            metrics={
                "sources": total_sources,
                "test_mode": bool(test_mode),
                "reset_master_in_test_mode": bool(reset_master_in_test_mode),
                "linger_seconds": float(linger_s),
                "max_empty_passes": int(empty_cap),
                "window_rows": int(win),
                "max_total_appends": int(max_append_cap),
                "max_rows_per_source": caps,
                "master_cols": len(MASTER_KEYS),
            },
        )

        status.write(
            job_id=job_id,
            tool="master",
            request_id=rid,
            state="RUNNING",
            phase="master",
            current=0,
            total=total_sources,
            message="Starting Master ingestion.",
            meta={
                "sources": total_sources,
                "test_mode": bool(test_mode),
                "reset_master_in_test_mode": bool(reset_master_in_test_mode),
                "linger_seconds": float(linger_s),
                "max_empty_passes": int(empty_cap),
                "window_rows": int(win),
                "max_total_appends": int(max_append_cap),
                "max_rows_per_source": caps,
                "master_cols": len(MASTER_KEYS),
            },
        )

        appended_total = 0
        passes = 0
        empty_passes = 0

        appended_by_source: dict[str, int] = {t: 0 for t in cleaned_sources}
        seen_domains_run: set[str] = set()

        while appended_total < max_append_cap:
            passes += 1
            if max_passes is not None and passes > int(max_passes):
                break

            pass_appended = 0

            for tab_name in cleaned_sources:
                cap = caps.get(tab_name)
                if cap is not None and appended_by_source.get(tab_name, 0) >= cap:
                    continue

                maps = _resolve_maps_for_tab(
                    tab_name,
                    domain_col_map=domain_col_map_eff,
                    column_map=column_map_eff,
                )

                last_done = watermarks.get(tab_name, 1)
                start_row = last_done + 1

                remaining_cap = None
                if cap is not None:
                    remaining_cap = max(0, cap - appended_by_source.get(tab_name, 0))
                    if remaining_cap <= 0:
                        continue

                read_rows = win if remaining_cap is None else min(win, max(1, remaining_cap))
                end_row = start_row + read_rows - 1

                # READS: budgeted by manager
                block = sheets_mgr.read_range(tab_name=tab_name, a1_range=f"A{start_row}:Z{end_row}")

                eff_n = _block_effective_rows(block)
                if eff_n <= 0:
                    continue

                if remaining_cap is not None:
                    eff_n = min(eff_n, remaining_cap)

                master_rows: list[list[str]] = []
                for r in block[:eff_n]:
                    company = _safe_get(r, maps.company_col)
                    domain = _safe_get(r, maps.domain_col)
                    website = _safe_get(r, maps.website_col)
                    location = _safe_get(r, maps.location_col)
                    qsrc = _safe_get(r, maps.query_source_col)

                    d_l = domain.lower().strip()
                    dup = "YES" if d_l and d_l in seen_domains_run else "NO"
                    if d_l:
                        seen_domains_run.add(d_l)

                    row_obj = {
                        "company": company,
                        "domain": domain,
                        "website": website,
                        "source_tool": tab_name,
                        "location": location,
                        "lead_query": qsrc,
                        "dup_in_run": dup,
                    }
                    master_rows.append([row_obj.get(k, "") for k in MASTER_KEYS])

                if master_rows:
                    # WRITE: buffered append (one append per tab per flush)
                    sheets_mgr.queue_append_rows(tab_name=TAB_MASTER, rows=master_rows)

                    n_app = len(master_rows)
                    appended_total += n_app
                    pass_appended += n_app
                    appended_by_source[tab_name] = appended_by_source.get(tab_name, 0) + n_app

                # Watermarks
                new_last = last_done + eff_n
                watermarks[tab_name] = new_last

                if not test_mode:
                    # Persist watermark (bounded scan). Acceptable frequency: once per processed window.
                    _write_watermark(sheets, source_tab=tab_name, last_row=new_last)

                if appended_total >= max_append_cap:
                    break

            # Ensure buffered writes land periodically (and before formatting)
            sheets_mgr.flush()

            msg = (
                f"Master pass={passes} appended_this_pass={pass_appended} "
                f"appended_total={appended_total} empty_passes={empty_passes}/{empty_cap}"
            )

            update_progress(
                job_id,
                phase="master",
                current=total_sources,
                total=total_sources,
                message=msg,
                metrics={
                    "passes": passes,
                    "appended_total": appended_total,
                    "empty_passes": empty_passes,
                    "appended_by_source": dict(appended_by_source),
                },
            )

            if STATUS_EVERY_N_PASSES and (passes % STATUS_EVERY_N_PASSES == 0) and status:
                status.write(
                    job_id=job_id,
                    tool="master",
                    request_id=rid,
                    state="RUNNING",
                    phase="master",
                    current=appended_total,
                    total=max_append_cap,
                    message=msg,
                    meta={
                        "passes": passes,
                        "appended_total": appended_total,
                        "empty_passes": empty_passes,
                        "appended_by_source": dict(appended_by_source),
                        "test_mode": bool(test_mode),
                    },
                )

            if pass_appended == 0:
                empty_passes += 1
                if empty_cap == 0 or empty_passes >= empty_cap:
                    break
                if linger_s > 0:
                    time.sleep(linger_s)
            else:
                empty_passes = 0

        # Final flush to guarantee everything is written
        sheets_mgr.flush()

        # ---------------------------------------------------------------------
        # Formatting (requested): layout + duplicate highlighting
        # ---------------------------------------------------------------------
        if apply_formatting and sheets:
            try:
                # NOTE: get_last_row reads a column; OK here since it's once at end.
                last_row = sheets.get_last_row(tab_name=TAB_MASTER, signal_col=0)
                if last_row < 2:
                    last_row = 2

                sheets.format_table_layout(
                    tab_name=TAB_MASTER,
                    n_cols=len(MASTER_KEYS),
                    last_row=last_row,
                    header_row=True,
                    auto_resize=True,
                    wrap_strategy_body="OVERFLOW_CELL",
                )

                dup_idx = MASTER_KEYS.index("dup_in_run")
                dup_letter = _col_index_to_a1(dup_idx)
                formula = f'=${dup_letter}2="YES"'

                sheet_id = sheets.get_sheet_id(TAB_MASTER)
                if sheet_id is not None:
                    rule = sheets.build_conditional_format_rule_custom_formula(
                        sheet_id=sheet_id,
                        start_row_index=1,
                        start_col_index=0,
                        end_col_index=len(MASTER_KEYS),
                        end_row_index=last_row,
                        formula=formula,
                        background_rgb=(1.0, 0.85, 0.85),
                    )
                    sheets.replace_conditional_format_rules(tab_name=TAB_MASTER, rules=[rule])

            except Exception:
                logger.exception("MASTER_FORMATTING_FAILED")

        msg = (
            f"Master job completed. appended_total={appended_total} "
            f"passes={passes} empty_passes={empty_passes}"
        )
        if appended_total >= max_append_cap:
            msg += " (stopped: max_total_appends reached)"
        elif max_passes is not None and passes > int(max_passes):
            msg += " (stopped: max_passes reached)"
        elif empty_cap == 0 or empty_passes >= empty_cap:
            msg += " (stopped: idle)"

        mark_succeeded(job_id, message=msg, metrics={"appended_total": appended_total, "passes": passes})
        log.info("JOB_SUCCEEDED | %s", msg)

        if status:
            status.write(
                job_id=job_id,
                tool="master",
                request_id=rid,
                state="SUCCEEDED",
                phase="master",
                current=appended_total,
                total=max_append_cap,
                message=msg,
                meta={
                    "appended_total": appended_total,
                    "passes": passes,
                    "empty_passes": empty_passes,
                    "appended_by_source": dict(appended_by_source),
                    "test_mode": bool(test_mode),
                    "reset_master_in_test_mode": bool(reset_master_in_test_mode),
                },
                force=True,
            )

    except Exception as exc:
        mark_failed(job_id, error=str(exc), message="Master job failed.")
        log.exception("JOB_FAILED | error=%s", exc)

        try:
            if status:
                status.write(
                    job_id=job_id,
                    tool="master",
                    request_id=rid or "",
                    state="FAILED",
                    phase="master",
                    current=0,
                    total=len(source_tabs or []),
                    message=str(exc),
                    meta={},
                    force=True,
                )
        except Exception:
            logger.exception("TOOL_STATUS_WRITE_FAILED")
