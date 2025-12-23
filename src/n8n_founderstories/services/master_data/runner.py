# =============================================================================
# C:\Projects\N8N-FounderStories\src\n8n_founderstories\services\master_data\runner.py
# =============================================================================

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from ..exports.sheets import SheetsClient, default_sheets_config
from ..jobs.logging import job_logger
from ..jobs.sheets_status import ToolStatusWriter
from ..jobs.store import mark_failed, mark_running, mark_succeeded, update_progress
from ..search_plan import SearchPlan

logger = logging.getLogger(__name__)

TAB_MASTER = "Master"
TAB_MASTER_STATE = "Master_State"

MASTER_HEADERS = [
    "Company",
    "Domain",
    "Website",
    "Source",
    "Location",
    "Query Source",
    "Repeated",
]

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

# How often to write RUNNING status messages (per pass).
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


def _load_watermarks(sheets: SheetsClient) -> dict[str, int]:
    rows = sheets.read_range(tab_name=TAB_MASTER_STATE, a1_range="A2:B")
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
    # NEW: linger mode so Master can start before upstream tools finish
    linger_seconds: float = 3.0,
    max_empty_passes: int = 10,
    # NEW: bounded reads (keeps reads light; no last-row scan)
    window_rows: int = 500,
    max_total_appends: int = 5000,  # safety cap for one run
) -> None:
    rid = (getattr(plan, "request_id", None) or "").strip()
    log = job_logger(__name__, tool="master", request_id=rid, job_id=job_id)

    sheets: SheetsClient | None = None
    status: ToolStatusWriter | None = None

    domain_col_map_eff = domain_col_map or DEFAULT_DOMAIN_COL_MAP
    column_map_eff = column_map or DEFAULT_COLUMN_MAP

    try:
        mark_running(job_id)

        sid = (spreadsheet_id or "").strip()
        if not sid:
            raise ValueError("spreadsheet_id must not be empty.")

        sheets = SheetsClient(config=default_sheets_config(spreadsheet_id=sid))
        status = ToolStatusWriter(sheets=sheets, spreadsheet_id=sid)

        # Ensure tabs exist + headers
        sheets.ensure_tab_with_header(TAB_MASTER, MASTER_HEADERS)
        sheets.ensure_tab_with_header(TAB_MASTER_STATE, STATE_HEADERS)
        status.ensure_ready()

        if hide_state_tab:
            try:
                sheets.hide_tab(tab_name=TAB_MASTER_STATE)
            except Exception:
                logger.exception("MASTER_HIDE_STATE_TAB_FAILED")

        # Best-effort tab layout controls (kept)
        if sheets and reorder_tabs:
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

        # Load watermarks once; update in-memory per pass
        watermarks = _load_watermarks(sheets)

        cleaned_sources = [t.strip() for t in (source_tabs or []) if (t or "").strip()]
        total_sources = len(cleaned_sources)

        update_progress(
            job_id,
            phase="master",
            current=0,
            total=total_sources,
            message="Starting Master ingestion (linger mode).",
            metrics={
                "sources": total_sources,
                "linger_seconds": float(linger_seconds),
                "max_empty_passes": int(max_empty_passes),
                "window_rows": int(window_rows),
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
            message="Starting Master ingestion (linger mode).",
            meta={
                "sources": total_sources,
                "linger_seconds": float(linger_seconds),
                "max_empty_passes": int(max_empty_passes),
                "window_rows": int(window_rows),
            },
        )

        appended_total = 0
        passes = 0
        empty_passes = 0

        # NOTE: This "Repeated" is within THIS master job run only.
        # If you want cross-run repeated marking, we can add a hidden Master_Index tab later.
        seen_domains_run: set[str] = set()

        linger_s = max(0.0, float(linger_seconds))
        empty_cap = max(0, int(max_empty_passes))
        win = max(10, int(window_rows))
        max_append_cap = max(1, int(max_total_appends))

        while appended_total < max_append_cap:
            passes += 1
            pass_appended = 0

            for tab_name in cleaned_sources:
                maps = _resolve_maps_for_tab(
                    tab_name,
                    domain_col_map=domain_col_map_eff,
                    column_map=column_map_eff,
                )

                last_done = watermarks.get(tab_name, 1)
                start_row = last_done + 1

                # Read a bounded window only (no last-row scan)
                end_row = start_row + win - 1
                block = sheets.read_range(tab_name=tab_name, a1_range=f"A{start_row}:Z{end_row}")

                eff_n = _block_effective_rows(block)
                if eff_n <= 0:
                    continue

                # Build Master rows
                master_rows: list[list[str]] = []
                for r in block[:eff_n]:
                    company = _safe_get(r, maps.company_col)
                    domain = _safe_get(r, maps.domain_col)
                    website = _safe_get(r, maps.website_col)
                    location = _safe_get(r, maps.location_col)
                    qsrc = _safe_get(r, maps.query_source_col)

                    d_l = domain.lower().strip()
                    repeated = "YES" if d_l and d_l in seen_domains_run else "NO"
                    if d_l:
                        seen_domains_run.add(d_l)

                    master_rows.append(
                        [
                            company,
                            domain,
                            website,
                            tab_name,
                            location,
                            qsrc,
                            repeated,
                        ]
                    )

                if master_rows:
                    sheets.append_rows(tab_name=TAB_MASTER, rows=master_rows)
                    appended_total += len(master_rows)
                    pass_appended += len(master_rows)

                # Advance watermark precisely by effective rows consumed
                new_last = last_done + eff_n
                watermarks[tab_name] = new_last
                _write_watermark(sheets, source_tab=tab_name, last_row=new_last)

                if appended_total >= max_append_cap:
                    break

            # Status/progress per pass
            msg = (
                f"Master pass={passes} appended_this_pass={pass_appended} "
                f"appended_total={appended_total} empty_passes={empty_passes}/{empty_cap}"
            )

            update_progress(
                job_id,
                phase="master",
                current=min(total_sources, total_sources),  # master isn't “per source” now; keep stable
                total=total_sources,
                message=msg,
                metrics={
                    "passes": passes,
                    "appended_total": appended_total,
                    "empty_passes": empty_passes,
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
                        "linger_seconds": linger_s,
                    },
                )

            # Linger/stop logic
            if pass_appended == 0:
                empty_passes += 1
                if empty_cap == 0 or empty_passes >= empty_cap:
                    break
                if linger_s > 0:
                    time.sleep(linger_s)
            else:
                empty_passes = 0

        # Formatting (optional; best-effort)
        if apply_formatting and sheets:
            try:
                sheet_id = sheets._get_sheet_id_by_title(TAB_MASTER)
                if sheet_id is not None:
                    rule = sheets.build_conditional_format_rule_custom_formula(
                        sheet_id=sheet_id,
                        start_row_index=1,
                        start_col_index=0,
                        end_col_index=7,
                        formula='=$G2="YES"',
                        background_rgb=(1.0, 0.85, 0.85),
                    )
                    sheets.replace_conditional_format_rules(tab_name=TAB_MASTER, rules=[rule])
            except Exception:
                logger.exception("MASTER_FORMATTING_FAILED")

        msg = f"Master job completed. appended_total={appended_total} passes={passes} empty_passes={empty_passes}"
        if appended_total >= max_append_cap:
            msg += " (stopped: max_total_appends reached)"
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
                meta={"appended_total": appended_total, "passes": passes, "empty_passes": empty_passes},
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
