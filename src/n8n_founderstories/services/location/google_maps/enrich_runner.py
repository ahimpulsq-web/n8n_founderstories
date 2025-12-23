from __future__ import annotations

# =============================================================================
# enrich_runner.py
#
# Queue-driven enrichment:
# - Reads bounded windows of GoogleMaps_EnrichQueue
# - Updates GoogleMaps rows by row number (no scans)
# - Updates queue state cells in batches
# - Uses ISO2/HL from queue (no hardcoded en/xx)
# =============================================================================

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, Tuple
from urllib.parse import urlparse

from ....core.utils.text import norm
from ....services.exports.sheets import SheetsClient, default_sheets_config
from ....services.exports.sheets_manager import GoogleSheetsManager
from ....services.jobs.logging import job_logger
from ....services.jobs.sheets_status import ToolStatusWriter
from ....services.jobs.store import mark_failed, mark_running, mark_succeeded, update_progress
from ....services.search_plan import SearchPlan
from ....services.storage import save_google_maps_output
from .client import GooglePlacesClient

logger = logging.getLogger(__name__)

TAB_STATUS = "Tool_Status"
TAB_MAIN = "GoogleMaps"

TAB_ENRICH_QUEUE = "GoogleMaps_EnrichQueue"
HEADERS_ENRICH_QUEUE = ["Place ID", "Sheet Row", "ISO2", "HL", "State", "Updated At", "Error"]

TAB_ENRICH_STATE = "GoogleMaps_EnrichQueue_State"
HEADERS_ENRICH_STATE = ["Job ID", "Last Queue Row", "Updated At (UTC)"]

# Queue columns (0-based)
Q_COL_PLACE_ID = 0
Q_COL_SHEET_ROW = 1
Q_COL_ISO2 = 2
Q_COL_HL = 3
Q_COL_STATE = 4
Q_COL_UPDATED_AT = 5
Q_COL_ERROR = 6

# Main sheet target columns
MAIN_COL_WEBSITE = "F"
MAIN_COL_DOMAIN = "G"
MAIN_COL_PHONE = "H"
MAIN_COL_GOOGLE_URL = "K"

# Queue update columns (A1)
QUEUE_COL_STATE = "E"
QUEUE_COL_UPDATED_AT = "F"
QUEUE_COL_ERROR = "G"

QUEUE_START_ROW = 2
DETAILS_FIELDS = "website,url,formatted_phone_number,international_phone_number"

STATE_PENDING = "PENDING"
STATE_DONE = "DONE"
STATE_FAILED = "FAILED"


def _utc_iso() -> str:
    return datetime.utcnow().isoformat()


def _safe_int(v: str | None) -> Optional[int]:
    try:
        x = int(norm(v))
        return x if x > 0 else None
    except Exception:
        return None


def _domain_from_website(website: str) -> str:
    w = norm(website)
    if not w:
        return ""
    try:
        host = urlparse(w).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""


def _maps_url_from_place_id(place_id: str) -> str:
    pid = norm(place_id)
    if not pid:
        return ""
    return f"https://www.google.com/maps/search/?api=1&query_place_id={pid}"


def _looks_like_language_tag(v: str) -> bool:
    s = norm(v)
    if not s or len(s) > 10:
        return False
    return all(ch.isalpha() or ch == "-" for ch in s)


def _resolve_language(hl: str | None, default_lang: str = "en") -> str:
    h = norm(hl)
    return h if h and _looks_like_language_tag(h) else default_lang


@dataclass(frozen=True)
class QueueItem:
    queue_row: int
    place_id: str
    sheet_row: int
    iso2: str
    hl: str


def _read_watermark(sheets: SheetsClient, *, job_id: str) -> int:
    jid = norm(job_id)
    if not jid:
        return QUEUE_START_ROW - 1

    rows = sheets.read_range(tab_name=TAB_ENRICH_STATE, a1_range="A2:C2001")
    for r in rows:
        if not r:
            continue
        if norm(r[0]) == jid:
            last_row = _safe_int(r[1])
            return last_row if last_row is not None else (QUEUE_START_ROW - 1)

    return QUEUE_START_ROW - 1


def _write_watermark(sheets: SheetsClient, *, job_id: str, last_queue_row: int) -> None:
    jid = norm(job_id)
    if not jid:
        return

    sheets.upsert_row_by_key(
        tab_name=TAB_ENRICH_STATE,
        key=jid,
        row_values=[jid, str(int(last_queue_row)), _utc_iso()],
        key_col_letter="A",
        start_row=2,
        max_scan_rows=2000,
    )


def _row_is_empty(r: List[str]) -> bool:
    if not r:
        return True
    return all(not norm(c) for c in r)


def _block_is_empty(block: List[List[str]]) -> bool:
    if not block:
        return True
    return all(_row_is_empty([norm(c) for c in (row or [])]) for row in block)


def _parse_queue_block(block: List[List[str]], *, start_row: int) -> List[QueueItem]:
    out: List[QueueItem] = []
    row_num = start_row

    for r in block:
        place_id = norm(r[Q_COL_PLACE_ID]) if len(r) > Q_COL_PLACE_ID else ""
        sheet_row_s = norm(r[Q_COL_SHEET_ROW]) if len(r) > Q_COL_SHEET_ROW else ""
        iso2 = norm(r[Q_COL_ISO2]) if len(r) > Q_COL_ISO2 else ""
        hl = norm(r[Q_COL_HL]) if len(r) > Q_COL_HL else ""
        state = norm(r[Q_COL_STATE]) if len(r) > Q_COL_STATE else ""

        if not place_id:
            row_num += 1
            continue

        sr = _safe_int(sheet_row_s)
        if sr is None:
            row_num += 1
            continue

        # eligible if blank or PENDING
        if state and state.upper() != STATE_PENDING:
            row_num += 1
            continue

        out.append(
            QueueItem(
                queue_row=row_num,
                place_id=place_id,
                sheet_row=sr,
                iso2=iso2,
                hl=hl,
            )
        )
        row_num += 1

    return out


def run_google_maps_enrich_job(
    *,
    job_id: str,
    plan: SearchPlan,
    spreadsheet_id: str,
    max_items: int = 500,
    batch_size: int = 200,
    linger_seconds: float = 3.0,
    max_empty_batches: int = 10,
) -> None:
    rid = norm(getattr(plan, "request_id", None))
    log = job_logger(__name__, tool="google_maps_enrich", request_id=rid, job_id=job_id)

    sheets: SheetsClient | None = None
    status: ToolStatusWriter | None = None

    processed = 0
    succeeded = 0
    failed = 0

    empty_batches = 0
    last_queue_row_written = QUEUE_START_ROW - 1

    try:
        mark_running(job_id)

        sid = norm(spreadsheet_id)
        if not sid:
            raise ValueError("spreadsheet_id must not be empty.")
        if not rid:
            raise ValueError("search_plan.request_id must not be empty.")

        max_items_eff = max(1, int(max_items))
        batch_eff = max(1, int(batch_size))
        linger_s = max(0.0, float(linger_seconds))
        max_empty = max(0, int(max_empty_batches))

        sheets = SheetsClient(config=default_sheets_config(spreadsheet_id=sid))
        _gsm = GoogleSheetsManager(client=sheets)
        status = ToolStatusWriter(sheets=sheets, spreadsheet_id=sid)

        sheets.ensure_tab(TAB_STATUS)
        sheets.ensure_tab(TAB_MAIN)
        sheets.ensure_tab_with_header(TAB_ENRICH_QUEUE, HEADERS_ENRICH_QUEUE)
        sheets.ensure_tab_with_header(TAB_ENRICH_STATE, HEADERS_ENRICH_STATE)

        # Keep queue/state hidden (best-effort)
        try:
            sheets.hide_tab(tab_name=TAB_ENRICH_QUEUE)
            sheets.hide_tab(tab_name=TAB_ENRICH_STATE)
        except Exception:
            logger.exception("GOOGLE_MAPS_ENRICH_HIDE_TABS_FAILED")

        start_from = _read_watermark(sheets, job_id=job_id) + 1
        if start_from < QUEUE_START_ROW:
            start_from = QUEUE_START_ROW

        update_progress(
            job_id,
            phase="enrich",
            current=0,
            total=max_items_eff,
            message=f"Starting enrichment from queue row {start_from}.",
            metrics={"batch_size": batch_eff, "linger_seconds": linger_s, "max_empty_batches": max_empty},
        )

        status.write(
            job_id=job_id,
            tool="google_maps_enrich",
            request_id=rid,
            state="RUNNING",
            phase="enrich",
            current=0,
            total=max_items_eff,
            message=f"Starting enrichment from queue row {start_from}.",
            meta={"batch_size": batch_eff, "start_row": start_from, "linger_seconds": linger_s, "max_empty_batches": max_empty},
        )

        cursor = start_from

        with GooglePlacesClient() as client:
            while processed < max_items_eff:
                end_row = cursor + batch_eff - 1
                block = sheets.read_range(tab_name=TAB_ENRICH_QUEUE, a1_range=f"A{cursor}:G{end_row}")

                if _block_is_empty(block):
                    empty_batches += 1

                    msg = (
                        f"Queue idle at {cursor}-{end_row} "
                        f"(empty={empty_batches}/{max_empty}). processed={processed} ok={succeeded} failed={failed}"
                    )
                    update_progress(
                        job_id,
                        phase="enrich",
                        current=processed,
                        total=max_items_eff,
                        message=msg,
                        metrics={"ok": succeeded, "failed": failed, "empty_batches": empty_batches},
                    )
                    status.write(
                        job_id=job_id,
                        tool="google_maps_enrich",
                        request_id=rid,
                        state="RUNNING",
                        phase="enrich",
                        current=processed,
                        total=max_items_eff,
                        message=msg,
                        meta={"cursor": cursor, "empty_batches": empty_batches, "ok": succeeded, "failed": failed},
                    )

                    if max_empty == 0 or empty_batches >= max_empty:
                        break

                    if linger_s > 0:
                        time.sleep(linger_s)

                    continue

                empty_batches = 0

                items = _parse_queue_block(block, start_row=cursor)

                # If block has data but no eligible items, advance cursor and watermark.
                if not items:
                    cursor = end_row + 1
                    last_queue_row_written = max(last_queue_row_written, end_row)
                    _write_watermark(sheets, job_id=job_id, last_queue_row=last_queue_row_written)
                    continue

                main_cell_updates: List[Tuple[str, str]] = []
                queue_cell_updates: List[Tuple[str, str]] = []

                for it in items:
                    if processed >= max_items_eff:
                        break

                    processed += 1
                    now_iso = _utc_iso()

                    # Resolve language/region from queue
                    hl_used = _resolve_language(it.hl, default_lang="en")
                    region = norm(it.iso2).lower() or "xx"

                    try:
                        data = client.place_details(
                            place_id=it.place_id,
                            language=hl_used,
                            region=region,
                            fields=DETAILS_FIELDS,
                        )

                        res = data.get("result") or {}
                        if not isinstance(res, dict):
                            res = {}

                        website = norm(res.get("website"))
                        g_url = norm(res.get("url")) or _maps_url_from_place_id(it.place_id)
                        phone = norm(res.get("international_phone_number")) or norm(res.get("formatted_phone_number"))
                        domain = _domain_from_website(website)

                        main_cell_updates.extend(
                            [
                                (f"{MAIN_COL_WEBSITE}{it.sheet_row}", website),
                                (f"{MAIN_COL_DOMAIN}{it.sheet_row}", domain),
                                (f"{MAIN_COL_PHONE}{it.sheet_row}", phone),
                                (f"{MAIN_COL_GOOGLE_URL}{it.sheet_row}", g_url),
                            ]
                        )

                        succeeded += 1
                        queue_cell_updates.extend(
                            [
                                (f"{QUEUE_COL_STATE}{it.queue_row}", STATE_DONE),
                                (f"{QUEUE_COL_UPDATED_AT}{it.queue_row}", now_iso),
                                (f"{QUEUE_COL_ERROR}{it.queue_row}", ""),
                            ]
                        )

                    except Exception as exc:
                        failed += 1
                        queue_cell_updates.extend(
                            [
                                (f"{QUEUE_COL_STATE}{it.queue_row}", STATE_FAILED),
                                (f"{QUEUE_COL_UPDATED_AT}{it.queue_row}", now_iso),
                                (f"{QUEUE_COL_ERROR}{it.queue_row}", str(exc)),
                            ]
                        )

                    last_queue_row_written = max(last_queue_row_written, it.queue_row)

                    if processed % 10 == 0:
                        msg = f"Enrich processed={processed} ok={succeeded} failed={failed}"
                        update_progress(job_id, phase="enrich", current=processed, total=max_items_eff, message=msg, metrics={"ok": succeeded, "failed": failed})
                        status.write(job_id=job_id, tool="google_maps_enrich", request_id=rid, state="RUNNING", phase="enrich", current=processed, total=max_items_eff, message=msg, meta={"ok": succeeded, "failed": failed, "cursor": cursor})

                # Batch write updates
                if main_cell_updates:
                    sheets.batch_update_cells(tab_name=TAB_MAIN, updates=main_cell_updates)
                if queue_cell_updates:
                    sheets.batch_update_cells(tab_name=TAB_ENRICH_QUEUE, updates=queue_cell_updates)

                # Advance cursor and watermark after the window
                cursor = end_row + 1
                _write_watermark(sheets, job_id=job_id, last_queue_row=last_queue_row_written)

        # Artifact summary
        try:
            save_google_maps_output(
                request_id=rid,
                prompt=norm(getattr(plan, "raw_prompt", None)),
                payload={
                    "job_id": job_id,
                    "kind": "enrich_summary",
                    "processed": processed,
                    "succeeded": succeeded,
                    "failed": failed,
                    "last_queue_row": last_queue_row_written,
                    "empty_batches": empty_batches,
                    "linger_seconds": linger_s,
                    "max_empty_batches": max_empty,
                },
                provider="google_maps",
                kind="enrich_summary",
            )
        except Exception:
            logger.exception("GOOGLE_MAPS_ENRICH_ARTIFACT_SAVE_FAILED")

        msg = f"Google Maps enrichment completed. processed={processed} ok={succeeded} failed={failed}"
        if processed >= max_items_eff:
            msg += " (stopped: max_items reached)"
        elif max_empty > 0 and empty_batches >= max_empty:
            msg += " (stopped: queue idle)"

        mark_succeeded(job_id, message=msg, metrics={"processed": processed, "ok": succeeded, "failed": failed})
        log.info("JOB_SUCCEEDED | %s", msg)

        if status:
            status.write(
                job_id=job_id,
                tool="google_maps_enrich",
                request_id=rid,
                state="SUCCEEDED",
                phase="enrich",
                current=processed,
                total=max_items_eff,
                message=msg,
                meta={"processed": processed, "ok": succeeded, "failed": failed},
                force=True,
            )

    except Exception as exc:
        mark_failed(job_id, error=str(exc), message="Google Maps enrichment job failed.")
        log.exception("JOB_FAILED | error=%s", exc)

        try:
            if status:
                status.write(
                    job_id=job_id,
                    tool="google_maps_enrich",
                    request_id=rid or "",
                    state="FAILED",
                    phase="enrich",
                    current=processed,
                    total=max_items if isinstance(max_items, int) else 500,
                    message=str(exc),
                    meta={"processed": processed, "ok": succeeded, "failed": failed},
                    force=True,
                )
        except Exception:
            logger.exception("TOOL_STATUS_WRITE_FAILED")
