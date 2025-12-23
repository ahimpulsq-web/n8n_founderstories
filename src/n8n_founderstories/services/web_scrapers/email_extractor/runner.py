from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from ....core.utils.net import normalize_domain
from ....core.utils.sheets_a1 import col_index_to_a1
from ....core.utils.text import norm

from ....services.exports.sheets import SheetsClient, default_sheets_config
from ....services.exports.sheets_manager import GoogleSheetsManager
from ....services.jobs.logging import job_logger
from ....services.jobs.sheets_status import ToolStatusWriter
from ....services.jobs.store import mark_failed, mark_running, mark_succeeded, update_progress

from .extractor import extract_emails_for_domain, choose_best_email

logger = logging.getLogger(__name__)

TAB_TOOL_STATUS = "Tool_Status"

# Runner state: watermark per sheet title (prevents reprocessing and supports linger mode).
TAB_EMAIL_STATE = "EmailExtractor_State"
EMAIL_STATE_HEADERS = ["Sheet Title", "Last Processed Row", "Updated At (UTC)"]

OUT_COLS = [
    "extracted_email",
    "extracted_email_all",
    "extraction_source_url",
    "extraction_status",
]


# -----------------------------------------------------------------------------
# Small helpers (kept local; specific to this runner)
# -----------------------------------------------------------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_is_empty(row: list[str]) -> bool:
    if not row:
        return True
    return all(not (c or "").strip() for c in row)


def _block_effective_rows(block: list[list[str]]) -> int:
    """
    Returns number of effective (non-empty) rows until the first fully-empty row.
    This makes bounded-window reads safe without knowing last_row.
    """
    n = 0
    for r in block:
        if _row_is_empty(r):
            break
        n += 1
    return n


def _read_header(sheets: SheetsClient, tab: str) -> list[str]:
    rows = sheets.read_range(tab_name=tab, a1_range="1:1")
    hdr = rows[0] if rows else []
    return [norm(h) for h in hdr]


def _ensure_output_columns_at_end(gsm: GoogleSheetsManager, tab: str, header: list[str]) -> list[str]:
    """
    Ensures OUT_COLS exist at the end of the header row.
    Flushes immediately to make subsequent range writes safe.
    """
    existing = {h: i for i, h in enumerate(header) if h}
    missing = [c for c in OUT_COLS if c not in existing]
    if not missing:
        return header

    new_header = list(header) + missing
    gsm.queue_values_update(tab_name=tab, a1_range="A1", values=[new_header])
    gsm.flush()
    return new_header


def _output_col_block(header: list[str]) -> tuple[int, int]:
    """
    Returns inclusive output block [start, end] column indices in the sheet header.
    """
    pos = {h: i for i, h in enumerate(header)}
    start = pos[OUT_COLS[0]]
    end = pos[OUT_COLS[-1]]
    if end < start:
        raise RuntimeError("Output columns are not contiguous / incorrect order.")
    return start, end


def _load_watermark(sheets: SheetsClient, *, sheet_title: str) -> int:
    """
    Reads EmailExtractor_State and returns last processed row for given sheet title.
    Watermark row numbers are 1-based sheet rows (same as Sheets UI).
    """
    key = norm(sheet_title)
    if not key:
        return 1

    rows = sheets.read_range(tab_name=TAB_EMAIL_STATE, a1_range="A2:B")
    for r in rows:
        if not r:
            continue
        if norm(r[0]) != key:
            continue
        try:
            return max(1, int(norm(r[1]) or "1"))
        except Exception:
            return 1
    return 1


def _write_watermark(sheets: SheetsClient, *, sheet_title: str, last_row: int) -> None:
    sheets.upsert_row_by_key(
        tab_name=TAB_EMAIL_STATE,
        key=sheet_title,
        row_values=[norm(sheet_title), str(int(last_row)), _utc_now_iso()],
        key_col_letter="A",
        start_row=2,
        max_scan_rows=2000,
    )


# -----------------------------------------------------------------------------
# Runner
# -----------------------------------------------------------------------------

def run_email_extractor_job(
    *,
    job_id: str,
    request_id: str,
    spreadsheet_id: str,
    sheet_title: str = "Master",
    apply_formatting: bool = False,  # placeholder for future parity
    # Linger mode (same concept as Master runner)
    linger_seconds: float = 3.0,
    max_empty_passes: int = 10,
    # Bounded reads (no last-row scan)
    window_rows: int = 500,
    max_total_updates: int = 5000,  # safety cap for one run
) -> None:
    rid = norm(request_id)
    sid = norm(spreadsheet_id)
    tab = norm(sheet_title) or "Master"

    log = job_logger(__name__, tool="email_extractor", request_id=rid, job_id=job_id)

    sheets: SheetsClient | None = None
    gsm: GoogleSheetsManager | None = None
    status: ToolStatusWriter | None = None

    processed = ok = invalid = not_found = 0
    passes = 0
    empty_passes = 0

    last_msg = "Starting email extraction (linger mode)."

    try:
        mark_running(job_id)

        if not rid:
            raise ValueError("request_id must not be empty.")
        if not sid:
            raise ValueError("spreadsheet_id must not be empty.")

        sheets = SheetsClient(config=default_sheets_config(spreadsheet_id=sid))
        gsm = GoogleSheetsManager(client=sheets)
        status = ToolStatusWriter(sheets=sheets, spreadsheet_id=sid, manager=gsm)

        # Ensure Tool_Status exists (consistent with other tools)
        sheets.ensure_tab(TAB_TOOL_STATUS)

        # Ensure state tab exists (watermark)
        sheets.ensure_tab_with_header(TAB_EMAIL_STATE, EMAIL_STATE_HEADERS)

        linger_s = max(0.0, float(linger_seconds))
        empty_cap = max(0, int(max_empty_passes))
        win = max(10, int(window_rows))
        max_updates = max(1, int(max_total_updates))

        update_progress(
            job_id,
            phase="extract",
            current=0,
            total=max_updates,
            message=last_msg,
            metrics={
                "sheet_title": tab,
                "window_rows": win,
                "max_empty_passes": empty_cap,
                "linger_seconds": linger_s,
                "max_total_updates": max_updates,
            },
        )

        status.write(
            job_id=job_id,
            tool="email_extractor",
            request_id=rid,
            state="RUNNING",
            phase="extract",
            current=0,
            total=max_updates,
            message=last_msg,
            meta={
                "sheet_title": tab,
                "window_rows": win,
                "max_empty_passes": empty_cap,
                "linger_seconds": linger_s,
                "max_total_updates": max_updates,
            },
        )

        # Computed once header is ready (inside linger loop)
        start_letter = ""
        end_letter = ""

        # Linger loop: wait until Master header exists and new domains appear
        while processed < max_updates:
            passes += 1

            # 0) Header readiness (linger-friendly)
            header = _read_header(sheets, tab)
            if not header:
                empty_passes += 1
                last_msg = (
                    f"Waiting for Master header. pass={passes} "
                    f"empty_passes={empty_passes}/{empty_cap} (Row 1 is empty)"
                )

                update_progress(
                    job_id,
                    phase="extract",
                    current=processed,
                    total=max_updates,
                    message=last_msg,
                    metrics={"passes": passes, "empty_passes": empty_passes},
                )
                status.write(
                    job_id=job_id,
                    tool="email_extractor",
                    request_id=rid,
                    state="RUNNING",
                    phase="extract",
                    current=processed,
                    total=max_updates,
                    message=last_msg,
                    meta={"passes": passes, "empty_passes": empty_passes},
                )

                if empty_cap == 0 or empty_passes >= empty_cap:
                    msg = f"Email extractor stopped: Master header missing after {empty_passes} passes."
                    mark_succeeded(job_id, message=msg, metrics={"processed": processed})
                    log.info("JOB_SUCCEEDED | %s", msg)

                    status.write(
                        job_id=job_id,
                        tool="email_extractor",
                        request_id=rid,
                        state="SUCCEEDED",
                        phase="extract",
                        current=processed,
                        total=max_updates,
                        message=msg,
                        meta={"passes": passes, "empty_passes": empty_passes},
                        force=True,
                    )
                    if gsm:
                        gsm.flush()
                    return

                if linger_s > 0:
                    time.sleep(linger_s)
                continue

            # Header is ready; ensure output cols + compute output A1 letters
            header = _ensure_output_columns_at_end(gsm, tab, header)
            out_start, out_end = _output_col_block(header)
            start_letter = col_index_to_a1(out_start)
            end_letter = col_index_to_a1(out_end)

            # 1) Watermark-based bounded read from column B (Domain)
            last_done = _load_watermark(sheets, sheet_title=tab)
            start_row = last_done + 1
            end_row = start_row + win - 1

            block = sheets.read_range(tab_name=tab, a1_range=f"B{start_row}:B{end_row}")
            eff_n = _block_effective_rows(block)

            if eff_n <= 0:
                empty_passes += 1
                last_msg = (
                    f"Waiting for new Master data. pass={passes} "
                    f"empty_passes={empty_passes}/{empty_cap} (Column B has no new rows)"
                )

                update_progress(
                    job_id,
                    phase="extract",
                    current=processed,
                    total=max_updates,
                    message=last_msg,
                    metrics={"passes": passes, "empty_passes": empty_passes, "last_done": last_done},
                )
                status.write(
                    job_id=job_id,
                    tool="email_extractor",
                    request_id=rid,
                    state="RUNNING",
                    phase="extract",
                    current=processed,
                    total=max_updates,
                    message=last_msg,
                    meta={"passes": passes, "empty_passes": empty_passes, "last_done": last_done},
                )

                if empty_cap == 0 or empty_passes >= empty_cap:
                    msg = (
                        f"Email extractor stopped: no new Master rows after {empty_passes} passes. "
                        f"processed={processed}"
                    )
                    mark_succeeded(job_id, message=msg, metrics={"processed": processed, "ok": ok, "invalid": invalid, "not_found": not_found})
                    log.info("JOB_SUCCEEDED | %s", msg)

                    status.write(
                        job_id=job_id,
                        tool="email_extractor",
                        request_id=rid,
                        state="SUCCEEDED",
                        phase="extract",
                        current=processed,
                        total=max_updates,
                        message=msg,
                        meta={"passes": passes, "empty_passes": empty_passes, "last_done": last_done},
                        force=True,
                    )
                    if gsm:
                        gsm.flush()
                    return

                if linger_s > 0:
                    time.sleep(linger_s)
                continue

            # We have effective rows. Reset idle counter and process the window.
            empty_passes = 0

            row_num = start_row
            for r in block[:eff_n]:
                if processed >= max_updates:
                    break

                raw_domain = r[0] if r else ""
                domain = normalize_domain(raw_domain)

                if not domain:
                    best = ""
                    all_emails = ""
                    src = ""
                    st = "invalid_domain"
                    invalid += 1
                else:
                    emails, src_url = extract_emails_for_domain(domain)
                    best_email = choose_best_email(domain, emails)

                    best = best_email or ""
                    all_emails = ", ".join(emails) if emails else ""
                    src = src_url or ""

                    if best_email:
                        st = "ok"
                        ok += 1
                    else:
                        st = "not_found"
                        not_found += 1

                # Write extracted cols for this exact Master row
                gsm.queue_values_update(
                    tab_name=tab,
                    a1_range=f"{start_letter}{row_num}:{end_letter}{row_num}",
                    values=[[best, all_emails, src, st]],
                )

                processed += 1
                row_num += 1

                if processed % 25 == 0:
                    last_msg = f"Processed={processed} | ok={ok} | invalid={invalid} | not_found={not_found}"
                    update_progress(
                        job_id,
                        phase="extract",
                        current=processed,
                        total=max_updates,
                        message=last_msg,
                        metrics={"ok": ok, "invalid": invalid, "not_found": not_found, "passes": passes},
                    )
                    status.write(
                        job_id=job_id,
                        tool="email_extractor",
                        request_id=rid,
                        state="RUNNING",
                        phase="extract",
                        current=processed,
                        total=max_updates,
                        message=last_msg,
                        meta={"ok": ok, "invalid": invalid, "not_found": not_found, "passes": passes},
                    )

            # Flush buffered writes for this batch
            if gsm:
                gsm.flush()

            # Advance watermark precisely by the number of effective rows consumed
            new_last = last_done + eff_n
            _write_watermark(sheets, sheet_title=tab, last_row=new_last)

            # Continue loop to process more windows until idle or max_updates

        msg = f"Email extraction completed. processed={processed} ok={ok} invalid={invalid} not_found={not_found}"
        mark_succeeded(
            job_id,
            message=msg,
            metrics={"processed": processed, "ok": ok, "invalid": invalid, "not_found": not_found},
        )
        log.info("JOB_SUCCEEDED | %s", msg)

        if status:
            status.write(
                job_id=job_id,
                tool="email_extractor",
                request_id=rid,
                state="SUCCEEDED",
                phase="extract",
                current=processed,
                total=max_updates,
                message=msg,
                meta={"processed": processed, "ok": ok, "invalid": invalid, "not_found": not_found},
                force=True,
            )
        if gsm:
            gsm.flush()

    except Exception as exc:
        try:
            if gsm:
                gsm.flush()
        except Exception:
            logger.exception("SHEETS_MANAGER_FINAL_FLUSH_FAILED")

        mark_failed(job_id, error=str(exc), message="Email extraction failed.")
        log.exception("JOB_FAILED | error=%s", exc)

        try:
            if status:
                status.write(
                    job_id=job_id,
                    tool="email_extractor",
                    request_id=rid or "",
                    state="FAILED",
                    phase="extract",
                    current=processed,
                    total=max_total_updates,
                    message=str(exc),
                    meta={"processed": processed, "ok": ok, "invalid": invalid, "not_found": not_found},
                    force=True,
                )
            if gsm:
                gsm.flush()
        except Exception:
            logger.exception("TOOL_STATUS_WRITE_FAILED")
