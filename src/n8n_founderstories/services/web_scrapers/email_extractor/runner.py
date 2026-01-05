from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from ....core.utils.net import normalize_domain
from ....core.utils.async_net import AsyncFetchConfig, AsyncFetcher
from ....core.utils.sheets_a1 import col_index_to_a1
from ....core.utils.text import norm

from ....services.exports.sheets import SheetsClient, default_sheets_config
from ....services.exports.sheets_manager import GoogleSheetsManager
from ....services.jobs.logging import job_logger
from ....services.jobs.sheets_status import ToolStatusWriter
from ....services.jobs.store import mark_failed, mark_running, mark_succeeded, update_progress

from .async_extractor import AsyncExtractionConfig, extract_emails_for_domain_async

logger = logging.getLogger(__name__)

TAB_TOOL_STATUS = "Tool_Status"

TAB_EMAIL_STATE = "EmailExtractor_State"
EMAIL_STATE_HEADERS = ["Sheet Title", "Last Processed Row", "Updated At (UTC)"]

# Dummy test value column (change/delete later)
TEST_MAIL_ID_VALUE = "ahimpulsq@gmail.com"

# ---------------------------------------------------------------------------
# MASTER INPUT HEADERS (schema-driven lookup)
# These MUST match your Master runner's header labels exactly.
# ---------------------------------------------------------------------------
MASTER_COMPANY_HEADER = "Company Name"
MASTER_DOMAIN_HEADER = "Primary Domain"

# ---------------------------------------------------------------------------
# SINGLE SOURCE OF TRUTH: Output schema (keys + labels)
# - Keys: stable in code
# - Labels: written into Google Sheet header row
# - Order here defines column order and write ordering
# ---------------------------------------------------------------------------
OUT_SCHEMA: list[tuple[str, str]] = [
    ("email_primary", "Primary Email"),
    ("email_candidates", "Email Candidates (email|source)"),
    ("email_source_url", "Best Source URL"),
    ("email_status", "Email Status"),
    ("email_reason", "Email Reason"),
    ("email_debug", "Email Debug"),
    ("test_recipient", "Test Recipient"),
    ("email_subject", "Email Subject"),
    ("email_body", "Email Body"),
]

OUT_KEYS = [k for (k, _label) in OUT_SCHEMA]
OUT_HEADERS = [label for (_k, label) in OUT_SCHEMA]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_is_empty(row: list[str]) -> bool:
    if not row:
        return True
    return all(not (c or "").strip() for c in row)


def _block_effective_rows(block: list[list[str]]) -> int:
    """
    Gap-safe: process up to the LAST non-empty row in this window.
    This avoids stopping early if there are blank domain cells but later rows have data.
    """
    last_non_empty = -1
    for i, r in enumerate(block):
        if not _row_is_empty(r):
            last_non_empty = i
    return last_non_empty + 1


def _read_header(sheets: SheetsClient, tab: str) -> list[str]:
    rows = sheets.read_range(tab_name=tab, a1_range="1:1")
    hdr = rows[0] if rows else []
    return [norm(h) for h in hdr]


def _find_col_idx(header: list[str], label: str) -> int:
    """
    Returns the 0-based column index for a given header label.
    Raises if missing (root-cause fail-fast).
    """
    target = norm(label)
    for i, h in enumerate(header):
        if norm(h) == target:
            return i
    raise RuntimeError(f"Required column header not found: {label!r}")


def _ensure_output_columns_at_end(gsm: GoogleSheetsManager, tab: str, header: list[str]) -> list[str]:
    """
    Ensures OUT_HEADERS exist, appended at end in schema order.
    Returns updated header list (labels).
    """
    existing = {h: i for i, h in enumerate(header) if h}
    missing_labels = [label for label in OUT_HEADERS if label not in existing]
    if not missing_labels:
        return header

    new_header = list(header) + missing_labels
    gsm.queue_values_update(tab_name=tab, a1_range="A1", values=[new_header])
    gsm.flush()
    return new_header


def _output_col_block(header: list[str]) -> tuple[int, int]:
    """
    Returns (start_col_idx, end_col_idx) inclusive for OUT_HEADERS block.
    Valid only when output labels are contiguous and ordered.
    """
    pos = {h: i for i, h in enumerate(header) if h}

    start = pos[OUT_HEADERS[0]]
    end = pos[OUT_HEADERS[-1]]
    if end < start:
        raise RuntimeError("Output columns are not contiguous / incorrect order.")
    return start, end


def _load_watermark(sheets: SheetsClient, *, sheet_title: str) -> int:
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


def _format_email_pairs(pairs: list[tuple[str, str]]) -> str:
    """
    Option A: one pair per line.
    """
    return "\n".join([f"{e}|{src}" for e, src in pairs])


async def _extract_batch_with_fetcher(
    *,
    fetcher: AsyncFetcher,
    extractor_cfg: AsyncExtractionConfig,
    domains: list[str],
) -> list[tuple[list[str], Optional[str], Optional[str], list[tuple[str, str]], str, str]]:
    async def one(d: str) -> tuple[list[str], Optional[str], Optional[str], list[tuple[str, str]], str, str]:
        return await extract_emails_for_domain_async(d, fetcher=fetcher, cfg=extractor_cfg)

    tasks = [asyncio.create_task(one(d)) for d in domains]
    return await asyncio.gather(*tasks)


def run_email_extractor_job(
    *,
    job_id: str,
    request_id: str,
    spreadsheet_id: str,
    sheet_title: str = "Master",
    apply_formatting: bool = False,  # optional; matches your ask
    linger_seconds: float = 3.0,
    max_empty_passes: int = 10,
    window_rows: int = 500,
    max_total_updates: int = 5000,
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

    loop: asyncio.AbstractEventLoop | None = None
    fetcher: AsyncFetcher | None = None
    extractor_cfg: AsyncExtractionConfig | None = None

    try:
        mark_running(job_id)

        if not rid:
            raise ValueError("request_id must not be empty.")
        if not sid:
            raise ValueError("spreadsheet_id must not be empty.")

        sheets = SheetsClient(config=default_sheets_config(spreadsheet_id=sid))
        gsm = GoogleSheetsManager(client=sheets)
        status = ToolStatusWriter(sheets=sheets, spreadsheet_id=sid, manager=gsm)

        sheets.ensure_tab(TAB_TOOL_STATUS)
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

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        fetch_cfg = AsyncFetchConfig(
            timeout_sec=12.0,
            user_agent="n8n_founderstories/1.0 (email-extractor)",
            max_bytes=800_000,
            max_global_concurrency=60,
            per_host_concurrency=1,
            per_host_min_delay_sec=0.3,
            max_retries=1,
            retry_backoff_sec=1.2,
        )
        fetcher = AsyncFetcher(fetch_cfg)

        extractor_cfg = AsyncExtractionConfig(
            fetch=fetch_cfg,
            max_pages_per_domain=14,
            stop_on_best_match=False,
            discover_links=True,
            max_discovered_links=8,
            discover_from_groups=3,
        )

        start_letter = ""
        end_letter = ""
        company_col_letter = ""
        domain_col_letter = ""

        while processed < max_updates:
            passes += 1

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

            # Find company/domain columns dynamically (header-driven)
            try:
                company_col_idx = _find_col_idx(header, MASTER_COMPANY_HEADER)
                domain_col_idx = _find_col_idx(header, MASTER_DOMAIN_HEADER)
            except Exception as exc:
                raise RuntimeError(
                    f"Master sheet missing required columns. "
                    f"Expected: {MASTER_COMPANY_HEADER!r}, {MASTER_DOMAIN_HEADER!r}. "
                    f"Actual header row: {header}"
                ) from exc

            company_col_letter = col_index_to_a1(company_col_idx)
            domain_col_letter = col_index_to_a1(domain_col_idx)

            # Ensure output columns exist in schema order
            header = _ensure_output_columns_at_end(gsm, tab, header)

            out_start, out_end = _output_col_block(header)
            start_letter = col_index_to_a1(out_start)
            end_letter = col_index_to_a1(out_end)

            last_done = _load_watermark(sheets, sheet_title=tab)
            start_row = last_done + 1
            end_row = start_row + win - 1

            # Read only the needed columns (company + domain) using dynamic A1 letters.
            # Read them separately to avoid building a huge contiguous range if columns move far apart.
            companies_col = sheets.read_range(
                tab_name=tab, a1_range=f"{company_col_letter}{start_row}:{company_col_letter}{end_row}"
            )
            domains_col = sheets.read_range(
                tab_name=tab, a1_range=f"{domain_col_letter}{start_row}:{domain_col_letter}{end_row}"
            )

            # Merge into row pairs by index
            max_len = max(len(companies_col), len(domains_col))
            block: list[list[str]] = []
            for i in range(max_len):
                c = companies_col[i][0] if i < len(companies_col) and companies_col[i] else ""
                d = domains_col[i][0] if i < len(domains_col) and domains_col[i] else ""
                block.append([c, d])

            eff_n = _block_effective_rows(block)

            if eff_n <= 0:
                empty_passes += 1
                last_msg = (
                    f"Waiting for new Master data. pass={passes} "
                    f"empty_passes={empty_passes}/{empty_cap} (No new rows)"
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
                    mark_succeeded(
                        job_id,
                        message=msg,
                        metrics={"processed": processed, "ok": ok, "invalid": invalid, "not_found": not_found},
                    )
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

            empty_passes = 0

            company_names: list[str] = []
            norm_domains: list[str] = []

            for r in block[:eff_n]:
                name = (r[0] if len(r) > 0 else "") if r else ""
                dom_raw = (r[1] if len(r) > 1 else "") if r else ""

                company_names.append(norm(name) or "")
                norm_domains.append(normalize_domain(dom_raw) or "")

            valid_idx: list[int] = []
            valid_domains: list[str] = []
            for i, d in enumerate(norm_domains):
                if d:
                    valid_idx.append(i)
                    valid_domains.append(d)

            results: list[tuple[list[str], Optional[str], Optional[str], list[tuple[str, str]], str, str]] = []
            if valid_domains:
                assert loop and fetcher and extractor_cfg
                results = loop.run_until_complete(
                    _extract_batch_with_fetcher(fetcher=fetcher, extractor_cfg=extractor_cfg, domains=valid_domains)
                )

            mapped: dict[int, tuple[list[str], Optional[str], Optional[str], list[tuple[str, str]], str, str]] = {}
            for pos, idx in enumerate(valid_idx):
                mapped[idx] = results[pos]

            row_num = start_row
            for i in range(eff_n):
                if processed >= max_updates:
                    break

                domain = norm_domains[i]

                if not domain:
                    best = ""
                    all_emails = ""
                    src = ""
                    st = "skipped"
                    reason = "missing_domain"
                    debug = "domain_cell_empty"
                    invalid += 1
                else:
                    emails_unique, best_email, best_src, pairs, reason0, debug0 = mapped.get(
                        i, ([], None, None, [], "fetch_failed", "no_result")
                    )

                    best = best_email or ""
                    all_emails = _format_email_pairs(pairs) if pairs else ""
                    src = best_src or ""

                    if best_email:
                        st = "ok"
                        ok += 1
                        reason = "ok"
                        debug = debug0 or ""
                    else:
                        st = "not_found"
                        not_found += 1
                        reason = reason0 or "not_found"
                        debug = debug0 or ""

                cname = company_names[i] or "Company"
                subject = f"Founders story on {cname}"
                body = f"Hello from N8N Founders story node on {cname}"

                out_obj = {
                    "email_primary": best,
                    "email_candidates": all_emails,
                    "email_source_url": src,
                    "email_status": st,
                    "email_reason": reason,
                    "email_debug": debug,
                    "test_recipient": TEST_MAIL_ID_VALUE,
                    "email_subject": subject,
                    "email_body": body,
                }

                gsm.queue_values_update(
                    tab_name=tab,
                    a1_range=f"{start_letter}{row_num}:{end_letter}{row_num}",
                    values=[[out_obj.get(k, "") for k in OUT_KEYS]],
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

            if gsm:
                gsm.flush()

            new_last = last_done + eff_n
            _write_watermark(sheets, sheet_title=tab, last_row=new_last)

        if gsm:
            gsm.flush()
            
        # ---------------------------------------------------------------------
        # Formatting (apply once after extraction is finished)
        # ---------------------------------------------------------------------
        if apply_formatting and sheets:
            try:
                # final last row (based on column A)
                last_row_fmt = sheets.get_last_row(tab_name=tab, signal_col=0)
                if last_row_fmt < 2:
                    last_row_fmt = 2

                # final header width (including appended OUT columns)
                header_final = _read_header(sheets, tab)
                n_cols = len(header_final)

                sheets.format_table_layout(
                    tab_name=tab,
                    n_cols=n_cols,
                    last_row=last_row_fmt,
                    header_row=True,
                    auto_resize=True,
                    # critical: makes '\n' show as multiple lines
                    wrap_strategy_body="WRAP",
                )
            except Exception:
                logger.exception("EMAIL_EXTRACTOR_FINAL_FORMATTING_FAILED")

        msg = f"Email extraction completed. processed={processed} ok={ok} invalid={invalid} not_found={not_found}"
        mark_succeeded(job_id, message=msg, metrics={"processed": processed, "ok": ok, "invalid": invalid, "not_found": not_found})
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

    finally:
        try:
            if loop and fetcher:
                loop.run_until_complete(fetcher.aclose())
        except Exception:
            logger.exception("FETCHER_CLOSE_FAILED")
        finally:
            try:
                if loop:
                    loop.close()
            except Exception:
                logger.exception("EVENT_LOOP_CLOSE_FAILED")
