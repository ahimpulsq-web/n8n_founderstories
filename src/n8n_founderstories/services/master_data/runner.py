"""
DB-first Master runner for tool-agnostic result aggregation.

This module replaces the Sheets-driven Master logic with a DB-first approach:
- Reads from tool DB tables (Hunter, Google Maps, etc.) using adapters
- Aggregates results into master_results table with idempotent upserts
- Tracks watermarks per tool for incremental ingestion
- Exports to Sheets only at job end (optional)
- Safe under parallel tool execution and rerunnable
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import List, Optional, Set

from ...core.config import settings
from ...core.utils.text import norm
from ..jobs import JobsSheetWriter
from ..jobs.store import (
    mark_failed,
    mark_running,
    mark_succeeded,
    update_progress,
)
from ..exports.sheets import SheetsClient, default_sheets_config
from ..exports.sheets_exporter import export_master_results

from .adapters import BaseSourceAdapter, get_available_adapters, get_adapter_by_name
from .models import MasterRow
from .repos import MasterResultsRepository, MasterWatermarkRepository, PermanentError

# ONLY enrichment dispatch: web scraper enrichment
from ..dispatcher.web_scraper_enrichment import dispatch_web_scraper_enrichment_job
from ..exports.email_sheets_sync import sync_combined_to_mail_content_sheet


logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    """Return current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()


def _env_bool(name: str, default: bool) -> bool:
    """
    Parse boolean-ish environment variables.
    Accepts: 1/0, true/false, yes/no, on/off.
    """
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    v = str(raw).strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def _create_audit_rows(*, request_id: str, results: List[dict]) -> List[List[str]]:
    """
    Create audit summary rows from results.
    """
    stats_by_tool: dict[str, dict] = {}

    for result in results:
        tool = result.get("source_tool", "Unknown")
        domain = result.get("domain", "").lower().strip()
        is_dup = result.get("dup_in_run", "NO") == "YES"

        if tool not in stats_by_tool:
            stats_by_tool[tool] = {
                "total": 0,
                "unique_domains": set(),
                "duplicates": 0,
                "last_updated": result.get("updated_at", ""),
            }

        stats_by_tool[tool]["total"] += 1
        if domain:
            stats_by_tool[tool]["unique_domains"].add(domain)
        if is_dup:
            stats_by_tool[tool]["duplicates"] += 1

        updated_at = result.get("updated_at", "")
        if updated_at > stats_by_tool[tool]["last_updated"]:
            stats_by_tool[tool]["last_updated"] = updated_at

    audit_rows = []
    for tool, stats in sorted(stats_by_tool.items()):
        row = [
            request_id,
            tool,
            str(stats["total"]),
            str(len(stats["unique_domains"])),
            str(stats["duplicates"]),
            stats["last_updated"],
        ]
        audit_rows.append(row)

    return audit_rows


def _has_data_for_request(request_id: str, adapters: List[BaseSourceAdapter]) -> bool:
    """
    Peek check: Determine if any adapter has data for this request.
    """
    for adapter in adapters:
        if not adapter.table_exists():
            continue

        try:
            success, _error, rows, _ = adapter.fetch_rows_after_watermark(
                request_id=request_id, watermark=None, limit=1
            )
            if success and rows:
                return True
        except Exception:
            continue

    return False


def _has_new_data_after_watermarks(
    request_id: str,
    adapters: List[BaseSourceAdapter],
    watermarks: dict[str, Optional[datetime]],
    dsn: Optional[str] = None,
) -> tuple[bool, Optional[str]]:
    """
    Delta check: Determine if any adapter has new data beyond stored watermarks.
    """
    # Services own their logging - use standard logger
    log = logger

    for adapter in adapters:
        tool_name = adapter.source_tool_name
        watermark = watermarks.get(tool_name)

        try:
            success, error, rows, _ = adapter.fetch_rows_after_watermark(
                request_id=request_id, watermark=watermark, limit=1
            )

            if not success:
                log.warning(
                    "MASTER_DELTA_CHECK_FETCH_FAILED | tool=%s | error=%s",
                    tool_name,
                    error,
                )
                continue

            if rows and len(rows) > 0:
                log.info(
                    "MASTER_DELTA_CHECK_HAS_DATA | tool=%s | watermark=%s | new_rows=1",
                    tool_name,
                    watermark.isoformat() if watermark else "None",
                )
                return True, tool_name

        except Exception as e:
            log.warning("MASTER_DELTA_CHECK_ERROR | tool=%s | error=%s", tool_name, e)
            continue

    log.info(
        "MASTER | DELTA_CHECK_COMPLETE | has_new_data=false | checked_tools=%d",
        len(adapters),
    )
    return False, None


def run_master_job_db_first_with_rerun(
    *,
    job_id: str,
    request_id: str,
    spreadsheet_id: str,
    lock_conn: any,
    source_tools: Optional[List[str]] = None,
    window_size: int = 500,
    max_empty_passes: int = 10,
    linger_seconds: float = 3.0,
    export_to_sheets: bool = True,
) -> None:
    """
    Wrapper for run_master_job_db_first with level-trigger rerun logic.
    """
    from .orchestration import pop_pending, release_master_lock

    rid = norm(request_id)
    # Services own their logging - use standard logger
    log = logger

    max_reruns = 5
    rerun_count = 0

    try:
        while rerun_count <= max_reruns:
            if rerun_count > 0:
                log.info(
                    "MASTER_RERUN | request_id=%s | rerun=%d/%d",
                    rid,
                    rerun_count,
                    max_reruns,
                )

            run_master_job_db_first(
                job_id=job_id,
                request_id=rid,
                spreadsheet_id=spreadsheet_id,
                lock_conn=lock_conn,
                source_tools=source_tools,
                window_size=window_size,
                max_empty_passes=max_empty_passes,
                linger_seconds=linger_seconds,
                export_to_sheets=export_to_sheets,
                _skip_lock_acquire=True,
            )

            was_pending = pop_pending(rid)

            if not was_pending:
                log.info("MASTER | COMPLETE | request_id=%s | reruns=%d", rid, rerun_count)
                break

            log.info("MASTER_POP_PENDING | was_pending=true | checking_delta=true | request_id=%s", rid)

            if source_tools:
                adapters = []
                for tool_name in source_tools:
                    adapter = get_adapter_by_name(tool_name)
                    if adapter and adapter.table_exists():
                        adapters.append(adapter)
            else:
                adapters = [a for a in get_available_adapters() if a.table_exists()]

            if not adapters:
                log.info("MASTER_RERUN_SKIPPED | reason=no_adapters | request_id=%s", rid)
                break

            watermark_repo = MasterWatermarkRepository()
            success, error, watermarks = watermark_repo.get_watermarks_for_request(rid)
            if not success:
                log.warning("MASTER_WATERMARK_LOAD_FAILED | error=%s | proceeding_with_rerun", error)
                watermarks = {}

            has_new_data, _tool_with_data = _has_new_data_after_watermarks(
                request_id=rid, adapters=adapters, watermarks=watermarks
            )
            if not has_new_data:
                log.info("MASTER_RERUN_SKIPPED | reason=no_new_rows_after_watermarks | request_id=%s", rid)
                break

            rerun_count += 1
            if rerun_count > max_reruns:
                log.warning("MASTER_MAX_RERUNS_REACHED | request_id=%s | max=%d", rid, max_reruns)
                break

    finally:
        release_master_lock(lock_conn, rid)


def run_master_job_db_first(
    *,
    job_id: str,
    request_id: str,
    spreadsheet_id: str,
    lock_conn: Optional[any] = None,
    source_tools: Optional[List[str]] = None,
    window_size: int = 500,
    max_empty_passes: int = 10,
    linger_seconds: float = 3.0,
    export_to_sheets: bool = True,
    _skip_lock_acquire: bool = False,
) -> None:
    """
    Run DB-first Master ingestion job with advisory lock protection.
    """
    rid = norm(request_id)
    if not rid:
        raise ValueError("request_id is required")

    sid = norm(spreadsheet_id)
    if not sid:
        raise ValueError("spreadsheet_id is required")

    # Services own their logging - use standard logger
    log = logger

    from .orchestration import acquire_master_lock, release_master_lock

    if _skip_lock_acquire:
        if not lock_conn:
            raise ValueError("lock_conn required when _skip_lock_acquire=True")
    else:
        lock_acquired, lock_conn_or_error = acquire_master_lock(rid)

        if not lock_acquired:
            if lock_conn_or_error is None:
                msg = "Master lock busy for this request_id - another Master is running"
                log.info("MASTER | LOCK_BUSY | request_id=%s | skipping", rid)
                mark_succeeded(job_id, message=msg, metrics={"skipped": True, "reason": "lock_busy"})
            else:
                msg = f"Failed to acquire Master lock: {lock_conn_or_error}"
                log.error("MASTER_LOCK_ERROR | request_id=%s | error=%s", rid, lock_conn_or_error)
                mark_failed(job_id, error=str(lock_conn_or_error), message=msg)
            return

        lock_conn = lock_conn_or_error

    sheets: Optional[SheetsClient] = None
    status: Optional[JobsSheetWriter] = None

    try:
        mark_running(job_id)

        status = JobsSheetWriter(sheet_id=sid)

        results_repo = MasterResultsRepository()
        watermark_repo = MasterWatermarkRepository()

        if source_tools:
            adapters = []
            for tool_name in source_tools:
                adapter = get_adapter_by_name(tool_name)
                if adapter and adapter.table_exists():
                    adapters.append(adapter)
                    log.info("MASTER | ADAPTER_ENABLED | tool=%s | table=%s", tool_name, adapter.source_table_name)
                else:
                    log.warning("MASTER_ADAPTER_SKIPPED | tool=%s | reason=table_not_found", tool_name)
        else:
            adapters = [a for a in get_available_adapters() if a.table_exists()]

        if not adapters:
            msg = "No source tools available"
            log.info("MASTER | NO_ADAPTERS | request_id=%s | reason=no_tables", rid)
            mark_succeeded(job_id, message=msg, metrics={"adapters": 0, "total_ingested": 0, "reason": "no_tables"})
            return

        has_data = _has_data_for_request(rid, adapters)
        if not has_data:
            msg = "No data found yet - tools haven't persisted data"
            log.info("MASTER | NO_DATA_YET | request_id=%s | adapters=%d", rid, len(adapters))
            mark_succeeded(
                job_id,
                message=msg,
                metrics={"adapters": len(adapters), "total_ingested": 0, "reason": "no_data_yet"},
            )
            return

        if not source_tools:
            adapters_with_data = []
            for adapter in adapters:
                success, _error, rows, _ = adapter.fetch_rows_after_watermark(request_id=rid, watermark=None, limit=1)
                if success and rows:
                    adapters_with_data.append(adapter)
                    log.info(
                        "MASTER_ADAPTER_AUTO_DETECTED | tool=%s | table=%s",
                        adapter.source_tool_name,
                        adapter.source_table_name,
                    )
            adapters = adapters_with_data

        if not adapters:
            msg = "No adapters with data found"
            log.info("MASTER | NO_ADAPTERS | request_id=%s | reason=no_data", rid)
            mark_succeeded(job_id, message=msg, metrics={"adapters": 0, "total_ingested": 0, "reason": "no_data"})
            return

        seen_domains_run: Set[str] = set()
        ingested_by_tool: dict[str, int] = {a.source_tool_name: 0 for a in adapters}
        total_ingested = 0

        # Initial Tool_Status update
        status.write(
            job_id=job_id,
            tool="master",
            request_id=rid,
            state="RUNNING",
            phase="ingestion",
            current=0,
            total=len(adapters),
            message=f"Processing {len(adapters)} source tools",
            meta={
                "adapters": len(adapters),
                "source_tools": [a.source_tool_name for a in adapters],
            },
        )

        for adapter_idx, adapter in enumerate(adapters, start=1):
            tool_name = adapter.source_tool_name

            success, error, watermark_obj = watermark_repo.get_watermark(rid, tool_name)
            if not success:
                log.warning("MASTER_WATERMARK_GET_FAILED | tool=%s | error=%s", tool_name, error)
                continue

            watermark = watermark_obj.last_seen_created_at if watermark_obj else None

            success, error, source_rows, new_watermark = adapter.fetch_rows_after_watermark(
                request_id=rid,
                watermark=watermark,
                limit=10000,
            )
            if not success:
                log.warning("MASTER_FETCH_FAILED | tool=%s | error=%s", tool_name, error)
                continue

            if not source_rows:
                log.info("MASTER | NO_NEW_ROWS | tool=%s | watermark=%s", tool_name, watermark)
                continue

            master_rows: List[MasterRow] = []
            for source_row in source_rows:
                master_row = adapter.normalize_to_master(source_row)
                if not master_row:
                    continue

                if master_row.domain:
                    d = master_row.domain.lower().strip()
                    master_row.dup_in_run = "YES" if d in seen_domains_run else "NO"
                    seen_domains_run.add(d)
                else:
                    master_row.dup_in_run = "NO"

                master_rows.append(master_row)

            if not master_rows:
                continue

            try:
                success, error, _affected = results_repo.upsert_many(master_rows)
                if not success:
                    log.error("MASTER_UPSERT_FAILED | tool=%s | error=%s", tool_name, error)
                    continue
            except PermanentError as e:
                log.error("MASTER_PERMANENT_ERROR | tool=%s | error=%s | failing_fast", tool_name, e)
                raise

            if new_watermark:
                success, error = watermark_repo.set_watermark(
                    request_id=rid,
                    source_tool=tool_name,
                    last_seen_created_at=new_watermark,
                    last_processed_count=len(master_rows),
                )
                if not success:
                    log.warning("MASTER_WATERMARK_SET_FAILED | tool=%s | error=%s", tool_name, error)

            ingested_by_tool[tool_name] += len(master_rows)
            total_ingested += len(master_rows)

            # Update Tool_Status after each adapter
            status.write(
                job_id=job_id,
                tool="master",
                request_id=rid,
                state="RUNNING",
                phase="ingestion",
                current=adapter_idx,
                total=len(adapters),
                message=f"Processed {adapter_idx}/{len(adapters)} tools, ingested {total_ingested} rows",
                meta={
                    "adapters_processed": adapter_idx,
                    "total_adapters": len(adapters),
                    "total_ingested": total_ingested,
                    "ingested_by_tool": ingested_by_tool,
                },
            )

        # Update status before export
        if export_to_sheets and settings.master_sheets_export_enabled:
            status.write(
                job_id=job_id,
                tool="master",
                request_id=rid,
                state="RUNNING",
                phase="export",
                current=total_ingested,
                total=total_ingested,
                message=f"Exporting {total_ingested} rows to Google Sheets",
                meta={
                    "total_ingested": total_ingested,
                    "ingested_by_tool": ingested_by_tool,
                },
            )
            try:
                success, error, results = results_repo.get_results_by_request(request_id=rid)
                if success:
                    sheets_rows = []
                    for result in results:
                        sheets_rows.append(
                            [
                                str(result.get("id", "")),                 # master_result_id
                                norm(result.get("company", "")),           # Organisation
                                norm(result.get("domain", "")),            # Domain
                                norm(result.get("source_tool", "")),       # Source
                                "",  # Company Name (web enrichment)
                                "",  # E-mail ID (web enrichment)
                                "",  # Contact Names (web enrichment)
                                "",  # Short Company Description (web enrichment)
                                "",  # Long Company Description (web enrichment)
                            ]
                        )

                    audit_rows = _create_audit_rows(request_id=rid, results=results)

                    export_master_results(
                        client=sheets,
                        job_id=job_id,
                        request_id=rid,
                        results_rows=sheets_rows,
                        audit_rows=audit_rows,
                    )
            except Exception as e:
                log.error("MASTER_EXPORT_FAILED | error=%s", e, exc_info=True)

        msg = f"Master ingestion complete. Ingested {total_ingested} rows from {len(ingested_by_tool)} tools."
        
        # Final Tool_Status update
        status.write(
            job_id=job_id,
            tool="master",
            request_id=rid,
            state="SUCCEEDED",
            phase="complete",
            current=total_ingested,
            total=total_ingested,
            message=msg,
            meta={
                "total_ingested": total_ingested,
                "ingested_by_tool": ingested_by_tool,
                "adapters": len(adapters),
            },
        )
        
        mark_succeeded(
            job_id,
            message=msg,
            metrics={
                "total_ingested": total_ingested,
                "by_tool": ingested_by_tool,
                "adapters": len(adapters),
                "single_pass": True,
            },
        )

        # ONLY dispatch: web scraper enrichment
        try:
            dispatch_web_scraper_enrichment_job(request_id=rid, spreadsheet_id=sid)
            log.info("MASTER | WEBSCRAPER_ENRICHMENT_DISPATCHED | request_id=%s", rid)
        except Exception as e:
            log.error(
                "MASTER | WEBSCRAPER_DISPATCH_FAILED | request_id=%s | error=%s",
                rid,
                e,
                exc_info=True,
            )

    except Exception as exc:
        # Update Tool_Status on failure
        if status:
            try:
                status.write(
                    job_id=job_id,
                    tool="master",
                    request_id=rid,
                    state="FAILED",
                    phase="error",
                    current=0,
                    total=0,
                    message=f"Master job failed: {str(exc)}",
                    meta={"error": str(exc)},
                )
            except Exception as status_err:
                log.warning("Failed to update Tool_Status on error: %s", status_err)
        
        mark_failed(job_id, error=str(exc), message="Master job failed")
        log.exception("JOB_FAILED | error=%s", exc)
    finally:
        if not _skip_lock_acquire and lock_conn:
            release_master_lock(lock_conn, rid)
