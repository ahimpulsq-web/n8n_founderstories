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
import time
from datetime import datetime, timezone
from typing import List, Optional, Set

from ...core.config import settings
from ...core.utils.text import norm
from ..jobs.logging import job_logger
from ..jobs.models import JobState
from ..jobs.sheets_status import ToolStatusWriter
from ..jobs.store import find_job_by_request_and_tool, mark_failed, mark_running, mark_succeeded, update_progress
from ..exports.sheets import SheetsClient, default_sheets_config
from ..exports.sheets_exporter import export_master_results

from .adapters import BaseSourceAdapter, get_available_adapters, get_adapter_by_name
from .models import MasterRow
from .repos import MasterResultsRepository, MasterWatermarkRepository, PermanentError
from ..company_enrichment.runner import run_company_enrichment_for_request



logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    """Return current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()


def _create_audit_rows(*, request_id: str, results: List[dict]) -> List[List[str]]:
    """
    Create audit summary rows from results.
    
    Args:
        request_id: Request identifier
        results: List of master_results dictionaries from DB
        
    Returns:
        List of audit rows in Sheets format
    """
    # Compute statistics by source tool
    stats_by_tool: dict[str, dict] = {}
    
    for result in results:
        tool = result.get('source_tool', 'Unknown')
        domain = result.get('domain', '').lower().strip()
        is_dup = result.get('dup_in_run', 'NO') == 'YES'
        
        if tool not in stats_by_tool:
            stats_by_tool[tool] = {
                'total': 0,
                'unique_domains': set(),
                'duplicates': 0,
                'last_updated': result.get('updated_at', ''),
            }
        
        stats_by_tool[tool]['total'] += 1
        if domain:
            stats_by_tool[tool]['unique_domains'].add(domain)
        if is_dup:
            stats_by_tool[tool]['duplicates'] += 1
        
        # Track latest updated_at
        updated_at = result.get('updated_at', '')
        if updated_at > stats_by_tool[tool]['last_updated']:
            stats_by_tool[tool]['last_updated'] = updated_at
    
    # Convert to rows
    audit_rows = []
    for tool, stats in sorted(stats_by_tool.items()):
        row = [
            request_id,
            tool,
            str(stats['total']),
            str(len(stats['unique_domains'])),
            str(stats['duplicates']),
            stats['last_updated'],
        ]
        audit_rows.append(row)
    
    return audit_rows




def _has_data_for_request(request_id: str, adapters: List[BaseSourceAdapter]) -> bool:
    """
    Peek check: Determine if any adapter has data for this request.
    
    This prevents Master from running before tools have persisted data,
    avoiding noisy "MASTER_NO_ADAPTERS" runs.
    
    Args:
        request_id: Request identifier
        adapters: List of adapters to check
        
    Returns:
        True if at least one adapter has data, False otherwise
    """
    for adapter in adapters:
        if not adapter.table_exists():
            continue
        
        try:
            # Cheap existence check: SELECT 1 ... LIMIT 1
            success, error, rows, _ = adapter.fetch_rows_after_watermark(
                request_id=request_id,
                watermark=None,
                limit=1
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
    dsn: Optional[str] = None
) -> tuple[bool, Optional[str]]:
    """
    Delta check: Determine if any adapter has new data beyond stored watermarks.
    
    This prevents rerun storms by checking if there's actually new source data
    before starting a rerun. Uses limit=1 for efficiency.
    
    Args:
        request_id: Request identifier
        adapters: List of adapters to check
        watermarks: Dict mapping tool_name -> watermark datetime
        dsn: Optional PostgreSQL DSN
        
    Returns:
        Tuple of (has_new_data, tool_with_data)
        - has_new_data: True if at least one adapter has new rows
        - tool_with_data: Name of first tool with new data, or None
    """
    log = job_logger(__name__, tool="master", request_id=request_id, job_id="delta_check")
    
    for adapter in adapters:
        tool_name = adapter.source_tool_name
        
        # Get watermark for this tool (None if not in dict)
        watermark = watermarks.get(tool_name)
        
        try:
            # Fetch exactly 1 row after watermark to check for new data
            success, error, rows, _ = adapter.fetch_rows_after_watermark(
                request_id=request_id,
                watermark=watermark,
                limit=1
            )
            
            if not success:
                log.warning(
                    "MASTER_DELTA_CHECK_FETCH_FAILED | tool=%s | error=%s",
                    tool_name,
                    error
                )
                continue
            
            if rows and len(rows) > 0:
                # Found new data!
                log.info(
                    "MASTER_DELTA_CHECK_HAS_DATA | tool=%s | watermark=%s | new_rows=1",
                    tool_name,
                    watermark.isoformat() if watermark else "None"
                )
                return True, tool_name
            else:
                log.debug(
                    "MASTER_DELTA_CHECK_NO_DATA | tool=%s | watermark=%s | new_rows=0",
                    tool_name,
                    watermark.isoformat() if watermark else "None"
                )
        
        except Exception as e:
            log.warning(
                "MASTER_DELTA_CHECK_ERROR | tool=%s | error=%s",
                tool_name,
                e
            )
            continue
    
    # No adapter has new data
    log.info("MASTER | \LDELTA_CHECK_COMPLETE | has_new_data=false | checked_tools=%d", len(adapters))
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
    
    This function:
    1. Runs Master once with the provided lock connection
    2. After completion, checks pending flag
    3. If pending, reruns Master (up to max_iterations for safety)
    4. Always releases lock when done
    
    Args:
        job_id: Job identifier
        request_id: Request identifier
        spreadsheet_id: Google Sheets spreadsheet ID
        lock_conn: PostgreSQL connection holding the advisory lock
        source_tools: Optional list of tool names to process
        window_size: Number of rows to fetch per adapter per pass
        max_empty_passes: Stop after this many passes with no new data
        linger_seconds: Sleep between passes
        export_to_sheets: Whether to export results to Sheets at end
    """
    from .orchestration import pop_pending, release_master_lock
    
    rid = norm(request_id)
    log = job_logger(__name__, tool="master", request_id=rid, job_id=job_id)
    
    max_reruns = 5  # Safety limit to prevent infinite loops
    rerun_count = 0
    
    try:
        while rerun_count <= max_reruns:
            if rerun_count > 0:
                log.info(
                    "MASTER_RERUN | request_id=%s | rerun=%d/%d",
                    rid,
                    rerun_count,
                    max_reruns
                )
            
            # Run Master once
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
                _skip_lock_acquire=True,  # Lock already held
            )
            
            # Check if pending flag was set during this run
            was_pending = pop_pending(rid)
            
            if not was_pending:
                # No pending triggers - we're done
                log.info("MASTER | \LCOMPLETE | request_id=%s | reruns=%d", rid, rerun_count)
                break
            
            # Pending flag was set - perform delta check before rerunning
            log.info(
                "MASTER_POP_PENDING | was_pending=true | checking_delta=true | request_id=%s",
                rid
            )
            
            # Get available adapters for delta check
            if source_tools:
                adapters = []
                for tool_name in source_tools:
                    adapter = get_adapter_by_name(tool_name)
                    if adapter and adapter.table_exists():
                        adapters.append(adapter)
            else:
                adapters = [a for a in get_available_adapters() if a.table_exists()]
            
            if not adapters:
                log.info(
                    "MASTER_RERUN_SKIPPED | reason=no_adapters | request_id=%s",
                    rid
                )
                break
            
            # Load current watermarks
            watermark_repo = MasterWatermarkRepository()
            success, error, watermarks = watermark_repo.get_watermarks_for_request(rid)
            
            if not success:
                log.warning(
                    "MASTER_WATERMARK_LOAD_FAILED | error=%s | proceeding_with_rerun",
                    error
                )
                watermarks = {}
            
            # Perform delta check
            has_new_data, tool_with_data = _has_new_data_after_watermarks(
                request_id=rid,
                adapters=adapters,
                watermarks=watermarks
            )
            
            if not has_new_data:
                # No new data - check pending one more time to catch race condition
                log.info(
                    "MASTER_RERUN_SKIPPED | reason=no_new_rows_after_watermarks | request_id=%s",
                    rid
                )
                
                # Double-check: pop_pending again to catch any new triggers during delta check
                was_pending_again = pop_pending(rid)
                if was_pending_again:
                    log.info(
                        "MASTER_RERUN_RACE_DETECTED | request_id=%s | rechecking_delta",
                        rid
                    )
                    # Re-check delta one more time
                    has_new_data_retry, tool_with_data_retry = _has_new_data_after_watermarks(
                        request_id=rid,
                        adapters=adapters,
                        watermarks=watermarks
                    )
                    if not has_new_data_retry:
                        log.info(
                            "MASTER_RERUN_SKIPPED_AFTER_DOUBLE_CHECK | reason=no_new_rows | request_id=%s",
                            rid
                        )
                        break
                    # Has new data after double-check - proceed with rerun
                    tool_with_data = tool_with_data_retry
                else:
                    # No new pending and no new data - exit
                    break
            
            # New data detected - proceed with rerun
            log.info(
                "MASTER_RERUN_CONFIRMED | reason=new_rows_detected | tool=%s | watermark=%s | request_id=%s",
                tool_with_data or "unknown",
                watermarks.get(tool_with_data).isoformat() if tool_with_data and watermarks.get(tool_with_data) else "None",
                rid
            )
            
            rerun_count += 1
            
            if rerun_count > max_reruns:
                log.warning(
                    "MASTER_MAX_RERUNS_REACHED | request_id=%s | max=%d",
                    rid,
                    max_reruns
                )
                break
    
    finally:
        # Always release lock
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
    
    This function:
    1. Acquires PostgreSQL advisory lock (single-flight per request_id) unless _skip_lock_acquire
    2. Validates inputs and marks job as running
    3. Performs peek check: exits early if no data exists yet
    4. Determines which tool adapters to run (auto-detect or explicit list)
    5. For each adapter, fetches new rows using watermarks
    6. Normalizes and upserts rows into master_results
    7. Updates watermarks and Tool_Status
    8. Stops when no new data or max empty passes reached
    9. Optionally exports to Sheets at end
    10. Releases advisory lock (unless _skip_lock_acquire)
    
    Design:
    - Incremental: Processes data as it becomes available
    - Idempotent: Safe to run multiple times
    - Tool-agnostic: Works with any tool via adapters
    - Concurrent-safe: Advisory lock prevents races
    - Peek check: Avoids noisy runs before data exists
    
    Args:
        job_id: Job identifier
        request_id: Request identifier (required)
        spreadsheet_id: Google Sheets spreadsheet ID
        lock_conn: Optional lock connection (if already acquired)
        source_tools: Optional list of tool names to process (e.g., ['HunterIO', 'GoogleMaps'])
                     If None, auto-detects by checking for rows in source tables
        window_size: Number of rows to fetch per adapter per pass
        max_empty_passes: Stop after this many passes with no new data
        linger_seconds: Sleep between passes (0 in test mode)
        export_to_sheets: Whether to export results to Sheets at end
        _skip_lock_acquire: Internal flag - skip lock acquisition (lock already held)
    """
    rid = norm(request_id)
    if not rid:
        raise ValueError("request_id is required")
    
    sid = norm(spreadsheet_id)
    if not sid:
        raise ValueError("spreadsheet_id is required")
    
    log = job_logger(__name__, tool="master", request_id=rid, job_id=job_id)
    
    # Acquire advisory lock to prevent concurrent Master runs for same request_id
    from .orchestration import acquire_master_lock, release_master_lock
    
    if _skip_lock_acquire:
        # Lock already acquired by wrapper function
        if not lock_conn:
            raise ValueError("lock_conn required when _skip_lock_acquire=True")
    else:
        # Acquire lock
        lock_acquired, lock_conn_or_error = acquire_master_lock(rid)
        
        if not lock_acquired:
            # Lock is busy or error occurred
            if lock_conn_or_error is None:
                # Lock is held by another Master process - this is normal
                msg = "Master lock busy for this request_id - another Master is running"
                log.info("MASTER | \LLOCK_BUSY | request_id=%s | skipping", rid)
                mark_succeeded(job_id, message=msg, metrics={"skipped": True, "reason": "lock_busy"})
            else:
                # Error acquiring lock
                msg = f"Failed to acquire Master lock: {lock_conn_or_error}"
                log.error("MASTER_LOCK_ERROR | request_id=%s | error=%s", rid, lock_conn_or_error)
                mark_failed(job_id, error=str(lock_conn_or_error), message=msg)
            return
        
        # Lock acquired successfully - store connection to release later
        lock_conn = lock_conn_or_error
    
    sheets: Optional[SheetsClient] = None
    status: Optional[ToolStatusWriter] = None
    
    try:
        mark_running(job_id)
        
        # Initialize Sheets client for Tool_Status updates
        sheets = SheetsClient(config=default_sheets_config(spreadsheet_id=sid))
        status = ToolStatusWriter(sheets=sheets, spreadsheet_id=sid)
        status.ensure_ready()
        
        # Initialize repositories
        results_repo = MasterResultsRepository()
        watermark_repo = MasterWatermarkRepository()
        
        # Peek check: Determine which adapters to run and check if any have data
        if source_tools:
            # Explicit list provided
            adapters = []
            for tool_name in source_tools:
                adapter = get_adapter_by_name(tool_name)
                if adapter and adapter.table_exists():
                    adapters.append(adapter)
                    log.info("MASTER | \LADAPTER_ENABLED | tool=%s | table=%s", tool_name, adapter.source_table_name)
                else:
                    log.warning("MASTER_ADAPTER_SKIPPED | tool=%s | reason=table_not_found", tool_name)
        else:
            # Auto-detect: get all available adapters
            adapters = [a for a in get_available_adapters() if a.table_exists()]
        
        # Peek check: Exit early if no data exists yet (avoids noisy runs)
        if not adapters:
            msg = "No source tools available"
            log.info("MASTER | \LNO_ADAPTERS | request_id=%s | reason=no_tables", rid)
            mark_succeeded(job_id, message=msg, metrics={"adapters": 0, "total_ingested": 0, "reason": "no_tables"})
            if status:
                status.write(
                    job_id=job_id,
                    tool="master",
                    request_id=rid,
                    state="SUCCEEDED",
                    phase="master",
                    current=0,
                    total=0,
                    message=msg,
                    meta={},
                    force=True,
                )
            return
        
        # Check if any adapter has data for this request
        has_data = _has_data_for_request(rid, adapters)
        
        if not has_data:
            msg = "No data found yet - tools haven't persisted data"
            log.info("MASTER | \LNO_DATA_YET | request_id=%s | adapters=%d", rid, len(adapters))
            mark_succeeded(job_id, message=msg, metrics={"adapters": len(adapters), "total_ingested": 0, "reason": "no_data_yet"})
            if status:
                status.write(
                    job_id=job_id,
                    tool="master",
                    request_id=rid,
                    state="SUCCEEDED",
                    phase="master",
                    current=0,
                    total=0,
                    message=msg,
                    meta={"adapters": len(adapters)},
                    force=True,
                )
            return
        
        # Filter adapters to only those with data (for auto-detect mode)
        if not source_tools:
            adapters_with_data = []
            for adapter in adapters:
                success, error, rows, _ = adapter.fetch_rows_after_watermark(
                    request_id=rid,
                    watermark=None,
                    limit=1
                )
                if success and rows:
                    adapters_with_data.append(adapter)
                    log.info(
                        "MASTER_ADAPTER_AUTO_DETECTED | tool=%s | table=%s",
                        adapter.source_tool_name,
                        adapter.source_table_name
                    )
            adapters = adapters_with_data
        
        if not adapters:
            msg = "No adapters with data found"
            log.info("MASTER | \LNO_ADAPTERS | request_id=%s | reason=no_data", rid)
            mark_succeeded(job_id, message=msg, metrics={"adapters": 0, "total_ingested": 0, "reason": "no_data"})
            if status:
                status.write(
                    job_id=job_id,
                    tool="master",
                    request_id=rid,
                    state="SUCCEEDED",
                    phase="master",
                    current=0,
                    total=0,
                    message=msg,
                    meta={},
                    force=True,
                )
            return
        
        total_adapters = len(adapters)
        log.info("MASTER | \LSINGLE_PASS_START | request_id=%s | adapters=%d | tools=%s", rid, total_adapters, [a.source_tool_name for a in adapters])
        
        update_progress(
            job_id,
            phase="master_ingestion",
            current=0,
            total=total_adapters,
            message=f"Starting Master single-pass ingestion with {total_adapters} tools",
            metrics={"adapters": total_adapters},
        )
        
        if status:
            status.write(
                job_id=job_id,
                tool="master",
                request_id=rid,
                state="RUNNING",
                phase="master_ingestion",
                current=0,
                total=total_adapters,
                message=f"Starting Master single-pass ingestion with {total_adapters} tools",
                meta={"adapters": total_adapters},
            )
        
        # Track seen domains for duplicate detection within this run
        seen_domains_run: Set[str] = set()
        
        # Track metrics per adapter
        ingested_by_tool: dict[str, int] = {a.source_tool_name: 0 for a in adapters}
        total_ingested = 0
        
        # Single-pass ingestion: process each adapter once
        for adapter in adapters:
            tool_name = adapter.source_tool_name
            
            # Get watermark for this tool
            success, error, watermark_obj = watermark_repo.get_watermark(rid, tool_name)
            if not success:
                log.warning("MASTER_WATERMARK_GET_FAILED | tool=%s | error=%s", tool_name, error)
                continue
            
            watermark = watermark_obj.last_seen_created_at if watermark_obj else None
            
            # Fetch ALL rows after watermark in single pass (no limit for single-pass mode)
            success, error, source_rows, new_watermark = adapter.fetch_rows_after_watermark(
                request_id=rid,
                watermark=watermark,
                limit=10000  # Large limit for single-pass - fetch all available data
            )
            
            if not success:
                log.warning("MASTER_FETCH_FAILED | tool=%s | error=%s", tool_name, error)
                continue
            
            if not source_rows:
                log.info("MASTER | \LNO_NEW_ROWS | tool=%s | watermark=%s", tool_name, watermark)
                continue
            
            # Normalize rows to Master schema
            master_rows: List[MasterRow] = []
            skipped_rows = 0
            no_domain_rows = 0
            
            for source_row in source_rows:
                master_row = adapter.normalize_to_master(source_row)
                if master_row:
                    # Track no-domain rows (for GoogleMaps discover stage)
                    if not master_row.domain or master_row.domain == "":
                        no_domain_rows += 1
                    
                    # Compute duplicate flag (only for rows with domains)
                    if master_row.domain:
                        domain_lower = master_row.domain.lower().strip()
                        if domain_lower in seen_domains_run:
                            master_row.dup_in_run = "YES"
                        else:
                            master_row.dup_in_run = "NO"
                            seen_domains_run.add(domain_lower)
                    else:
                        # No domain = not a duplicate (unique by place_id)
                        master_row.dup_in_run = "NO"
                    
                    master_rows.append(master_row)
                else:
                    skipped_rows += 1
            
            if not master_rows:
                log.debug("MASTER_NO_VALID_ROWS | tool=%s | fetched=%d | skipped=%d", tool_name, len(source_rows), skipped_rows)
                continue
            
            if no_domain_rows > 0:
                log.info("MASTER | \LNO_DOMAIN_ROWS | tool=%s | no_domain=%d | total=%d", tool_name, no_domain_rows, len(master_rows))
            
            # Upsert into master_results with fail-fast on permanent errors
            try:
                success, error, affected = results_repo.upsert_many(master_rows)
                if not success:
                    log.error("MASTER_UPSERT_FAILED | tool=%s | error=%s", tool_name, error)
                    continue
            except PermanentError as e:
                # Schema/constraint error - fail immediately, don't retry
                log.error("MASTER_PERMANENT_ERROR | tool=%s | error=%s | failing_fast", tool_name, e)
                raise
            
            log.info(
                "MASTER_INGESTED | tool=%s | rows=%d | affected=%d | new_watermark=%s",
                tool_name,
                len(master_rows),
                affected,
                new_watermark.isoformat() if new_watermark else "None"
            )
            
            # Update watermark
            if new_watermark:
                success, error = watermark_repo.set_watermark(
                    request_id=rid,
                    source_tool=tool_name,
                    last_seen_created_at=new_watermark,
                    last_processed_count=len(master_rows)
                )
                if not success:
                    log.warning("MASTER_WATERMARK_SET_FAILED | tool=%s | error=%s", tool_name, error)
            
            # Update metrics
            ingested_by_tool[tool_name] += len(master_rows)
            total_ingested += len(master_rows)
            
            # Update progress after each tool
            update_progress(
                job_id,
                phase="master_ingestion",
                current=total_ingested,
                total=total_ingested,
                message=f"Ingested {len(master_rows)} rows from {tool_name} (total: {total_ingested})",
                metrics={
                    "total_ingested": total_ingested,
                    "by_tool": ingested_by_tool,
                },
            )
        
        log.info("MASTER | \LSINGLE_PASS_COMPLETE | request_id=%s | total_ingested=%d | tools=%s", rid, total_ingested, list(ingested_by_tool.keys()))

        # Export to Sheets FIRST (before enrichment)
        # This ensures rows exist in Sheets before enrichment tries to update them
        if export_to_sheets and settings.master_sheets_export_enabled:
            log.info("MASTER | \LEXPORT_START | request_id=%s | total_ingested=%d", rid, total_ingested)
            
            update_progress(
                job_id,
                phase="master_export",
                current=0,
                total=1,
                message="Exporting Master results to Sheets",
                metrics={"total_ingested": total_ingested},
            )
            
            try:
                # Fetch all results for this request from DB
                success, error, results = results_repo.get_results_by_request(request_id=rid)
                
                if not success:
                    log.error("MASTER_EXPORT_FETCH_FAILED | error=%s", error)
                else:
                    # Convert DB results to Sheets format (Master-owned fields only)
                    # Enrichment columns (Emails, Contacts, Extraction Status, Debug Message)
                    # will be empty initially and updated incrementally by enrichment_sheets_sync
                    sheets_rows = []
                    for result in results:
                        # Build row with all 8 columns matching HEADERS_MASTER_MAIN
                        # Column 0: master_result_id (stable row key for deterministic matching)
                        # Columns 1-3: Master-owned fields
                        # Columns 4-7: Enrichment fields (empty, updated by enrichment sync)
                        row = [
                            str(result.get('id', '')),                 # master_result_id (row key)
                            norm(result.get('company', '')),           # Organisation
                            norm(result.get('domain', '')),            # Domain
                            norm(result.get('source_tool', '')),       # Source
                            '',                                         # Emails (enrichment)
                            '',                                         # Contacts (enrichment)
                            '',                                         # Extraction Status (enrichment)
                            '',                                         # Debug Message (enrichment)
                        ]
                        sheets_rows.append(row)
                    
                    # Create audit rows
                    audit_rows = _create_audit_rows(request_id=rid, results=results)
                    
                    # Export to Sheets
                    export_master_results(
                        client=sheets,
                        job_id=job_id,
                        request_id=rid,
                        results_rows=sheets_rows,
                        audit_rows=audit_rows,
                    )
                    log.info("MASTER | \LEXPORT_COMPLETE | request_id=%s | rows=%d", rid, len(sheets_rows))
            except Exception as e:
                log.error("MASTER_EXPORT_FAILED | error=%s", e, exc_info=True)
                # Don't fail the job if export fails
        elif not export_to_sheets:
            log.info("MASTER_EXPORT_SKIPPED | reason=export_to_sheets_disabled | request_id=%s", rid)
        elif not settings.master_sheets_export_enabled:
            log.info("MASTER_EXPORT_SKIPPED | reason=master_sheets_export_disabled | request_id=%s", rid)
        
        # Job succeeded - mark Master as SUCCEEDED before dispatching enrichment
        msg = f"Master single-pass ingestion completed. Ingested {total_ingested} rows from {len(ingested_by_tool)} tools."
        mark_succeeded(
            job_id,
            message=msg,
            metrics={
                "total_ingested": total_ingested,
                "by_tool": ingested_by_tool,
                "adapters": total_adapters,
                "single_pass": True,
            }
        )
        
        log.info("MASTER | SUCCEEDED | %s", msg)
        
        if status:
            status.write(
                job_id=job_id,
                tool="master",
                request_id=rid,
                state="SUCCEEDED",
                phase="master_complete",
                current=total_ingested,
                total=total_ingested,
                message=msg,
                meta={
                    "total_ingested": total_ingested,
                    "by_tool": ingested_by_tool,
                    "single_pass": True,
                },
                force=True,
            )
        
        # AFTER Master is marked SUCCEEDED, dispatch enrichment as separate job
        # This ensures enrichment runs independently and Master status is not affected by enrichment failures
        try:
            from ..background_jobs import submit_job
            from .._dispatch_enrichment import dispatch_enrichment_job
            
            log.info("MASTER | DISPATCHING_ENRICHMENT | request_id=%s | spreadsheet_id=%s", rid, sid)
            
            # Dispatch enrichment job asynchronously
            submit_job(
                dispatch_enrichment_job,
                request_id=rid,
                spreadsheet_id=sid,
            )
            
            log.info("MASTER | ENRICHMENT_DISPATCHED | request_id=%s", rid)
            
        except Exception as e:
            # Log error but do not fail Master job - it already succeeded
            log.error(
                "MASTER | ENRICHMENT_DISPATCH_FAILED | request_id=%s | error=%s",
                rid,
                e,
                exc_info=True
            )
    
    except Exception as exc:
        mark_failed(job_id, error=str(exc), message="Master job failed")
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
                    total=0,
                    message=str(exc),
                    meta={},
                    force=True,
                )
        except Exception:
            logger.exception("TOOL_STATUS_WRITE_FAILED")
    
    finally:
        # Release the advisory lock only if we acquired it
        if not _skip_lock_acquire and lock_conn:
            release_master_lock(lock_conn, rid)
