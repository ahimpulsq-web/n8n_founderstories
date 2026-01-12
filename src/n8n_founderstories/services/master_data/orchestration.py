"""
Master orchestration utilities for DB-first architecture.

This module provides:
1. trigger_master_job: Shared function to trigger Master from tool runners
2. Advisory lock mechanism: Prevents concurrent Master runs for same request_id
3. Tool-agnostic design: Works with any tool without modification
"""

from __future__ import annotations

import hashlib
import logging
from typing import Optional

from ...core.config import settings
from ...core.utils.text import norm
from ..database.connection import get_connection_context, DatabaseConnectionError
from ..jobs.store import create_job, find_job_by_request_and_tool
from ..jobs.models import JobState

logger = logging.getLogger(__name__)


def _compute_lock_id(request_id: str) -> int:
    """
    Compute a PostgreSQL advisory lock ID from request_id.
    
    PostgreSQL advisory locks use bigint (int64), so we hash the request_id
    and take the lower 63 bits to ensure a positive integer.
    
    Args:
        request_id: Request identifier
        
    Returns:
        Integer lock ID suitable for pg_advisory_lock
    """
    # Hash the request_id to get a consistent integer
    hash_bytes = hashlib.sha256(request_id.encode('utf-8')).digest()
    # Take first 8 bytes and convert to int
    lock_id = int.from_bytes(hash_bytes[:8], byteorder='big', signed=False)
    # Ensure positive by taking lower 63 bits
    lock_id = lock_id & 0x7FFFFFFFFFFFFFFF
    return lock_id


def acquire_master_lock(request_id: str, dsn: Optional[str] = None) -> tuple[bool, Optional[any]]:
    """
    Acquire PostgreSQL advisory lock for Master execution.
    
    This function uses pg_try_advisory_lock which:
    - Returns immediately (non-blocking)
    - Returns true if lock acquired, false if already held
    - Lock is automatically released when connection closes
    
    Args:
        request_id: Request identifier to lock on
        dsn: Optional PostgreSQL DSN (uses config if None)
        
    Returns:
        Tuple of (success, connection_or_error)
        - If success=True: connection object (caller must close it to release lock)
        - If success=False: error message or None if lock busy
    """
    if not dsn:
        from ..database.config import db_config
        dsn = db_config.postgres_dsn
    
    if not dsn:
        return False, "No PostgreSQL DSN configured"
    
    lock_id = _compute_lock_id(request_id)
    
    try:
        # Create connection (don't use context manager - caller must manage it)
        from ..database.connection import get_connection
        conn = get_connection(dsn)
        
        # Try to acquire lock (non-blocking)
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
            acquired = cur.fetchone()[0]
        
        if acquired:
            logger.info(
                "MASTER_LOCK_ACQUIRED | request_id=%s | lock_id=%d",
                request_id,
                lock_id
            )
            return True, conn
        else:
            # Lock is held by another process
            conn.close()
            logger.info(
                "MASTER_LOCK_BUSY | request_id=%s | lock_id=%d | skipping",
                request_id,
                lock_id
            )
            return False, None
            
    except DatabaseConnectionError as e:
        logger.warning(
            "MASTER_LOCK_DB_ERROR | request_id=%s | error=%s",
            request_id,
            e
        )
        return False, str(e)
    except Exception as e:
        logger.error(
            "MASTER_LOCK_ERROR | request_id=%s | error=%s",
            request_id,
            e,
            exc_info=True
        )
        return False, str(e)


def release_master_lock(conn: any, request_id: str) -> None:
    """
    Release PostgreSQL advisory lock and close connection.
    
    Args:
        conn: PostgreSQL connection holding the lock
        request_id: Request identifier (for logging)
    """
    if not conn:
        return
    
    lock_id = _compute_lock_id(request_id)
    
    try:
        # Explicitly release lock (though it will auto-release on close)
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
        
        logger.info(
            "MASTER_LOCK_RELEASED | request_id=%s | lock_id=%d",
            request_id,
            lock_id
        )
    except Exception as e:
        logger.warning(
            "MASTER_LOCK_RELEASE_ERROR | request_id=%s | error=%s",
            request_id,
            e
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


def mark_pending(request_id: str, triggered_by: Optional[str] = None, dsn: Optional[str] = None) -> bool:
    """
    Mark Master as pending for this request_id.
    
    This is called when a tool tries to trigger Master but the lock is busy.
    Master will check this flag after releasing the lock and rerun if needed.
    
    Args:
        request_id: Request identifier
        triggered_by: Tool name that triggered (for logging)
        dsn: Optional PostgreSQL DSN
        
    Returns:
        True if successfully marked pending, False otherwise
    """
    if not dsn:
        from ..database.config import db_config
        dsn = db_config.postgres_dsn
    
    if not dsn:
        logger.warning("MASTER_MARK_PENDING_NO_DSN | request_id=%s", request_id)
        return False
    
    try:
        with get_connection_context(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO master_run_state (request_id, pending, last_trigger_by, trigger_count)
                    VALUES (%s, TRUE, %s, 1)
                    ON CONFLICT (request_id)
                    DO UPDATE SET
                        pending = TRUE,
                        last_trigger_by = EXCLUDED.last_trigger_by,
                        trigger_count = master_run_state.trigger_count + 1,
                        updated_at = NOW()
                """, (request_id, triggered_by))
                conn.commit()
        
        logger.info(
            "MASTER_MARKED_PENDING | request_id=%s | triggered_by=%s",
            request_id,
            triggered_by or "unknown"
        )
        return True
        
    except Exception as e:
        logger.error(
            "MASTER_MARK_PENDING_ERROR | request_id=%s | error=%s",
            request_id,
            e,
            exc_info=True
        )
        return False


def pop_pending(request_id: str, dsn: Optional[str] = None) -> bool:
    """
    Check and clear pending flag for this request_id.
    
    This is called by Master after releasing the lock to check if it needs to rerun.
    Uses atomic UPDATE...RETURNING to avoid race conditions.
    
    Args:
        request_id: Request identifier
        dsn: Optional PostgreSQL DSN
        
    Returns:
        True if pending flag was set (Master should rerun), False otherwise
    """
    if not dsn:
        from ..database.config import db_config
        dsn = db_config.postgres_dsn
    
    if not dsn:
        return False
    
    try:
        with get_connection_context(dsn) as conn:
            with conn.cursor() as cur:
                # Atomically check and clear pending flag
                cur.execute("""
                    UPDATE master_run_state
                    SET pending = FALSE, updated_at = NOW()
                    WHERE request_id = %s AND pending = TRUE
                    RETURNING pending
                """, (request_id,))
                result = cur.fetchone()
                conn.commit()
                
                was_pending = result is not None
                
                if was_pending:
                    logger.info(
                        "MASTER_POP_PENDING | request_id=%s | was_pending=true | will_rerun=true",
                        request_id
                    )
                else:
                    logger.debug(
                        "MASTER_POP_PENDING | request_id=%s | was_pending=false",
                        request_id
                    )
                
                return was_pending
                
    except Exception as e:
        logger.error(
            "MASTER_POP_PENDING_ERROR | request_id=%s | error=%s",
            request_id,
            e,
            exc_info=True
        )
        return False


def trigger_master_job(
    *,
    request_id: str,
    spreadsheet_id: str,
    source_tool: Optional[str] = None,
) -> tuple[bool, Optional[str], Optional[str]]:
    """
    Trigger Master ingestion job for a request with level-trigger orchestration.
    
    This function is called by tool runners after successful completion.
    It implements production-grade level-trigger orchestration:
    - If Master already running: Marks pending and returns (avoids lock contention)
    - If lock acquired: Runs Master immediately
    - If lock busy: Marks pending and returns (Master will rerun after releasing lock)
    - No triggers are lost
    
    The Master runner will:
    - Acquire advisory lock (single-flight per request_id)
    - Ingest data in single pass from all available tools
    - Update watermarks
    - Check pending flag after releasing lock and rerun if needed (with delta check)
    - Export to Sheets (optional, only if data ingested)
    
    Design:
    - Non-blocking: Returns immediately
    - Idempotent: Safe to call multiple times
    - Tool-agnostic: Works with any tool
    - Level-trigger: Ensures no triggers are lost
    - Optimized: Checks job state before lock attempt to reduce contention
    
    Args:
        request_id: Request identifier
        spreadsheet_id: Google Sheets spreadsheet ID
        source_tool: Optional tool name that triggered this (for logging)
        
    Returns:
        Tuple of (success, error_message, job_id)
    """
    rid = norm(request_id)
    sid = norm(spreadsheet_id)
    
    if not rid:
        return False, "request_id is required", None
    
    if not sid:
        return False, "spreadsheet_id is required", None
    
    # Optimization: Check if Master is already running/queued before attempting lock
    # This reduces lock contention and DB connections during long-running Master
    master_job = find_job_by_request_and_tool(request_id=rid, tool="master")
    
    if master_job and master_job.state in {JobState.QUEUED, JobState.RUNNING}:
        # Master is already running - just mark pending and return
        logger.info(
            "MASTER_ALREADY_RUNNING | request_id=%s | triggered_by=%s | master_job_id=%s | state=%s | marking_pending=true",
            rid,
            source_tool or "unknown",
            master_job.job_id,
            master_job.state.value
        )
        mark_pending(rid, triggered_by=source_tool)
        return True, None, None  # Success - trigger recorded as pending
    
    # Try to acquire lock (non-blocking)
    lock_acquired, lock_conn_or_error = acquire_master_lock(rid)
    
    if not lock_acquired:
        # Lock is busy or error occurred
        if lock_conn_or_error is None:
            # Lock is held by another Master - mark pending and return
            logger.info(
                "MASTER_LOCK_BUSY | request_id=%s | triggered_by=%s | marking_pending=true",
                rid,
                source_tool or "unknown"
            )
            mark_pending(rid, triggered_by=source_tool)
            return True, None, None  # Success - trigger recorded as pending
        else:
            # Error acquiring lock
            error_msg = f"Failed to acquire Master lock: {lock_conn_or_error}"
            logger.error(
                "MASTER_LOCK_ERROR | request_id=%s | error=%s",
                rid,
                lock_conn_or_error
            )
            return False, error_msg, None
    
    # Lock acquired - run Master immediately
    lock_conn = lock_conn_or_error
    
    try:
        # Import here to avoid circular dependency
        from uuid import uuid4
        from ..background_jobs import submit_job
        from .runner import run_master_job_db_first_with_rerun
        
        # Generate job ID
        job_id = f"master_{uuid4().hex}"
        
        # Create job record
        create_job(
            job_id=job_id,
            tool="master",
            request_id=rid,
            meta={
                "spreadsheet_id": sid,
                "triggered_by": source_tool,
                "db_first": True,
            }
        )
        
        # Submit background job with lock connection
        # The runner will handle lock release and pending check
        submit_job(
            run_master_job_db_first_with_rerun,
            job_id=job_id,
            request_id=rid,
            spreadsheet_id=sid,
            lock_conn=lock_conn,
        )
        
        logger.info(
            "MASTER_JOB_TRIGGERED | request_id=%s | job_id=%s | triggered_by=%s | lock_acquired=true",
            rid,
            job_id,
            source_tool or "unknown"
        )
        
        return True, None, job_id
        
    except Exception as e:
        # Release lock on error
        release_master_lock(lock_conn, rid)
        error_msg = f"Failed to trigger Master job: {e}"
        logger.error(
            "MASTER_TRIGGER_ERROR | request_id=%s | error=%s",
            rid,
            e,
            exc_info=True
        )
        return False, error_msg, None


def should_trigger_master(
    *,
    job_id: str,
    request_id: str,
    tool: str,
) -> bool:
    """
    Determine if Master should be triggered for this job completion.
    
    Policy:
    - Trigger on SUCCEEDED state only
    - Don't trigger if Master is already running for this request
    
    Args:
        job_id: Job identifier
        request_id: Request identifier
        tool: Tool name
        
    Returns:
        True if Master should be triggered, False otherwise
    """
    # Check if Master is already running for this request
    master_job = find_job_by_request_and_tool(request_id=request_id, tool="master")
    
    if master_job and master_job.state in {JobState.QUEUED, JobState.RUNNING}:
        logger.debug(
            "MASTER_ALREADY_RUNNING | request_id=%s | master_job_id=%s | state=%s",
            request_id,
            master_job.job_id,
            master_job.state.value
        )
        # Master is already running, it will pick up our data
        return False
    
    return True