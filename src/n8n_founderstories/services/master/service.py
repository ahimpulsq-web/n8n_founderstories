"""
Master service orchestration.

Coordinates master sync from different sources with advisory locking.
Uses global sheets infrastructure for export.

The crawler is now a separate worker that monitors master_results for new entries.
"""

from __future__ import annotations
import logging
from typing import Literal

from ...core.db import get_conn
from ...core.logging.tags import log_db
from ..sheets.exports import master as sheets_export
from .lock import advisory_lock
from .repo import ensure_table, merge_and_upsert_request_candidates
from .sources import fetch_hunter_candidates, fetch_google_maps_candidates

logger = logging.getLogger(__name__)


def sync_from_source(
    source: Literal["hunter", "google_maps"],
    job_id: str,
    sheet_id: str | None = None,
    request_id: str | None = None
) -> dict:
    """
    Sync master table from a specific source using request-based merge.
    
    Flow:
    1. Open DB connection
    2. Acquire advisory lock (serializes concurrent syncs)
    3. Fetch candidates from source table (grouped by request_id)
    4. Merge candidates per request_id with cross-source dedupe and Hunter ownership
    5. Commit and return stats
    6. Auto-trigger Master Sheets export (if sheet_id provided)
    
    The crawler worker will automatically pick up new entries where crawl_status IS NULL.
    
    This function is idempotent: calling multiple times with same job_id
    will not corrupt data. Implements request-based merge with:
    - Domain union per request_id
    - Cross-source deduplication
    - Hunter ownership priority for job_id
    
    Args:
        source: Source name ("hunter" or "google_maps")
        job_id: Job identifier to sync
        sheet_id: Google Sheets ID for auto export (optional)
        request_id: Request ID for filtering (optional, if None syncs all requests for job_id)
        
    Returns:
        Dict with sync stats: {
            "source": str,
            "job_id": str,
            "requests_processed": int,
            "total_rows": int
        }
        
    Raises:
        ValueError: If source is invalid
        psycopg.Error: If database operation fails
    """
    if source not in ("hunter", "google_maps"):
        raise ValueError(f"Invalid source: {source}. Must be 'hunter' or 'google_maps'")
    
    logger.info("MASTER | action=SYNC_START | source=%s | job_id=%s | request_id=%s",
                source, job_id, request_id or "ALL")
    
    # Get DB connection
    conn = get_conn()
    
    try:
        # Ensure table exists
        ensure_table(conn)
        
        # Acquire advisory lock to serialize syncs
        with advisory_lock(conn):
            logger.debug("MASTER | action=LOCK_ACQUIRED | source=%s | job_id=%s", source, job_id)
            
            # Fetch candidates from source (grouped by request_id)
            if source == "hunter":
                candidates = fetch_hunter_candidates(job_id, conn, sheet_id)
            else:  # google_maps
                candidates = fetch_google_maps_candidates(job_id, conn, sheet_id)
            
            # Group candidates by request_id
            candidates_by_request: dict[str, list] = {}
            for candidate in candidates:
                req_id = candidate.request_id or "unknown"
                if req_id not in candidates_by_request:
                    candidates_by_request[req_id] = []
                candidates_by_request[req_id].append(candidate)
            
            # Merge candidates per request_id
            total_rows = 0
            requests_processed = 0
            
            for req_id, req_candidates in candidates_by_request.items():
                try:
                    row_count = merge_and_upsert_request_candidates(
                        conn=conn,
                        source=source,
                        request_id=req_id,
                        job_id=job_id,
                        candidates=req_candidates
                    )
                    total_rows += row_count
                    requests_processed += 1
                except Exception as e:
                    logger.error(
                        "MASTER | action=MERGE_ERROR | source=%s | job_id=%s | request_id=%s | error=%s",
                        source,
                        job_id,
                        req_id,
                        str(e)
                    )
                    # Continue with other requests
                    continue
            
            # Commit transaction
            conn.commit()
            
            # DATABASE log with source
            log_db(logger, service="MASTER", table="mstr_results", source=source, rows=total_rows)
        
        # Note: Crawler worker will automatically pick up new entries
        # where crawl_status IS NULL. No need to trigger it here.
        logger.debug(
            "MASTER | SYNC_COMPLETE | source=%s | job_id=%s | new_domains=%d | crawler_will_process=true",
            source,
            job_id,
            total_rows
        )
        
        # Trigger Master Sheets export AFTER crawler completes
        # Only if sheet_id is provided by the calling service
        if sheet_id:
            try:
                sheets_export.export_to_sheet(
                    sheet_id=sheet_id,
                    job_id=job_id
                )
            except Exception as e:
                # Log error but don't fail the sync
                logger.error(
                    "MASTER | EXPORT_ERROR | source=%s | job_id=%s | error=%s",
                    source,
                    job_id,
                    str(e)
                )
        
        logger.info(
            "MASTER | action=SYNC_COMPLETE | source=%s | job_id=%s | requests=%d | rows=%d",
            source,
            job_id,
            requests_processed,
            total_rows
        )
        
        return {
            "source": source,
            "job_id": job_id,
            "requests_processed": requests_processed,
            "total_rows": total_rows
        }
    
    except Exception as e:
        logger.error(
            "MASTER | action=SYNC_ERROR | source=%s | job_id=%s | error=%s",
            source,
            job_id,
            str(e),
            exc_info=True
        )
        # Rollback on error
        conn.rollback()
        raise
    
    finally:
        # Close connection
        conn.close()


def export_to_sheets(
    sheet_id: str | None = None,
    tab_name: str | None = None,
    job_id: str | None = None,
    request_id: str | None = None,
) -> dict:
    """
    Export Master results to Google Sheets using global infrastructure.
    
    This is a thin wrapper around the global sheets export module.
    
    Args:
        sheet_id: Google Sheets spreadsheet ID (optional, uses MASTER_SHEET_ID env var)
        tab_name: Tab name (optional, uses MASTER_SHEET_TAB env var or default)
        job_id: Job ID to filter results (optional, if None exports all results)
        request_id: Request ID to filter results (optional, takes precedence over job_id)
        
    Returns:
        Dict with export stats from sheets_export.export_to_sheet()
        
    Raises:
        ValueError: If sheet_id is not provided and MASTER_SHEET_ID is not set
        Exception: If database or Sheets API operation fails
    """
    return sheets_export.export_to_sheet(
        sheet_id=sheet_id,
        tab_name=tab_name,
        job_id=job_id,
        request_id=request_id,
    )