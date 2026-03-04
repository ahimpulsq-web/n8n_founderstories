"""
Sheets Updater Worker - Background service for live Google Sheets updates.

Continuously monitors mstr_results and updates Google Sheets with:
1. Tool Status - Shows crawler progress per request
2. Master Results - Shows all domains with crawl_status
3. Leads - Shows enriched results with emails and send status

This worker runs independently and updates sheets every N seconds for all
active sheet_ids found in the mstr_results table.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional
from datetime import datetime

import psycopg

from n8n_founderstories.core.db import get_conn
from n8n_founderstories.services.sheets.exports import jobs_tool_status, master, leads, mails

logger = logging.getLogger(__name__)

# Global stop flag for graceful shutdown
_stop_flag = False

# Track last update timestamps per request_id to avoid unnecessary updates
# Format: {request_id: {"last_update": datetime, "last_enrichment_update": datetime, "last_mail_content_update": datetime, "last_mail_tracker_update": datetime}}
_last_update_timestamps: Dict[str, Dict[str, Optional[datetime]]] = {}


def stop_worker():
    """Signal the worker to stop gracefully."""
    global _stop_flag
    _stop_flag = True
    logger.info("SHEETS_UPDATER | Stop signal received")


def get_active_sheets(conn: psycopg.Connection[Any]) -> list[tuple[str, str]]:
    """
    Get all active sheet_ids and their associated request_ids from mstr_results.
    
    Returns list of (sheet_id, request_id) tuples for sheets that need updating.
    
    Args:
        conn: Active database connection
        
    Returns:
        List of (sheet_id, request_id) tuples
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT sheet_id, request_id
            FROM mstr_results
            WHERE sheet_id IS NOT NULL
              AND sheet_id != ''
            ORDER BY sheet_id
        """)
        
        return [(row[0], row[1]) for row in cur.fetchall()]


def get_latest_enrichment_timestamp(conn: psycopg.Connection[Any], request_id: str) -> Optional[datetime]:
    """
    Get the latest updated_at timestamp from enrichment_results for a request.
    
    This timestamp indicates when enrichment data was last modified.
    If no enrichment data exists or table doesn't exist yet, returns None.
    
    Args:
        conn: Active database connection
        request_id: Request ID to check
        
    Returns:
        Latest updated_at timestamp or None if no enrichment data exists or table doesn't exist
    """
    try:
        with conn.cursor() as cur:
            # Check if enrichment_results table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'enrichment_results'
                )
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                logger.debug("SHEETS_UPDATER | enrichment_results table does not exist yet")
                return None
            
            # Get latest timestamp
            cur.execute("""
                SELECT MAX(updated_at) as latest_update
                FROM enrichment_results
                WHERE request_id = %s
            """, (request_id,))
            
            row = cur.fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        logger.debug("SHEETS_UPDATER | Error checking enrichment timestamp: %s", str(e))
        return None


def get_latest_mail_content_timestamp(conn: psycopg.Connection[Any], request_id: str) -> Optional[datetime]:
    """
    Get the latest updated_at timestamp from mail_content for a request.
    
    This timestamp indicates when mail content was last modified.
    If no mail content exists or table doesn't exist yet, returns None.
    
    Args:
        conn: Active database connection
        request_id: Request ID to check
        
    Returns:
        Latest updated_at timestamp or None if no mail content exists or table doesn't exist
    """
    try:
        with conn.cursor() as cur:
            # Check if mail_content table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'mail_content'
                )
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                logger.debug("SHEETS_UPDATER | mail_content table does not exist yet")
                return None
            
            # Get latest updated_at timestamp for rows with content
            cur.execute("""
                SELECT MAX(updated_at) as latest_update
                FROM mail_content
                WHERE request_id = %s
                  AND content IS NOT NULL
                  AND content != ''
            """, (request_id,))
            
            row = cur.fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        logger.debug("SHEETS_UPDATER | Error checking mail_content timestamp: %s", str(e))
        return None


def get_latest_mail_tracker_timestamp(conn: psycopg.Connection[Any], request_id: str) -> Optional[datetime]:
    """
    Get the latest updated_at timestamp from mail_tracker for emails in this request.
    
    This checks if any emails from enrichment_results for this request have been
    updated in mail_tracker (e.g., sent in another request). This enables cross-request
    tracking so old request sheets show CONTACTED when emails are sent in new requests.
    
    Args:
        conn: Active database connection
        request_id: Request ID to check
        
    Returns:
        Latest updated_at timestamp from mail_tracker for this request's emails,
        or None if no tracking data exists
    """
    try:
        with conn.cursor() as cur:
            # Check if mail_tracker table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'mail_tracker'
                )
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                logger.debug("SHEETS_UPDATER | mail_tracker table does not exist yet")
                return None
            
            # Get latest updated_at from mail_tracker for emails in this request's enrichment_results
            # This query finds the most recent mail_tracker update for any email that appears
            # in this request's enrichment_results, regardless of which request sent the email
            cur.execute("""
                SELECT MAX(mt.updated_at) as latest_update
                FROM mail_tracker mt
                INNER JOIN enrichment_results e
                    ON (e.email::jsonb->>'email') = mt.email
                WHERE e.request_id = %s
                  AND e.email IS NOT NULL
                  AND e.email != ''
                  AND (e.email::jsonb->>'email') IS NOT NULL
                  AND (e.email::jsonb->>'email') != ''
            """, (request_id,))
            
            row = cur.fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        logger.debug("SHEETS_UPDATER | Error checking mail_tracker timestamp: %s", str(e))
        return None


def should_update_sheets(
    request_id: str,
    latest_enrichment_ts: Optional[datetime],
    latest_mail_content_ts: Optional[datetime],
    latest_mail_tracker_ts: Optional[datetime]
) -> tuple[bool, bool, bool]:
    """
    Determine which sheets should be updated based on enrichment, mail_content, and mail_tracker timestamp changes.
    
    Returns separate flags for different sheet groups:
    - enrichment_changed: Update Tool Status, Master, and Leads (enrichment-dependent sheets)
    - mail_changed: Update Master, Leads, and Mails (mail-dependent sheets)
    - any_changed: Update any sheets at all
    
    This prevents unnecessary API calls when no data has changed.
    
    Mail tracking includes both mail_content (this request's emails) and mail_tracker (cross-request tracking).
    When emails are sent in other requests, mail_tracker timestamps change, triggering updates to show CONTACTED status.
    
    Args:
        request_id: Request ID to check
        latest_enrichment_ts: Latest enrichment updated_at timestamp from DB
        latest_mail_content_ts: Latest mail_content updated_at timestamp from DB
        latest_mail_tracker_ts: Latest mail_tracker updated_at timestamp for this request's emails
        
    Returns:
        Tuple of (enrichment_changed, mail_changed, any_changed)
    """
    global _last_update_timestamps
    
    # If no data exists yet, skip update
    if latest_enrichment_ts is None and latest_mail_content_ts is None and latest_mail_tracker_ts is None:
        logger.debug("SHEETS_UPDATER | SKIP_NO_DATA | request_id=%s", request_id[:8])
        return (False, False, False)
    
    # Check if we have a cached state for this request
    if request_id not in _last_update_timestamps:
        # First time seeing this request - update all sheets
        logger.debug("SHEETS_UPDATER | FIRST_UPDATE | request_id=%s", request_id[:8])
        _last_update_timestamps[request_id] = {
            "last_update": datetime.now(),
            "last_enrichment_update": latest_enrichment_ts,
            "last_mail_content_update": latest_mail_content_ts,
            "last_mail_tracker_update": latest_mail_tracker_ts
        }
        has_enrichment = latest_enrichment_ts is not None
        has_mail = latest_mail_content_ts is not None or latest_mail_tracker_ts is not None
        return (has_enrichment, has_mail, has_enrichment or has_mail)
    
    # Compare with cached timestamps
    cached_enrichment_ts = _last_update_timestamps[request_id].get("last_enrichment_update")
    cached_mail_content_ts = _last_update_timestamps[request_id].get("last_mail_content_update")
    cached_mail_tracker_ts = _last_update_timestamps[request_id].get("last_mail_tracker_update")
    
    # Check if enrichment data has changed (timestamp comparison)
    enrichment_changed = (
        latest_enrichment_ts is not None and
        (cached_enrichment_ts is None or latest_enrichment_ts > cached_enrichment_ts)
    )
    
    # Check if mail content has changed (timestamp comparison)
    mail_content_changed = (
        latest_mail_content_ts is not None and
        (cached_mail_content_ts is None or latest_mail_content_ts > cached_mail_content_ts)
    )
    
    # Check if mail tracker has changed (cross-request tracking)
    mail_tracker_changed = (
        latest_mail_tracker_ts is not None and
        (cached_mail_tracker_ts is None or latest_mail_tracker_ts > cached_mail_tracker_ts)
    )
    
    # Mail changed if either mail_content or mail_tracker changed
    mail_changed = mail_content_changed or mail_tracker_changed
    
    if enrichment_changed or mail_changed:
        # Data has changed - update cache and return flags
        change_type = []
        if enrichment_changed:
            change_type.append("enrichment")
        if mail_content_changed:
            change_type.append("mail_content")
        if mail_tracker_changed:
            change_type.append("mail_tracker")
        
        logger.debug(
            "SHEETS_UPDATER | DATA_CHANGED | request_id=%s | changed=%s",
            request_id[:8],
            ", ".join(change_type)
        )
        _last_update_timestamps[request_id] = {
            "last_update": datetime.now(),
            "last_enrichment_update": latest_enrichment_ts,
            "last_mail_content_update": latest_mail_content_ts,
            "last_mail_tracker_update": latest_mail_tracker_ts
        }
        return (enrichment_changed, mail_changed, True)
    
    # No changes detected - skip update
    logger.debug(
        "SHEETS_UPDATER | SKIP_NO_CHANGES | request_id=%s",
        request_id[:8]
    )
    return (False, False, False)


def update_sheets_for_request(
    sheet_id: str,
    request_id: str,
    update_enrichment_sheets: bool = True,
    update_mail_sheets: bool = True
) -> None:
    """
    Update sheets for a specific request based on what data changed.
    
    Optimized to avoid unnecessary API calls:
    - Tool Status: Only updated when enrichment data changes
    - Master: Updated when either enrichment or mail data changes
    - Leads: Updated when enrichment OR mail data changes (shows send_status from mail_content)
    - Mails: Only updated when mail content changes
    
    Args:
        sheet_id: Google Sheets ID to update
        request_id: Request ID to filter data
        update_enrichment_sheets: Update enrichment-dependent sheets (Tool Status)
        update_mail_sheets: Update mail-dependent sheets (Leads, Mails)
    """
    # Tool Status - only update if enrichment changed
    if update_enrichment_sheets:
        try:
            jobs_tool_status.export_to_sheet(
                sheet_id=sheet_id,
                request_id=request_id,
                suppress_log=True
            )
            logger.debug("SHEETS_UPDATER | TOOL_STATUS_UPDATED | sheet_id=%s | request_id=%s",
                        sheet_id[:8], request_id[:8])
        except Exception as e:
            logger.error("SHEETS_UPDATER | TOOL_STATUS_ERROR | sheet_id=%s | request_id=%s | error=%s",
                        sheet_id[:8], request_id[:8], str(e))
    
    # Master - always update if any data changed (contains both enrichment and mail status)
    try:
        master.export_to_sheet(
            sheet_id=sheet_id,
            request_id=request_id,
            suppress_log=True
        )
        logger.debug("SHEETS_UPDATER | MASTER_UPDATED | sheet_id=%s | request_id=%s",
                    sheet_id[:8], request_id[:8])
    except Exception as e:
        logger.error("SHEETS_UPDATER | MASTER_ERROR | sheet_id=%s | request_id=%s | error=%s",
                    sheet_id[:8], request_id[:8], str(e))
    
    # Leads - update if enrichment OR mail changed (shows send_status from mail_content)
    if update_enrichment_sheets or update_mail_sheets:
        try:
            leads.export_to_sheet(
                sheet_id=sheet_id,
                request_id=request_id,
                suppress_log=True
            )
            logger.debug("SHEETS_UPDATER | LEADS_UPDATED | sheet_id=%s | request_id=%s",
                        sheet_id[:8], request_id[:8])
        except Exception as e:
            logger.error("SHEETS_UPDATER | LEADS_ERROR | sheet_id=%s | request_id=%s | error=%s",
                        sheet_id[:8], request_id[:8], str(e))
    
    # Mails - only update if mail content changed
    if update_mail_sheets:
        try:
            mails.export_to_sheet(
                sheet_id=sheet_id,
                request_id=request_id,
                suppress_log=True
            )
            logger.debug("SHEETS_UPDATER | MAILS_UPDATED | sheet_id=%s | request_id=%s",
                        sheet_id[:8], request_id[:8])
        except Exception as e:
            logger.error("SHEETS_UPDATER | MAILS_ERROR | sheet_id=%s | request_id=%s | error=%s",
                        sheet_id[:8], request_id[:8], str(e))


def run_worker(
    poll_interval_s: float = 30.0,
    max_iterations: int | None = None
) -> None:
    """
    Run the sheets updater worker in a continuous loop.
    
    The worker:
    1. Polls mstr_results for active sheet_ids
    2. For each request, checks if enrichment_results or mail_content has changed
    3. Only updates sheets if data has changed (avoids unnecessary API calls)
    4. Updates Tool Status, Master, Leads, and Mails tabs when changes detected
    5. Sleeps between iterations
    
    Change Detection:
    - Tracks last enrichment_results.updated_at timestamp per request
    - Tracks last mail_content changes per request
    - Only updates sheets when enrichment data or mail content is modified
    - Prevents wasting API quota on unchanged data
    
    Args:
        poll_interval_s: Seconds to wait between update cycles (default: 30.0)
        max_iterations: Maximum iterations before stopping (None = infinite)
    """
    global _stop_flag
    iteration = 0
    
    logger.info("SHEETS_UPDATER | Starting sheets updater worker (poll_interval=%ds)", poll_interval_s)
    
    try:
        while True:
            iteration += 1
            
            # Check stop flag
            if _stop_flag:
                break
            
            if max_iterations and iteration > max_iterations:
                break
            
            try:
                # Get active sheets
                conn = get_conn()
                try:
                    active_sheets = get_active_sheets(conn)
                    
                    if not active_sheets:
                        logger.debug("SHEETS_UPDATER | No active sheets to update")
                        time.sleep(poll_interval_s)
                        continue
                    
                    logger.info("SHEETS_UPDATER | CHECKING | active_sheets=%d", len(active_sheets))
                    
                    # Check each request for changes and update if needed
                    updated_count = 0
                    skipped_count = 0
                    
                    for sheet_id, request_id in active_sheets:
                        try:
                            # Check if enrichment data, mail content, or mail tracker has changed
                            latest_enrichment_ts = get_latest_enrichment_timestamp(conn, request_id)
                            latest_mail_content_ts = get_latest_mail_content_timestamp(conn, request_id)
                            latest_mail_tracker_ts = get_latest_mail_tracker_timestamp(conn, request_id)
                            
                            enrichment_changed, mail_changed, any_changed = should_update_sheets(
                                request_id, latest_enrichment_ts, latest_mail_content_ts, latest_mail_tracker_ts
                            )
                            
                            if any_changed:
                                # Data changed - update only the relevant sheets
                                update_sheets_for_request(
                                    sheet_id,
                                    request_id,
                                    update_enrichment_sheets=enrichment_changed,
                                    update_mail_sheets=mail_changed
                                )
                                updated_count += 1
                            else:
                                # No changes - skip update to save API quota
                                skipped_count += 1
                                
                        except Exception as e:
                            logger.error("SHEETS_UPDATER | UPDATE_ERROR | sheet_id=%s | request_id=%s | error=%s",
                                       sheet_id[:8], request_id[:8], str(e))
                            # Continue with next sheet
                            continue
                    
                    logger.info(
                        "SHEETS_UPDATER | CYCLE_COMPLETE | updated=%d | skipped=%d | total=%d",
                        updated_count,
                        skipped_count,
                        len(active_sheets)
                    )
                    
                finally:
                    conn.close()
                
                # Sleep before next cycle
                time.sleep(poll_interval_s)
            
            except KeyboardInterrupt:
                break
            
            except Exception as e:
                logger.error("SHEETS_UPDATER | ERROR | error=%s", str(e))
                # Sleep longer on error to avoid tight error loop
                time.sleep(poll_interval_s * 2)
    
    except Exception as e:
        logger.error("SHEETS_UPDATER | FATAL_ERROR | error=%s", str(e))
        raise
    
    finally:
        logger.info("SHEETS_UPDATER | Worker stopped")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)-8s %(name)s | %(message)s"
    )
    
    # Run worker
    run_worker()