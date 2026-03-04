"""
Jobs Tool Status data fetcher for Google Sheets export.

Handles data retrieval from the jobs store and preparation for Tool Status export.
Fetches job records from disk-based storage and formats them for sheet display.

Classification:
- Role: HOW to fetch and format data from jobs store
- No Google Sheets API calls
- No sheet layout definitions
- Pure data retrieval and transformation

Usage:
    from services.sheets.data_fetchers import jobs_tool_status
    
    rows = jobs_tool_status.fetch_rows_for_sheet()
"""

from __future__ import annotations

import logging

from n8n_founderstories.core.db import get_conn
from n8n_founderstories.services.jobs import list_jobs

logger = logging.getLogger(__name__)


def get_crawler_status_for_request(request_id: str) -> tuple[int, int, str]:
    """
    Get crawler status for a specific request_id.
    
    Queries mstr_results to count completed vs total domains for this request.
    
    Args:
        request_id: Request ID to get crawler status for
        
    Returns:
        Tuple of (completed_count, total_count, status)
        - completed_count: Number of domains with crawl_status != NULL
        - total_count: Total number of domains for this request
        - status: "RUNNING" if any pending, "SUCCEEDED" if all complete
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Get total and completed counts
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(crawl_status) as completed
                FROM mstr_results
                WHERE request_id = %s
            """, (request_id,))
            
            row = cur.fetchone()
            if not row:
                return (0, 0, "SUCCEEDED")
            
            total = row[0]
            completed = row[1]
            
            # Determine status
            if completed >= total:
                status = "SUCCEEDED"
            else:
                status = "RUNNING"
            
            return (completed, total, status)
    finally:
        conn.close()
def get_extraction_status_for_request(request_id: str) -> tuple[int, int, str]:
    """
    Get extraction status for a specific request_id.
    
    Queries mstr_results to count completed vs total domains for this request.
    
    Args:
        request_id: Request ID to get extraction status for
        
    Returns:
        Tuple of (completed_count, total_count, status)
        - completed_count: Number of domains with extraction_status != NULL
        - total_count: Total number of domains for this request
        - status: "RUNNING" if any pending, "SUCCEEDED" if all complete
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Get total and completed counts
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(extraction_status) as completed
                FROM mstr_results
                WHERE request_id = %s
            """, (request_id,))
            
            row = cur.fetchone()
            if not row:
                return (0, 0, "SUCCEEDED")
            
            total = row[0]
            completed = row[1]
            
            # Determine status
            if completed >= total:
                status = "SUCCEEDED"
            else:
                status = "RUNNING"
            
            return (completed, total, status)
    finally:
        conn.close()

# ============================================================================
# DATA FETCHER
# ============================================================================

def fetch_rows_for_sheet(request_id: str | None = None) -> list[list[str]]:
    """
    Fetch job records from the jobs store for Tool Status sheet export.
    
    Retrieves jobs from the disk-based jobs.json store and formats them
    as rows for the Tool Status sheet. Adds a custom crawler status row
    showing progress (completed/total) for the request.
    
    Args:
        request_id: Optional request ID to filter jobs. If provided, only jobs
                   matching this request_id will be returned. If None, all jobs
                   are returned.
    
    Returns:
        List of rows, where each row is [tool, state, request_id, job_id].
        Format: ["hunteriov2", "SUCCEEDED", "req_123", "htrio__abc"]
                ["googlemapsv2", "SUCCEEDED", "req_123", "gmap__xyz"]
                ["crawl (5/10)", "RUNNING", "req_123", ""]  # Always last, no job_id
        
    Example:
        >>> rows = fetch_rows_for_sheet(request_id="req_abc123")
        >>> rows[0]
        ['google_maps', 'SUCCEEDED', 'req_abc123', 'gmap__xyz789']
    """
    # ========================================================================
    # STEP 1: Fetch all jobs from store
    # ========================================================================
    
    jobs = list_jobs()
    
    if not jobs:
        logger.debug("No jobs found in store")
        return []
    
    # ========================================================================
    # STEP 2: Filter by request_id if provided
    # ========================================================================
    
    if request_id:
        jobs = [job for job in jobs if job.request_id == request_id]
        logger.debug(f"Filtered to {len(jobs)} jobs for request_id={request_id}")
    
    # ========================================================================
    # STEP 3: Filter out crawler jobs (we'll add custom crawler row later)
    # ========================================================================
    
    jobs = [job for job in jobs if job.tool.lower() != "crawler"]
    
    # ========================================================================
    # STEP 4: Convert to sheet rows (4 columns: tool, state, request_id, job_id)
    # ========================================================================
    
    rows = []
    
    # Add regular jobs with job_id
    for job in jobs:
        row = [
            job.tool,
            job.state.value,  # Convert enum to string
            job.request_id,
            job.job_id,
        ]
        rows.append(row)
    
    # ========================================================================
    # STEP 5: Add custom crawler status row (no job_id)
    # ========================================================================
    
    if request_id:
        # Get crawler stats for this specific request
        completed, total, status = get_crawler_status_for_request(request_id)
        crawler_row = [
            f"crawl ({completed}/{total})",
            status,
            request_id,
            "",  # Empty job_id for crawler
        ]
        rows.append(crawler_row)
    
    # ========================================================================
    # STEP 6: Add custom extraction status row (no job_id)
    # ========================================================================
    
    if request_id:
        # Get extraction stats for this specific request
        completed, total, status = get_extraction_status_for_request(request_id)
        extraction_row = [
            f"extraction ({completed}/{total})",
            status,
            request_id,
            "",  # Empty job_id for extraction
        ]
        rows.append(extraction_row)
    
    logger.debug(f"Fetched {len(rows)} records for Tool Status sheet (crawler + extraction rows at end)")
    
    return rows