"""
Master data fetcher for Google Sheets export.

Fetches consolidated results from mstr_results with source-specific sorting.
Stage 1: Clean 7-column layout with simplified sorting.

Classification:
- Role: HOW to fetch and sort data from database
- No Google Sheets API calls
- No sheet layout definitions
- Pure data retrieval and transformation

Usage:
    from services.sheets.data_fetchers import master
    
    rows = master.fetch_rows_for_sheet(
        conn=db_connection,
        job_id="job_123",
        request_id="req_abc",
    )
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

# ============================================================================
# DATA FETCHER
# ============================================================================

def fetch_rows_for_sheet(
    conn: psycopg.Connection[Any],
    *,
    job_id: str | None = None,
    request_id: str | None = None,
) -> list[list[str]]:
    """
    Fetch Master results for Google Sheets export with source-specific sorting.
    
    Stage 1 Behavior:
    1. Fetches rows from mstr_results table (7 columns)
    2. Applies simplified sorting: source priority (hunter first), then organization, then domain
    3. Returns rows in deterministic order
    
    Returns rows in the exact order/format expected by Google Sheets Master tab:
    [request_id, job_id, organization, source, domain, crawl_status, extract_status, enrichment_status, mail_write_status, mail_send_status]
    
    Sorting Rules (Stage 1):
    - Source priority: "hunter" first, then "hunter, google_maps", then "google_maps"
    - Within same source: organization ASC, then domain ASC
    - Treat "hunter, google_maps" as hunter-priority for sorting
    
    Mail Send Status Logic:
    - Shows "CONTACTED" only if email was sent (SENT status in mail_tracker) for THIS request_id
    - Does NOT show cross-request contacted status (unlike Leads tab)
    - Master tab reflects only the current request's data from mstr_results
    - This ensures Master tab is the source of truth for the current request
    
    Args:
        conn: Active psycopg connection
        job_id: Job ID to filter results (optional, if None returns all results)
        request_id: Request ID to filter results (optional, takes precedence over job_id)
        
    Returns:
        List of rows, where each row is:
        [request_id, job_id, organization, source, domain, crawl_status, extract_status, enrichment_status, mail_write_status, mail_send_status]
    """
    with conn.cursor() as cur:
        # Build query based on filters
        # LEFT JOIN with enrichment_results to get email, then with mail_tracker to check if contacted
        # Note: SELECT order matches new table column order
        if request_id:
            cur.execute("""
                SELECT
                    m.request_id,
                    m.job_id,
                    m.organization,
                    m.source,
                    m.domain,
                    m.crawl_status,
                    m.extraction_status,
                    m.enrichment_status,
                    m.mail_write_status,
                    m.mail_send_status,
                    CASE
                        WHEN mt.email IS NOT NULL AND mt.send_status = 'SENT' AND mt.request_id = m.request_id THEN true
                        ELSE false
                    END as is_contacted
                FROM mstr_results m
                LEFT JOIN enrichment_results e ON m.request_id = e.request_id AND m.domain = e.domain
                LEFT JOIN mail_tracker mt ON (e.email::jsonb->>'email') = mt.email AND mt.request_id = m.request_id
                WHERE m.request_id = %s
                  AND m.domain IS NOT NULL
                  AND m.domain != ''
            """, (request_id,))
        elif job_id:
            cur.execute("""
                SELECT
                    m.request_id,
                    m.job_id,
                    m.organization,
                    m.source,
                    m.domain,
                    m.crawl_status,
                    m.extraction_status,
                    m.enrichment_status,
                    m.mail_write_status,
                    m.mail_send_status,
                    CASE
                        WHEN mt.email IS NOT NULL AND mt.send_status = 'SENT' AND mt.request_id = m.request_id THEN true
                        ELSE false
                    END as is_contacted
                FROM mstr_results m
                LEFT JOIN enrichment_results e ON m.request_id = e.request_id AND m.domain = e.domain
                LEFT JOIN mail_tracker mt ON (e.email::jsonb->>'email') = mt.email AND mt.request_id = m.request_id
                WHERE m.job_id = %s
                  AND m.domain IS NOT NULL
                  AND m.domain != ''
            """, (job_id,))
        else:
            cur.execute("""
                SELECT
                    m.request_id,
                    m.job_id,
                    m.organization,
                    m.source,
                    m.domain,
                    m.crawl_status,
                    m.extraction_status,
                    m.enrichment_status,
                    m.mail_write_status,
                    m.mail_send_status,
                    CASE
                        WHEN mt.email IS NOT NULL AND mt.send_status = 'SENT' AND mt.request_id = m.request_id THEN true
                        ELSE false
                    END as is_contacted
                FROM mstr_results m
                LEFT JOIN enrichment_results e ON m.request_id = e.request_id AND m.domain = e.domain
                LEFT JOIN mail_tracker mt ON (e.email::jsonb->>'email') = mt.email AND mt.request_id = m.request_id
                WHERE m.domain IS NOT NULL
                  AND m.domain != ''
            """)
        
        rows = cur.fetchall()
    
    if not rows:
        logger.debug(f"No rows found for job_id={job_id}, request_id={request_id}")
        return []
    
    # Convert to list of dicts for sorting
    row_dicts = []
    for request_id_val, job_id_val, organization, source, domain, crawl_status, extraction_status, enrichment_status, mail_write_status, mail_send_status, is_contacted in rows:
        # Normalize domain
        domain = domain.strip().lower() if domain else ""
        if not domain:
            continue
        
        # Determine source priority for sorting
        # "hunter" = 0 (highest priority)
        # "hunter, google_maps" = 1 (hunter-priority)
        # "google_maps" = 2 (lowest priority)
        if source == "hunter":
            source_priority = 0
        elif source == "hunter, google_maps":
            source_priority = 1
        else:  # google_maps
            source_priority = 2
        
        # Map crawl_status for clean Sheets representation
        # NULL → NOT_STARTED, succeeded → SUCCEEDED, failed → FAILED
        if crawl_status is None or crawl_status == "":
            sheets_crawl_status = "NOT_STARTED"
        else:
            sheets_crawl_status = crawl_status.upper()
        
        # Map extraction_status for clean Sheets representation
        # NULL → NOT_STARTED, succeeded → SUCCEEDED, failed → FAILED
        if extraction_status is None or extraction_status == "":
            sheets_extract_status = "NOT_STARTED"
        else:
            sheets_extract_status = extraction_status.upper()
        
        # Map enrichment_status for clean Sheets representation
        # NULL → NOT_STARTED, succeeded → SUCCEEDED, failed → FAILED
        if enrichment_status is None or enrichment_status == "":
            sheets_enrichment_status = "NOT_STARTED"
        else:
            sheets_enrichment_status = enrichment_status.upper()
        
        # Map mail_write_status for clean Sheets representation
        # NULL → NOT_STARTED, succeeded → SUCCEEDED, failed → FAILED
        if mail_write_status is None or mail_write_status == "":
            sheets_mail_write_status = "NOT_STARTED"
        else:
            sheets_mail_write_status = mail_write_status.upper()
        
        # Map mail_send_status for clean Sheets representation
        # Priority: is_contacted (from mail_tracker for THIS request) > mail_send_status (from mstr_results)
        # If email was contacted in THIS request (SENT in mail_tracker for this request_id), show CONTACTED
        # Otherwise use mail_send_status from mstr_results
        # Note: Unlike Leads tab, Master does NOT show cross-request contacted status
        if is_contacted:
            sheets_mail_send_status = "CONTACTED"
        elif mail_send_status is None or mail_send_status == "":
            sheets_mail_send_status = "NOT_STARTED"
        else:
            sheets_mail_send_status = mail_send_status.upper()
        
        row_dicts.append({
            "request_id": request_id_val or "",
            "job_id": job_id_val or "",
            "source": source or "",
            "organization": organization or "",
            "domain": domain,
            "crawl_status": sheets_crawl_status,
            "extract_status": sheets_extract_status,
            "enrichment_status": sheets_enrichment_status,
            "mail_write_status": sheets_mail_write_status,
            "mail_send_status": sheets_mail_send_status,
            "source_priority": source_priority
        })
    
    # Sort by: source_priority ASC, organization ASC, domain ASC
    row_dicts.sort(key=lambda x: (
        x["source_priority"],
        x["organization"].lower(),
        x["domain"]
    ))
    
    # Convert to output format (organization before source)
    all_rows = [
        [
            row["request_id"],
            row["job_id"],
            row["organization"],
            row["source"],
            row["domain"],
            row["crawl_status"],
            row["extract_status"],
            row["enrichment_status"],
            row["mail_write_status"],
            row["mail_send_status"]
        ]
        for row in row_dicts
    ]
    
    logger.debug(
        f"Fetched {len(all_rows)} rows with source-priority sorting "
        f"(job_id={job_id}, request_id={request_id})"
    )
    
    return all_rows