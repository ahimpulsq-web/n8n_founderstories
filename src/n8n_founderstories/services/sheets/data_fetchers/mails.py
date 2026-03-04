"""
Mails data fetcher for Google Sheets export.

Fetches mail content data from mail_content table.
Only includes rows where content exists.

Classification:
- Role: HOW to fetch and format data from database
- No Google Sheets API calls
- No sheet layout definitions
- Pure data retrieval and transformation

Usage:
    from services.sheets.data_fetchers import mails
    
    rows = mails.fetch_rows_for_sheet(
        conn=db_connection,
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
    Fetch Mails results for Google Sheets export.
    
    Fetches from mail_content table and formats for Mails tab.
    Only includes rows where content exists (content IS NOT NULL AND content != '').
    
    Mails tab behavior:
    - Shows send_status directly from mail_content for THIS request only
    - Does NOT show cross-request contacted status (unlike Leads tab)
    - Comments may include "Contacted from request_id XXX" for cross-request tracking
    - This allows users to see the actual status for each request independently
    
    Returns rows in the exact order/format expected by Google Sheets Mails tab:
    [request_id, job_id, organisation, domain, company, email, contacts,
     test_recipient, subject, content, send_status, comments]
    
    Column details:
    - request_id, job_id: Hidden columns
    - organisation: Organization name from mail_content
    - domain: Company website domain
    - company: Company name from mail_content
    - email: Contact email from mail_content
    - contacts: Contact persons from mail_content
    - test_recipient: Blank (user fills)
    - subject: Email subject line from mail_content
    - content: Email content/body from mail_content
    - send_status: "CONTACTED" if previously sent, otherwise from mail_content (VERIFY or CONTACTED)
    - comments: Comments from mail_content (may include "Sent from request_id XXX" for cross-request)
    
    Args:
        conn: Active psycopg connection
        job_id: Job ID to filter results (optional, if None returns all results)
        request_id: Request ID to filter results (optional, takes precedence over job_id)
        
    Returns:
        List of rows, where each row is a list of 12 values matching HEADERS
    """
    with conn.cursor() as cur:
        # Build query based on filters
        # Only fetch rows where content exists
        # LEFT JOIN with mail_tracker to get error message from thread_id when status is FAILED
        if request_id:
            cur.execute("""
                SELECT
                    mc.request_id,
                    mc.job_id,
                    mc.organisation,
                    mc.domain,
                    mc.company,
                    mc.email,
                    mc.contacts,
                    mc.subject,
                    mc.content,
                    mc.send_status,
                    mc.comments,
                    mt.thread_id,
                    mt.send_status as tracker_send_status
                FROM mail_content mc
                LEFT JOIN mail_tracker mt ON mc.request_id = mt.request_id AND mc.domain = mt.domain
                WHERE mc.request_id = %s
                  AND mc.content IS NOT NULL
                  AND mc.content != ''
                ORDER BY mc.domain ASC
            """, (request_id,))
        elif job_id:
            cur.execute("""
                SELECT
                    mc.request_id,
                    mc.job_id,
                    mc.organisation,
                    mc.domain,
                    mc.company,
                    mc.email,
                    mc.contacts,
                    mc.subject,
                    mc.content,
                    mc.send_status,
                    mc.comments,
                    mt.thread_id,
                    mt.send_status as tracker_send_status
                FROM mail_content mc
                LEFT JOIN mail_tracker mt ON mc.request_id = mt.request_id AND mc.domain = mt.domain
                WHERE mc.job_id = %s
                  AND mc.content IS NOT NULL
                  AND mc.content != ''
                ORDER BY mc.domain ASC
            """, (job_id,))
        else:
            cur.execute("""
                SELECT
                    mc.request_id,
                    mc.job_id,
                    mc.organisation,
                    mc.domain,
                    mc.company,
                    mc.email,
                    mc.contacts,
                    mc.subject,
                    mc.content,
                    mc.send_status,
                    mc.comments,
                    mt.thread_id,
                    mt.send_status as tracker_send_status
                FROM mail_content mc
                LEFT JOIN mail_tracker mt ON mc.request_id = mt.request_id AND mc.domain = mt.domain
                WHERE mc.content IS NOT NULL
                  AND mc.content != ''
                ORDER BY mc.domain ASC
            """)
        
        rows = cur.fetchall()
    
    if not rows:
        logger.debug(f"No mail content found for job_id={job_id}, request_id={request_id}")
        return []
    
    # Format rows for Mails sheet
    formatted_rows = []
    for row in rows:
        (
            request_id_val,
            job_id_val,
            organisation,
            domain,
            company,
            email,
            contacts,
            subject,
            content,
            send_status,
            comments,
            thread_id,
            tracker_send_status,
        ) = row
        
        # Determine final send_status:
        # 1. If mail_tracker has FAILED status, show FAILED
        # 2. Otherwise use mail_content send_status (should be CONTACTED, READY, or VERIFY)
        # Note: FAILED status is ONLY in mail_tracker, never in mail_content
        if tracker_send_status and tracker_send_status.upper() == "FAILED":
            final_send_status = "FAILED"
        elif send_status:
            final_send_status = send_status
        else:
            final_send_status = "VERIFY"
        
        # Use comments from mail_content (may include "Contacted from request_id XXX" for cross-request)
        display_comments = comments or ""
        
        # Format row according to spec
        formatted_row = [
            request_id_val or "",           # Column A (hidden)
            job_id_val or "",               # Column B (hidden)
            organisation or "",             # Column C - organisation
            domain or "",                   # Column D - domain
            company or "",                  # Column E - company
            email or "",                    # Column F - email
            contacts or "",                 # Column G - contacts
            "",                             # Column H - test_recipient (blank, user fills)
            subject or "",                  # Column I - subject
            content or "",                  # Column J - content
            final_send_status,              # Column K - send_status (CONTACTED if previously sent)
            display_comments,               # Column L - comments (error message if FAILED)
        ]
        
        formatted_rows.append(formatted_row)
    
    logger.debug(
        f"Fetched {len(formatted_rows)} mail content rows for Mails sheet "
        f"(job_id={job_id}, request_id={request_id})"
    )
    
    return formatted_rows