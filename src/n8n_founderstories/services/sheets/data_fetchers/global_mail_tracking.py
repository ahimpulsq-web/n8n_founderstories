"""
Global Mail Tracking data fetcher for Google Sheets export.

Fetches mail tracking data from mail_tracker table for global tracking sheet.
This sheet tracks ALL sent emails across ALL requests.

Classification:
- Role: HOW to fetch and format data from database
- No Google Sheets API calls
- No sheet layout definitions
- Pure data retrieval and transformation

Usage:
    from services.sheets.data_fetchers import global_mail_tracking
    
    rows = global_mail_tracking.fetch_rows_for_sheet(
        conn=db_connection,
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
    request_id: str | None = None,
) -> list[list[str]]:
    """
    Fetch Global Mail Tracking results for Google Sheets export.
    
    Fetches from mail_tracker table and formats for Global Mail Tracking sheet.
    This sheet contains ALL sent emails across ALL requests for global tracking.
    
    Returns rows in the exact order/format expected by Google Sheets:
    [request_id, sent_mail_id, Company, Domain, Contacts, E-Mail,
     Send Status, Sent At, Reply Status, Received At, comments]
    
    Column details:
    - request_id: Request identifier
    - sent_mail_id: Unique email send ID from email service
    - Company: Company name
    - Domain: Company domain
    - Contacts: Contact names
    - E-Mail: Email address
    - Send Status: Send status (sent, failed, etc.)
    - Sent At: Timestamp when email was sent (human-friendly format)
    - Reply Status: Reply status (replied, no_reply, etc.)
    - Received At: Timestamp when reply was received (human-friendly format)
    - comments: Additional comments
    
    Args:
        conn: Active psycopg connection
        request_id: Request ID to filter results (optional, if None returns all results)
        
    Returns:
        List of rows, where each row is a list of 13 values matching HEADERS
    """
    with conn.cursor() as cur:
        # Build query based on filters
        if request_id:
            cur.execute("""
                SELECT
                    request_id,
                    thread_id,
                    company,
                    domain,
                    contacts,
                    email,
                    subject,
                    content,
                    send_status,
                    sent_at,
                    reply_status,
                    received_at,
                    comments
                FROM mail_tracker
                WHERE request_id = %s
                ORDER BY sent_at DESC NULLS LAST, created_at DESC
            """, (request_id,))
        else:
            cur.execute("""
                SELECT
                    request_id,
                    thread_id,
                    company,
                    domain,
                    contacts,
                    email,
                    subject,
                    content,
                    send_status,
                    sent_at,
                    reply_status,
                    received_at,
                    comments
                FROM mail_tracker
                ORDER BY sent_at DESC NULLS LAST, created_at DESC
            """)
        
        rows = cur.fetchall()
    
    if not rows:
        logger.debug(f"No mail tracking data found for request_id={request_id}")
        return []
    
    # Format rows for Global Mail Tracking sheet
    formatted_rows = []
    for row in rows:
        (
            request_id_val,
            thread_id,
            company,
            domain,
            contacts,
            email,
            subject,
            content,
            send_status,
            sent_at,
            reply_status,
            received_at,
            comments,
        ) = row
        
        # Format timestamps to human-friendly format: "YYYY-MM-DD HH:MM:SS"
        def format_timestamp(ts):
            if ts:
                # Convert to local time and format as "YYYY-MM-DD HH:MM:SS"
                return ts.strftime("%Y-%m-%d %H:%M:%S")
            return ""
        
        sent_at_str = format_timestamp(sent_at)
        received_at_str = format_timestamp(received_at)
        
        # Capitalize status values for consistency
        send_status_upper = send_status.upper() if send_status else ""
        reply_status_upper = reply_status.upper() if reply_status else ""
        
        # Format row according to spec (includes Action column)
        formatted_row = [
            request_id_val or "",           # Column A - request_id (hidden)
            thread_id or "",                # Column B - thread_id
            company or "",                  # Column C - Company
            domain or "",                   # Column D - Domain
            contacts or "",                 # Column E - Contacts (hidden)
            email or "",                    # Column F - E-Mail
            send_status_upper,              # Column G - Send Status (SENT/FAILED/PENDING)
            sent_at_str,                    # Column H - Sent At
            reply_status_upper,             # Column I - Reply Status (RECEIVED/REPLIED/etc)
            received_at_str,                # Column J - Received At
            "",                             # Column K - Action (empty for now)
            comments or "",                 # Column L - comments
        ]
        
        formatted_rows.append(formatted_row)
    
    logger.debug(
        f"Fetched {len(formatted_rows)} mail tracking rows for Global Mail Tracking sheet "
        f"(request_id={request_id})"
    )
    
    return formatted_rows