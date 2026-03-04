"""
Leads data fetcher for Google Sheets export.

Fetches enriched results from enrichment_results table.
Only includes rows where enrichment was successful.

Classification:
- Role: HOW to fetch and format data from database
- No Google Sheets API calls
- No sheet layout definitions
- Pure data retrieval and transformation

Usage:
    from services.sheets.data_fetchers import leads
    
    rows = leads.fetch_rows_for_sheet(
        conn=db_connection,
        request_id="req_abc",
    )
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

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
    Fetch Leads results for Google Sheets export.
    
    Fetches from enrichment_results table and formats for Leads tab.
    Only includes rows where email exists and status='succeeded'.
    
    Returns rows in the exact order/format expected by Google Sheets Leads tab:
    [request_id, job_id, organisation, domain, company, company_score, email, email_score,
     contacts, description, verification_link, send_status, comments]
    
    Column details:
    - request_id, job_id: Hidden columns
    - organisation: From mstr_results (via enriched_results)
    - domain: Company website
    - company: From company column (LLM extracted, name only)
    - company_score: From company column (hidden, for conditional formatting)
    - email: From email column (email string only)
    - email_score: From email column (hidden, for conditional formatting)
    - contacts: From contacts field (formatted as "Name (Role), Name2 (Role2)")
    - description: From description field (LLM extracted)
    - verification_link: First evidence URL from emails_json (parsed)
    - send_status: Default "VERIFY"
    - comments: Blank (user fills)
    
    Args:
        conn: Active psycopg connection
        job_id: Job ID to filter results (optional, if None returns all results)
        request_id: Request ID to filter results (optional, takes precedence over job_id)
        
    Returns:
        List of rows, where each row is a list of 13 values matching HEADERS
    """
    with conn.cursor() as cur:
        # Build query based on filters
        # Query company and email columns which contain JSON objects with scores
        # LEFT JOIN with mail_content to get send_status
        # LEFT JOIN with mail_tracker to check if email was previously contacted (only SENT, not FAILED)
        if request_id:
            cur.execute("""
                SELECT DISTINCT ON (e.domain)
                    e.request_id,
                    e.job_id,
                    e.organization,
                    e.domain,
                    e.company,
                    e.email,
                    e.contacts,
                    e.description,
                    e.emails_json,
                    m.send_status,
                    CASE
                        WHEN mt.email IS NOT NULL AND mt.send_status = 'SENT' THEN true
                        ELSE false
                    END as is_contacted,
                    mt_current.send_status as current_tracker_status
                FROM enrichment_results e
                LEFT JOIN mail_content m ON e.request_id = m.request_id AND e.domain = m.domain
                LEFT JOIN mail_tracker mt ON (e.email::jsonb->>'email') = mt.email AND mt.send_status = 'SENT'
                LEFT JOIN mail_tracker mt_current ON e.request_id = mt_current.request_id AND e.domain = mt_current.domain
                WHERE e.request_id = %s
                  AND e.email IS NOT NULL
                  AND e.email != ''
                  AND e.status = 'succeeded'
                  AND (e.email::jsonb->>'email') IS NOT NULL
                  AND (e.email::jsonb->>'email') != ''
                ORDER BY e.domain ASC, e.created_at DESC
            """, (request_id,))
        elif job_id:
            cur.execute("""
                SELECT DISTINCT ON (e.domain)
                    e.request_id,
                    e.job_id,
                    e.organization,
                    e.domain,
                    e.company,
                    e.email,
                    e.contacts,
                    e.description,
                    e.emails_json,
                    m.send_status,
                    CASE
                        WHEN mt.email IS NOT NULL AND mt.send_status = 'SENT' THEN true
                        ELSE false
                    END as is_contacted,
                    mt_current.send_status as current_tracker_status
                FROM enrichment_results e
                LEFT JOIN mail_content m ON e.request_id = m.request_id AND e.domain = m.domain
                LEFT JOIN mail_tracker mt ON (e.email::jsonb->>'email') = mt.email AND mt.send_status = 'SENT'
                LEFT JOIN mail_tracker mt_current ON e.request_id = mt_current.request_id AND e.domain = mt_current.domain
                WHERE e.job_id = %s
                  AND e.email IS NOT NULL
                  AND e.email != ''
                  AND e.status = 'succeeded'
                  AND (e.email::jsonb->>'email') IS NOT NULL
                  AND (e.email::jsonb->>'email') != ''
                ORDER BY e.domain ASC, e.created_at DESC
            """, (job_id,))
        else:
            cur.execute("""
                SELECT DISTINCT ON (e.domain)
                    e.request_id,
                    e.job_id,
                    e.organization,
                    e.domain,
                    e.company,
                    e.email,
                    e.contacts,
                    e.description,
                    e.emails_json,
                    m.send_status,
                    CASE
                        WHEN mt.email IS NOT NULL AND mt.send_status = 'SENT' THEN true
                        ELSE false
                    END as is_contacted
                FROM enrichment_results e
                LEFT JOIN mail_content m ON e.request_id = m.request_id AND e.domain = m.domain
                LEFT JOIN mail_tracker mt ON (e.email::jsonb->>'email') = mt.email AND mt.send_status = 'SENT'
                WHERE e.email IS NOT NULL
                  AND e.email != ''
                  AND e.status = 'succeeded'
                  AND (e.email::jsonb->>'email') IS NOT NULL
                  AND (e.email::jsonb->>'email') != ''
                ORDER BY e.domain ASC, e.created_at DESC
            """)
        
        rows = cur.fetchall()
    
    if not rows:
        logger.debug(f"No enriched results found for job_id={job_id}, request_id={request_id}")
        return []
    
    # Format rows for Leads sheet
    formatted_rows = []
    for row in rows:
        (
            request_id_val,
            job_id_val,
            organization,
            domain,
            company,
            email,
            contacts,
            description,
            emails_json,
            mail_send_status,
            is_contacted,
            current_tracker_status,
        ) = row
        
        # Parse company column to extract name and score
        company_name, company_score = _parse_company_column(company)
        
        # Parse email column to extract email string and score
        email_str, email_score = _parse_email_column(email)
        
        # Parse emails_json to extract verification link
        verification_link = _parse_verification_link(emails_json, email_str)
        
        # Determine send_status:
        # 1. If email was previously contacted (exists in mail_tracker with SENT), mark as CONTACTED
        # 2. If current request has FAILED in mail_tracker, show as READY (maintain state)
        # 3. Otherwise use mail_content send_status if available
        # 4. Default to VERIFY if no mail_content exists
        # Note: FAILED status is only in mail_tracker, never in mail_content
        # FAILED status should only appear in Mails tab, not in Leads tab
        if is_contacted:
            send_status = "CONTACTED"
        elif current_tracker_status and current_tracker_status.upper() == "FAILED":
            send_status = "READY"  # Maintain state as if send hadn't been attempted
        elif mail_send_status:
            send_status = mail_send_status
        else:
            send_status = "VERIFY"
        
        # Format row according to spec
        formatted_row = [
            request_id_val or "",           # Column A (hidden)
            job_id_val or "",               # Column B (hidden)
            organization or "",             # Column C - organisation
            domain or "",                   # Column D - domain
            company_name or "",             # Column E - company (name only)
            company_score or 0.0,           # Column F - company_score (hidden, for formatting)
            email_str or "",                # Column G - email (email string only)
            email_score or 0.0,             # Column H - email_score (hidden, for formatting)
            contacts or "",                 # Column I - contacts
            description or "",              # Column J - description
            verification_link or "",        # Column K - verification_link
            send_status,                    # Column L - send_status (from mail_content or default)
            "",                             # Column M - comments (blank, user fills)
        ]
        
        formatted_rows.append(formatted_row)
    
    logger.debug(
        f"Fetched {len(formatted_rows)} enriched results for Leads sheet "
        f"(job_id={job_id}, request_id={request_id})"
    )
    
    return formatted_rows


def _parse_company_column(company: Optional[str]) -> tuple[str, float]:
    """
    Parse company column to extract company name and score.
    
    Expected format from aggregate worker (single object):
    {
        "name": "Company Name",
        "score": 0.95,
        "page_type": "home"
    }
    
    Args:
        company: JSON string with company data
        
    Returns:
        Tuple of (company_name, company_score)
    """
    if not company:
        return "", 0.0
    
    try:
        data = json.loads(company)
        
        if isinstance(data, dict):
            name = data.get("name", "")
            score = data.get("score", 0.0)
            return name, float(score)
        
        return "", 0.0
    except (json.JSONDecodeError, AttributeError, TypeError, ValueError) as e:
        logger.warning(f"Failed to parse company column: {e}")
        return "", 0.0


def _parse_email_column(email: Optional[str]) -> tuple[str, float]:
    """
    Parse email column to extract email string and score.
    
    Expected format from aggregate worker (single object):
    {
        "email": "hello@example.com",
        "score": 0.8387,
        "page_type": "impressum"
    }
    
    Args:
        email: JSON string with email data
        
    Returns:
        Tuple of (email_string, email_score)
    """
    if not email:
        return "", 0.0
    
    try:
        data = json.loads(email)
        
        if isinstance(data, dict):
            email_str = data.get("email", "")
            score = data.get("score", 0.0)
            return email_str, float(score)
        
        return "", 0.0
    except (json.JSONDecodeError, AttributeError, TypeError, ValueError) as e:
        logger.warning(f"Failed to parse email column: {e}")
        return "", 0.0


def _parse_verification_link(emails_json: Optional[str], target_email: str) -> str:
    """
    Parse emails_json to extract verification link for the target email.
    
    Expected format (array of email objects with evidence):
    [
        {
            "email": "hello@example.com",
            "evidence": {
                "url": "https://example.com/impressum",
                "page_type": "impressum",
                "quote": "..."
            }
        }
    ]
    
    OR with multiple evidence:
    [
        {
            "email": "hello@example.com",
            "evidence": [
                {"url": "https://example.com/impressum", "page_type": "impressum", "quote": "..."},
                {"url": "https://example.com/", "page_type": "home", "quote": "..."}
            ]
        }
    ]
    
    Args:
        emails_json: JSON string with email evidence data
        target_email: The email to find evidence for
        
    Returns:
        First evidence URL or empty string
    """
    if not emails_json or not target_email:
        return ""
    
    try:
        data = json.loads(emails_json)
        
        if not isinstance(data, list):
            return ""
        
        # Find the email entry matching target_email
        for email_entry in data:
            if not isinstance(email_entry, dict):
                continue
            
            email = email_entry.get("email", "")
            if email == target_email:
                evidence = email_entry.get("evidence")
                
                # Handle single evidence object
                if isinstance(evidence, dict):
                    return evidence.get("url", "")
                
                # Handle evidence array
                elif isinstance(evidence, list) and len(evidence) > 0:
                    first_evidence = evidence[0]
                    if isinstance(first_evidence, dict):
                        return first_evidence.get("url", "")
        
        return ""
    
    except (json.JSONDecodeError, AttributeError, TypeError) as e:
        logger.warning(f"Failed to parse emails_json for verification link: {e}")
        return ""