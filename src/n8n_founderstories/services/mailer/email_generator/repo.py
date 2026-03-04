"""
Email Content Generator database repository module.

Handles PostgreSQL persistence for email content generation data.
Provides simple, lightweight database operations with optimized UPSERT logic.

Key features:
- Stores email content with all enrichment data
- Deduplication by (request_id, domain)
- Incremental updates preserve historical data across reruns
- All rows for a request_id use the latest job_id
- Optimized for high-volume batch processing

Table schema:
    mail_content (
        request_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        organisation TEXT,
        domain TEXT NOT NULL,
        company TEXT,
        company_score NUMERIC,
        email TEXT,
        email_score NUMERIC,
        contacts TEXT,
        description TEXT,
        verification_link TEXT,
        test_recipient TEXT,
        subject TEXT,
        content TEXT,
        send_status TEXT,
        comments TEXT,
        PRIMARY KEY (request_id, domain)
    )

Architecture:
    API Endpoint
         ↓
    Repo (THIS MODULE) - database persistence
         ↓
    PostgreSQL (mail_content table)

API Usage:
    # Setup
    ensure_table(conn, job_id)
    
    # Per batch (called many times)
    upsert_batch_rows(conn, request_id, job_id, batch)
    
    # Once at end (called once)
    total_rows = finalize_request_job_id(conn, request_id, job_id)

Performance:
    - upsert_batch_rows(): O(batch_size) per call
    - finalize_request_job_id(): O(1) per request
    - Scales efficiently to millions of rows
"""

from __future__ import annotations

import logging
import time
from typing import Any

import psycopg
from psycopg.errors import DeadlockDetected, SerializationFailure

from n8n_founderstories.core.utils.domain import normalize_domain

logger = logging.getLogger(__name__)

# Deadlock retry configuration
MAX_DEADLOCK_RETRIES = 3
DEADLOCK_RETRY_DELAY_MS = 100  # Initial delay in milliseconds
DEADLOCK_BACKOFF_MULTIPLIER = 2  # Exponential backoff multiplier

# ============================================================================
# TABLE MANAGEMENT
# ============================================================================


def ensure_table(conn: psycopg.Connection[Any], job_id: str) -> None:
    """
    Ensure the mail_content table exists in the database.
    
    Creates the table if it doesn't exist. This is idempotent and safe
    to call on every run.
    
    Table schema:
    - request_id: API request identifier (primary key component)
    - job_id: Service-level run identifier (always updated to latest)
    - spreadsheet_id: Google Sheet ID for tracking
    - organisation: Organization name from source
    - domain: Normalized company domain (primary key component)
    - company: Company name
    - email: Contact email address
    - contacts: Contact names/roles
    - description: Company description
    - verification_link: Link to verify company info
    - subject: Email subject line
    - content: Email content body
    - send_status: Email send status (e.g., "VERIFY", "SENT", "FAILED")
    - comments: Additional comments
    
    Primary key: (request_id, domain)
    - Enforces deduplication per request by domain
    - Same domain in different requests = separate rows
    - Same domain in same request = single row (latest wins)
    
    Job ID behavior:
    - All rows for a request_id are always reassigned to the latest job_id
    - This allows tracking which service run last updated the data
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        job_id: The service-level run identifier (for logging only)
    """
    with conn.cursor() as cur:
        # Wrap table creation in exception handling to handle concurrent creation
        cur.execute("""
            DO $$
            BEGIN
                CREATE TABLE IF NOT EXISTS mail_content (
                    request_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    spreadsheet_id TEXT,
                    organisation TEXT,
                    domain TEXT NOT NULL,
                    company TEXT,
                    email TEXT,
                    contacts TEXT,
                    description TEXT,
                    verification_link TEXT,
                    subject TEXT,
                    content TEXT,
                    send_status TEXT,
                    comments TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (request_id, domain)
                );
            EXCEPTION
                WHEN duplicate_object THEN NULL;
                WHEN unique_violation THEN NULL;
            END $$;
        """)
        
        # Add updated_at column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'mail_content'
                    AND column_name = 'updated_at'
                ) THEN
                    ALTER TABLE mail_content ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                END IF;
            END $$;
        """)
    logger.debug(f"MAILERV1 | table=mail_content | state=READY | job_id={job_id}")


# ============================================================================
# DATA PERSISTENCE
# ============================================================================

def upsert_batch_rows(
    conn: psycopg.Connection[Any],
    request_id: str,
    job_id: str,
    rows: list[dict[str, Any]]
) -> int:
    """
    UPSERT a batch of rows efficiently (called per batch during run).
    
    This function only UPSERTs the new batch without updating job_id
    for existing untouched rows. This is efficient for incremental updates.
    
    Performance: O(batch_size) - only touches new rows
    
    Note: Call finalize_request_job_id() once at the end of the run
    to ensure all rows have the latest job_id.
    
    Deadlock Prevention:
    - Sorts batch by domain to ensure consistent lock acquisition order
    - Implements retry logic with exponential backoff
    - Uses ON CONFLICT to minimize lock contention
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        request_id: The request identifier (primary key component)
        job_id: The service-level run identifier (always latest)
        rows: List of dicts with keys:
            - spreadsheet_id: Google Sheet ID (optional)
            - organisation: Organization name (optional)
            - domain: Company domain (required)
            - company: Company name (optional)
            - email: Contact email (optional)
            - contacts: Contact names (optional)
            - description: Company description (optional)
            - verification_link: Verification link (optional)
            - subject: Email subject (optional)
            - content: Email content (optional)
            - send_status: Send status (optional)
            - comments: Comments (optional)
    
    Returns:
        Number of rows upserted in this batch
    
    Raises:
        psycopg.Error: If database operation fails after all retries
    """
    if not rows:
        logger.debug(f"MAILERV1 | action=UPSERT_SKIP | request_id={request_id} | reason=no_new_rows")
        return 0
    
    return _upsert_batch_rows_with_retry(conn, request_id, job_id, rows)


def _upsert_batch_rows_with_retry(
    conn: psycopg.Connection[Any],
    request_id: str,
    job_id: str,
    rows: list[dict[str, Any]],
    retry_count: int = 0
) -> int:
    """
    Internal implementation with retry logic for deadlock handling.
    
    This function implements exponential backoff retry logic and sorts
    rows by domain to ensure consistent lock acquisition order across
    concurrent transactions.
    
    Args:
        conn: Active psycopg connection
        request_id: The request identifier
        job_id: The service-level run identifier
        rows: List of row dicts to upsert
        retry_count: Current retry attempt (internal use)
    
    Returns:
        Number of rows upserted in this batch
    
    Raises:
        psycopg.Error: If database operation fails after all retries
    """
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO mail_content
                (request_id, job_id, spreadsheet_id, organisation, domain, company,
                 email, contacts, description, verification_link,
                 subject, content, send_status, comments, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (request_id, domain)
                DO UPDATE SET
                    job_id = EXCLUDED.job_id,
                    spreadsheet_id = EXCLUDED.spreadsheet_id,
                    organisation = EXCLUDED.organisation,
                    company = EXCLUDED.company,
                    email = EXCLUDED.email,
                    contacts = EXCLUDED.contacts,
                    description = EXCLUDED.description,
                    verification_link = EXCLUDED.verification_link,
                    subject = EXCLUDED.subject,
                    content = EXCLUDED.content,
                    send_status = EXCLUDED.send_status,
                    comments = EXCLUDED.comments,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            # Prepare batch with normalized domains
            batch = []
            for row in rows:
                domain = normalize_domain(row["domain"]) or row["domain"]
                batch.append((
                    request_id,
                    job_id,
                    row.get("spreadsheet_id"),
                    row.get("organisation"),
                    domain,
                    row.get("company"),
                    row.get("email"),
                    row.get("contacts"),
                    row.get("description"),
                    row.get("verification_link"),
                    row.get("subject"),
                    row.get("content"),
                    row.get("send_status", "VERIFY"),
                    row.get("comments"),
                ))
            
            # Sort batch by domain (5th element) to ensure consistent lock order
            # This prevents circular wait conditions in concurrent transactions
            batch.sort(key=lambda x: x[4])  # domain is at index 4
            
            # Execute UPSERT for new batch
            cur.executemany(sql, batch)
            upserted_count = len(batch)
            
            logger.debug(
                f"MAILERV1 | action=UPSERT_BATCH | request_id={request_id} | job_id={job_id} | batch_size={upserted_count}"
            )
            
        return upserted_count
    
    except (DeadlockDetected, SerializationFailure) as e:
        # Handle deadlock with retry logic
        if retry_count < MAX_DEADLOCK_RETRIES:
            # Calculate exponential backoff delay
            delay_ms = DEADLOCK_RETRY_DELAY_MS * (DEADLOCK_BACKOFF_MULTIPLIER ** retry_count)
            delay_sec = delay_ms / 1000.0
            
            logger.warning(
                f"MAILERV1 | action=UPSERT_DEADLOCK_RETRY | request_id={request_id} | job_id={job_id} | "
                f"retry={retry_count + 1}/{MAX_DEADLOCK_RETRIES} | delay_ms={delay_ms} | batch_size={len(rows)} | error={e}"
            )
            
            # Rollback the failed transaction
            conn.rollback()
            
            # Wait before retrying
            time.sleep(delay_sec)
            
            # Retry with incremented counter
            return _upsert_batch_rows_with_retry(conn, request_id, job_id, rows, retry_count + 1)
        else:
            # Max retries exceeded
            logger.error(
                f"MAILERV1 | action=UPSERT_DEADLOCK_MAX_RETRIES | request_id={request_id} | job_id={job_id} | "
                f"retries={MAX_DEADLOCK_RETRIES} | batch_size={len(rows)} | error={e}"
            )
            raise
    
    except Exception as e:
        logger.error(
            f"MAILERV1 | action=UPSERT_ERROR | request_id={request_id} | job_id={job_id} | error={e}"
        )
        raise


def finalize_request_job_id(
    conn: psycopg.Connection[Any],
    request_id: str,
    job_id: str
) -> int:
    """
    Update ALL rows for request_id to latest job_id (called once at end of run).
    
    This maintains the job_id invariant: one request_id → one job_id.
    Call this ONCE after all batches are processed to stamp all rows
    with the final job_id.
    
    Performance: O(1) - single UPDATE with WHERE clause
    
    Job ID Invariant:
    - One request_id → One job_id (always the latest)
    - After any run, ALL rows for request_id have the same job_id
    - job_id represents the "current full state" of the request
    
    Example:
    - Run 2 processes 300 batches for rid1
    - Each batch calls upsert_batch_rows() (300 times)
    - At end, call finalize_request_job_id() ONCE
    - Result: All rows have latest job_id, only 1 UPDATE instead of 300
    
    Deadlock Prevention:
    - Uses row-level locking with FOR UPDATE SKIP LOCKED
    - Implements retry logic with exponential backoff
    - Processes rows in consistent order (by domain) to prevent circular waits
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        request_id: The request identifier
        job_id: The final job_id to stamp all rows with
    
    Returns:
        Total number of rows for this request_id
    
    Raises:
        psycopg.Error: If database operation fails after all retries
    """
    return _finalize_request_job_id_with_retry(conn, request_id, job_id)


def _finalize_request_job_id_with_retry(
    conn: psycopg.Connection[Any],
    request_id: str,
    job_id: str,
    retry_count: int = 0
) -> int:
    """
    Internal implementation with retry logic for deadlock handling.
    
    This function implements exponential backoff retry logic to handle
    database deadlocks gracefully. It uses row-level locking to minimize
    contention and processes rows in a consistent order.
    
    Args:
        conn: Active psycopg connection
        request_id: The request identifier
        job_id: The final job_id to stamp all rows with
        retry_count: Current retry attempt (internal use)
    
    Returns:
        Total number of rows for this request_id
    
    Raises:
        psycopg.Error: If database operation fails after all retries
    """
    try:
        with conn.cursor() as cur:
            # Use row-level locking with consistent ordering to prevent deadlocks
            # ORDER BY domain ensures consistent lock acquisition order across transactions
            # FOR UPDATE SKIP LOCKED allows other transactions to proceed with different rows
            cur.execute(
                """
                UPDATE mail_content
                SET job_id = %s
                WHERE (request_id, domain) IN (
                    SELECT request_id, domain
                    FROM mail_content
                    WHERE request_id = %s AND job_id != %s
                    ORDER BY domain
                    FOR UPDATE SKIP LOCKED
                )
                """,
                (job_id, request_id, job_id)
            )
            updated_count = cur.rowcount
            
            if updated_count > 0:
                logger.debug(
                    f"MAILERV1 | action=FINALIZE_JOB_ID | request_id={request_id} | job_id={job_id} | updated={updated_count}"
                )
            
            # Get final row count for this request
            cur.execute(
                "SELECT COUNT(*) FROM mail_content WHERE request_id = %s",
                (request_id,)
            )
            total_rows = cur.fetchone()[0]
            
        logger.info(
            f"MAILERV1 | action=FINALIZE_COMPLETE | request_id={request_id} | job_id={job_id} | updated={updated_count} | total={total_rows}"
        )
        return total_rows
    
    except (DeadlockDetected, SerializationFailure) as e:
        # Handle deadlock with retry logic
        if retry_count < MAX_DEADLOCK_RETRIES:
            # Calculate exponential backoff delay
            delay_ms = DEADLOCK_RETRY_DELAY_MS * (DEADLOCK_BACKOFF_MULTIPLIER ** retry_count)
            delay_sec = delay_ms / 1000.0
            
            logger.warning(
                f"MAILERV1 | action=DEADLOCK_RETRY | request_id={request_id} | job_id={job_id} | "
                f"retry={retry_count + 1}/{MAX_DEADLOCK_RETRIES} | delay_ms={delay_ms} | error={e}"
            )
            
            # Rollback the failed transaction
            conn.rollback()
            
            # Wait before retrying
            time.sleep(delay_sec)
            
            # Retry with incremented counter
            return _finalize_request_job_id_with_retry(conn, request_id, job_id, retry_count + 1)
        else:
            # Max retries exceeded
            logger.error(
                f"MAILERV1 | action=DEADLOCK_MAX_RETRIES | request_id={request_id} | job_id={job_id} | "
                f"retries={MAX_DEADLOCK_RETRIES} | error={e}"
            )
            raise
    
    except Exception as e:
        logger.error(
            f"MAILERV1 | action=FINALIZE_ERROR | request_id={request_id} | job_id={job_id} | error={e}"
        )
def update_mail_content_send_status(
    conn: psycopg.Connection[Any],
    request_id: str,
    domain: str,
    send_status: str,
) -> None:
    """
    Update the send_status of a mail_content record.
    
    This function is called when an email is tracked as sent via the mail_tracker endpoint.
    It updates the send_status to "CONTACTED" when successfully sent.
    Note: FAILED status should NOT be passed to this function - only CONTACTED for successful sends.
    
    Args:
        conn: Active psycopg connection
        request_id: Request identifier
        domain: Domain
        send_status: New send status (should be "CONTACTED" for successful sends)
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE mail_content
            SET send_status = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE request_id = %s
              AND domain = %s
        """, (send_status, request_id, domain))
        
        rows_updated = cur.rowcount
        
        logger.debug(
            f"MAIL_CONTENT | action=UPDATE_SEND_STATUS | request_id={request_id} | "
            f"domain={domain} | status={send_status} | rows_updated={rows_updated}"
        )


def update_cross_request_comments_mail_content(
    conn: psycopg.Connection[Any],
    email: str,
    source_request_id: str,
    send_status: str,
) -> None:
    """
    Update ONLY comments in mail_content for all OTHER requests that have the same email.
    
    When an email is sent successfully in one request, all other requests with the same
    email should have their comments updated to indicate where it was sent from.
    The send_status should NOT be changed - it stays as it was in each request.
    
    Logic:
    - If send_status is "CONTACTED": Update OTHER requests' comments with "Contacted from request_id XXX"
    - If send_status is anything else: Do nothing (we only track successful sends)
    - The source request (where email was sent) keeps empty comments
    - send_status is NOT modified in other requests
    
    Args:
        conn: Active psycopg connection
        email: Email address that was sent
        source_request_id: Request ID where the email was sent from
        send_status: Send status ("CONTACTED" for successful sends)
    """
    # Only update for successful sends (CONTACTED status)
    if send_status != "CONTACTED":
        return
    
    comment_text = f"Contacted from request_id {source_request_id}"
    
    with conn.cursor() as cur:
        # Update all OTHER requests (not the source request) that have this email
        # ONLY update comments, do NOT change send_status
        # Note: email column can be either plain text OR JSON
        # Handle both formats: plain text email or JSON with {"email": "..."}
        cur.execute("""
            UPDATE mail_content
            SET comments = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE email IS NOT NULL
              AND email != ''
              AND (
                  -- Case 1: Plain text email (direct match)
                  email = %s
                  OR
                  -- Case 2: JSON format (extract email field)
                  (email ~ '^\\s*\\{.*\\}\\s*$' AND (email::jsonb->>'email') = %s)
              )
              AND request_id != %s
        """, (comment_text, email, email, source_request_id))
        
        rows_updated = cur.rowcount
        
        logger.debug(
            f"MAIL_CONTENT | action=UPDATE_CROSS_REQUEST_COMMENTS | email={email} | "
            f"source_request_id={source_request_id} | rows_updated={rows_updated}"
        )