"""
Mail Tracker database repository module.

Handles PostgreSQL persistence for email tracking data.
Provides simple, lightweight database operations with optimized UPSERT logic.

Key features:
- Tracks email campaigns with send and reply status
- Deduplication by (request_id, domain, email)
- Incremental updates preserve historical data across reruns
- All rows for a request_id use the latest job_id
- Optimized for high-volume batch processing

Table schema:
    mail_tracker (
        request_id TEXT NOT NULL,
        thread_id TEXT,
        company TEXT NOT NULL,
        domain TEXT NOT NULL,
        contacts TEXT,
        email TEXT NOT NULL,
        subject TEXT,
        content TEXT,
        send_status TEXT,
        sent_at TIMESTAMP WITH TIME ZONE,
        reply_status TEXT,
        received_at TIMESTAMP WITH TIME ZONE,
        comments TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
        PRIMARY KEY (request_id, domain, email)
    )

Architecture:
    Mailer Service
         ↓
    Repo (THIS MODULE) - database persistence
         ↓
    PostgreSQL (mail_tracker table)

API Usage:
    # Setup
    ensure_table(conn)
    
    # Per batch (called many times)
    upsert_batch_rows(conn, request_id, batch)

Performance:
    - upsert_batch_rows(): O(batch_size) per call
    - finalize_request_job_id(): O(1) per request
    - Scales efficiently to millions of rows
"""

from __future__ import annotations

import logging
from typing import Any, Optional
from datetime import datetime

import psycopg

logger = logging.getLogger(__name__)

# ============================================================================
# TABLE MANAGEMENT
# ============================================================================


def ensure_table(conn: psycopg.Connection[Any]) -> None:
    """
    Ensure the mail_tracker table exists in the database.
    
    Creates the table if it doesn't exist. This is idempotent and safe
    to call on every run.
    
    Table schema:
    - request_id: Request identifier (primary key component)
    - thread_id: Unique thread identifier for the email conversation
    - company: Company name
    - domain: Normalized company domain (primary key component)
    - contacts: Contact names (comma-separated or JSON)
    - email: Email address (primary key component)
    - subject: Email subject line
    - content: Email content/body
    - send_status: Status of email send (e.g., "sent", "pending", "failed")
    - sent_at: Timestamp when email was sent
    - reply_status: Status of reply (e.g., "replied", "no_reply", "bounced")
    - received_at: Timestamp when reply was received
    - comments: Additional notes or comments
    - created_at: When record was created
    - updated_at: When record was last updated
    
    Primary key: (request_id, domain, email)
    - Enforces deduplication per request by domain and email
    - Same email to same domain in different requests = separate rows
    - Same email to same domain in same request = single row (latest wins)
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
    """
    with conn.cursor() as cur:
        # Wrap table creation in exception handling to handle concurrent creation
        cur.execute("""
            DO $$
            BEGIN
                CREATE TABLE IF NOT EXISTS mail_tracker (
                    request_id TEXT NOT NULL,
                    thread_id TEXT,
                    company TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    contacts TEXT,
                    email TEXT NOT NULL,
                    subject TEXT,
                    content TEXT,
                    send_status TEXT,
                    sent_at TIMESTAMP WITH TIME ZONE,
                    reply_status TEXT,
                    received_at TIMESTAMP WITH TIME ZONE,
                    comments TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                    PRIMARY KEY (request_id, domain, email)
                );
            EXCEPTION
                WHEN duplicate_object THEN NULL;
                WHEN unique_violation THEN NULL;
            END $$;
        """)
        
        # Create indexes for efficient queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_mail_tracker_request_id
            ON mail_tracker(request_id)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_mail_tracker_domain
            ON mail_tracker(domain)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_mail_tracker_send_status
            ON mail_tracker(send_status)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_mail_tracker_reply_status
            ON mail_tracker(reply_status)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_mail_tracker_updated_at
            ON mail_tracker(updated_at)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_mail_tracker_email
            ON mail_tracker(email)
        """)
        
        conn.commit()
    
    logger.debug("MAIL_TRACKER | table=mail_tracker | state=READY")


# ============================================================================
# DATA PERSISTENCE
# ============================================================================

def upsert_batch_rows(
    conn: psycopg.Connection[Any],
    request_id: str,
    rows: list[dict[str, Any]]
) -> int:
    """
    UPSERT a batch of rows efficiently (called per batch during run).
    
    This function UPSERTs email tracking records for a given request.
    
    Performance: O(batch_size) - only touches new rows
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        request_id: The request identifier (primary key component)
        rows: List of dicts with keys:
            - thread_id: Unique email send ID (optional)
            - company: Company name (required)
            - domain: Company domain (required)
            - contacts: Contact names (optional)
            - email: Email address (required)
            - subject: Email subject (optional)
            - content: Email content (optional)
            - send_status: Send status (optional)
            - sent_at: Sent timestamp (optional)
            - reply_status: Reply status (optional)
            - received_at: Received timestamp (optional)
            - comments: Comments (optional)
    
    Returns:
        Number of rows upserted in this batch
    
    Raises:
        psycopg.Error: If database operation fails
    """
    if not rows:
        logger.debug(f"MAIL_TRACKER | action=UPSERT_SKIP | request_id={request_id} | reason=no_new_rows")
        return 0
    
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO mail_tracker
                (request_id, thread_id, company, domain, contacts, email, subject, content,
                 send_status, sent_at, reply_status, received_at, comments, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                ON CONFLICT (request_id, domain, email)
                DO UPDATE SET
                    thread_id = EXCLUDED.thread_id,
                    company = EXCLUDED.company,
                    contacts = EXCLUDED.contacts,
                    subject = EXCLUDED.subject,
                    content = EXCLUDED.content,
                    send_status = EXCLUDED.send_status,
                    sent_at = EXCLUDED.sent_at,
                    reply_status = EXCLUDED.reply_status,
                    received_at = EXCLUDED.received_at,
                    comments = EXCLUDED.comments,
                    updated_at = now()
            """
            
            # Prepare batch
            batch = []
            for row in rows:
                batch.append((
                    request_id,
                    row.get("thread_id"),
                    row["company"],
                    row["domain"],
                    row.get("contacts"),
                    row["email"],
                    row.get("subject"),
                    row.get("content"),
                    row.get("send_status"),
                    row.get("sent_at"),
                    row.get("reply_status"),
                    row.get("received_at"),
                    row.get("comments"),
                ))
            
            # Execute UPSERT for new batch
            cur.executemany(sql, batch)
            upserted_count = len(batch)
            
            logger.debug(
                f"MAIL_TRACKER | action=UPSERT_BATCH | request_id={request_id} | batch_size={upserted_count}"
            )
            
        return upserted_count
    
    except Exception as e:
        logger.error(
            f"MAIL_TRACKER | action=UPSERT_ERROR | request_id={request_id} | error={e}"
        )
        raise


# ============================================================================
# QUERY HELPERS
# ============================================================================

def get_emails_by_status(
    conn: psycopg.Connection[Any],
    request_id: str,
    send_status: Optional[str] = None,
    reply_status: Optional[str] = None,
) -> list[dict[str, Any]]:
    """
    Get emails filtered by send and/or reply status.
    
    Args:
        conn: Active psycopg connection
        request_id: Request identifier
        send_status: Filter by send status (optional)
        reply_status: Filter by reply status (optional)
    
    Returns:
        List of email tracking records
    """
    with conn.cursor() as cur:
        conditions = ["request_id = %s"]
        params: list[Any] = [request_id]
        
        if send_status:
            conditions.append("send_status = %s")
            params.append(send_status)
        
        if reply_status:
            conditions.append("reply_status = %s")
            params.append(reply_status)
        
        where_clause = " AND ".join(conditions)
        
        cur.execute(f"""
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
                comments,
                created_at,
                updated_at
            FROM mail_tracker
            WHERE {where_clause}
            ORDER BY created_at DESC
        """, params)
        
        results = []
        for row in cur.fetchall():
            results.append({
                "request_id": row[0],
                "thread_id": row[1],
                "company": row[2],
                "domain": row[3],
                "contacts": row[4],
                "email": row[5],
                "subject": row[6],
                "content": row[7],
                "send_status": row[8],
                "sent_at": row[9],
                "reply_status": row[10],
                "received_at": row[11],
                "comments": row[12],
                "created_at": row[13],
                "updated_at": row[14],
            })
        
        return results


def update_send_status(
    conn: psycopg.Connection[Any],
    request_id: str,
    domain: str,
    email: str,
    send_status: str,
    sent_at: Optional[datetime] = None,
    comments: Optional[str] = None,
) -> None:
    """
    Update the send status of an email.
    
    Args:
        conn: Active psycopg connection
        request_id: Request identifier
        domain: Domain
        email: Email address
        send_status: New send status
        sent_at: Timestamp when sent (optional)
        comments: Additional comments (optional)
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE mail_tracker
            SET send_status = %s,
                sent_at = COALESCE(%s, sent_at),
                comments = COALESCE(%s, comments),
                updated_at = now()
            WHERE request_id = %s
              AND domain = %s
              AND email = %s
        """, (send_status, sent_at, comments, request_id, domain, email))
        
        logger.debug(
            f"MAIL_TRACKER | action=UPDATE_SEND_STATUS | request_id={request_id} | domain={domain} | email={email} | status={send_status}"
        )

def update_cross_request_comments(
    conn: psycopg.Connection[Any],
    email: str,
    source_request_id: str,
    send_status: str,
) -> None:
    """
    Update comments AND send_status in mail_tracker for all OTHER requests that have the same email.
    
    When an email is sent successfully in one request, all other requests with the same
    email should have their status changed to SENT and comments updated to indicate where it was sent from.
    
    Logic:
    - If send_status is "SENT": Update OTHER requests with status=SENT and "Sent from request_id XXX"
    - If send_status is "FAILED": Do nothing (we only track successful sends)
    - The source request (where email was sent) keeps its original comments
    
    Args:
        conn: Active psycopg connection
        email: Email address that was sent
        source_request_id: Request ID where the email was sent from
        send_status: Send status ("SENT" or "FAILED")
    """
    # Only update for successful sends
    if send_status != "SENT":
        return
    
    comment_text = f"Sent from request_id {source_request_id}"
    
    with conn.cursor() as cur:
        # Update all OTHER requests (not the source request) that have this email
        # Set send_status to SENT and add comment
        cur.execute("""
            UPDATE mail_tracker
            SET send_status = 'SENT',
                comments = %s,
                updated_at = now()
            WHERE email = %s
              AND request_id != %s
        """, (comment_text, email, source_request_id))
        
        rows_updated = cur.rowcount
        
        logger.debug(
            f"MAIL_TRACKER | action=UPDATE_CROSS_REQUEST_STATUS_AND_COMMENTS | email={email} | "
            f"source_request_id={source_request_id} | rows_updated={rows_updated}"
        )

def update_reply_status(
    conn: psycopg.Connection[Any],
    request_id: str,
    domain: str,
    email: str,
    reply_status: str,
    received_at: Optional[datetime] = None,
    comments: Optional[str] = None,
) -> None:
    """
    Update the reply status of an email.
    
    Args:
        conn: Active psycopg connection
        request_id: Request identifier
        domain: Domain
        email: Email address
        reply_status: New reply status
        received_at: Timestamp when reply received (optional)
        comments: Additional comments (optional)
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE mail_tracker
            SET reply_status = %s,
                received_at = COALESCE(%s, received_at),
                comments = COALESCE(%s, comments),
                updated_at = now()
            WHERE request_id = %s
              AND domain = %s
              AND email = %s
        """, (reply_status, received_at, comments, request_id, domain, email))
        
        logger.debug(
            f"MAIL_TRACKER | action=UPDATE_REPLY_STATUS | request_id={request_id} | domain={domain} | email={email} | status={reply_status}"
        )


def update_reply_status_by_thread_id(
    conn: psycopg.Connection[Any],
    thread_id: str,
    received_at: datetime,
) -> bool:
    """
    Update the reply status of an email by thread_id.
    
    This function is used by the reply tracker endpoint to mark emails as replied
    when a reply is received. It searches for the email by thread_id and updates
    the reply_status to "received" with the provided timestamp.
    
    Args:
        conn: Active psycopg connection
        thread_id: Thread identifier to search for
        received_at: Timestamp when reply was received
    
    Returns:
        True if a record was found and updated, False otherwise
    """
    with conn.cursor() as cur:
        # Update the record and return whether any row was affected
        cur.execute("""
            UPDATE mail_tracker
            SET reply_status = 'received',
                received_at = %s,
                updated_at = now()
            WHERE thread_id = %s
        """, (received_at, thread_id))
        
        rows_updated = cur.rowcount
        
        if rows_updated > 0:
            logger.info(
                f"MAIL_TRACKER | action=UPDATE_REPLY_BY_THREAD | thread_id={thread_id} | "
                f"status=received | rows_updated={rows_updated}"
            )
            return True
        else:
            logger.warning(
                f"MAIL_TRACKER | action=UPDATE_REPLY_BY_THREAD | thread_id={thread_id} | "
                f"status=NOT_FOUND"
            )
            return False