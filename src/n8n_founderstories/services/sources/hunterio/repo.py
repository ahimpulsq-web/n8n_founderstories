"""
Hunter.io database repository module.

Handles PostgreSQL persistence for Hunter.io lead data.
Provides simple, lightweight database operations with optimized UPSERT logic.

Key features:
- Stores leads with domains only (no domain = no storage)
- Deduplication by (request_id, domain)
- Incremental updates preserve historical data across reruns
- All rows for a request_id use the latest job_id
- Optimized for high-volume batch processing

Table schema:
    htr_results (
        request_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        domain TEXT NOT NULL,
        organization TEXT NOT NULL,
        location TEXT NOT NULL,
        headcount TEXT NOT NULL,
        query TEXT NOT NULL,
        PRIMARY KEY (request_id, domain)
    )

Architecture:
    Orchestrator
         ↓
    Repo (THIS MODULE) - database persistence
         ↓
    PostgreSQL (htr_results table)

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
from typing import Any

import psycopg

from n8n_founderstories.core.utils.domain import normalize_domain

logger = logging.getLogger(__name__)

# ============================================================================
# TABLE MANAGEMENT
# ============================================================================


def ensure_table(conn: psycopg.Connection[Any], job_id: str) -> None:
    """
    Ensure the htr_results table exists in the database.
    
    Creates the table if it doesn't exist. This is idempotent and safe
    to call on every run.
    
    Table schema:
    - request_id: API request identifier (primary key component)
    - job_id: Service-level run identifier (always updated to latest)
    - domain: Normalized company domain (primary key component)
    - organization: Company name
    - location: Formatted location string (e.g., "DE/Berlin" or "DE" or "-")
    - headcount: Headcount bucket (e.g., "1-10", "11-50")
    - query: Search term (keyword or query)
    
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
                CREATE TABLE IF NOT EXISTS htr_results (
                    request_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    organization TEXT NOT NULL,
                    location TEXT NOT NULL,
                    headcount TEXT NOT NULL,
                    query TEXT NOT NULL,
                    PRIMARY KEY (request_id, domain)
                );
            EXCEPTION
                WHEN duplicate_object THEN NULL;
                WHEN unique_violation THEN NULL;
            END $$;
        """)
    logger.debug(f"HUNTERIOV2 | table=htr_results | state=READY | job_id={job_id}")


# ============================================================================
# DATA PERSISTENCE
# ============================================================================

def upsert_batch_rows(
    conn: psycopg.Connection[Any],
    request_id: str,
    job_id: str,
    rows: list[dict[str, str]]
) -> int:
    """
    UPSERT a batch of rows efficiently (called per batch during run).
    
    This function only UPSERTs the new batch without updating job_id
    for existing untouched rows. This is efficient for incremental updates.
    
    Performance: O(batch_size) - only touches new rows
    
    Note: Call finalize_request_job_id() once at the end of the run
    to ensure all rows have the latest job_id.
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        request_id: The request identifier (primary key component)
        job_id: The service-level run identifier (always latest)
        rows: List of dicts with keys:
            - domain: Company domain (required)
            - organization: Company name (required)
            - location: Formatted location (required)
            - headcount: Headcount bucket (required)
            - query or term: Search query (required, backward compatible)
    
    Returns:
        Number of rows upserted in this batch
    
    Raises:
        psycopg.Error: If database operation fails
    """
    if not rows:
        logger.debug(f"HUNTERIOV2 | action=UPSERT_SKIP | request_id={request_id} | reason=no_new_rows")
        return 0
    
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO htr_results
                (request_id, job_id, domain, organization, location, headcount, query)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (request_id, domain)
                DO UPDATE SET
                    job_id = EXCLUDED.job_id,
                    organization = EXCLUDED.organization,
                    location = EXCLUDED.location,
                    headcount = EXCLUDED.headcount,
                    query = EXCLUDED.query
            """
            
            # Prepare batch with normalized domains
            batch = []
            for row in rows:
                domain = normalize_domain(row["domain"]) or row["domain"]
                batch.append((
                    request_id,
                    job_id,
                    domain,
                    row["organization"],
                    row["location"],
                    row["headcount"],
                    row.get("query") or row.get("term", ""),
                ))
            
            # Execute UPSERT for new batch
            cur.executemany(sql, batch)
            upserted_count = len(batch)
            
            logger.debug(
                f"HUNTERIOV2 | action=UPSERT_BATCH | request_id={request_id} | job_id={job_id} | batch_size={upserted_count}"
            )
            
        return upserted_count
    
    except Exception as e:
        logger.error(
            f"HUNTERIOV2 | action=UPSERT_ERROR | request_id={request_id} | job_id={job_id} | error={e}"
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
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        request_id: The request identifier
        job_id: The final job_id to stamp all rows with
    
    Returns:
        Total number of rows for this request_id
    
    Raises:
        psycopg.Error: If database operation fails
    """
    try:
        with conn.cursor() as cur:
            # Update ALL rows for this request to latest job_id
            cur.execute(
                """
                UPDATE htr_results
                SET job_id = %s
                WHERE request_id = %s AND job_id != %s
                """,
                (job_id, request_id, job_id)
            )
            updated_count = cur.rowcount
            
            if updated_count > 0:
                logger.debug(
                    f"HUNTERIOV2 | action=FINALIZE_JOB_ID | request_id={request_id} | job_id={job_id} | updated={updated_count}"
                )
            
            # Get final row count for this request
            cur.execute(
                "SELECT COUNT(*) FROM htr_results WHERE request_id = %s",
                (request_id,)
            )
            total_rows = cur.fetchone()[0]
            
        logger.info(
            f"HUNTERIOV2 | action=FINALIZE_COMPLETE | request_id={request_id} | job_id={job_id} | updated={updated_count} | total={total_rows}"
        )
        return total_rows
    
    except Exception as e:
        logger.error(
            f"HUNTERIOV2 | action=FINALIZE_ERROR | request_id={request_id} | job_id={job_id} | error={e}"
        )
        raise

