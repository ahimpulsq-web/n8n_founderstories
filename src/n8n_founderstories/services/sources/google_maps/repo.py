"""
Google Maps Places database repository module.

Handles PostgreSQL persistence for Google Maps Places lead data.
Provides simple, lightweight database operations with optimized UPSERT logic.

Key features:
- Stores leads with domains only (no domain = no storage)
- Deduplication by (request_id, domain)
- Incremental updates preserve historical data across reruns
- All rows for a request_id use the latest job_id
- Optimized for high-volume batch processing

Table schema:
    gmaps_results (
        request_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        domain TEXT NOT NULL,
        organization TEXT NOT NULL,
        location TEXT NOT NULL,
        query TEXT NOT NULL,
        PRIMARY KEY (request_id, domain)
    )

Architecture:
    Orchestrator
         ↓
    Repo (THIS MODULE) - database persistence
         ↓
    PostgreSQL (gmaps_results table)

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

logger = logging.getLogger(__name__)

# ============================================================================
# TABLE MANAGEMENT
# ============================================================================


def ensure_table(conn: psycopg.Connection[Any], job_id: str) -> None:
    """
    Ensure the gmaps_results table exists in the database.
    
    Creates the table if it doesn't exist. This is idempotent and safe
    to call on every run.
    
    Table schema:
    - request_id: API request identifier (primary key component)
    - job_id: Service-level run identifier (always updated to latest)
    - domain: Normalized company domain (primary key component)
    - organization: Company/place name
    - location: Formatted location string (e.g., "FR/Île-de-France/Paris" or "FR" or "")
    - query: Search query text
    
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
                CREATE TABLE IF NOT EXISTS gmaps_results (
                    request_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    organization TEXT NOT NULL,
                    location TEXT NOT NULL,
                    query TEXT NOT NULL,
                    PRIMARY KEY (request_id, domain)
                );
            EXCEPTION
                WHEN duplicate_object THEN NULL;
                WHEN unique_violation THEN NULL;
            END $$;
        """)
    logger.debug(f"GOOGLEMAPSV2 | table=gmaps_results | state=READY | job_id={job_id}")


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
            - domain: Normalized company domain (required)
            - organization: Company name (required)
            - location: Formatted location (required)
            - query or text_query: Search query (required, backward compatible)
    
    Returns:
        Number of rows upserted in this batch
    
    Raises:
        psycopg.Error: If database operation fails
    """
    if not rows:
        logger.debug(f"GOOGLEMAPSV2 | action=UPSERT_SKIP | request_id={request_id} | reason=no_new_rows")
        return 0
    
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO gmaps_results
                (request_id, job_id, domain, organization, location, query)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (request_id, domain)
                DO UPDATE SET
                    job_id = EXCLUDED.job_id,
                    organization = EXCLUDED.organization,
                    location = EXCLUDED.location,
                    query = EXCLUDED.query
            """
            
            # Prepare batch (domains already normalized in orchestrator)
            batch = []
            for row in rows:
                batch.append((
                    request_id,
                    job_id,
                    row["domain"],
                    row["organization"],
                    row["location"],
                    row.get("query") or row.get("text_query", ""),
                ))
            
            # Execute UPSERT for new batch
            cur.executemany(sql, batch)
            upserted_count = len(batch)
            
            logger.debug(
                f"GOOGLEMAPSV2 | action=UPSERT_BATCH | request_id={request_id} | job_id={job_id} | batch_size={upserted_count}"
            )
            
        return upserted_count
    
    except Exception as e:
        logger.error(
            f"GOOGLEMAPSV2 | action=UPSERT_ERROR | request_id={request_id} | job_id={job_id} | error={e}"
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
                UPDATE gmaps_results
                SET job_id = %s
                WHERE request_id = %s AND job_id != %s
                """,
                (job_id, request_id, job_id)
            )
            updated_count = cur.rowcount
            
            if updated_count > 0:
                logger.debug(
                    f"GOOGLEMAPSV2 | action=FINALIZE_JOB_ID | request_id={request_id} | job_id={job_id} | updated={updated_count}"
                )
            
            # Get final row count for this request
            cur.execute(
                "SELECT COUNT(*) FROM gmaps_results WHERE request_id = %s",
                (request_id,)
            )
            total_rows = cur.fetchone()[0]
            
        logger.info(
            f"GOOGLEMAPSV2 | action=FINALIZE_COMPLETE | request_id={request_id} | job_id={job_id} | updated={updated_count} | total={total_rows}"
        )
        return total_rows
    
    except Exception as e:
        logger.error(
            f"GOOGLEMAPSV2 | action=FINALIZE_ERROR | request_id={request_id} | job_id={job_id} | error={e}"
        )
        raise


def append_places_page(
    request_id: str,
    text_query: str,
    language: str,
    page_size: int,
    max_pages: int,
    country: str,
    state: str | None,
    city: str | None,
    page_no: int,
    returned: int,
    kept: int,
    leads_preview: list[dict[str, Any]],
    response: dict[str, Any] | None,
) -> None:
    """
    Log Google Places API page results for tracking and debugging.
    
    This function logs each page of results from the Places API to help track:
    - How many results were returned vs kept per page
    - Which locations and queries are being searched
    - Preview of leads found on each page
    
    Args:
        request_id: The request identifier
        text_query: Search query text (e.g., "software company")
        language: Language code for search
        page_size: Number of results per page
        max_pages: Maximum pages to fetch
        country: Country code (e.g., "US", "FR")
        state: State/region name (optional)
        city: City name (optional)
        page_no: Current page number (1-based)
        returned: Number of results returned by API
        kept: Number of results kept after filtering
        leads_preview: Preview of leads found (for debugging)
        response: Raw API response (optional, for debugging)
    """
    location_parts = [p for p in [city, state, country] if p]
    location_str = "/".join(location_parts) if location_parts else "unknown"
    
    logger.debug(
        f"GOOGLEMAPSV2 | PLACES_PAGE | request_id={request_id} | "
        f"query={text_query!r} | location={location_str} | "
        f"page={page_no}/{max_pages} | returned={returned} | kept={kept}"
    )


def append_places_location_summary(
    request_id: str,
    text_query: str,
    country: str,
    state: str | None,
    city: str | None,
    total_returned: int,
    total_kept: int,
) -> None:
    """
    Log summary of Google Places API results for a location.
    
    This function logs aggregate results for a specific location to help track:
    - Total results returned vs kept per location
    - Success rate of filtering logic
    - Which locations are most productive
    
    Args:
        request_id: The request identifier
        text_query: Search query text (e.g., "software company")
        country: Country code (e.g., "US", "FR")
        state: State/region name (optional)
        city: City name (optional)
        total_returned: Total results returned by API for this location
        total_kept: Total results kept after filtering for this location
    """
    location_parts = [p for p in [city, state, country] if p]
    location_str = "/".join(location_parts) if location_parts else "unknown"
    
    kept_rate = (total_kept / total_returned * 100) if total_returned > 0 else 0
    
    logger.info(
        f"GOOGLEMAPSV2 | LOCATION_SUMMARY | request_id={request_id} | "
        f"query={text_query!r} | location={location_str} | "
        f"returned={total_returned} | kept={total_kept} | rate={kept_rate:.1f}%"
    )