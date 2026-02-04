"""
PostgreSQL repository for HunterIOV2 results.
Simple, lightweight persistence with upsert logic.
"""
from __future__ import annotations

import logging
from typing import Any

import psycopg

from n8n_founderstories.core.logging.tags import log_db

logger = logging.getLogger(__name__)


def ensure_table(conn: psycopg.Connection[Any], job_id: str) -> None:
    """
    Create the hunterio_results table if it doesn't exist.
    
    Table schema:
    - job_id: Service-level run identifier (primary key component)
    - domain: Company domain (lowercase, primary key component)`
    - request_id: Request identifier (metadata only)
    - organization: Company name
    - location: Formatted location string (e.g., "DE/Berlin" or "DE" or "-")
    - headcount: Headcount bucket (e.g., "1-10", "11-50")
    - term: Search term (keyword or query)
    - created_at: First insertion timestamp
    - updated_at: Last update timestamp
    
    Primary key: (job_id, domain) - enforces dedupe per job
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        job_id: The service-level run identifier
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hunterio_results (
                request_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                organization TEXT NOT NULL,
                domain TEXT NOT NULL,
                location TEXT NOT NULL,
                headcount TEXT NOT NULL,
                term TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (job_id, domain)
            )
        """)
    log_db(logger, service="HUNTERIOV2", level="debug", job_id=job_id, table="hunterio_results", state="READY")


def upsert_rows(conn: psycopg.Connection[Any], job_id: str, request_id: str, rows: list[dict[str, str]]) -> int:
    """
    Insert or update rows in hunterio_results table.
    
    On conflict (job_id, domain):
    - Update request_id, organization, location, headcount, term, updated_at
    - "Latest wins" logic - no scoring or ranking
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        job_id: The service-level run identifier (primary key)
        request_id: The request identifier (metadata)
        rows: List of dicts with keys: domain, organization, location, headcount, term
    
    Returns:
        Number of rows processed
    
    Raises:
        psycopg.Error: If database operation fails
    """
    if not rows:
        return 0
    
    try:
        with conn.cursor() as cur:
            # Prepare batch insert with ON CONFLICT
            sql = """
                INSERT INTO hunterio_results
                (job_id, domain, request_id, organization, location, headcount, term, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (job_id, domain)
                DO UPDATE SET
                    request_id = EXCLUDED.request_id,
                    organization = EXCLUDED.organization,
                    location = EXCLUDED.location,
                    headcount = EXCLUDED.headcount,
                    term = EXCLUDED.term,
                    updated_at = NOW()
            """
            
            # Build batch of values
            batch = [
                (
                    job_id,
                    row["domain"],
                    request_id,
                    row["organization"],
                    row["location"],
                    row["headcount"],
                    row["term"],
                )
                for row in rows
            ]
            
            # Execute batch
            cur.executemany(sql, batch)
            
        log_db(logger, service="HUNTERIOV2", level="debug", job_id=job_id, action="UPSERT", rows=len(rows))
        return len(rows)
    
    except Exception as e:
        log_db(logger, service="HUNTERIOV2", level="error", job_id=job_id, action="UPSERT", err=str(e))
        raise