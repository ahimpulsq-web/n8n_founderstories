"""
PostgreSQL repository for Google Maps Places V2 results.
Simple, lightweight persistence with upsert logic.
Only stores leads with websites, deduped by (job_id, website).
"""
from __future__ import annotations

import logging
from typing import Any

import psycopg

from n8n_founderstories.core.logging.tags import log_db

logger = logging.getLogger(__name__)


def ensure_table(conn: psycopg.Connection[Any], job_id: str) -> None:
    """
    Create the googlemaps_places_results table if it doesn't exist.
    
    Table schema:
    - request_id: Request identifier (metadata only)
    - job_id: Service-level run identifier (primary key component)
    - organization: Company/place name
    - website: Website URL (primary key component)
    - description: Editorial summary/description
    - location: Formatted location string (e.g., "FR/Île-de-France/Paris" or "FR" or "")
    - text_query: Search query text
    - created_at: First insertion timestamp
    - updated_at: Last update timestamp
    
    Primary key: (job_id, website) - enforces dedupe per job by website
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        job_id: The service-level run identifier
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS googlemaps_places_results (
                request_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                organization TEXT NOT NULL,
                website TEXT NOT NULL,
                description TEXT NOT NULL,
                location TEXT NOT NULL,
                text_query TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (job_id, website)
            )
        """)
    log_db(logger, service="GOOGLEMAPSV2", level="debug", job_id=job_id, table="googlemaps_places_results", state="READY")


def upsert_rows(conn: psycopg.Connection[Any], job_id: str, request_id: str, rows: list[dict[str, str]]) -> int:
    """
    Insert or update rows in googlemaps_places_results table.
    
    On conflict (job_id, website):
    - Update request_id, organization, description, location, text_query, updated_at
    - "Latest wins" logic - no scoring or ranking
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        job_id: The service-level run identifier (primary key)
        request_id: The request identifier (metadata)
        rows: List of dicts with keys: text_query, location, organization, website, description
    
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
                INSERT INTO googlemaps_places_results
                (job_id, website, request_id, organization, description, location, text_query, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (job_id, website)
                DO UPDATE SET
                    request_id = EXCLUDED.request_id,
                    organization = EXCLUDED.organization,
                    description = EXCLUDED.description,
                    location = EXCLUDED.location,
                    text_query = EXCLUDED.text_query,
                    updated_at = NOW()
            """
            
            # Build batch of values
            batch = [
                (
                    job_id,
                    row["website"],
                    request_id,
                    row["organization"],
                    row["description"],
                    row["location"],
                    row["text_query"],
                )
                for row in rows
            ]
            
            # Execute batch
            cur.executemany(sql, batch)
            
        log_db(logger, service="GOOGLEMAPSV2", level="debug", job_id=job_id, action="UPSERT", rows=len(rows))
        return len(rows)
    
    except Exception as e:
        log_db(logger, service="GOOGLEMAPSV2", level="error", job_id=job_id, action="UPSERT", err=str(e))
        raise