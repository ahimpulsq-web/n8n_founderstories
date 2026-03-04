"""
PostgreSQL repository for MasterV2 results.

Handles upsert logic with domain deduplication and source tracking.
Stage 1: Clean status table with 5 columns only.
"""

from __future__ import annotations
import logging
from typing import Any

import psycopg

from .models import LeadCandidate

logger = logging.getLogger(__name__)


def ensure_table(conn: psycopg.Connection[Any]) -> None:
    """
    Create the mstr_results table if it doesn't exist.
    
    Stage 1 Schema (7 columns):
    - request_id: TEXT NOT NULL (request identifier from source, primary key component)
    - job_id: TEXT NOT NULL (job identifier from source, updated per merge rules)
    - sheet_id: TEXT NULL (Google Sheet ID where results should be written)
    - organization: TEXT NULL (best-effort, hunter preferred)
    - source: TEXT NOT NULL (source name: hunter, google_maps, or "hunter, google_maps" for common)
    - domain: TEXT NOT NULL (dedupe key, normalized, lowercase, primary key component)
    - crawl_status: TEXT NULL (crawl status: NULL=not started, "running", "succeeded", "failed")
    
    Primary key: (request_id, domain) - enforces dedupe per request by domain
    
    Column order: request_id, job_id, sheet_id, organization, source, domain, crawl_status
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
    """
    with conn.cursor() as cur:
        # Wrap table creation in exception handling to handle concurrent creation
        cur.execute("""
            DO $$
            BEGIN
                CREATE TABLE IF NOT EXISTS mstr_results (
                    request_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    sheet_id TEXT,
                    organization TEXT,
                    source TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    crawl_status TEXT,
                    PRIMARY KEY (request_id, domain)
                );
            EXCEPTION
                WHEN duplicate_object THEN NULL;
                WHEN unique_violation THEN NULL;
            END $$;
        """)
        
        # Add sheet_id column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'mstr_results'
                    AND column_name = 'sheet_id'
                ) THEN
                    ALTER TABLE mstr_results ADD COLUMN sheet_id TEXT;
                END IF;
            END $$;
        """)
        
        # Add crawl_status column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'mstr_results'
                    AND column_name = 'crawl_status'
                ) THEN
                    ALTER TABLE mstr_results ADD COLUMN crawl_status TEXT;
                END IF;
            END $$;
        """)
        
        # Add extraction_status column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'mstr_results'
                    AND column_name = 'extraction_status'
                ) THEN
                    ALTER TABLE mstr_results ADD COLUMN extraction_status TEXT;
                END IF;
            END $$;
        """)
        
        # Add enrichment_status column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'mstr_results'
                    AND column_name = 'enrichment_status'
                ) THEN
                    ALTER TABLE mstr_results ADD COLUMN enrichment_status TEXT;
                END IF;
            END $$;
        """)
        
        # Add mail_write_status column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'mstr_results'
                    AND column_name = 'mail_write_status'
                ) THEN
                    ALTER TABLE mstr_results ADD COLUMN mail_write_status TEXT;
                END IF;
            END $$;
        """)
        
        # Add mail_send_status column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'mstr_results'
                    AND column_name = 'mail_send_status'
                ) THEN
                    ALTER TABLE mstr_results ADD COLUMN mail_send_status TEXT;
                END IF;
            END $$;
        """)
    
    logger.debug("MASTER | table=mstr_results | state=READY")


def merge_and_upsert_request_candidates(
    conn: psycopg.Connection[Any],
    source: str,
    request_id: str,
    job_id: str,
    candidates: list[LeadCandidate]
) -> int:
    """
    Merge new candidates with existing candidates for a request_id and upsert with merge rules.
    
    This implements request-based merge with cross-source deduplication and Hunter ownership priority.
    
    Merge Logic:
    1. Load existing rows for this request_id
    2. Merge with new candidates (union of domains)
    3. Apply cross-source deduplication rules
    4. Apply job_id ownership rules (Hunter always owns)
    5. Delete all existing rows for request_id
    6. Insert merged rows
    
    Rules:
    - Request-based merge: domains = UNION(old + new) per request_id
    - Cross-source dedupe:
      * Only Google Maps → source = "google_maps"
      * Only Hunter → source = "hunter"
      * Both → source = "hunter, google_maps"
    - Job ownership priority (CRITICAL):
      * Hunter ALWAYS owns job_id for common domains
      * Google Maps can only update job_id if domain is Google Maps-only
      * If domain exists in both sources, job_id = latest Hunter job_id
    - Organization rules:
      * Hunter overwrites always if present
      * Google Maps fills only if empty
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        source: Source name ("hunter" or "google_maps")
        request_id: Request identifier (primary key component)
        job_id: Job identifier from source
        candidates: List of LeadCandidate instances
        
    Returns:
        Final row count for this request_id after merge
        
    Raises:
        psycopg.Error: If database operation fails
    """
    # Step 0: Empty guard - if no candidates, skip merge but don't delete
    if not candidates:
        logger.debug(
            "MASTER | action=MERGE_SKIP | source=%s | request_id=%s | reason=no_new_candidates",
            source,
            request_id
        )
        return 0
    
    try:
        with conn.cursor() as cur:
            # Step 1: Load existing rows for this request_id (including sheet_id and crawl_status)
            cur.execute("""
                SELECT domain, organization, source, job_id, sheet_id, crawl_status
                FROM mstr_results
                WHERE request_id = %s
            """, (request_id,))
            
            existing_rows = cur.fetchall()
            
            # Build dict of existing domains -> row data
            # Note: SELECT order doesn't need to match table column order
            existing_map: dict[str, dict[str, str]] = {}
            for domain, organization, existing_source, existing_job_id, sheet_id, crawl_status in existing_rows:
                existing_map[domain] = {
                    "domain": domain,
                    "organization": organization or "",
                    "source": existing_source or "",
                    "job_id": existing_job_id or "",
                    "sheet_id": sheet_id or "",
                    "crawl_status": crawl_status or ""
                }
            
            logger.debug(
                "MASTER | action=MERGE_LOAD | source=%s | request_id=%s | existing_domains=%d",
                source,
                request_id,
                len(existing_map)
            )
            
            # Step 2: Merge domains (union)
            merged_map: dict[str, dict[str, str]] = dict(existing_map)
            
            for candidate in candidates:
                domain = candidate.domain
                
                if domain in merged_map:
                    # Domain exists - apply merge rules
                    existing = merged_map[domain]
                    existing_source = existing["source"]
                    
                    # Step 3: Cross-source deduplication
                    # Determine new source value
                    if source == "hunter":
                        if "hunter" not in existing_source:
                            # Add hunter to source
                            if existing_source == "google_maps":
                                new_source = "hunter, google_maps"
                            else:
                                new_source = "hunter"
                        else:
                            # Hunter already in source
                            new_source = existing_source
                    else:  # source == "google_maps"
                        if "google_maps" not in existing_source:
                            # Add google_maps to source
                            if existing_source == "hunter":
                                new_source = "hunter, google_maps"
                            else:
                                new_source = "google_maps"
                        else:
                            # Google Maps already in source
                            new_source = existing_source
                    
                    # Step 4: Job ownership priority
                    # Hunter ALWAYS owns job_id for common domains
                    if source == "hunter":
                        # Hunter always updates job_id
                        new_job_id = job_id
                    else:  # source == "google_maps"
                        # Google Maps can only update if domain is NOT hunter-owned
                        if "hunter" in new_source:
                            # Domain is common or hunter-only - keep existing job_id
                            new_job_id = existing["job_id"]
                        else:
                            # Domain is google_maps-only - can update
                            new_job_id = job_id
                    
                    # Organization: Hunter preferred
                    if candidate.organization:
                        if source == "hunter":
                            # Hunter always overwrites
                            new_org = candidate.organization
                        else:
                            # Google Maps only fills if empty
                            new_org = existing["organization"] or candidate.organization
                    else:
                        new_org = existing["organization"]
                    
                    # CRITICAL: Preserve sheet_id and crawl_status when merging
                    # sheet_id is preserved from existing, or updated from candidate if new
                    new_sheet_id = candidate.sheet_id or existing.get("sheet_id", "")
                    
                    merged_map[domain] = {
                        "domain": domain,
                        "organization": new_org,
                        "source": new_source,
                        "job_id": new_job_id,
                        "sheet_id": new_sheet_id,
                        "crawl_status": existing.get("crawl_status", "")
                    }
                else:
                    # New domain - leave crawl_status as NULL
                    # The crawler runner will set it to 'reused' if domain was crawled before
                    # or 'succeeded'/'failed' after actual crawling
                    crawl_status = ""
                    
                    merged_map[domain] = {
                        "domain": domain,
                        "organization": candidate.organization or "",
                        "source": source,
                        "job_id": job_id,
                        "sheet_id": candidate.sheet_id or "",
                        "crawl_status": crawl_status
                    }
            
            logger.debug(
                "MASTER | action=MERGE_COMBINE | source=%s | request_id=%s | new_candidates=%d | merged_domains=%d",
                source,
                request_id,
                len(candidates),
                len(merged_map)
            )
            
            # Step 5: Delete all existing rows for this request_id
            cur.execute(
                "DELETE FROM mstr_results WHERE request_id = %s",
                (request_id,)
            )
            deleted_count = cur.rowcount
            
            logger.debug(
                "MASTER | action=MERGE_DELETE | source=%s | request_id=%s | deleted=%d",
                source,
                request_id,
                deleted_count
            )
            
            # Step 6: Check for previous successful extractions (reuse logic)
            domains_to_check = [row_data["domain"] for row_data in merged_map.values()]
            
            # Query for domains with previous successful extractions
            placeholders = ','.join(['%s'] * len(domains_to_check))
            cur.execute(f"""
                SELECT DISTINCT domain
                FROM mstr_results
                WHERE domain IN ({placeholders})
                  AND extraction_status = 'succeeded'
            """, domains_to_check)
            
            previously_succeeded_domains = {row[0] for row in cur.fetchall()}
            
            # Step 7: Insert merged rows (preserving sheet_id, crawl_status, and setting extraction_status)
            sql = """
                INSERT INTO mstr_results
                (request_id, job_id, sheet_id, organization, source, domain, crawl_status, extraction_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            batch = [
                (
                    request_id,
                    row_data["job_id"],
                    row_data["sheet_id"] or None,
                    row_data["organization"] or None,
                    row_data["source"],
                    row_data["domain"],
                    row_data["crawl_status"] or None,
                    # CRITICAL: Only set extraction_status to 'succeeded' if:
                    # 1. Domain was successfully extracted before (in previously_succeeded_domains)
                    # 2. AND domain has been crawled in current request (crawl_status is not empty)
                    # This ensures proper workflow order: crawl → extract → enrich
                    'succeeded' if (
                        row_data["domain"] in previously_succeeded_domains
                        and row_data["crawl_status"] in ('succeeded', 'reused')
                    ) else None
                )
                for row_data in merged_map.values()
            ]
            
            cur.executemany(sql, batch)
            inserted_count = len(batch)
            reused_count = sum(1 for row in batch if row[7] == 'succeeded')
            
            logger.debug(
                "MASTER | action=MERGE_INSERT | source=%s | request_id=%s | job_id=%s | inserted=%d | reused_extractions=%d",
                source,
                request_id,
                job_id,
                inserted_count,
                reused_count
            )
        
        logger.info(
            "MASTER | action=MERGE_COMPLETE | source=%s | request_id=%s | job_id=%s | final_rows=%d",
            source,
            request_id,
            job_id,
            inserted_count
        )
        return inserted_count
    
    except Exception as e:
        logger.error(
            "MASTER | action=MERGE_ERROR | source=%s | request_id=%s | job_id=%s | error=%s",
            source,
            request_id,
            job_id,
            str(e)
        )
        raise


def update_enrichment_status(
    conn: psycopg.Connection[Any],
    request_id: str,
    domain: str,
    status: str
) -> None:
    """
    Update enrichment_status for a specific domain.
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        request_id: Request identifier
        domain: Domain to update
        status: New status ('succeeded', 'failed')
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE mstr_results
            SET enrichment_status = %s
            WHERE request_id = %s AND domain = %s
            """,
            (status, request_id, domain)
        )
    
    logger.debug(
        "MASTER | action=UPDATE_ENRICHMENT_STATUS | request_id=%s | domain=%s | status=%s",
        request_id,
        domain,
        status
    )


def mark_enrichment_failed_for_failed_extraction(conn: psycopg.Connection[Any]) -> int:
    """
    Mark enrichment_status as 'failed' for all domains where extraction_status is 'failed'.
    
    This ensures we don't try to aggregate domains that have no extraction data.
    Should be called periodically or at startup.
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
    
    Returns:
        Number of rows updated
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE mstr_results
            SET enrichment_status = 'failed'
            WHERE extraction_status = 'failed'
              AND (enrichment_status IS NULL OR enrichment_status != 'failed')
        """)
        updated_count = cur.rowcount
    
    if updated_count > 0:
        logger.info(
            "MASTER | action=MARK_ENRICHMENT_FAILED | updated_count=%d",
            updated_count
        )
    
    return updated_count


def update_mail_write_status(
    conn: psycopg.Connection[Any],
    request_id: str,
    domain: str,
    status: str
) -> None:
    """
    Update mail_write_status for a specific domain.
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        request_id: Request identifier
        domain: Domain to update
        status: New status ('succeeded', 'failed')
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE mstr_results
            SET mail_write_status = %s
            WHERE request_id = %s AND domain = %s
            """,
            (status, request_id, domain)
        )
    
    logger.debug(
        "MASTER | action=UPDATE_MAIL_WRITE_STATUS | request_id=%s | domain=%s | status=%s",
        request_id,
        domain,
        status
    )


def update_mail_send_status(
    conn: psycopg.Connection[Any],
    request_id: str,
    domain: str,
    status: str
) -> None:
    """
    Update mail_send_status for a specific domain.
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        request_id: Request identifier
        domain: Domain to update
        status: New status ('contacted', 'not_started')
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE mstr_results
            SET mail_send_status = %s
            WHERE request_id = %s AND domain = %s
            """,
            (status, request_id, domain)
        )
    
    logger.debug(
        "MASTER | action=UPDATE_MAIL_SEND_STATUS | request_id=%s | domain=%s | status=%s",
        request_id,
        domain,
        status
    )