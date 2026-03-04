"""
═══════════════════════════════════════════════════════════════════════════════
LLM EXTRACTION RESULTS REPOSITORY - Database Persistence Layer
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [INFRASTRUCTURE] - Database access layer for LLM extraction results

PURPOSE:
    Manages the llm_ext_results table which stores LLM-extracted company data.
    Follows the same pattern as det_ext_results for deterministic extraction.
    
    CRITICAL: One row per PAGE (not per domain) with UNIQUE(domain, url)

TABLE SCHEMA (llm_ext_results):
    ┌─────────────────┬──────────┬─────────────────────────────────────────┐
    │ Column          │ Type     │ Description                             │
    ├─────────────────┼──────────┼─────────────────────────────────────────┤
    │ id              │ UUID     │ Primary key (auto-generated)            │
    │ domain          │ TEXT     │ Normalized domain (lowercase)           │
    │ url             │ TEXT     │ Full page URL                           │
    │ page_type       │ TEXT     │ Page type (home/impressum/contact/etc)  │
    │ company_json    │ TEXT     │ JSON object with company_name           │
    │ description_json│ TEXT     │ JSON object with short_description      │
    │ emails_json     │ TEXT     │ JSON array of email objects             │
    │ contacts_json   │ TEXT     │ JSON array of contact objects           │
    │ created_at      │ TIMESTAMP│ When record was created                 │
    └─────────────────┴──────────┴─────────────────────────────────────────┘
    
    UNIQUE constraint: (domain, url) - ONE ROW PER PAGE
    Indexes:
        - (domain, url) - For page lookups
        - (domain) - For domain queries

DATA FORMAT:
    company_json: JSON object with company name
        {
            "value": "Example Corp",
            "evidence": {
                "quote": "Example Corp GmbH"
            }
        }
    
    description_json: JSON object with short description
        {
            "value": "We provide innovative solutions...",
            "evidence": {
                "quote": "We provide innovative solutions..."
            }
        }
    
    emails_json: JSON array of email objects
        [
            {
                "email": "info@example.com",
                "evidence": {
                    "quote": "Contact us at info@example.com"
                }
            }
        ]
    
    contacts_json: JSON array of contact objects
        [
            {
                "name": "John Doe",
                "role": "CEO",
                "evidence": {
                    "quote": "John Doe, CEO of Example Corp"
                }
            }
        ]

KEY FUNCTIONS:
    - ensure_table(): Create table and indexes (idempotent)
    - upsert_page_extraction(): Insert or update LLM extraction for a page
    - get_next_unprocessed_page(): Get next page to process (crawl order)

USAGE PATTERN:
    1. Worker calls get_next_unprocessed_page()
    2. Worker processes page with LLM
    3. Worker calls upsert_page_extraction() to store result
    4. Repeat

IDEMPOTENCY:
    - upsert_page_extraction() uses ON CONFLICT DO UPDATE
    - Safe to call multiple times with same (domain, url)
    - Last write wins for conflicts

PERFORMANCE:
    - Indexed on (domain, url) for fast lookups
    - LEFT JOIN with crawl_results for unprocessed pages
    - ORDER BY created_at ASC for strict crawl order

DEPENDENCIES:
    - psycopg: PostgreSQL database driver
    - Called by: LLMExtractionWorker

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)


# =============================================================================
# TABLE MANAGEMENT
# =============================================================================

def ensure_table(conn: psycopg.Connection[Any]) -> None:
    """
    Create the llm_ext_results table if it doesn't exist.
    
    This function is idempotent and safe to call on every run.
    
    Table schema (ONE ROW PER PAGE PER REQUEST):
    - id: UUID primary key (auto-generated)
    - request_id: Request identifier (from crawl_results)
    - job_id: Job identifier (from crawl_results)
    - domain: Normalized company domain (e.g., "example.com")
    - url: Full page URL
    - page_type: Page classification (impressum, contact, home, etc.)
    - company_json: JSON string with company_name object
    - description_json: JSON string with short_description object
    - emails_json: JSON string with list of email objects
    - contacts_json: JSON string with list of contact objects
    - status: Processing status ("pending", "succeeded", "failed")
    - error: Error message if status is "failed"
    - created_at: Timestamp when record was inserted/updated
    
    UNIQUE constraint on (request_id, domain, url):
    - Each page URL can exist once per request_id
    - Allows same domain+url across different requests (for reuse tracking)
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
    
    Example:
        with psycopg.connect(dsn) as conn:
            ensure_table(conn)
            conn.commit()
    """
    with conn.cursor() as cur:
        # Wrap table creation in exception handling to handle concurrent creation
        # Multiple workers may try to create the table simultaneously
        cur.execute("""
            DO $$
            BEGIN
                -- Try to create the table
                CREATE TABLE IF NOT EXISTS llm_ext_results (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    request_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    url TEXT NOT NULL,
                    page_type TEXT,
                    company_json TEXT,
                    description_json TEXT,
                    emails_json TEXT,
                    contacts_json TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    error TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                    UNIQUE(request_id, domain, url)
                );
            EXCEPTION
                WHEN duplicate_object THEN
                    -- Table already exists (created by another worker), ignore
                    NULL;
                WHEN unique_violation THEN
                    -- Type already exists (concurrent creation), ignore
                    NULL;
            END $$;
        """)
        
        # Migration: Drop old constraint if it exists and add new one
        cur.execute("""
            DO $$
            BEGIN
                -- Drop old constraint if it exists
                IF EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'llm_ext_results_domain_url_key'
                ) THEN
                    ALTER TABLE llm_ext_results DROP CONSTRAINT llm_ext_results_domain_url_key;
                END IF;
                
                -- Add new constraint if it doesn't exist
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'llm_ext_results_request_id_domain_url_key'
                ) THEN
                    ALTER TABLE llm_ext_results ADD CONSTRAINT llm_ext_results_request_id_domain_url_key
                    UNIQUE (request_id, domain, url);
                END IF;
            END $$;
        """)
        
        # Create indexes for efficient queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_ext_results_domain_url
            ON llm_ext_results(domain, url)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_ext_results_domain
            ON llm_ext_results(domain)
        """)
        
        # Add indexes for request_id and job_id
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_ext_results_request_id
            ON llm_ext_results(request_id)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_ext_results_job_id
            ON llm_ext_results(job_id)
        """)
        
        conn.commit()
    
    logger.info("llm_ext_results table ensured with one-row-per-page schema")


# =============================================================================
# DATA PERSISTENCE
# =============================================================================

def upsert_page_extraction(
    conn: psycopg.Connection[Any],
    domain: str,
    url: str,
    page_type: Optional[str],
    company_json: Optional[str],
    description_json: Optional[str],
    emails_json: Optional[str],
    contacts_json: Optional[str],
    status: str = "succeeded",
    error: Optional[str] = None,
    request_id: str = "",
    job_id: str = "",
) -> None:
    """
    Insert or update LLM extraction result for a page.
    
    This function uses UPSERT (INSERT ... ON CONFLICT DO UPDATE) to ensure
    idempotency. If the (domain, url) already exists, the row is updated.
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        domain: Normalized domain (e.g., "example.com")
        url: Full page URL
        page_type: Page type (home/impressum/contact/etc) or None
        company_json: JSON string with company object or None
        description_json: JSON string with description object or None
        emails_json: JSON string with emails array or None
        contacts_json: JSON string with contacts array or None
        status: Processing status ("pending", "succeeded", "failed")
        error: Error message if status is "failed"
        request_id: Request identifier (from crawl_results)
        job_id: Job identifier (from crawl_results)
    
    Example:
        with psycopg.connect(dsn) as conn:
            upsert_page_extraction(
                conn,
                domain="example.com",
                url="https://example.com/impressum",
                page_type="impressum",
                company_json='{"value": "Example Corp", "evidence": {...}}',
                description_json=None,
                emails_json='[{"email": "info@example.com", "evidence": {...}}]',
                contacts_json='[{"name": "John Doe", "role": "CEO", "evidence": {...}}]',
                request_id="req123",
                job_id="job456",
            )
            conn.commit()
    
    Notes:
        - Caller must commit the transaction
        - Safe to call multiple times with same (domain, url)
        - Last write wins for conflicts
        - NULL values are allowed for optional fields
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO llm_ext_results (
                request_id,
                job_id,
                domain,
                url,
                page_type,
                company_json,
                description_json,
                emails_json,
                contacts_json,
                status,
                error,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (request_id, domain, url)
            DO UPDATE SET
                request_id = EXCLUDED.request_id,
                job_id = EXCLUDED.job_id,
                page_type = EXCLUDED.page_type,
                company_json = EXCLUDED.company_json,
                description_json = EXCLUDED.description_json,
                emails_json = EXCLUDED.emails_json,
                contacts_json = EXCLUDED.contacts_json,
                status = EXCLUDED.status,
                error = EXCLUDED.error,
                created_at = now()
        """, (
            request_id,
            job_id,
            domain,
            url,
            page_type,
            company_json,
            description_json,
            emails_json,
            contacts_json,
            status,
            error,
        ))
    
    logger.debug(
        "LLM_EXT_RESULTS_UPSERTED domain=%s url=%s page_type=%s",
        domain,
        url,
        page_type,
    )
def copy_domain_results(
    conn: psycopg.Connection[Any],
    from_request_id: str,
    from_job_id: str,
    to_request_id: str,
    to_job_id: str,
    domain: str
) -> int:
    """
    Copy all LLM extraction results for a domain from one request to another.
    
    This enables reuse of previously extracted data without re-extraction.
    
    Args:
        conn: Active psycopg connection
        from_request_id: Source request identifier
        from_job_id: Source job identifier
        to_request_id: Target request identifier
        to_job_id: Target job identifier
        domain: Domain to copy
        
    Returns:
        Number of rows copied
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO llm_ext_results (
                request_id, job_id, domain, url, page_type,
                company_json, description_json, emails_json, contacts_json,
                status, error
            )
            SELECT
                %s, %s, domain, url, page_type,
                company_json, description_json, emails_json, contacts_json,
                status, error
            FROM llm_ext_results
            WHERE request_id = %s
              AND job_id = %s
              AND domain = %s
            ON CONFLICT (request_id, domain, url) DO NOTHING
        """, (to_request_id, to_job_id, from_request_id, from_job_id, domain))
        
        rows_copied = cur.rowcount
    
    logger.info(f"Copied {rows_copied} llm_ext_results rows for domain {domain} from request {from_request_id} to {to_request_id}")
    
    return rows_copied


# =============================================================================
# WORKER QUERIES
# =============================================================================

def get_next_unprocessed_page(conn: psycopg.Connection[Any]) -> Optional[Dict[str, Any]]:
    """
    Get the next unprocessed page from crawl_results in strict crawl order.
    
    This query:
    1. LEFT JOINs crawl_results with llm_ext_results
    2. Filters for pages not yet processed (WHERE l.url IS NULL)
    3. Orders by created_at ASC (strict crawl order)
    4. Returns LIMIT 1 (single page at a time)
    
    Args:
        conn: Active psycopg connection
    
    Returns:
        Dictionary with page data or None if no unprocessed pages:
        {
            "id": uuid,
            "domain": str,
            "url": str,
            "page_type": str,
            "markdown": str,
            "created_at": datetime,
        }
    
    Example:
        with psycopg.connect(dsn) as conn:
            page = get_next_unprocessed_page(conn)
            if page:
                # Process page with LLM
                result = process_page(page)
                # Store result
                upsert_page_extraction(conn, ...)
                conn.commit()
    
    Notes:
        - Returns None if all pages are processed
        - Strict ORDER BY created_at ASC ensures crawl order
        - LIMIT 1 ensures single-threaded processing
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                c.request_id,
                c.job_id,
                c.domain,
                c.url,
                c.page_type,
                c.markdown,
                c.created_at,
                m.sheet_id
            FROM crawl_results c
            LEFT JOIN llm_ext_results l
              ON c.request_id = l.request_id AND c.domain = l.domain AND c.url = l.url
            LEFT JOIN mstr_results m
              ON c.request_id = m.request_id AND c.domain = m.domain
            WHERE l.url IS NULL
              AND c.status = 'succeeded'
              AND c.markdown IS NOT NULL
            ORDER BY c.created_at ASC
            LIMIT 1
        """)
        
        return cur.fetchone()


def get_previous_extraction_success(
    conn: psycopg.Connection[Any],
    domain: str,
    current_request_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Check if extraction results already exist for this domain from a previous request.
    
    This enables reuse of extraction results across different requests, similar to crawl reuse.
    Works at the domain level - if ANY extraction succeeded for this domain in a previous request,
    we can reuse ALL extraction results for that domain.
    
    Args:
        conn: Active psycopg connection
        domain: Domain to check
        current_request_id: Current request ID (to exclude from search)
    
    Returns:
        Dictionary with request_id and job_id if found, None otherwise
        Contains: request_id, job_id
    
    Example:
        with psycopg.connect(dsn) as conn:
            prev = get_previous_extraction_success(conn, "example.com", "req123")
            if prev:
                # Copy previous extraction results
                copy_extraction_results(conn, "example.com", prev["request_id"], "req123")
    """
    with conn.cursor(row_factory=dict_row) as cur:
        # Check if this domain has extraction results in a previous request
        # Just check llm_ext_results - if pages exist with status='succeeded', we can reuse them
        cur.execute("""
            SELECT
                request_id,
                job_id,
                COUNT(*) as page_count
            FROM llm_ext_results
            WHERE domain = %s
              AND request_id != %s
              AND status = 'succeeded'
            GROUP BY request_id, job_id
            ORDER BY MIN(created_at) DESC
            LIMIT 1
        """, (domain, current_request_id))
        
        result = cur.fetchone()
        
        if result:
            logger.debug(
                "EXTRACT_REUSE | FOUND | domain=%s | source_request=%s | target_request=%s | pages=%d",
                domain,
                result["request_id"],
                current_request_id,
                result["page_count"],
            )
        
        return result


def copy_extraction_results(
    conn: psycopg.Connection[Any],
    domain: str,
    source_request_id: str,
    target_request_id: str,
) -> int:
    """
    Copy ALL extraction results for a domain from a previous request to the current request.
    
    This is used for extraction reuse - when a domain was already extracted by a previous
    request, we copy ALL page results for that domain instead of re-extracting.
    
    Args:
        conn: Active psycopg connection
        domain: Domain to copy results for
        source_request_id: Source request ID to copy from
        target_request_id: Target request ID to copy to
    
    Returns:
        Number of pages copied
    
    Example:
        with psycopg.connect(dsn) as conn:
            prev = get_previous_extraction_success(conn, "example.com", "req123")
            if prev:
                copied = copy_extraction_results(conn, "example.com", prev["request_id"], "req456")
                print(f"Copied {copied} pages")
                conn.commit()
    """
    with conn.cursor() as cur:
        # Copy all extraction results for this domain from source to target request
        cur.execute("""
            INSERT INTO llm_ext_results (
                request_id,
                job_id,
                domain,
                url,
                page_type,
                company_json,
                description_json,
                emails_json,
                contacts_json,
                status,
                error,
                created_at
            )
            SELECT
                %s as request_id,
                job_id,
                domain,
                url,
                page_type,
                company_json,
                description_json,
                emails_json,
                contacts_json,
                status,
                error,
                NOW() as created_at
            FROM llm_ext_results
            WHERE domain = %s
              AND request_id = %s
              AND status = 'succeeded'
            ON CONFLICT (request_id, domain, url) DO NOTHING
        """, (target_request_id, domain, source_request_id))
        
        return cur.rowcount


def get_next_unprocessed_pages_batch(
    conn: psycopg.Connection[Any],
    batch_size: int = 6,
) -> list[Dict[str, Any]]:
    """
    Get a batch of unprocessed pages for concurrent extraction.
    
    This function retrieves multiple pages at once to enable concurrent processing.
    Pages are selected in strict crawl order (ORDER BY created_at ASC).
    
    Args:
        conn: Active psycopg connection
        batch_size: Number of pages to fetch (default: 6, matches llm_max_concurrency)
    
    Returns:
        List of page dictionaries, each containing:
        - request_id: Request identifier
        - job_id: Job identifier
        - domain: Domain name
        - url: Page URL
        - page_type: Page type (impressum, home, contact, etc.)
        - markdown: Page content in markdown
        - created_at: Timestamp when page was crawled
        - sheet_id: Google Sheet ID from mstr_results
    
    Query Logic:
        - LEFT JOIN with llm_ext_results to find unprocessed pages
        - LEFT JOIN with mstr_results to get sheet_id
        - WHERE l.url IS NULL: Page not yet in llm_ext_results
        - WHERE c.status = 'succeeded': Only process successfully crawled pages
        - WHERE c.markdown IS NOT NULL: Skip pages without content
        - ORDER BY c.created_at ASC: Maintain crawl order
        - LIMIT batch_size: Fetch multiple pages for concurrent processing
    
    Example:
        with psycopg.connect(dsn) as conn:
            pages = get_next_unprocessed_pages_batch(conn, batch_size=6)
            # Process pages concurrently
            await asyncio.gather(*[process_page(p) for p in pages])
    
    Notes:
        - Returns empty list if no unprocessed pages
        - Maintains strict crawl order across batches
        - Batch size should match llm_max_concurrency for optimal throughput
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                c.request_id,
                c.job_id,
                c.domain,
                c.url,
                c.page_type,
                c.markdown,
                c.created_at,
                m.sheet_id
            FROM crawl_results c
            LEFT JOIN llm_ext_results l
              ON c.request_id = l.request_id AND c.domain = l.domain AND c.url = l.url
            LEFT JOIN mstr_results m
              ON c.request_id = m.request_id AND c.domain = m.domain
            WHERE l.url IS NULL
              AND c.status = 'succeeded'
              AND c.markdown IS NOT NULL
            ORDER BY c.created_at ASC
            LIMIT %s
        """, (batch_size,))
        
        return cur.fetchall()


def update_domain_extraction_status(
    conn: psycopg.Connection[Any],
    request_id: str,
    domain: str,
    status: Optional[str] = None,
) -> None:
    """
    Update extraction_status in mstr_results for a domain after all pages are processed.
    
    This function checks if all pages for a domain have been processed (either succeeded
    or failed) and updates the extraction_status in mstr_results accordingly.
    
    Status Logic:
        - "succeeded": All pages processed successfully (status = 'succeeded')
        - "failed": At least one page failed (status = 'failed')
        - "reused": Extraction results copied from previous request
        - NULL: Not all pages processed yet
    
    Args:
        conn: Active psycopg connection
        request_id: Request ID from mstr_results
        domain: Domain to check
        status: Optional status to force (e.g., "reused"). If provided, skips automatic detection.
    
    Example:
        with psycopg.connect(dsn) as conn:
            # After processing a page (auto-detect status)
            upsert_page_extraction(conn, domain, url, ...)
            update_domain_extraction_status(conn, request_id, domain)
            conn.commit()
            
            # After reusing extraction (force status)
            copy_extraction_results(conn, domain, source_req, target_req)
            update_domain_extraction_status(conn, request_id, domain, status="reused")
            conn.commit()
    
    Notes:
        - Only updates if ALL pages for domain are processed (unless status is forced)
        - Caller must commit the transaction
        - Safe to call after each page (will only update when complete)
    """
    with conn.cursor() as cur:
        # If status is forced (e.g., "reused"), use it directly
        if status:
            extraction_status = status
        else:
            # Check if all pages for this domain are processed
            # Use request_id to ensure we're checking the right pages
            # Include both succeeded and failed crawls
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE l.status IS NULL) as pending_count,
                    COUNT(*) FILTER (WHERE l.status = 'failed') as failed_count,
                    COUNT(*) FILTER (WHERE c.status = 'succeeded') as succeeded_crawl_count,
                    COUNT(*) as total_count
                FROM crawl_results c
                LEFT JOIN llm_ext_results l
                  ON c.domain = l.domain AND c.url = l.url AND c.request_id = l.request_id
                WHERE c.request_id = %s
                  AND c.domain = %s
            """, (request_id, domain))
            
            result = cur.fetchone()
            if not result or result[3] == 0:  # total_count == 0
                return
            
            pending_count, failed_count, succeeded_crawl_count, total_count = result
            
            # If there are still pending pages, don't update status
            if pending_count > 0:
                return
            
            # All pages processed - determine final status
            if failed_count > 0:
                extraction_status = "failed"
            else:
                extraction_status = "succeeded"
        
        # Update mstr_results extraction_status
        cur.execute("""
            UPDATE mstr_results
            SET extraction_status = %s
            WHERE request_id = %s
              AND domain = %s
        """, (extraction_status, request_id, domain))
        
        # If extraction failed, also mark enrichment as failed
        if extraction_status == "failed":
            cur.execute("""
                UPDATE mstr_results
                SET enrichment_status = 'failed'
                WHERE request_id = %s
                  AND domain = %s
            """, (request_id, domain))
        
        logger.info(
            "EXTRACTION_STATUS_UPDATED request_id=%s domain=%s extraction_status=%s enrichment_status=%s (total=%d, failed=%d)",
            request_id,
            domain,
            extraction_status,
            "failed" if extraction_status == "failed" else "NULL",
            total_count,
            failed_count,
        )
def mark_failed_crawls_as_failed_extraction(conn: psycopg.Connection[Any]) -> int:
    """
    Create failed extraction entries for pages where crawl failed.
    
    This function finds pages in crawl_results with status='failed' that don't
    have corresponding entries in llm_ext_results, and creates failed extraction
    entries with the same error message from the crawl.
    
    This ensures:
    1. Failed crawls are visible in llm_ext_results
    2. extraction_status in mstr_results is properly set to 'failed'
    3. Error messages are preserved from crawl to extraction
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
    
    Returns:
        Number of failed extraction entries created
    
    Example:
        with psycopg.connect(dsn) as conn:
            count = mark_failed_crawls_as_failed_extraction(conn)
            conn.commit()
            logger.info(f"Created {count} failed extraction entries")
    
    Notes:
        - Should be called periodically or at worker startup
        - Idempotent: won't create duplicates due to UNIQUE constraint
        - Preserves error messages from crawl_results
        - Automatically updates extraction_status in mstr_results for affected domains
    """
    with conn.cursor() as cur:
        # First, get the list of affected (request_id, domain) pairs before inserting
        cur.execute("""
            SELECT DISTINCT c.request_id, c.domain
            FROM crawl_results c
            LEFT JOIN llm_ext_results l
              ON c.request_id = l.request_id
              AND c.domain = l.domain
              AND c.url = l.url
            WHERE c.status = 'failed'
              AND l.url IS NULL
        """)
        
        affected_domains = cur.fetchall()
        
        # Insert failed extraction entries for failed crawls
        cur.execute("""
            INSERT INTO llm_ext_results (
                request_id,
                job_id,
                domain,
                url,
                page_type,
                status,
                error,
                created_at
            )
            SELECT
                c.request_id,
                c.job_id,
                c.domain,
                c.url,
                c.page_type,
                'failed',
                c.error,
                c.created_at
            FROM crawl_results c
            LEFT JOIN llm_ext_results l
              ON c.request_id = l.request_id
              AND c.domain = l.domain
              AND c.url = l.url
            WHERE c.status = 'failed'
              AND l.url IS NULL
            ON CONFLICT (request_id, domain, url) DO NOTHING
        """)
        
        inserted_count = cur.rowcount
        
        if inserted_count > 0:
            logger.info(
                "MARK_FAILED_CRAWLS | inserted_count=%d",
                inserted_count
            )
            
            # Update extraction_status in mstr_results for each affected domain
            for request_id, domain in affected_domains:
                update_domain_extraction_status(conn, request_id, domain)
        
        return inserted_count


def get_extraction_progress(
    conn: psycopg.Connection[Any],
    request_id: str,
) -> Dict[str, int]:
    """
    Get extraction progress statistics for a request.
    
    Returns counts of domains by extraction status for progress tracking.
    
    Args:
        conn: Active psycopg connection
        request_id: Request ID to get progress for
    
    Returns:
        Dictionary with progress counts:
        {
            "total": int,        # Total domains in request
            "completed": int,    # Domains with extraction_status set
            "succeeded": int,    # Domains with extraction_status = 'succeeded'
            "failed": int,       # Domains with extraction_status = 'failed'
            "pending": int,      # Domains with extraction_status IS NULL
        }
    
    Example:
        with psycopg.connect(dsn) as conn:
            progress = get_extraction_progress(conn, request_id)
            print(f"Extraction: {progress['completed']}/{progress['total']}")
    
    Notes:
        - Used for progress reporting in tool status
        - Matches crawl progress reporting pattern
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE extraction_status IS NOT NULL) as completed,
                COUNT(*) FILTER (WHERE extraction_status = 'succeeded') as succeeded,
                COUNT(*) FILTER (WHERE extraction_status = 'failed') as failed,
                COUNT(*) FILTER (WHERE extraction_status IS NULL) as pending
            FROM mstr_results
            WHERE request_id = %s
        """, (request_id,))
        
        result = cur.fetchone()
        if not result:
            return {
                "total": 0,
                "completed": 0,
                "succeeded": 0,
                "failed": 0,
                "pending": 0,
            }
        
        total, completed, succeeded, failed, pending = result
        
        return {
            "total": total or 0,
            "completed": completed or 0,
            "succeeded": succeeded or 0,
            "failed": failed or 0,
            "pending": pending or 0,
        }
        return cur.fetchone()