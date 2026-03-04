"""
═══════════════════════════════════════════════════════════════════════════════
CRAWL RESULTS REPOSITORY - Database Persistence Layer
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [INFRASTRUCTURE] - Database access layer for crawl results

PURPOSE:
    Manages the crawl_results table which stores all crawled page content.
    Enables global reuse of crawl results across different requests.

TABLE SCHEMA (crawl_results):
    ┌─────────────────┬──────────┬─────────────────────────────────────────┐
    │ Column          │ Type     │ Description                             │
    ├─────────────────┼──────────┼─────────────────────────────────────────┤
    │ request_id      │ TEXT     │ Request identifier                      │
    │ job_id          │ TEXT     │ Job identifier                          │
    │ domain          │ TEXT     │ Normalized domain (lowercase)           │
    │ url             │ TEXT     │ Full page URL                           │
    │ page_type       │ TEXT     │ home/contact/impressum/privacy/about    │
    │ contact_case    │ TEXT     │ Contact discovery case (1, 2, 3, etc.)  │
    │ cleaned_html    │ TEXT     │ Sanitized HTML content                  │
    │ markdown        │ TEXT     │ Markdown content                        │
    │ status          │ TEXT     │ succeeded/failed                        │
    │ error           │ TEXT     │ Error message if failed                 │
    └─────────────────┴──────────┴─────────────────────────────────────────┘
    
    Primary Key: (request_id, domain, url)
    Indexes:
        - (domain, status) - For global reuse check
        - (request_id) - For request-scoped queries

GLOBAL REUSE MECHANISM:
    1. has_global_success(domain) - Check if domain was crawled successfully before
    2. get_previous_success(domain) - Get request_id/job_id of previous success
    3. copy_domain_results(from_request, to_request, domain) - Copy results
    
    This enables efficient reuse of crawl results across different requests,
    avoiding redundant crawling of the same domains.

KEY FUNCTIONS:
    - ensure_table(): Create table and indexes (idempotent)
    - upsert_page(): Insert or update a single crawled page
    - has_global_success(): Check if domain was crawled successfully (any request)
    - get_previous_success(): Get source request_id for reuse
    - copy_domain_results(): Copy all pages from one request to another

USAGE PATTERN:
    1. runner.py checks has_global_success(domain)
    2. If True: copy_domain_results() to reuse previous crawl
    3. If False: service.py crawls domain, runner.py calls upsert_page()

IDEMPOTENCY:
    - upsert_page() uses ON CONFLICT DO UPDATE
    - Safe to call multiple times with same data
    - Last write wins for conflicts

PERFORMANCE:
    - Indexed on (domain, status) for fast global lookups
    - Indexed on (request_id) for fast request-scoped queries
    - Bulk copy via INSERT...SELECT for efficient reuse

DEPENDENCIES:
    - psycopg: PostgreSQL database driver
    - Called by: runner.py

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations
import logging
from typing import Any

import psycopg

logger = logging.getLogger(__name__)


def ensure_table(conn: psycopg.Connection[Any]) -> None:
    """
    Create the crawl_results table if it doesn't exist.
    
    Simplified schema (one row per crawled page):
    - request_id: TEXT NOT NULL (request identifier)
    - job_id: TEXT NOT NULL (job identifier)
    - domain: TEXT NOT NULL (normalized domain)
    - url: TEXT NOT NULL (page URL)
    - page_type: TEXT NOT NULL (home/contact/impressum/privacy/about/other)
    - contact_case: TEXT NULL (contact discovery case: 1, 2, 3, 4, 5.1, 5.2, 5.3)
    - cleaned_html: TEXT NULL (sanitized HTML content)
    - markdown: TEXT NULL (markdown content)
    - status: TEXT NOT NULL (succeeded/failed)
    - error: TEXT NULL (error message if failed)
    - created_at: TIMESTAMP WITH TIME ZONE (timestamp when record was created)
    
    Primary key: (request_id, domain, url) - request-scoped uniqueness
    Indexes:
    - (domain, status) for global reuse check
    - (request_id) for request fetch
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
    """
    with conn.cursor() as cur:
        # Wrap table creation in exception handling to handle concurrent creation
        cur.execute("""
            DO $$
            BEGIN
                CREATE TABLE IF NOT EXISTS crawl_results (
                    request_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    url TEXT NOT NULL,
                    page_type TEXT NOT NULL,
                    contact_case TEXT,
                    cleaned_html TEXT,
                    markdown TEXT,
                    status TEXT NOT NULL,
                    error TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                    PRIMARY KEY (request_id, domain, url)
                );
            EXCEPTION
                WHEN duplicate_object THEN NULL;
                WHEN unique_violation THEN NULL;
            END $$;
        """)
        
        # Create indexes
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_crawl_results_domain_status
            ON crawl_results (domain, status)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_crawl_results_request_id
            ON crawl_results (request_id)
        """)
    
    logger.debug("CRAWL | table=crawl_results | state=READY")


def upsert_page(
    conn: psycopg.Connection[Any],
    request_id: str,
    job_id: str,
    domain: str,
    url: str,
    page_type: str,
    contact_case: str | None,
    cleaned_html: str | None,
    markdown: str | None,
    status: str,
    error: str | None
) -> None:
    """
    Upsert a single crawled page into crawl_results table.
    
    Rules:
    - Primary key is (request_id, domain, url)
    - On conflict, update all fields
    
    Args:
        conn: Active psycopg connection
        request_id: Request identifier
        job_id: Job identifier
        domain: Normalized domain
        url: Page URL
        page_type: Page type (home/contact/impressum/privacy/about/other)
        contact_case: Contact discovery case (1, 2, 3, 4, 5.1, 5.2, 5.3) (optional)
        cleaned_html: Sanitized HTML content (optional)
        markdown: Markdown content (optional)
        status: Status (succeeded/failed)
        error: Error message if failed (optional)
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO crawl_results (
                request_id, job_id, domain, url, page_type, contact_case,
                cleaned_html, markdown, status, error
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (request_id, domain, url)
            DO UPDATE SET
                job_id = EXCLUDED.job_id,
                page_type = EXCLUDED.page_type,
                contact_case = EXCLUDED.contact_case,
                cleaned_html = EXCLUDED.cleaned_html,
                markdown = EXCLUDED.markdown,
                status = EXCLUDED.status,
                error = EXCLUDED.error
        """, (
            request_id,
            job_id,
            domain,
            url,
            page_type,
            contact_case,
            cleaned_html,
            markdown,
            status,
            error
        ))


def has_global_success(conn: psycopg.Connection[Any], domain: str) -> bool:
    """
    Check if a domain has been successfully crawled before (globally).
    
    This checks across ALL request_ids to enable reuse.
    
    Args:
        conn: Active psycopg connection
        domain: Domain to check
        
    Returns:
        True if domain was crawled successfully before, False otherwise
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS(
                SELECT 1
                FROM crawl_results
                WHERE domain = %s
                  AND status = 'succeeded'
                LIMIT 1
            )
        """, (domain,))
        
        row = cur.fetchone()
        return bool(row[0]) if row else False


def get_previous_success(
    conn: psycopg.Connection[Any],
    domain: str
) -> tuple[str, str] | None:
    """
    Get the request_id and job_id of a previous successful crawl for a domain.
    
    UPDATED: Only checks crawl_status, not extraction_status.
    This allows reuse as soon as crawling is complete, without waiting for extraction.
    
    Args:
        conn: Active psycopg connection
        domain: Domain to check
        
    Returns:
        Tuple of (request_id, job_id) or None if no previous success
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.request_id, c.job_id
            FROM crawl_results c
            INNER JOIN mstr_results m
              ON c.request_id = m.request_id AND c.domain = m.domain
            WHERE c.domain = %s
              AND c.status = 'succeeded'
              AND m.crawl_status = 'succeeded'
            LIMIT 1
        """, (domain,))
        
        row = cur.fetchone()
        return (row[0], row[1]) if row else None


def check_domain_being_processed(
    conn: psycopg.Connection[Any],
    domain: str,
    current_request_id: str
) -> bool:
    """
    Check if a domain is currently being processed by ANY request (globally).
    
    This prevents concurrent processing of the same domain across different requests.
    
    Args:
        conn: Active psycopg connection
        domain: Domain to check
        current_request_id: The current request_id (to exclude from check)
        
    Returns:
        True if domain is being processed by another request, False otherwise
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS(
                SELECT 1
                FROM mstr_results
                WHERE domain = %s
                  AND request_id != %s
                  AND crawl_status = 'processing'
            )
        """, (domain, current_request_id))
        
        row = cur.fetchone()
        return bool(row[0]) if row else False


def copy_domain_results(
    conn: psycopg.Connection[Any],
    from_request_id: str,
    from_job_id: str,
    to_request_id: str,
    to_job_id: str,
    domain: str
) -> int:
    """
    Copy all successful crawl results for a domain from one request to another.
    
    This enables reuse of previously crawled data without re-crawling.
    
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
            INSERT INTO crawl_results (
                request_id, job_id, domain, url, page_type, contact_case,
                cleaned_html, markdown, status, error
            )
            SELECT
                %s, %s, domain, url, page_type, contact_case,
                cleaned_html, markdown, status, error
            FROM crawl_results
            WHERE request_id = %s
              AND job_id = %s
              AND domain = %s
              AND status = 'succeeded'
            ON CONFLICT (request_id, domain, url) DO NOTHING
        """, (to_request_id, to_job_id, from_request_id, from_job_id, domain))
        
        return cur.rowcount