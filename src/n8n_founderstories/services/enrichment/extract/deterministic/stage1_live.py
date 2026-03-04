"""
Stage 1: Live Deterministic Email Extraction

This module implements LIVE email extraction that runs inside the crawl worker,
immediately after each page is fetched. This is Stage 1 of a two-stage process:

STAGE 1 (THIS MODULE - LIVE IN CRAWL WORKER):
- Extract emails from single page immediately after fetch
- Use ONLY parse_emails_from_text() - NO filtering, NO ranking
- Store unique emails per page (dedupe within page) as JSON array in ONE ROW PER PAGE
- Insert/update det_ext_results table with UPSERT
- Zero prioritization logic

STAGE 2 (EXISTING MODULES - ENRICHMENT SERVICE):
- Read from det_ext_results table
- Parse emails_json arrays
- Apply filtering, ranking, prioritization
- Deduplicate and select top emails
- Return final results

DATA MODEL:
- ONE row per (domain, url) with UNIQUE constraint
- emails_json: JSON array of unique emails found on that page (dedupe within page)
- Example: '["info@x.com","contact@x.com"]'
- Empty pages store as '[]'
- UPSERT behavior: re-crawling same URL updates the row

CRITICAL RULES FOR STAGE 1:
✅ DO: Extract raw emails using parse_emails_from_text()
✅ DO: Store unique emails per page (dedupe within page) in JSON array
✅ DO: Preserve extraction order
✅ DO: Use cleaned_html (fallback to markdown)
✅ DO: UPSERT into det_ext_results (idempotent)
✅ DO: Store empty array [] if no emails found

❌ DON'T: Call apply_standard_filters()
❌ DON'T: Call merge_email_sources()
❌ DON'T: Call select_top_emails()
❌ DON'T: Call prioritize_emails()
❌ DON'T: Use DeterministicExtractor class
❌ DON'T: Apply max_emails limit
❌ DON'T: Use page_type priority
❌ DON'T: Use domain matching
❌ DON'T: Filter system emails
❌ DON'T: Filter asset emails
❌ DON'T: Filter by quality score

Author: N8N FounderStories Team
Last Modified: 2026-02-18
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import psycopg

from ...models import PageArtifact
from .core.parser import parse_emails_from_text
from .utils.domain_utils import normalize_domain


# ============================================================================
# LOGGING SETUP
# ============================================================================

logger = logging.getLogger(__name__)


# ============================================================================
# TABLE MANAGEMENT
# ============================================================================

def ensure_table(conn: psycopg.Connection[Any]) -> None:
    """
    Ensure the det_ext_results table exists in the database.
    
    Creates the table if it doesn't exist. This is idempotent and safe
    to call on every run.
    
    Table schema (ONE ROW PER PAGE):
    - id: UUID primary key (auto-generated)
    - request_id: Request identifier (from crawl_results)
    - job_id: Job identifier (from crawl_results)
    - domain: Normalized company domain (e.g., "example.com")
    - url: Full URL of the crawled page
    - page_type: Page classification (impressum, contact, home, etc.) - can be NULL
    - emails_json: JSON string containing list of unique emails found on that page (dedupe within page)
                   Example: '["info@x.com","contact@x.com"]'
    - created_at: Timestamp when record was inserted
    
    UNIQUE constraint on (domain, url):
    - Each page URL is crawled only once
    - If page is re-crawled, row is updated (upsert behavior)
    - One row per page containing unique emails found on that page (order preserved)
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
    """
    with conn.cursor() as cur:
        # Wrap table creation in exception handling to handle concurrent creation
        cur.execute("""
            DO $$
            BEGIN
                CREATE TABLE IF NOT EXISTS det_ext_results (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    request_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    url TEXT NOT NULL,
                    page_type TEXT,
                    emails_json TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                    UNIQUE(request_id, domain, url)
                );
            EXCEPTION
                WHEN duplicate_object THEN NULL;
                WHEN unique_violation THEN NULL;
            END $$;
        """)
        
        # Migration: Fix UNIQUE constraint if it exists with old schema
        # This ensures existing databases get the correct constraint
        try:
            # Check if old constraint exists
            cur.execute("""
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_name = 'det_ext_results'
                AND constraint_type = 'UNIQUE'
                AND constraint_name = 'det_ext_results_domain_url_key'
            """)
            
            if cur.fetchone():
                logger.info("Migrating det_ext_results UNIQUE constraint from (domain, url) to (request_id, domain, url)")
                
                # Drop old constraint
                cur.execute("""
                    ALTER TABLE det_ext_results
                    DROP CONSTRAINT det_ext_results_domain_url_key
                """)
                
                # Add new constraint
                cur.execute("""
                    ALTER TABLE det_ext_results
                    ADD CONSTRAINT det_ext_results_request_id_domain_url_key
                    UNIQUE (request_id, domain, url)
                """)
                
                logger.info("Successfully migrated det_ext_results UNIQUE constraint")
        except Exception as e:
            # If migration fails, log but don't crash - table might already have correct constraint
            logger.warning(f"Could not migrate det_ext_results constraint (might already be correct): {e}")
        
        # Create indexes for efficient queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_det_ext_results_domain
            ON det_ext_results(domain)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_det_ext_results_url
            ON det_ext_results(url)
        """)
        
        # Add indexes for request_id and job_id
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_det_ext_results_request_id
            ON det_ext_results(request_id)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_det_ext_results_job_id
            ON det_ext_results(job_id)
        """)
        
        conn.commit()
    
    logger.info("det_ext_results table ensured with one-row-per-page schema")


# ============================================================================
# STAGE 1 EXTRACTION MODELS
# ============================================================================

class Stage1Result:
    """
    Result of Stage 1 extraction for a single page.
    
    Attributes:
        page_url: URL of the page processed
        emails_found: Number of emails extracted from page
        emails_inserted: Always 1 if successful (one row per page), 0 if failed
        success: Whether extraction succeeded
        error: Error message if failed
    """
    
    def __init__(
        self,
        page_url: str,
        emails_found: int = 0,
        emails_inserted: int = 0,
        success: bool = True,
        error: Optional[str] = None,
    ):
        self.page_url = page_url
        self.emails_found = emails_found
        self.emails_inserted = emails_inserted  # 1 if row inserted/updated, 0 if failed
        self.success = success
        self.error = error
    
    def __repr__(self) -> str:
        if self.success:
            return (
                f"Stage1Result(page_url={self.page_url}, "
                f"found={self.emails_found}, row_upserted={self.emails_inserted})"
            )
        else:
            return (
                f"Stage1Result(page_url={self.page_url}, "
                f"success=False, error={self.error})"
            )


# ============================================================================
# STAGE 1 EXTRACTION FUNCTIONS
# ============================================================================

def extract_emails_from_page(page: PageArtifact) -> list[str]:
    """
    Extract and filter emails from a single page.
    
    This function:
    1. Gets text content from page (cleaned_html or markdown)
    2. Calls parse_emails_from_text() to extract emails (with deduplication)
    3. Applies business filtering to remove invalid emails
    4. Returns unique, valid emails found on that page
    
    CRITICAL: This function does NOT:
    - Rank emails or apply prioritization
    - Apply domain preference filtering
    - Limit number of emails (no max_emails)
    
    Args:
        page: PageArtifact with content to extract from
        
    Returns:
        List of unique email addresses found on that page (order preserved)
        
    Examples:
        >>> page = PageArtifact(url="https://example.com", cleaned_html="Contact: info@example.com")
        >>> extract_emails_from_page(page)
        ['info@example.com']
    """
    # Step 1: Get text content from page
    # Prefer cleaned_html, fallback to markdown
    text = (getattr(page, "cleaned_html", "") or "").strip()
    
    if not text:
        # Fallback to markdown if cleaned_html is empty
        text = (getattr(page, "markdown", "") or "").strip()
    
    if not text:
        # No content to extract from
        logger.debug(f"No content found in page: {page.url}")
        return []
    
    # Step 2: Extract emails using ONLY parse_emails_from_text()
    # This is the ONLY extraction function we use in Stage 1
    # IMPORTANT: dedupe=True to store unique emails per page (dedupe within page)
    try:
        emails = parse_emails_from_text(text, dedupe=True)
        
        # Step 3: Apply business filtering to remove invalid emails
        # This prevents noise like "20kontakt@..." from reaching the database
        from .validators.filters import apply_standard_filters
        
        emails = apply_standard_filters(
            emails,
            company_domain=None  # Stage-1 should not enforce domain preference yet
        )
        
        logger.debug(f"Extracted {len(emails)} emails from {page.url}")
        return emails
    except Exception as e:
        logger.error(f"Error extracting emails from {page.url}: {e}")
        return []


def insert_stage1_results(
    conn: psycopg.Connection[Any],
    request_id: str,
    job_id: str,
    domain: str,
    page_url: str,
    page_type: Optional[str],
    emails: list[str],
) -> int:
    """
    Insert/update Stage 1 extraction results into det_ext_results table.
    
    This function stores unique emails found on that page as a JSON array in a single row per page.
    Uses UPSERT (INSERT ... ON CONFLICT) for idempotent behavior.
    
    Behavior:
    - If (domain, url) doesn't exist: INSERT new row
    - If (domain, url) exists: UPDATE emails_json and page_type
    - One row per page containing unique emails found on that page (order preserved)
    - Empty email list stores as "[]"
    
    Args:
        conn: Database connection
        request_id: Request identifier (from crawl_results)
        job_id: Job identifier (from crawl_results)
        domain: Normalized domain (e.g., "example.com")
        page_url: Full URL of the page
        page_type: Page type classification (can be None)
        emails: List of unique emails found on that page (order preserved)
        
    Returns:
        1 if row was inserted/updated
        
    Raises:
        Exception: On database error (after rollback)
        
    Examples:
        >>> insert_stage1_results(
        ...     conn, "req123", "job456", "example.com", "https://example.com/contact",
        ...     "contact", ["info@example.com", "sales@example.com"]
        ... )
        1  # One row inserted with emails_json='["info@example.com","sales@example.com"]'
    """
    import json
    
    # Normalize domain
    domain_norm = normalize_domain(domain)
    
    # Convert emails list to JSON string
    # Preserve order (emails already deduped within page)
    emails_json = json.dumps(emails)
    
    # UPSERT query: insert or update on conflict
    # Note: UNIQUE constraint is now (request_id, domain, url) so this will only update
    # if the same request_id tries to insert the same domain+url again (idempotent)
    query = """
        INSERT INTO det_ext_results (request_id, job_id, domain, url, page_type, emails_json)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (request_id, domain, url)
        DO UPDATE SET
            job_id = EXCLUDED.job_id,
            page_type = EXCLUDED.page_type,
            emails_json = EXCLUDED.emails_json,
            created_at = now()
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, (request_id, job_id, domain_norm, page_url, page_type, emails_json))
        conn.commit()
        
        logger.info(
            f"Upserted {len(emails)} emails for {domain_norm} "
            f"from {page_url} (page_type={page_type})"
        )
        return 1
    except Exception as e:
        logger.error(
            f"Error upserting Stage 1 results for {domain_norm}: {e}"
        )
        conn.rollback()
        raise


def run_stage1_for_page(
    domain: str,
    page: PageArtifact,
    conn: psycopg.Connection[Any],
    request_id: str = "",
    job_id: str = "",
) -> Stage1Result:
    """
    Run Stage 1 extraction for a single page and insert results.
    
    This is the main entry point for Stage 1 extraction.
    Call this function immediately after fetching a page in the crawl worker.
    
    Process:
    1. Extract emails from page (no filtering)
    2. Insert all emails into det_ext_results
    3. Return result summary
    
    Args:
        domain: Company domain being crawled
        page: PageArtifact with fetched content
        conn: Database connection
        request_id: Request identifier (from crawl_results)
        job_id: Job identifier (from crawl_results)
        
    Returns:
        Stage1Result with extraction summary
        
    Examples:
        >>> result = run_stage1_for_page(
        ...     domain="example.com",
        ...     page=page_artifact,
        ...     conn=db_conn,
        ...     request_id="req123",
        ...     job_id="job456"
        ... )
        >>> print(f"Found {result.emails_found} emails")
    """
    page_url = str(getattr(page, "final_url", None) or getattr(page, "url", ""))
    
    try:
        # Step 1: Extract emails from page (raw, no filtering)
        emails = extract_emails_from_page(page)
        
        # Step 2: Get page type from metadata (if available)
        page_type = None
        try:
            page_type = (page.meta.get("page_type") or "").strip().lower() or None
        except Exception:
            pass
        
        # Step 3: Insert/update row in database (even if no emails found)
        # Store empty array [] if no emails found
        inserted_count = insert_stage1_results(
            conn=conn,
            request_id=request_id,
            job_id=job_id,
            domain=domain,
            page_url=page_url,
            page_type=page_type,
            emails=emails,  # Can be empty list
        )
        
        if not emails:
            logger.debug(f"No emails found on {page_url}, stored empty array")
        
        return Stage1Result(
            page_url=page_url,
            emails_found=len(emails),
            emails_inserted=inserted_count,
            success=True,
        )
        
    except Exception as e:
        logger.error(f"Stage 1 extraction failed for {page_url}: {e}")
        return Stage1Result(
            page_url=page_url,
            emails_found=0,
            emails_inserted=0,
            success=False,
            error=str(e),
        )


# ============================================================================
# BATCH PROCESSING (OPTIONAL)
# ============================================================================

def run_stage1_for_pages(
    domain: str,
    pages: list[PageArtifact],
    conn: psycopg.Connection[Any],
) -> list[Stage1Result]:
    """
    Run Stage 1 extraction for multiple pages.
    
    This is useful for batch processing, but typically Stage 1 runs
    one page at a time in the crawl worker.
    
    Args:
        domain: Company domain being crawled
        pages: List of PageArtifact objects
        conn: Database connection
        
    Returns:
        List of Stage1Result objects
    """
    results = []
    
    for page in pages:
        result = run_stage1_for_page(domain, page, conn)
        results.append(result)
    
    return results


# ============================================================================
# QUERY FUNCTIONS (FOR STAGE 2)
# ============================================================================

def get_stage1_results_for_domain(
    conn: psycopg.Connection[Any],
    domain: str,
) -> list[dict]:
    """
    Retrieve all Stage 1 results for a domain.
    
    This is used by Stage 2 (enrichment) to get raw extraction results
    for aggregation, filtering, and ranking.
    
    Args:
        conn: Database connection
        domain: Domain to query
        
    Returns:
        List of dictionaries with extraction results
        
    Examples:
        >>> results = get_stage1_results_for_domain(conn, "example.com")
        >>> for result in results:
        ...     print(result["emails_json"], result["url"])
    """
    domain_norm = normalize_domain(domain)
    
    query = """
        SELECT
            id,
            domain,
            url,
            page_type,
            emails_json,
            created_at
        FROM det_ext_results
        WHERE domain = %s
        ORDER BY created_at DESC
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (domain_norm,))
        rows = cur.fetchall()
        
        # Get column names
        columns = [desc[0] for desc in cur.description]
        
        return [
            {
                "id": str(row[columns.index("id")]),
                "domain": row[columns.index("domain")],
                "url": row[columns.index("url")],
                "page_type": row[columns.index("page_type")],
                "emails_json": row[columns.index("emails_json")],
                "created_at": row[columns.index("created_at")],
            }
            for row in rows
        ]


def count_stage1_results_for_domain(
    conn: psycopg.Connection[Any],
    domain: str,
) -> int:
    """
    Count Stage 1 results for a domain.
    
    Args:
        conn: Database connection
        domain: Domain to query
        
    Returns:
        Number of extraction results
    """
    domain_norm = normalize_domain(domain)
    
    query = "SELECT COUNT(*) FROM det_ext_results WHERE domain = %s"
    
    with conn.cursor() as cur:
        cur.execute(query, (domain_norm,))
        count = cur.fetchone()[0]
    
    return count or 0


def delete_stage1_results_for_domain(
    conn: psycopg.Connection[Any],
    domain: str,
) -> int:
    """
    Delete all Stage 1 results for a domain.
    
    This is useful for cleanup or re-crawling.
    
    Args:
        conn: Database connection
        domain: Domain to delete results for
        
    Returns:
        Number of rows deleted
    """
    domain_norm = normalize_domain(domain)
    
    query = "DELETE FROM det_ext_results WHERE domain = %s"
    
    with conn.cursor() as cur:
        cur.execute(query, (domain_norm,))
        count = cur.rowcount
    
    conn.commit()
    
    logger.info(f"Deleted {count} Stage 1 results for {domain_norm}")
def copy_domain_results(
    conn: psycopg.Connection[Any],
    from_request_id: str,
    from_job_id: str,
    to_request_id: str,
    to_job_id: str,
    domain: str
) -> int:
    """
    Copy all deterministic extraction results for a domain from one request to another.
    
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
    domain_norm = normalize_domain(domain)
    
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO det_ext_results (
                request_id, job_id, domain, url, page_type, emails_json
            )
            SELECT
                %s, %s, domain, url, page_type, emails_json
            FROM det_ext_results
            WHERE request_id = %s
              AND job_id = %s
              AND domain = %s
            ON CONFLICT (request_id, domain, url) DO NOTHING
        """, (to_request_id, to_job_id, from_request_id, from_job_id, domain_norm))
        
        rows_copied = cur.rowcount
    
    logger.info(f"Copied {rows_copied} det_ext_results rows for domain {domain_norm} from request {from_request_id} to {to_request_id}")
    
    return rows_copied
    return count