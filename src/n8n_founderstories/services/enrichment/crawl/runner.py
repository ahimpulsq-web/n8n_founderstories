"""
═══════════════════════════════════════════════════════════════════════════════
CRAWLER RUNNER - Request-Scoped Domain Crawling
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [CORE] - Core crawling logic for processing domains

PURPOSE:
    Crawls all domains for a specific request_id where crawl_status IS NULL.
    Implements global reuse of previously crawled domains to avoid duplicate work.

ARCHITECTURE:
    Called by: worker.py (for each pending request_id)
    Uses: service.py (domain crawler), repo.py (persistence)
    Updates: master_results.crawl_status, crawl_results table

ALGORITHM:
    1. Load domains from master_results WHERE request_id = X AND crawl_status IS NULL
    2. For each domain:
       a. Check if domain was successfully crawled before (globally, any request_id)
       b. If YES: Copy previous results to current request_id (REUSE)
       c. If NO: Actually crawl the domain (CRAWL)
       d. Update master_results.crawl_status = 'succeeded' or 'failed'
    3. Commit after each domain (progress is saved incrementally)

GLOBAL REUSE:
    - Checks crawl_results table across ALL request_ids
    - If domain was crawled successfully before, reuses those results
    - Significantly reduces crawling time for repeated domains
    - Only successful crawls are reused (failed domains are retried)

IDEMPOTENCY:
    - Safe to call multiple times for same request_id
    - Only processes domains with crawl_status IS NULL
    - Skips already-processed domains automatically

LOGGING:
    Format: CRAWL | <remaining_count> | <domain> | <request_id> | <job_id> | <sheet_id> | <status>
    Example: CRAWL | 1115 | example.com | req_123 | job_456 | 1A2B3C4D5E6F | SUCCESS
    
    Status values:
    - SUCCESS: Domain crawled successfully
    - REUSED: Domain results copied from previous crawl
    - FAILED: <error>: Crawl failed with error message
    - ERROR: <error>: Exception during processing

CONFIGURATION (Environment Variables):
    - CRAWL4AI_TIMEOUT_S: Page load timeout (default: 30.0)
    - CRAWL4AI_HEADLESS: Run browser headless (default: true)
    - CRAWL4AI_WAIT_AFTER_LOAD_S: Wait after page load (default: 0.1)
    - CRAWL_MAX_CONCURRENCY: Max concurrent browser tabs per domain (default: 3)
    - DOMAIN_CONCURRENCY: Max concurrent domains to process (default: 4)

PERFORMANCE:
    - Processes domains concurrently (configurable via DOMAIN_CONCURRENCY)
    - Commits after each domain (no data loss on crash)
    - Reuses browser instance across all domains
    - Global reuse reduces redundant crawling
    - Batch processing for optimal throughput

DEPENDENCIES:
    - crawl4ai_client.py: Browser automation
    - service.py: Domain crawling business logic
    - repo.py: Database persistence

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations
import asyncio
import logging
from typing import Any

import psycopg

from n8n_founderstories.core.db import get_conn
from .crawl4ai_client import Crawl4AIClient, Crawl4AIClientConfig
from .service import DomainCrawlerService, DomainCrawlConfig
from . import repo as crawl_repo
from ..extract.deterministic import ensure_table as ensure_det_ext_table

logger = logging.getLogger(__name__)


def _determine_page_type(page: Any) -> str:
    """
    Determine page type from PageArtifact.meta.
    
    Args:
        page: PageArtifact with meta dict containing page_type
        
    Returns:
        Page type string (home/contact/impressum/privacy/about/other)
    """
    meta = page.meta or {}
    page_type = meta.get("page_type", "").strip().lower()
    known_types = {"home", "contact", "impressum", "privacy", "about"}
    return page_type if page_type in known_types else "other"


def _determine_status(page: Any) -> tuple[str, str | None]:
    """
    Determine status and error from PageArtifact.
    
    Args:
        page: PageArtifact with error, cleaned_html, and markdown fields
        
    Returns:
        Tuple of (status, error_msg) where status is 'succeeded' or 'failed'
    """
    if page.error:
        return "failed", page.error
    has_content = bool((page.cleaned_html or "").strip() or (page.markdown or "").strip())
    return ("succeeded", None) if has_content else ("failed", "empty_content")


async def _crawl_domain(
    domain: str,
    request_id: str,
    job_id: str,
    client: Crawl4AIClient,
    crawl_config: DomainCrawlConfig,
    conn: psycopg.Connection[Any]
) -> tuple[bool, str]:
    """
    Crawl a single domain and persist results.
    
    NOTE: Stage1 extraction should be skipped here - done by caller after lock release.
    
    Returns:
        Tuple of (success: bool, error_msg: str)
    """
    
    try:
        
        # Crawl domain using existing DomainCrawlerService
        # NOTE: We don't pass db_conn here to avoid holding database connection during crawl
        # Stage1 extraction will be done separately after crawl completes
        crawler_service = DomainCrawlerService(client)
        
        artifacts = await crawler_service.crawl_domain(
            domain=domain,
            cfg=crawl_config,
            db_conn=None,  # Don't pass connection - avoid DB operations during crawl
            request_id=request_id,
            job_id=job_id
        )
        # NOTE: Skip stage1 extraction here - will be done by caller after lock release
        
        pages_saved = 0
        first_error = None
        
        # Get case from artifacts
        case = artifacts.meta.get("contact_case", "unknown")
        
        # Persist homepage
        if artifacts.homepage:
            page_type = _determine_page_type(artifacts.homepage)
            status, error = _determine_status(artifacts.homepage)
            
            if error and not first_error:
                first_error = error
            
            crawl_repo.upsert_page(
                conn=conn,
                request_id=request_id,
                job_id=job_id,
                domain=domain,
                url=str(artifacts.homepage.url),
                page_type=page_type,
                contact_case=case,
                cleaned_html=artifacts.homepage.cleaned_html,
                markdown=artifacts.homepage.markdown,
                status=status,
                error=error
            )
            
            if status == "succeeded":
                pages_saved += 1
        
        # Persist additional pages
        for page in artifacts.pages:
            page_type = _determine_page_type(page)
            status, error = _determine_status(page)
            
            if error and not first_error:
                first_error = error
            
            crawl_repo.upsert_page(
                conn=conn,
                request_id=request_id,
                job_id=job_id,
                domain=domain,
                url=str(page.url),
                page_type=page_type,
                contact_case=case,
                cleaned_html=page.cleaned_html,
                markdown=page.markdown,
                status=status,
                error=error
            )
            
            if status == "succeeded":
                pages_saved += 1
        
        # Commit after each domain
        conn.commit()
        
        # Determine success (at least one page succeeded)
        domain_success = pages_saved > 0
        error_msg = first_error[:100] if first_error and not domain_success else ""
        
        return domain_success, error_msg
    
    except Exception as e:
        error_type = type(e).__name__
        error_str = str(e)
        error_msg = f"{error_type}: {error_str[:200]}" if error_str else error_type
        return False, error_msg[:100]


def run_for_request(request_id: str, job_id: str) -> dict:
    """
    Run crawler for all domains in master_results where crawl_status IS NULL.
    
    This function is idempotent and safe to call multiple times.
    
    Algorithm:
    1. Load domains from master_results where crawl_status IS NULL
    2. For each domain:
       a. Check if domain was crawled successfully before (globally)
       b. If yes: copy previous results to new request_id/job_id
       c. If no: actually crawl the domain
       d. Update master_results.crawl_status accordingly
    3. Commit after each domain to avoid losing progress
    
    Args:
        request_id: Request identifier
        job_id: Job identifier
        
    Returns:
        Dict with stats: {
            "request_id": str,
            "job_id": str,
            "domains_processed": int,
            "domains_crawled": int,
            "domains_reused": int,
            "domains_succeeded": int,
            "domains_failed": int
        }
    """
    
    conn = None
    
    try:
        # Open DB connection
        conn = get_conn()
        
        # Ensure tables exist
        crawl_repo.ensure_table(conn)
        conn.commit()  # Commit after first table to avoid lock conflicts
        ensure_det_ext_table(conn)  # Stage 1 deterministic extraction table (has its own commit)
        
        # Step 0: No-op guard - check if there are any domains to process
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM mstr_results
                WHERE request_id = %s
                  AND crawl_status IS NULL
            """, (request_id,))
            
            row = cur.fetchone()
            pending_count = row[0] if row else 0
        
        if pending_count == 0:
            return {
                "request_id": request_id,
                "job_id": job_id,
                "domains_processed": 0,
                "domains_crawled": 0,
                "domains_reused": 0,
                "domains_succeeded": 0,
                "domains_failed": 0
            }
        
        # Step 1: Load domains to process and get sheet_id
        with conn.cursor() as cur:
            cur.execute("""
                SELECT domain, sheet_id
                FROM mstr_results
                WHERE request_id = %s
                  AND crawl_status IS NULL
                ORDER BY domain
                LIMIT 1
            """, (request_id,))
            
            first_row = cur.fetchone()
            if not first_row:
                return {
                    "request_id": request_id,
                    "job_id": job_id,
                    "domains_processed": 0,
                    "domains_crawled": 0,
                    "domains_reused": 0,
                    "domains_succeeded": 0,
                    "domains_failed": 0
                }
            
            sheet_id = first_row[1] if first_row[1] else "N/A"
            
            # Get all domains
            cur.execute("""
                SELECT domain
                FROM mstr_results
                WHERE request_id = %s
                  AND crawl_status IS NULL
                ORDER BY domain
            """, (request_id,))
            
            domains = [row[0] for row in cur.fetchall()]
        
        if not domains:
            return {
                "request_id": request_id,
                "job_id": job_id,
                "domains_processed": 0,
                "domains_crawled": 0,
                "domains_reused": 0,
                "domains_succeeded": 0,
                "domains_failed": 0
            }
        
        # Read crawl config from env
        import os
        timeout_s = float(os.getenv("CRAWL4AI_TIMEOUT_S", "30.0"))
        headless = os.getenv("CRAWL4AI_HEADLESS", "true").lower() in ("true", "1", "yes")
        wait_after_load_s = float(os.getenv("CRAWL4AI_WAIT_AFTER_LOAD_S", "0.1"))
        max_concurrency = int(os.getenv("CRAWL_MAX_CONCURRENCY", "3"))
        domain_concurrency = int(os.getenv("DOMAIN_CONCURRENCY", "4"))
        
        # Create Crawl4AI client config
        client_config = Crawl4AIClientConfig(
            headless=headless,
            timeout_s=timeout_s,
            wait_after_load_s=wait_after_load_s,
            max_concurrency=max_concurrency
        )
        
        crawl_config = DomainCrawlConfig(
            depth1_max_pages=10,
            depth1_max_new_links=500
        )
        # Stats
        domains_processed = 0
        domains_crawled = 0
        domains_reused = 0
        domains_succeeded = 0
        domains_failed = 0
        
        # Process domains concurrently using worker pattern
        async def process_domains():
            nonlocal domains_processed, domains_crawled, domains_reused, domains_succeeded, domains_failed
            
            # Create single shared Crawl4AI client
            async with Crawl4AIClient(client_config) as client:
                # Create semaphore to limit concurrent workers
                semaphore = asyncio.Semaphore(domain_concurrency)
                
                # Create lock to serialize access to shared Crawl4AI client
                # The client is not fully thread-safe for concurrent operations
                client_lock = asyncio.Lock()
                
                async def worker():
                    """Worker that continuously claims and processes domains."""
                    nonlocal domains_processed, domains_crawled, domains_reused, domains_succeeded, domains_failed
                    
                    while True:
                        # Claim next domain atomically
                        task_conn = None
                        domain = None
                        
                        try:
                            task_conn = get_conn()
                            
                            # Atomic claim: mark as 'processing' and return domain
                            # CRITICAL: Must filter by request_id in UPDATE to avoid corrupting other requests
                            with task_conn.cursor() as cur:
                                cur.execute("""
                                    UPDATE mstr_results
                                    SET crawl_status = 'processing',
                                        crawl_processing_started_at = NOW()
                                    WHERE request_id = %s
                                      AND domain = (
                                        SELECT domain
                                        FROM mstr_results
                                        WHERE request_id = %s
                                          AND crawl_status IS NULL
                                        ORDER BY domain
                                        LIMIT 1
                                        FOR UPDATE SKIP LOCKED
                                    )
                                    RETURNING domain
                                """, (request_id, request_id))
                                
                                row = cur.fetchone()
                                if not row:
                                    # No more domains to process
                                    return
                                
                                domain = row[0]
                            
                            task_conn.commit()
                            
                            # Close connection immediately after claiming - don't hold it during crawl
                            task_conn.close()
                            task_conn = None
                            
                            # Get a fresh connection for remaining count
                            task_conn = get_conn()
                            
                            # Get remaining count for logging
                            with task_conn.cursor() as cur:
                                cur.execute("""
                                    SELECT COUNT(*)
                                    FROM mstr_results
                                    WHERE crawl_status IS NULL OR crawl_status = 'processing'
                                """)
                                remaining = cur.fetchone()[0]
                            
                            # Check if domain is being processed by another request
                            is_being_processed = crawl_repo.check_domain_being_processed(
                                task_conn, domain, request_id
                            )
                            
                            if is_being_processed:
                                # Domain is being processed by another request - wait for it to complete
                                logger.info(
                                    "CRAWL | %d | %s | %s | %s | %s | WAITING (domain being processed by another request)",
                                    remaining, domain, request_id, job_id, sheet_id
                                )
                                
                                # Wait for the other request to finish processing this domain
                                # Poll every 2 seconds for up to 5 minutes
                                max_wait_iterations = 150  # 5 minutes / 2 seconds
                                wait_iteration = 0
                                
                                while wait_iteration < max_wait_iterations:
                                    await asyncio.sleep(2)
                                    wait_iteration += 1
                                    
                                    # Check if domain is still being processed
                                    still_processing = crawl_repo.check_domain_being_processed(
                                        task_conn, domain, request_id
                                    )
                                    
                                    if not still_processing:
                                        # Domain is no longer being processed - check if it succeeded
                                        prev_success = crawl_repo.get_previous_success(task_conn, domain)
                                        
                                        if prev_success:
                                            # Domain was successfully crawled by another request - reuse it
                                            from_request_id, from_job_id = prev_success
                                            
                                            # Copy crawl_results
                                            crawl_repo.copy_domain_results(
                                                conn=task_conn,
                                                from_request_id=from_request_id,
                                                from_job_id=from_job_id,
                                                to_request_id=request_id,
                                                to_job_id=job_id,
                                                domain=domain
                                            )
                                            
                                            # Check if extraction results exist
                                            with task_conn.cursor() as cur:
                                                cur.execute("""
                                                    SELECT extraction_status
                                                    FROM mstr_results
                                                    WHERE request_id = %s AND domain = %s
                                                """, (from_request_id, domain))
                                                row = cur.fetchone()
                                                source_extraction_status = row[0] if row else None
                                            
                                            # Copy extraction results if they exist
                                            if source_extraction_status == 'succeeded':
                                                from n8n_founderstories.services.enrichment.extract.deterministic.stage1_live import copy_domain_results as copy_det_results
                                                copy_det_results(
                                                    conn=task_conn,
                                                    from_request_id=from_request_id,
                                                    from_job_id=from_job_id,
                                                    to_request_id=request_id,
                                                    to_job_id=job_id,
                                                    domain=domain
                                                )
                                                
                                                from n8n_founderstories.services.enrichment.extract.llm.storage.repository import copy_domain_results as copy_llm_results
                                                copy_llm_results(
                                                    conn=task_conn,
                                                    from_request_id=from_request_id,
                                                    from_job_id=from_job_id,
                                                    to_request_id=request_id,
                                                    to_job_id=job_id,
                                                    domain=domain
                                                )
                                                
                                                with task_conn.cursor() as cur:
                                                    cur.execute("""
                                                        UPDATE mstr_results
                                                        SET crawl_status = 'reused',
                                                            extraction_status = 'reused',
                                                            crawl_processing_started_at = NULL
                                                        WHERE request_id = %s AND domain = %s
                                                    """, (request_id, domain))
                                            else:
                                                with task_conn.cursor() as cur:
                                                    cur.execute("""
                                                        UPDATE mstr_results
                                                        SET crawl_status = 'reused',
                                                            extraction_status = NULL,
                                                            crawl_processing_started_at = NULL
                                                        WHERE request_id = %s AND domain = %s
                                                    """, (request_id, domain))
                                            
                                            task_conn.commit()
                                            
                                            domains_processed += 1
                                            domains_reused += 1
                                            domains_succeeded += 1
                                            
                                            logger.info(
                                                "CRAWL | %d | %s | %s | %s | %s | REUSED (after wait)",
                                                remaining, domain, request_id, job_id, sheet_id
                                            )
                                            break
                                        else:
                                            # Domain processing failed in other request - reset and let this worker try
                                            with task_conn.cursor() as cur:
                                                cur.execute("""
                                                    UPDATE mstr_results
                                                    SET crawl_status = NULL
                                                    WHERE request_id = %s AND domain = %s
                                                """, (request_id, domain))
                                            task_conn.commit()
                                            
                                            logger.info(
                                                "CRAWL | %d | %s | %s | %s | %s | RETRY (other request failed)",
                                                remaining, domain, request_id, job_id, sheet_id
                                            )
                                            # Exit worker loop to let it be picked up again
                                            return
                                
                                if wait_iteration >= max_wait_iterations:
                                    # Timeout waiting for other request - reset and continue
                                    logger.warning(
                                        "CRAWL | %d | %s | %s | %s | %s | TIMEOUT (waited 5 minutes)",
                                        remaining, domain, request_id, job_id, sheet_id
                                    )
                                    with task_conn.cursor() as cur:
                                        cur.execute("""
                                            UPDATE mstr_results
                                            SET crawl_status = NULL
                                            WHERE request_id = %s AND domain = %s
                                        """, (request_id, domain))
                                    task_conn.commit()
                                    return
                                
                                # Skip to next iteration
                                continue
                            
                            # Check for global reuse
                            prev_success = crawl_repo.get_previous_success(task_conn, domain)
                            
                            if prev_success:
                                # Reuse previous successful crawl
                                from_request_id, from_job_id = prev_success
                                
                                # Copy crawl_results
                                crawl_repo.copy_domain_results(
                                    conn=task_conn,
                                    from_request_id=from_request_id,
                                    from_job_id=from_job_id,
                                    to_request_id=request_id,
                                    to_job_id=job_id,
                                    domain=domain
                                )
                                
                                # Check if extraction results exist for the source domain
                                with task_conn.cursor() as cur:
                                    cur.execute("""
                                        SELECT extraction_status
                                        FROM mstr_results
                                        WHERE request_id = %s AND domain = %s
                                    """, (from_request_id, domain))
                                    row = cur.fetchone()
                                    source_extraction_status = row[0] if row else None
                                
                                # Copy extraction results only if they exist
                                if source_extraction_status == 'succeeded':
                                    # Copy det_ext_results (deterministic extraction)
                                    from n8n_founderstories.services.enrichment.extract.deterministic.stage1_live import copy_domain_results as copy_det_results
                                    copy_det_results(
                                        conn=task_conn,
                                        from_request_id=from_request_id,
                                        from_job_id=from_job_id,
                                        to_request_id=request_id,
                                        to_job_id=job_id,
                                        domain=domain
                                    )
                                    
                                    # Copy llm_ext_results (LLM extraction)
                                    from n8n_founderstories.services.enrichment.extract.llm.storage.repository import copy_domain_results as copy_llm_results
                                    copy_llm_results(
                                        conn=task_conn,
                                        from_request_id=from_request_id,
                                        from_job_id=from_job_id,
                                        to_request_id=request_id,
                                        to_job_id=job_id,
                                        domain=domain
                                    )
                                    
                                    # Mark as reused for both crawl and extraction
                                    with task_conn.cursor() as cur:
                                        cur.execute("""
                                            UPDATE mstr_results
                                            SET crawl_status = 'reused',
                                                extraction_status = 'reused',
                                                crawl_processing_started_at = NULL
                                            WHERE request_id = %s AND domain = %s
                                        """, (request_id, domain))
                                else:
                                    # Only crawl results exist - mark crawl as reused, extraction as NULL
                                    with task_conn.cursor() as cur:
                                        cur.execute("""
                                            UPDATE mstr_results
                                            SET crawl_status = 'reused',
                                                extraction_status = NULL,
                                                crawl_processing_started_at = NULL
                                            WHERE request_id = %s AND domain = %s
                                        """, (request_id, domain))
                                
                                task_conn.commit()
                                
                                domains_processed += 1
                                domains_reused += 1
                                domains_succeeded += 1
                                
                                logger.info(
                                    "CRAWL | %d | %s | %s | %s | %s | REUSED",
                                    remaining, domain, request_id, job_id, sheet_id
                                )
                            
                            else:
                                # Close connection before crawling - don't hold it during slow operation
                                task_conn.close()
                                task_conn = None
                                
                                # Actually crawl the domain with a timeout to prevent infinite hangs
                                # Note: Concurrency is controlled by the Crawl4AI engine's internal semaphores
                                # (global limiter, engine semaphore, per-domain semaphore)
                                crawl_conn = None
                                try:
                                    # Set a reasonable timeout: 2 minutes per domain
                                    # Get a fresh connection just for the crawl
                                    crawl_conn = get_conn()
                                    
                                    # Acquire lock to serialize access to shared Crawl4AI client
                                    async with client_lock:
                                        success, error_msg = await asyncio.wait_for(
                                            _crawl_domain(
                                                domain=domain,
                                                request_id=request_id,
                                                job_id=job_id,
                                                client=client,
                                                crawl_config=crawl_config,
                                                conn=crawl_conn
                                            ),
                                            timeout=120.0  # 2 minutes
                                        )
                                    
                                except asyncio.TimeoutError:
                                    success = False
                                    error_msg = "Crawl timeout (120s)"
                                finally:
                                    if crawl_conn:
                                        crawl_conn.close()
                                
                                # Get fresh connection for status update
                                task_conn = get_conn()
                                
                                # Update status
                                status = "succeeded" if success else "failed"
                                with task_conn.cursor() as cur:
                                    cur.execute("""
                                        UPDATE mstr_results
                                        SET crawl_status = %s,
                                            crawl_processing_started_at = NULL
                                        WHERE request_id = %s AND domain = %s
                                    """, (status, request_id, domain))
                                
                                # If crawl failed, create placeholder in llm_ext_results
                                if not success:
                                    with task_conn.cursor() as cur:
                                        cur.execute("""
                                            INSERT INTO llm_ext_results
                                            (request_id, job_id, domain, url, page_type, company_json, description_json,
                                             emails_json, contacts_json, status, error)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                            ON CONFLICT (request_id, domain, url) DO UPDATE
                                            SET request_id = EXCLUDED.request_id,
                                                job_id = EXCLUDED.job_id,
                                                status = EXCLUDED.status,
                                                error = EXCLUDED.error
                                        """, (
                                            request_id,
                                            job_id,
                                            domain,
                                            f"https://{domain}/",  # Placeholder URL
                                            None,  # page_type
                                            None,  # company_json
                                            None,  # description_json
                                            None,  # emails_json
                                            None,  # contacts_json
                                            "failed",  # status
                                            f"Crawl failed: {error_msg}"  # error
                                        ))
                                    
                                    # Update extraction_status in mstr_results
                                    with task_conn.cursor() as cur:
                                        cur.execute("""
                                            UPDATE mstr_results
                                            SET extraction_status = 'failed'
                                            WHERE request_id = %s AND domain = %s
                                        """, (request_id, domain))
                                
                                task_conn.commit()
                                
                                domains_processed += 1
                                domains_crawled += 1
                                
                                if success:
                                    domains_succeeded += 1
                                    status_msg = "SUCCESS"
                                else:
                                    domains_failed += 1
                                    status_msg = f"FAILED: {error_msg}"
                                
                                logger.info(
                                    "CRAWL | %d | %s | %s | %s | %s | %s",
                                    remaining, domain, request_id, job_id, sheet_id, status_msg
                                )
                        
                        except Exception as e:
                            # Mark as failed if we claimed a domain
                            if domain and task_conn:
                                try:
                                    with task_conn.cursor() as cur:
                                        cur.execute("""
                                            UPDATE mstr_results
                                            SET crawl_status = 'failed'
                                            WHERE request_id = %s AND domain = %s
                                        """, (request_id, domain))
                                    task_conn.commit()
                                    
                                    domains_processed += 1
                                    domains_failed += 1
                                    
                                    logger.error("CRAWL | ERROR | domain=%s | error=%s", domain, str(e), exc_info=True)
                                except Exception as db_error:
                                    logger.error("CRAWL | DB_ERROR | domain=%s | error=%s", domain, str(db_error), exc_info=True)
                        
                        finally:
                            if task_conn:
                                task_conn.close()
                
                # Create worker tasks limited by semaphore
                # Start all workers (semaphore is used inside worker, not wrapping it)
                workers = [worker() for _ in range(domain_concurrency)]
                await asyncio.gather(*workers, return_exceptions=True)
        
        # Run async processing with timeout
        try:
            # Set a reasonable timeout: 10 minutes per domain * number of domains
            # But cap at 30 minutes total to prevent infinite hangs
            timeout_seconds = min(len(domains) * 600, 1800)  # 10 min per domain, max 30 min
            asyncio.run(asyncio.wait_for(process_domains(), timeout=timeout_seconds))
        except asyncio.TimeoutError:
            logger.error(
                "CRAWLER | TIMEOUT | request_id=%s | job_id=%s | timeout=%ds | domains_processed=%d/%d",
                request_id,
                job_id,
                timeout_seconds,
                domains_processed,
                len(domains)
            )
            # Mark remaining domains as failed
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE mstr_results
                    SET crawl_status = 'failed',
                        crawl_processing_started_at = NULL
                    WHERE request_id = %s
                      AND crawl_status = 'processing'
                """, (request_id,))
            conn.commit()
        
        return {
            "request_id": request_id,
            "job_id": job_id,
            "domains_processed": domains_processed,
            "domains_crawled": domains_crawled,
            "domains_reused": domains_reused,
            "domains_succeeded": domains_succeeded,
            "domains_failed": domains_failed
        }
    
    except Exception:
        raise
    
    finally:
        if conn:
            conn.close()