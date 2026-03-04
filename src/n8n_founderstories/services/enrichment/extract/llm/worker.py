"""
═══════════════════════════════════════════════════════════════════════════════
LLM EXTRACTION WORKER - Background Page Processor
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [WORKER] - Background daemon for LLM extraction

PURPOSE:
    Continuously processes pages from crawl_results in strict crawl order,
    extracts structured data using LLM, and stores results in llm_ext_results.

WORKER BEHAVIOR:
    - Runs in daemon thread (started at app startup)
    - Polls every 5 seconds for unprocessed pages
    - Processes MULTIPLE pages concurrently (configurable via llm_max_concurrency)
    - Maintains strict crawl order (ORDER BY created_at ASC)
    - Never blocks crawler
    - Handles errors gracefully

PROCESSING FLOW:
    1. Query batch of unprocessed pages (LEFT JOIN with llm_ext_results)
    2. Process pages concurrently using asyncio.gather:
       a. Select prompt based on page_type
       b. Call LLM with temperature=0.0
       c. Parse JSON response
       d. Store in llm_ext_results (UPSERT)
    3. Repeat with next batch

PROMPT SELECTION:
    page_type == "impressum" → build_case1_contact_prompt_impressum()
    page_type == "home"      → build_case1_short_about_prompt_homepage()
    page_type in ("contact", "privacy") → build_case3_contact_prompt_page()
    Other types              → Skip (do not process)

ERROR HANDLING:
    - Invalid JSON → Store empty JSON objects, log error
    - LLM failure → Log error, continue to next page
    - Missing markdown → Skip page
    - Database errors → Log and retry on next poll

INTEGRATION:
    Started in main.py startup event:
    
    _llm_worker_thread = threading.Thread(
        target=lambda: run_worker(poll_interval_s=5.0),
        name="LLMExtractionWorker",
        daemon=True
    )
    _llm_worker_thread.start()

LOGGING:
    Format: EXTRACT | <count> | <domain> | <request_id> | <job_id> | <sheet_id> | <status>
    Example: EXTRACT | 1 | example.com | req_123 | job_456 | 1A2B3C4D5E6F | SUCCESS
    
    Status values:
    - SUCCESS: Page extracted successfully
    - FAILED: <error>: Extraction failed with error message
    - ERROR: <error>: Exception during processing
    
    Additional logs:
    - LLM_EXTRACT | START | poll_interval=5.0s
    - LLM_EXTRACT | INIT | Ensuring tables exist...
    - LLM_EXTRACT | RUNNING | Entering main loop...
    - LLM_EXTRACT | PROCESSING | domain | url | page_type
    - LLM_EXTRACT | SKIPPED | domain | url | reason
    - LLM_EXTRACT | POLLING | poll_count=N | No unprocessed pages found

CRITICAL CONSTRAINTS:
    ✅ Never block crawler
    ✅ Concurrent processing (bounded by llm_max_concurrency)
    ✅ Preserve crawl order (ORDER BY created_at ASC)
    ✅ Idempotent (UPSERT on conflict)
    ✅ Never reprocess already processed pages
    ✅ Handle empty markdown safely
    ✅ Handle LLM errors safely

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import traceback
from typing import Any, Dict, Optional

import psycopg

from n8n_founderstories.core.config import settings
from n8n_founderstories.services.master import repo as master_repo
from .adapters import OpenRouterLLMRouter
from .prompts import (
    build_case1_contact_prompt_impressum,
    build_case1_short_about_prompt_homepage,
    build_case3_contact_prompt_page,
)
from .storage import (
    ensure_table,
    upsert_page_extraction,
    get_next_unprocessed_page,
    get_next_unprocessed_pages_batch,
    update_domain_extraction_status,
    mark_failed_crawls_as_failed_extraction,
    get_previous_extraction_success,
    copy_extraction_results,
)
from .utils import parse_json_strict, extract_assistant_text

logger = logging.getLogger(__name__)


# =============================================================================
# WORKER CONFIGURATION
# =============================================================================

# LLM Router (shared across all worker iterations)
_router: Optional[OpenRouterLLMRouter] = None

# Global counter for processed pages
_processed_count = 0


def _get_router() -> OpenRouterLLMRouter:
    """Get or create the global LLM router instance."""
    global _router
    if _router is None:
        _router = OpenRouterLLMRouter(temperature=0.0)
    return _router


# =============================================================================
# PROMPT SELECTION
# =============================================================================

def _select_prompt(page_type: Optional[str], markdown: str) -> Optional[str]:
    """
    Select the appropriate prompt based on page_type.
    
    Args:
        page_type: Page type from crawl_results
        markdown: Page markdown content
    
    Returns:
        Prompt string or None if page should be skipped
    
    Prompt Selection Logic:
        page_type == "impressum" → build_case1_contact_prompt_impressum()
        page_type == "home"      → build_case1_short_about_prompt_homepage()
        page_type in ("contact", "privacy") → build_case3_contact_prompt_page()
        Other types              → None (skip)
    """
    if not page_type:
        return None
    
    page_type_lower = page_type.lower()
    
    if page_type_lower == "impressum":
        return build_case1_contact_prompt_impressum(markdown=markdown)
    elif page_type_lower == "home":
        return build_case1_short_about_prompt_homepage(markdown=markdown)
    elif page_type_lower in ("contact", "privacy"):
        return build_case3_contact_prompt_page(markdown=markdown)
    else:
        return None


# =============================================================================
# JSON EXTRACTION
# =============================================================================

def _extract_json_fields(llm_response: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Extract and serialize JSON fields from LLM response.
    
    Args:
        llm_response: Parsed JSON response from LLM
    
    Returns:
        Dictionary with serialized JSON fields:
        {
            "company_json": str | None,
            "description_json": str | None,
            "emails_json": str | None,
            "contacts_json": str | None,
        }
    
    Notes:
        - Returns None for missing fields
        - Serializes objects/arrays to JSON strings
        - Handles missing keys gracefully
    """
    result: Dict[str, Optional[str]] = {
        "company_json": None,
        "description_json": None,
        "emails_json": None,
        "contacts_json": None,
    }
    
    # Extract company_name
    if "company_name" in llm_response and llm_response["company_name"]:
        result["company_json"] = json.dumps(llm_response["company_name"])
    
    # Extract short_description
    if "short_description" in llm_response and llm_response["short_description"]:
        result["description_json"] = json.dumps(llm_response["short_description"])
    
    # Extract emails
    if "emails" in llm_response and llm_response["emails"]:
        result["emails_json"] = json.dumps(llm_response["emails"])
    
    # Extract contacts
    if "contacts" in llm_response and llm_response["contacts"]:
        result["contacts_json"] = json.dumps(llm_response["contacts"])
    
    return result


# =============================================================================
# PAGE PROCESSING
# =============================================================================

async def _process_page(page: Dict[str, Any]) -> bool:
    """
    Process a single page with LLM extraction.
    
    Args:
        page: Page data from crawl_results
    
    Returns:
        True if processing succeeded, False otherwise
    
    Processing Steps:
        1. Select prompt based on page_type
        2. Call LLM with temperature=0.0
        3. Parse JSON response
        4. Extract and serialize fields
        5. Store in llm_ext_results
    
    Error Handling:
        - Invalid JSON → Store empty objects, log error
        - LLM failure → Log error, return False
        - Missing markdown → Skip, return False
    """
    global _processed_count
    _processed_count += 1
    
    request_id = page["request_id"]
    job_id = page.get("job_id", "")
    sheet_id = page.get("sheet_id", "")
    domain = page["domain"]
    url = page["url"]
    page_type = page.get("page_type")
    markdown = page.get("markdown", "")
    
    # Skip if no markdown - but mark as failed in database
    if not markdown or not markdown.strip():
        logger.warning(
            "LLM_EXTRACT | SKIPPED | domain=%s | url=%s | reason=empty_markdown",
            domain,
            url,
        )
        # Mark as failed so it doesn't get picked up again
        try:
            with psycopg.connect(settings.postgres_dsn) as conn:
                upsert_page_extraction(
                    conn,
                    domain=domain,
                    url=url,
                    page_type=page_type,
                    company_json=None,
                    description_json=None,
                    emails_json=None,
                    contacts_json=None,
                    status="failed",
                    error="Empty markdown content",
                    request_id=request_id,
                    job_id=job_id,
                )
                
                # Update domain extraction status
                update_domain_extraction_status(conn, request_id, domain)
                
                conn.commit()
        except Exception as e:
            logger.error("LLM_EXTRACT | DB_ERROR | Failed to mark skipped page: %s", str(e))
        return False
    
    # Select prompt based on page_type
    prompt = _select_prompt(page_type, markdown)
    
    if prompt is None:
        logger.info(
            "LLM_EXTRACT | SKIPPED | domain=%s | url=%s | page_type=%s | reason=no_matching_prompt",
            domain,
            url,
            page_type,
        )
        # Mark as failed so it doesn't get picked up again
        try:
            with psycopg.connect(settings.postgres_dsn) as conn:
                upsert_page_extraction(
                    conn,
                    domain=domain,
                    url=url,
                    page_type=page_type,
                    company_json=None,
                    description_json=None,
                    emails_json=None,
                    contacts_json=None,
                    status="failed",
                    error=f"No matching prompt for page_type: {page_type}",
                    request_id=request_id,
                    job_id=job_id,
                )
                
                # Update domain extraction status
                update_domain_extraction_status(conn, request_id, domain)
                
                conn.commit()
        except Exception as e:
            logger.error("LLM_EXTRACT | DB_ERROR | Failed to mark skipped page: %s", str(e))
        return False
    
    try:
        # Call LLM
        router = _get_router()
        llm_response_raw = await router.complete(prompt)
        
        # Extract assistant text
        assistant_text = extract_assistant_text(llm_response_raw)
        
        # Parse JSON
        status = "succeeded"
        error = None
        try:
            llm_response = parse_json_strict(assistant_text)
        except json.JSONDecodeError as e:
            logger.error(
                "LLM_EXTRACT | JSON_PARSE_ERROR | domain=%s | url=%s | error=%s",
                domain,
                url,
                str(e),
            )
            # Store empty JSON objects on parse failure but mark as failed
            llm_response = {}
            status = "failed"
            error = f"JSON parse error: {str(e)}"
        
        # Extract and serialize fields
        fields = _extract_json_fields(llm_response)
        
        # Store in database
        with psycopg.connect(settings.postgres_dsn) as conn:
            upsert_page_extraction(
                conn,
                domain=domain,
                url=url,
                page_type=page_type,
                company_json=fields["company_json"],
                description_json=fields["description_json"],
                emails_json=fields["emails_json"],
                contacts_json=fields["contacts_json"],
                status=status,
                error=error,
                request_id=request_id,
                job_id=job_id,
            )
            
            # Update domain extraction status if all pages are done
            update_domain_extraction_status(conn, request_id, domain)
            
            conn.commit()
            
            # Get remaining domain count for logging
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM mstr_results
                    WHERE extraction_status IS NULL
                """)
                remaining_count = cur.fetchone()[0]
        
        # Log in crawler format: EXTRACT | count | domain | request_id | job_id | sheet_id | STATUS
        log_status = "SUCCESS" if status == "succeeded" else f"FAILED: {error}"
        logger.info(
            "EXTRACT | %d | %s | %s | %s | %s | %s",
            remaining_count,
            domain,
            request_id,
            job_id,
            sheet_id,
            log_status,
        )
        
        return True
        
    except Exception as e:
        error_msg = str(e)
        
        # Store failed status in database
        try:
            with psycopg.connect(settings.postgres_dsn) as conn:
                upsert_page_extraction(
                    conn,
                    domain=domain,
                    url=url,
                    page_type=page_type,
                    company_json=None,
                    description_json=None,
                    emails_json=None,
                    contacts_json=None,
                    status="failed",
                    error=error_msg[:500],  # Truncate long errors
                    request_id=request_id,
                    job_id=job_id,
                )
                
                # Update domain extraction status
                update_domain_extraction_status(conn, request_id, domain)
                
                conn.commit()
                
                # Get remaining domain count for logging
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM mstr_results
                        WHERE extraction_status IS NULL
                    """)
                    remaining_count = cur.fetchone()[0]
                
            # Log in crawler format: EXTRACT | count | domain | request_id | job_id | sheet_id | FAILED: error
            logger.info(
                "EXTRACT | %d | %s | %s | %s | %s | %s",
                remaining_count,
                domain,
                request_id,
                job_id,
                sheet_id,
                f"FAILED: {error_msg[:100]}",  # Truncate error in log
            )
        except Exception as db_error:
            # For database errors, we can't get remaining count, so use 0
            logger.error(
                "EXTRACT | %d | %s | %s | %s | %s | %s",
                0,
                domain,
                request_id,
                job_id,
                sheet_id,
                f"ERROR: {str(db_error)[:50]}",
            )
        
        return False


# =============================================================================
# WORKER LOOP
# =============================================================================

def run_worker(poll_interval_s: float = 5.0) -> None:
    """
    Run the LLM extraction worker loop.
    
    This function runs continuously in a daemon thread, polling for
    unprocessed pages and processing them one at a time.
    
    Args:
        poll_interval_s: Seconds to wait between polls (default: 5.0)
    
    Worker Loop:
        1. Ensure table exists
        2. Get batch of unprocessed pages (ORDER BY created_at ASC)
        3. If pages found: process them concurrently
        4. If no pages: sleep for poll_interval_s
        5. Repeat
    
    Notes:
        - Runs in daemon thread (stops with application)
        - Concurrent processing (bounded by llm_max_concurrency)
        - Maintains strict crawl order within batches
        - Never blocks crawler
    """
    # Get batch size from settings
    batch_size = getattr(settings, "llm_max_concurrency", 6)
    
    # Ensure tables exist on startup
    try:
        with psycopg.connect(settings.postgres_dsn) as conn:
            # Ensure mstr_results table exists (needed for sheet_id lookup)
            master_repo.ensure_table(conn)
            # Ensure crawl_results table exists (needed for page queries)
            from n8n_founderstories.services.enrichment.crawl import repo as crawl_repo
            crawl_repo.ensure_table(conn)
            # Ensure llm_ext_results table exists
            ensure_table(conn)
            
            # Mark failed crawls as failed extraction
            failed_count = mark_failed_crawls_as_failed_extraction(conn)
            conn.commit()
            
    except Exception as e:
        logger.error(
            "LLM_EXTRACT | TABLE_INIT_ERROR | error=%s | traceback=%s",
            str(e),
            traceback.format_exc(),
        )
        return
    
    # Create event loop for async operations
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Main worker loop
    poll_count = 0
    while True:
        try:
            poll_count += 1
            
            # Every 10 polls, check for failed crawls and mark them as failed extraction
            if poll_count % 10 == 0:
                try:
                    with psycopg.connect(settings.postgres_dsn) as conn:
                        failed_count = mark_failed_crawls_as_failed_extraction(conn)
                        conn.commit()
                        if failed_count > 0:
                            logger.info("LLM_EXTRACT | MARKED_FAILED | count=%d", failed_count)
                except Exception as e:
                    logger.error("LLM_EXTRACT | MARK_FAILED_ERROR | error=%s", str(e))
            
            # Check for domains where crawl was reused - copy extraction too
            try:
                with psycopg.connect(settings.postgres_dsn) as conn:
                    # Get domains where crawl_status = 'reused' and extraction_status IS NULL
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT m.domain, m.request_id, m.job_id, m.sheet_id
                            FROM mstr_results m
                            WHERE m.crawl_status = 'reused'
                              AND m.extraction_status IS NULL
                            LIMIT 10
                        """)
                        domains_to_copy = cur.fetchall()
                    
                    # For each domain with reused crawl, try to copy extraction
                    for domain, request_id, job_id, sheet_id in domains_to_copy:
                        # Find previous successful extraction for this domain
                        prev_result = get_previous_extraction_success(conn, domain, request_id)
                        
                        if prev_result:
                            # Copy extraction results
                            copied = copy_extraction_results(
                                conn,
                                domain=domain,
                                source_request_id=prev_result["request_id"],
                                target_request_id=request_id
                            )
                            
                            if copied > 0:
                                # Update mstr_results to mark as reused
                                with conn.cursor() as cur:
                                    cur.execute("""
                                        UPDATE mstr_results
                                        SET extraction_status = 'reused'
                                        WHERE request_id = %s AND domain = %s
                                    """, (request_id, domain))
                                
                                conn.commit()
                                
                                # Get remaining count
                                with conn.cursor() as cur:
                                    cur.execute("""
                                        SELECT COUNT(*)
                                        FROM mstr_results
                                        WHERE extraction_status IS NULL
                                    """)
                                    remaining = cur.fetchone()[0]
                                
                                # Log in same format as CRAWL logs
                                logger.info(
                                    "EXTRACT | %d | %s | %s | %s | %s | REUSED",
                                    remaining, domain, request_id, job_id, sheet_id or "N/A"
                                )
                        else:
                            # No previous extraction found yet
                            # Could be: 1) Never extracted, 2) Failed, 3) Still processing
                            # Leave extraction_status as NULL so it will be extracted normally
                            logger.debug(
                                "EXTRACT | NO_REUSE | domain=%s | request_id=%s | reason=no_previous_success",
                                domain, request_id
                            )
            except Exception as e:
                logger.error("LLM_EXTRACT | REUSE_CHECK_ERROR | error=%s", str(e))
            
            # Get batch of unprocessed pages
            with psycopg.connect(settings.postgres_dsn) as conn:
                pages = get_next_unprocessed_pages_batch(conn, batch_size=batch_size)
            
            if pages:
                # Process pages concurrently using asyncio.gather
                tasks = [_process_page(page) for page in pages]
                loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            else:
                # No pages to process, sleep for poll interval
                time.sleep(poll_interval_s)
                
        except KeyboardInterrupt:
            logger.info("LLM_EXTRACT | INTERRUPTED")
            break
        except Exception as e:
            logger.error(
                "LLM_EXTRACT | LOOP_ERROR | error=%s | traceback=%s",
                str(e),
                traceback.format_exc(),
            )
            # Sleep before retrying
            time.sleep(poll_interval_s)
    
    logger.info("LLM_EXTRACT | STOPPED")