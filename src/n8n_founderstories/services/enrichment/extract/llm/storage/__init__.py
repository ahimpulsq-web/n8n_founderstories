"""
=============================================================================
PACKAGE: LLM Extraction Storage
=============================================================================

CLASSIFICATION: Storage Package
LAYER: Data Access Layer

PURPOSE:
    Provides database storage for LLM extraction results.
    One row per PAGE (not per domain) with UNIQUE(domain, url).

MODULES:
    - repository: Database repository for llm_ext_results table

EXPORTS:
    - ensure_table: Create llm_ext_results table
    - upsert_page_extraction: Insert/update LLM extraction for a page
    - get_next_unprocessed_page: Get next page to process (crawl order)

USAGE:
    from n8n_founderstories.services.enrichment.extract.llm.storage import (
        ensure_table,
        upsert_page_extraction,
        get_next_unprocessed_page,
    )
    
    # Ensure table exists
    ensure_table(conn)
    
    # Get next page to process
    page = get_next_unprocessed_page(conn)
    
    # Store extraction result
    upsert_page_extraction(
        conn=conn,
        domain=page["domain"],
        url=page["url"],
        page_type=page["page_type"],
        company_json=company_json,
        description_json=description_json,
        emails_json=emails_json,
        contacts_json=contacts_json,
    )
=============================================================================
"""
from .repository import (
    ensure_table,
    upsert_page_extraction,
    get_next_unprocessed_page,
    get_next_unprocessed_pages_batch,
    update_domain_extraction_status,
    mark_failed_crawls_as_failed_extraction,
    get_extraction_progress,
    copy_domain_results,
    get_previous_extraction_success,
    copy_extraction_results,
)

__all__ = [
    "ensure_table",
    "upsert_page_extraction",
    "get_next_unprocessed_page",
    "get_next_unprocessed_pages_batch",
    "update_domain_extraction_status",
    "mark_failed_crawls_as_failed_extraction",
    "get_extraction_progress",
    "copy_domain_results",
    "get_previous_extraction_success",
    "copy_extraction_results",
]