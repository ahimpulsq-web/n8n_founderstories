"""
=============================================================================
PACKAGE: LLM Extraction Core
=============================================================================

CLASSIFICATION: Core Business Logic Package
LAYER: Business Logic

PURPOSE:
    Provides core LLM extraction functionality including extraction orchestration
    and result normalization.

MODULES:
    - extractor: Main extraction orchestrator
    - normalizer: Result normalization functions

EXPORTS:
    - extract: Main extraction function (orchestrates entire extraction process)

USAGE:
    from n8n_founderstories.services.enrichment.extract.llm.core import extract
    
    # Extract company data from crawled pages
    extraction = await extract(
        domain="example.com",
        crawl_meta={"contact_case": "1", ...},
        pages=[page1, page2, ...],
        router=router,
    )
    
    # Result contains:
    # - extraction.company: Company name with evidence
    # - extraction.emails: List of emails with evidence
    # - extraction.contacts: List of contacts with evidence
    # - extraction.about: Company descriptions with evidence

NOTES:
    - This is the main entry point for LLM extraction
    - Handles case-based routing (Impressum vs No Impressum)
    - Normalizes results into consistent format
    - Validates and sanitizes all extracted data
=============================================================================
"""
from .extractor import extract

__all__ = [
    "extract",
]