"""
=============================================================================
PACKAGE: LLM Extraction Module
=============================================================================

CLASSIFICATION: Business Logic Package
LAYER: Service Layer

PURPOSE:
    Provides LLM-based extraction of company information from crawled web pages.
    This module orchestrates the complete extraction pipeline including prompt
    construction, LLM API calls, result normalization, and data validation.

ARCHITECTURE:
    The module is organized into logical subdirectories for better separation
    of concerns and maintainability:
    
    core/           - Core extraction logic and orchestration
    prompts/        - Prompt templates and builders
    adapters/       - External service adapters (OpenRouter)
    utils/          - Utility functions (sanitization, helpers)
    deprecated/     - Legacy code (not for production use)

EXTRACTION PIPELINE:
    1. Input Preparation: Organize crawled pages by type (home, impressum, contact)
    2. Case Detection: Determine extraction strategy based on available pages
    3. Prompt Construction: Build case-specific prompts with markdown content
    4. LLM Execution: Call OpenRouter API with constructed prompts
    5. Result Parsing: Parse JSON responses from LLM
    6. Data Sanitization: Truncate quotes, validate emails
    7. Normalization: Convert to standardized LLMExtraction format

EXTRACTION CASES:
    Case 1, 2, 5.1, 5.2 (Impressum Path):
        - Primary: Impressum page for contact data
        - Secondary: Homepage for company description
        - Optimal for German/Austrian companies with Impressum
    
    Case 3, 5.3 (No Impressum):
        - Primary: Homepage for company description
        - Secondary: Contact/Privacy pages for contact data
        - Fallback for companies without Impressum

MAIN EXPORTS:
    - extract: Main extraction function (from core.extractor)

USAGE:
    from n8n_founderstories.services.enrichment.extract.llm import extract
    from n8n_founderstories.services.enrichment.extract.llm.adapters import (
        OpenRouterLLMRouter
    )
    
    # Initialize router
    router = OpenRouterLLMRouter()
    
    # Extract company data
    extraction = await extract(
        domain="example.com",
        crawl_meta={
            "contact_case": "1",
            "contact_selected_links": ["https://example.com/impressum", ...],
            "about_selected_links": ["https://example.com/about"],
        },
        pages=[page1, page2, ...],
        router=router,
    )
    
    # Access results
    company_name = extraction.company.name if extraction.company else None
    emails = [e.email for e in extraction.emails]
    contacts = [c.name for c in extraction.contacts]
    description = extraction.about.short_description if extraction.about else None

DATA MODELS:
    Input:
        - PageArtifact: Crawled page with markdown content
        - crawl_meta: Dictionary with case info and selected links
    
    Output:
        - LLMExtraction: Complete extraction result
            - company: LLMCompany (name + evidence)
            - emails: List[LLMEmail] (email + evidence)
            - contacts: List[LLMContact] (name, role + evidence)
            - about: LLMAbout (descriptions + evidence)

QUALITY ASSURANCE:
    - Email Validation: Regex validation prevents invalid email formats
    - Quote Truncation: Automatic truncation to 300 chars prevents validation errors
    - Evidence Tracking: All extracted data includes source URL and quote
    - Error Handling: Graceful degradation on LLM failures

DEPENDENCIES:
    Internal:
        - models: Data structures (LLMExtraction, Evidence, etc.)
        - openrouter: Global OpenRouter client
    
    External:
        - OpenRouter API: LLM inference service

NOTES:
    - All LLM responses are sanitized before Pydantic validation
    - Evidence quotes are verbatim from markdown (with truncation)
    - Email validation prevents pydantic.ValidationError
    - Results are deterministic given same inputs
    - Database functionality has been removed (see deprecated/)

MIGRATION FROM OLD STRUCTURE:
    Old:
        from n8n_founderstories.services.enrichment.extract.llm import (
            DomainLLMWorker,
            LLMQueueRepository,
        )
    
    New:
        from n8n_founderstories.services.enrichment.extract.llm import extract
        from n8n_founderstories.services.enrichment.extract.llm.adapters import (
            OpenRouterLLMRouter
        )

DEPRECATED:
    - DomainLLMWorker: Database functionality removed (see deprecated/)
    - LLMQueueRepository: Deleted (database tables removed)
    - LLMResultsRepository: Deleted (database tables removed)
    - service.py: Moved to deprecated/ (use core.extract instead)
=============================================================================
"""
# NOTE: core.extract temporarily disabled - depends on PageArtifact model
# from .core import extract

from .storage import (
    ensure_table,
    upsert_page_extraction,
    get_next_unprocessed_page,
)

__all__ = [
    # "extract",  # Temporarily disabled - depends on PageArtifact
    "ensure_table",
    "upsert_page_extraction",
    "get_next_unprocessed_page",
]

# Version info
__version__ = "2.0.0"  # Major refactor with storage layer