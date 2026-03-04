"""
Deterministic Email Extraction System

This package provides production-ready deterministic email extraction from web pages.
It uses regex patterns, text normalization, and intelligent prioritization to extract
high-quality business email addresses.

Key Features:
- Robust email parsing with obfuscation handling
- Intelligent prioritization based on page type and domain matching
- Comprehensive validation and filtering
- Performance tracking and metrics
- Clean, modular architecture with separation of concerns
- Live Stage 1 extraction in crawl worker

Quick Start:
    from .core import extract_emails_from_pages
    from ...models import PageArtifact
    
    pages = [PageArtifact(url="https://example.com", cleaned_html="...")]
    result = extract_emails_from_pages("example.com", pages)
    
    for email in result.emails:
        print(email.email, email.source_url)

Stage 1 Live Extraction:
    from .stage1_live import run_stage1_for_page
    
    # In crawl worker, after each page fetch:
    result = await run_stage1_for_page(domain, page, db_conn)

Architecture:
    - config: Configuration and constants
    - core: Main extraction logic (parser, prioritizer, extractor)
    - validators: Email validation and filtering
    - utils: Text normalization and domain utilities
    - metrics: Performance tracking and telemetry
    - stage1_live: Live extraction for crawl worker

Author: N8N FounderStories Team
Last Modified: 2026-02-18
Version: 2.0.0 (Production-Ready Refactor)
"""

from __future__ import annotations

# ============================================================================
# PUBLIC API - STAGE 1 LIVE EXTRACTION (CRAWL WORKER)
# ============================================================================

from .stage1_live import (
    ensure_table,
    run_stage1_for_page,
    run_stage1_for_pages,
    extract_emails_from_page,
    get_stage1_results_for_domain,
    count_stage1_results_for_domain,
    delete_stage1_results_for_domain,
    copy_domain_results,
    Stage1Result,
)

# ============================================================================
# PUBLIC API - STAGE 2 EXTRACTION FUNCTIONS (ENRICHMENT SERVICE)
# ============================================================================

from .core.extractor import (
    extract_emails_from_pages,
    extract_emails_from_text,
    quick_extract,
    DeterministicExtractor,
)

# ============================================================================
# PUBLIC API - MODELS AND TYPES
# ============================================================================

from .core.prioritizer import EmailPriority, EmailSource
from .core.extractor import ExtractionMetrics

# ============================================================================
# PUBLIC API - CONFIGURATION
# ============================================================================

from .config import config, DeterministicConfig

# ============================================================================
# PUBLIC API - UTILITIES
# ============================================================================

from .utils import (
    normalize_text,
    clean_email_text,
    normalize_domain,
    extract_domain_from_email,
    domains_match,
)

# ============================================================================
# PUBLIC API - VALIDATORS
# ============================================================================

from .validators import (
    is_valid_email,
    is_plausible_email,
    should_filter_email,
    filter_email_list,
)

# ============================================================================
# PUBLIC API - METRICS
# ============================================================================

from .metrics import (
    ExtractionStats,
    track_extraction,
    get_global_stats,
    reset_global_stats,
)

# ============================================================================
# VERSION AND METADATA
# ============================================================================

__version__ = "2.0.0"
__author__ = "N8N FounderStories Team"
__all__ = [
    # Stage 1 Live Extraction (Crawl Worker)
    "ensure_table",
    "run_stage1_for_page",
    "run_stage1_for_pages",
    "extract_emails_from_page",
    "get_stage1_results_for_domain",
    "count_stage1_results_for_domain",
    "delete_stage1_results_for_domain",
    "copy_domain_results",
    "Stage1Result",
    
    # Stage 2 Extraction Functions (Enrichment Service)
    "extract_emails_from_pages",
    "extract_emails_from_text",
    "quick_extract",
    "DeterministicExtractor",
    
    # Models and types
    "EmailPriority",
    "EmailSource",
    "ExtractionMetrics",
    
    # Configuration
    "config",
    "DeterministicConfig",
    
    # Utilities
    "normalize_text",
    "clean_email_text",
    "normalize_domain",
    "extract_domain_from_email",
    "domains_match",
    
    # Validators
    "is_valid_email",
    "is_plausible_email",
    "should_filter_email",
    "filter_email_list",
    
    # Metrics
    "ExtractionStats",
    "track_extraction",
    "get_global_stats",
    "reset_global_stats",
]


# ============================================================================
# BACKWARD COMPATIBILITY
# ============================================================================

def extract(domain: str, pages: list) -> object:
    """
    Backward-compatible extraction function.
    
    This maintains compatibility with the old API while using the new implementation.
    
    Args:
        domain: Company domain
        pages: List of PageArtifact objects
        
    Returns:
        DeterministicExtraction object
        
    Note:
        This function is provided for backward compatibility.
        New code should use extract_emails_from_pages() instead.
    """
    return extract_emails_from_pages(domain, pages)


# ============================================================================
# MODULE DOCUMENTATION
# ============================================================================

def get_module_info() -> dict:
    """
    Get information about this module.
    
    Returns:
        Dictionary with module metadata
        
    Examples:
        >>> info = get_module_info()
        >>> print(info["version"])
        2.0.0
    """
    return {
        "name": "deterministic",
        "version": __version__,
        "author": __author__,
        "description": "Production-ready deterministic email extraction",
        "features": [
            "Robust email parsing",
            "Intelligent prioritization",
            "Comprehensive validation",
            "Performance tracking",
            "Modular architecture",
        ],
        "modules": {
            "config": "Configuration and constants",
            "core": "Main extraction logic",
            "validators": "Email validation and filtering",
            "utils": "Text and domain utilities",
            "metrics": "Performance tracking",
        },
    }


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

def _example_basic_usage():
    """
    Example: Basic email extraction from pages.
    
    This example shows the most common usage pattern.
    """
    from ...models import PageArtifact
    
    # Create page artifacts
    pages = [
        PageArtifact(
            url="https://example.com/contact",
            cleaned_html="Contact us at info@example.com",
            meta={"page_type": "contact"},
        ),
        PageArtifact(
            url="https://example.com/impressum",
            cleaned_html="Email: sales@example.com",
            meta={"page_type": "impressum"},
        ),
    ]
    
    # Extract emails
    result = extract_emails_from_pages("example.com", pages, max_emails=5)
    
    # Process results
    for email in result.emails:
        print(f"Found: {email.email} on {email.source_url}")


def _example_advanced_usage():
    """
    Example: Advanced usage with custom extractor.
    
    This example shows how to use the extractor class directly
    for more control over the extraction process.
    """
    from ...models import PageArtifact
    
    # Create extractor with custom settings
    extractor = DeterministicExtractor(
        domain="example.com",
        max_emails=10,
        filter_generic=True,
    )
    
    # Prepare pages
    pages = [
        PageArtifact(
            url="https://example.com",
            cleaned_html="...",
        ),
    ]
    
    # Extract
    result = extractor.extract(pages)
    
    # Get metrics
    metrics = extractor.get_metrics()
    print(f"Processed {metrics.pages_processed} pages")
    print(f"Found {metrics.final_emails} emails")
    print(f"Took {metrics.extraction_time_ms}ms")


def _example_quick_extraction():
    """
    Example: Quick extraction from HTML content.
    
    This example shows the simplest way to extract emails
    from a single HTML block.
    """
    html = """
    <html>
        <body>
            <h1>Contact Us</h1>
            <p>Email us at info@example.com</p>
            <p>Or call our sales team: sales@example.com</p>
        </body>
    </html>
    """
    
    emails = quick_extract("example.com", html, page_type="contact")
    print(f"Found emails: {emails}")


# Note: These example functions are for documentation purposes only.
# They are not meant to be called directly.