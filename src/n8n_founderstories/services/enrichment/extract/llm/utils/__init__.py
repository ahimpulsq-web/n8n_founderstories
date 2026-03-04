"""
=============================================================================
PACKAGE: LLM Extraction Utilities
=============================================================================

CLASSIFICATION: Utility Package
LAYER: Utilities

PURPOSE:
    Provides utility functions and classes for LLM extraction operations.

MODULES:
    - helpers: URL normalization, page indexing, JSON parsing
    - sanitizer: Data sanitization and quote truncation

EXPORTS:
    From helpers:
        - normalize_url
        - parse_json_strict
        - extract_assistant_text
    
    From sanitizer:
        - truncate_quote
        - sanitize_extraction
        - sanitize_evidence_dict
        - MAX_EVIDENCE_QUOTE_LEN

USAGE:
    from n8n_founderstories.services.enrichment.extract.llm.utils import (
        normalize_url,
        sanitize_extraction,
    )
=============================================================================
"""
from .helpers import (
    normalize_url,
    parse_json_strict,
    extract_assistant_text,
)
from .sanitizer import (
    truncate_quote,
    sanitize_extraction,
    sanitize_evidence_dict,
    MAX_EVIDENCE_QUOTE_LEN,
)

__all__ = [
    # Helpers
    "normalize_url",
    "parse_json_strict",
    "extract_assistant_text",
    # Sanitizer
    "truncate_quote",
    "sanitize_extraction",
    "sanitize_evidence_dict",
    "MAX_EVIDENCE_QUOTE_LEN",
]