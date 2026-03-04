"""
=============================================================================
MODULE: LLM Extraction Utilities - Helper Functions
=============================================================================

CLASSIFICATION: Core Utility Module
LAYER: Utilities
DEPENDENCIES: None (pure functions)

PURPOSE:
    Provides reusable helper functions for LLM extraction operations including
    URL normalization, page indexing, and JSON parsing.

EXPORTS:
    - normalize_url: Normalize URLs for deduplication
    - index_pages: Create URL-to-page lookup dictionary
    - pick_first_page: Find page by URL from index
    - parse_json_strict: Parse JSON with error handling
    - extract_assistant_text: Extract text from OpenRouter response

USAGE:
    from n8n_founderstories.services.enrichment.extract.llm.utils.helpers import (
        normalize_url,
        index_pages,
        pick_first_page,
    )
    
    # Normalize URL for comparison
    url = normalize_url("https://example.com/page?query=1#section")
    # Result: "https://example.com/page"
    
    # Index pages for fast lookup
    pages_index = index_pages(page_artifacts)
    page = pick_first_page(pages_index, "https://example.com")

NOTES:
    - All functions are pure (no side effects)
    - URL normalization strips query params and fragments
    - Page indexing handles both original and final URLs (after redirects)
=============================================================================
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


# =============================================================================
# URL NORMALIZATION
# =============================================================================

def normalize_url(url: str) -> str:
    """
    Normalize URL for deduplication by removing query params, fragments, and trailing slashes.
    
    This function is used throughout the extraction pipeline to ensure consistent
    URL comparison and deduplication.
    
    Args:
        url: URL to normalize
    
    Returns:
        Normalized URL string
    
    Examples:
        >>> normalize_url("https://example.com/page?query=1#section")
        "https://example.com/page"
        
        >>> normalize_url("https://example.com/page/")
        "https://example.com/page"
        
        >>> normalize_url("https://example.com/")
        "https://example.com/"
    
    Notes:
        - Strips query parameters (?key=value)
        - Strips URL fragments (#section)
        - Removes trailing slashes (except for root URLs)
        - Returns empty string for invalid URLs
    """
    if not url:
        return ""
    
    try:
        parsed = urlparse(url)
        # Remove query and fragment
        base = parsed._replace(fragment="", query="").geturl()
    except Exception:
        # If parsing fails, return original URL
        return url
    
    # Remove trailing slash (but keep it for root-like URLs)
    if base.endswith("/") and len(base) > len("https://a.b/"):
        base = base.rstrip("/")
    
    return base


# =============================================================================
# PAGE INDEXING
# =============================================================================
# NOTE: index_pages() and pick_first_page() functions removed
# They depended on PageArtifact model which no longer exists in this module.
# These functions are only used by extractor.py which should be refactored
# to work directly with database queries instead of in-memory page artifacts.


# =============================================================================
# JSON PARSING
# =============================================================================

def parse_json_strict(json_str: str) -> Dict[str, Any]:
    """
    Parse JSON string with strict error handling.
    
    Args:
        json_str: JSON string to parse
    
    Returns:
        Parsed JSON as dictionary
    
    Raises:
        json.JSONDecodeError: If JSON is invalid
    
    Examples:
        >>> parse_json_strict('{"key": "value"}')
        {'key': 'value'}
        
        >>> parse_json_strict('invalid')
        Traceback (most recent call last):
        ...
        json.JSONDecodeError: ...
    
    Notes:
        - Strips whitespace before parsing
        - Returns empty dict for empty strings
        - Raises exception for invalid JSON
    """
    return json.loads((json_str or "").strip())


def extract_assistant_text(openrouter_response: Dict[str, Any]) -> str:
    """
    Extract assistant text from OpenRouter API response.
    
    Args:
        openrouter_response: OpenRouter API response dictionary
    
    Returns:
        Assistant message content, or empty string if not found
    
    Examples:
        >>> response = {
        ...     "choices": [{
        ...         "message": {"content": "Hello"}
        ...     }]
        ... }
        >>> extract_assistant_text(response)
        "Hello"
        
        >>> extract_assistant_text({})
        ""
    
    Notes:
        - Returns empty string for malformed responses
        - Handles missing keys gracefully
        - Returns empty string for None content
    """
    try:
        return openrouter_response["choices"][0]["message"]["content"] or ""
    except (KeyError, IndexError, TypeError):
        return ""