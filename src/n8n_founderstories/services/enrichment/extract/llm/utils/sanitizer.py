"""
Quote sanitization for LLM extraction to prevent Pydantic validation errors.

This module provides utilities to safely truncate Evidence.quote fields that exceed
the maximum allowed length (300 characters), preventing ValidationError crashes.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

# Maximum length for Evidence.quote field (from models.py)
MAX_EVIDENCE_QUOTE_LEN = 300


def truncate_quote(q: Optional[str], max_len: int = MAX_EVIDENCE_QUOTE_LEN) -> Optional[str]:
    """
    Safely truncate a quote string to the maximum allowed length.
    
    Args:
        q: Quote string to truncate (can be None)
        max_len: Maximum allowed length (default: 300)
    
    Returns:
        Truncated quote string, or None if input was falsy
    
    Examples:
        >>> truncate_quote(None)
        None
        >>> truncate_quote("")
        ""
        >>> truncate_quote("short")
        "short"
        >>> truncate_quote("x" * 500)
        "xxx...xxx"  # 300 chars
    """
    if not q:
        return q
    
    # Strip whitespace first
    q = q.strip()
    
    # Return as-is if within limit
    if len(q) <= max_len:
        return q
    
    # Truncate to max_len
    return q[:max_len]


def sanitize_extraction(
    extraction: Any,
    logger_instance: Optional[logging.Logger] = None,
    domain: Optional[str] = None,
) -> Any:
    """
    Recursively sanitize an extraction object by truncating quote fields.
    
    This function walks through nested dicts, lists, and objects looking for
    fields named "quote" and truncates them to MAX_EVIDENCE_QUOTE_LEN.
    
    CRITICAL: This must be called BEFORE Pydantic validation to prevent
    ValidationError crashes.
    
    Args:
        extraction: Raw extraction data (dict, list, or any object)
        logger_instance: Logger for warnings (default: module logger)
        domain: Domain being processed (for logging context)
    
    Returns:
        Sanitized extraction object (same type as input)
    
    Examples:
        >>> data = {"evidence": {"quote": "x" * 500, "url": "https://a.com"}}
        >>> sanitized = sanitize_extraction(data)
        >>> len(sanitized["evidence"]["quote"])
        300
    """
    log = logger_instance or logger
    
    # Handle None
    if extraction is None:
        return extraction
    
    # Handle dict
    if isinstance(extraction, dict):
        result = {}
        for key, value in extraction.items():
            # Truncate quote fields
            if key == "quote" and isinstance(value, str):
                original_len = len(value)
                truncated = truncate_quote(value)
                
                # Log warning if truncation occurred
                if truncated and len(truncated) < original_len:
                    log.warning(
                        "LLM | QUOTE_TRUNCATED | domain=%s | original_len=%d | max_len=%d",
                        domain or "unknown",
                        original_len,
                        MAX_EVIDENCE_QUOTE_LEN,
                    )
                
                result[key] = truncated
            else:
                # Recursively sanitize nested structures
                result[key] = sanitize_extraction(value, log, domain)
        
        return result
    
    # Handle list
    if isinstance(extraction, list):
        return [sanitize_extraction(item, log, domain) for item in extraction]
    
    # Handle other types (str, int, bool, etc.) - return as-is
    return extraction


def sanitize_evidence_dict(
    evidence_dict: Dict[str, Any],
    logger_instance: Optional[logging.Logger] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Sanitize a single evidence dictionary before Evidence() construction.
    
    This is a convenience wrapper around sanitize_extraction for the common
    case of sanitizing a single evidence dict.
    
    Args:
        evidence_dict: Evidence dictionary with 'quote' and 'url' keys
        logger_instance: Logger for warnings (default: module logger)
        domain: Domain being processed (for logging context)
    
    Returns:
        Sanitized evidence dictionary
    
    Examples:
        >>> ev = {"quote": "x" * 500, "url": "https://a.com"}
        >>> sanitized = sanitize_evidence_dict(ev)
        >>> len(sanitized["quote"])
        300
    """
    return sanitize_extraction(evidence_dict, logger_instance, domain)