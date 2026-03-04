"""
Text Normalization Utilities for Email Extraction

This module provides functions for cleaning and normalizing text to improve
email extraction accuracy. It handles:
- HTML entity decoding
- Zero-width character removal
- Email obfuscation pattern replacement
- Whitespace normalization

The normalization process is designed to handle various anti-scraping techniques
commonly used on websites to hide email addresses from bots.

Author: N8N FounderStories Team
Last Modified: 2026-02-18
"""

from __future__ import annotations

import html as _html
import re
from typing import Optional

from ..config import config


# ============================================================================
# COMPILED REGEX PATTERNS (for performance)
# ============================================================================

# Zero-width characters that should be removed
# These are often used to break up email addresses to prevent scraping
_ZERO_WIDTH_CHARS = re.compile(r"[\u200B-\u200D\uFEFF\u00AD]")

# Trailing punctuation that often sticks to emails in text
_TRAILING_PUNCT = re.compile(r"[)\].,;:!]+$")

# Leading punctuation that might appear before emails
_LEADING_PUNCT = re.compile(r"^[(\[.,;:!]+")

# Compile obfuscation patterns from config
_OBFUSCATION_PATTERNS = [
    (re.compile(pattern, re.IGNORECASE), replacement)
    for pattern, replacement in config.normalization.obfuscation_patterns
]


# ============================================================================
# TEXT NORMALIZATION FUNCTIONS
# ============================================================================

def normalize_text(text: Optional[str]) -> str:
    """
    Normalize text for email extraction by handling various obfuscation techniques.
    
    This function performs the following operations in order:
    1. HTML entity decoding (e.g., &amp; -> &, &#64; -> @)
    2. Zero-width character removal
    3. Newline/carriage return normalization
    4. Email obfuscation pattern replacement (e.g., [at] -> @, [dot] -> .)
    5. Additional text replacements from config
    
    Args:
        text: Raw text that may contain obfuscated emails
        
    Returns:
        Normalized text with obfuscations resolved
        
    Examples:
        >>> normalize_text("contact [at] example [dot] com")
        'contact @ example . com'
        
        >>> normalize_text("info&#64;company.de")
        'info@company.de'
        
        >>> normalize_text("hello\u200B@\u200Bworld.com")
        'hello@world.com'
    """
    if not text:
        return ""
    
    # Step 1: Decode HTML entities
    # This handles cases like &#64; (@ symbol) or &amp; (& symbol)
    normalized = _html.unescape(text)
    
    # Step 2: Remove zero-width characters
    # These are invisible characters used to break up email patterns
    normalized = _ZERO_WIDTH_CHARS.sub("", normalized)
    
    # Step 3: Normalize whitespace
    # Replace newlines and carriage returns with spaces for consistent processing
    normalized = normalized.replace("\r", " ").replace("\n", " ")
    
    # Step 4: Apply regex-based obfuscation pattern replacements
    # This handles patterns like [at], (at), [dot], (dot), etc.
    for pattern, replacement in _OBFUSCATION_PATTERNS:
        normalized = pattern.sub(replacement, normalized)
    
    # Step 5: Apply simple text replacements
    # This handles remaining obfuscation patterns not caught by regex
    for old, new in config.normalization.text_replacements.items():
        normalized = normalized.replace(old, new)
    
    return normalized


def clean_email_text(email: Optional[str]) -> str:
    """
    Clean and normalize an extracted email address.
    
    This function:
    1. Strips whitespace
    2. Converts to lowercase
    3. Removes leading/trailing punctuation
    4. Strips boundary characters from config
    
    Args:
        email: Raw email address that may have extra characters
        
    Returns:
        Cleaned email address in lowercase
        
    Examples:
        >>> clean_email_text("  Contact@Example.COM  ")
        'contact@example.com'
        
        >>> clean_email_text("<info@company.de>")
        'info@company.de'
        
        >>> clean_email_text("(sales@business.com)")
        'sales@business.com'
    """
    if not email:
        return ""
    
    # Step 1: Basic whitespace stripping and lowercase conversion
    cleaned = email.strip().lower()
    
    # Step 2: Remove trailing punctuation (e.g., "email@example.com," -> "email@example.com")
    cleaned = _TRAILING_PUNCT.sub("", cleaned)
    
    # Step 3: Remove leading punctuation (e.g., "(email@example.com" -> "email@example.com")
    cleaned = _LEADING_PUNCT.sub("", cleaned)
    
    # Step 4: Strip boundary characters from config
    # This handles various brackets, quotes, and other wrapper characters
    cleaned = cleaned.strip(config.normalization.boundary_chars)
    
    return cleaned


def remove_percent_encoding(text: Optional[str]) -> str:
    """
    Remove percent-encoding (URL encoding) from text.
    
    This is useful for handling mailto: links that may contain encoded characters.
    For example: "info%20at%20example.com" -> "info at example.com"
    
    Args:
        text: Text that may contain percent-encoded characters
        
    Returns:
        Decoded text
        
    Examples:
        >>> remove_percent_encoding("info%40example.com")
        'info@example.com'
        
        >>> remove_percent_encoding("hello%20world")
        'hello world'
    """
    if not text:
        return ""
    
    from urllib.parse import unquote
    
    try:
        return unquote(text)
    except Exception:
        # If decoding fails, return original text
        return text


def extract_text_between(
    text: str,
    start_marker: str,
    end_marker: str,
    include_markers: bool = False
) -> list[str]:
    """
    Extract all text segments between start and end markers.
    
    This is useful for extracting content from specific HTML tags or patterns.
    
    Args:
        text: Source text to search
        start_marker: Starting marker/delimiter
        end_marker: Ending marker/delimiter
        include_markers: Whether to include markers in results
        
    Returns:
        List of extracted text segments
        
    Examples:
        >>> extract_text_between("<a>link1</a><a>link2</a>", "<a>", "</a>")
        ['link1', 'link2']
        
        >>> extract_text_between("(email1)(email2)", "(", ")", include_markers=True)
        ['(email1)', '(email2)']
    """
    if not text or not start_marker or not end_marker:
        return []
    
    results = []
    start_idx = 0
    
    while True:
        # Find next start marker
        start_pos = text.find(start_marker, start_idx)
        if start_pos == -1:
            break
        
        # Find corresponding end marker
        end_pos = text.find(end_marker, start_pos + len(start_marker))
        if end_pos == -1:
            break
        
        # Extract segment
        if include_markers:
            segment = text[start_pos:end_pos + len(end_marker)]
        else:
            segment = text[start_pos + len(start_marker):end_pos]
        
        results.append(segment)
        start_idx = end_pos + len(end_marker)
    
    return results


def truncate_text(text: Optional[str], max_length: Optional[int] = None) -> str:
    """
    Truncate text to maximum length if specified.
    
    This helps prevent processing extremely large text blocks that could
    cause performance issues.
    
    Args:
        text: Text to truncate
        max_length: Maximum length (uses config default if None)
        
    Returns:
        Truncated text
        
    Examples:
        >>> truncate_text("hello world", 5)
        'hello'
        
        >>> truncate_text("short", 100)
        'short'
    """
    if not text:
        return ""
    
    if max_length is None:
        max_length = config.limits.max_text_length
    
    if len(text) <= max_length:
        return text
    
    return text[:max_length]


def normalize_whitespace(text: Optional[str]) -> str:
    """
    Normalize whitespace in text by collapsing multiple spaces into one.
    
    This helps clean up text that may have irregular spacing.
    
    Args:
        text: Text with potentially irregular whitespace
        
    Returns:
        Text with normalized whitespace
        
    Examples:
        >>> normalize_whitespace("hello    world")
        'hello world'
        
        >>> normalize_whitespace("  multiple   spaces  ")
        'multiple spaces'
    """
    if not text:
        return ""
    
    # Replace multiple spaces with single space
    normalized = re.sub(r'\s+', ' ', text)
    
    # Strip leading/trailing whitespace
    return normalized.strip()


# ============================================================================
# BATCH PROCESSING UTILITIES
# ============================================================================

def normalize_text_batch(texts: list[str]) -> list[str]:
    """
    Normalize multiple text strings in batch.
    
    This is more efficient than calling normalize_text() individually
    when processing many texts.
    
    Args:
        texts: List of texts to normalize
        
    Returns:
        List of normalized texts
    """
    return [normalize_text(text) for text in texts]


def clean_email_batch(emails: list[str]) -> list[str]:
    """
    Clean multiple email addresses in batch.
    
    Args:
        emails: List of emails to clean
        
    Returns:
        List of cleaned emails
    """
    return [clean_email_text(email) for email in emails]