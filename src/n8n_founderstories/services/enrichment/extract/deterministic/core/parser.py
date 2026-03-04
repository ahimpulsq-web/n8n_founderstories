"""
Email Parser Module

This module handles the extraction of email addresses from text content.
It includes:
- Regex-based email extraction
- Mailto link parsing
- Percent-encoding handling
- Text normalization integration

Author: N8N FounderStories Team
Last Modified: 2026-02-18
"""

from __future__ import annotations

import re
from typing import Optional
from urllib.parse import unquote

from ..config import config
from ..utils.text_normalizer import normalize_text, clean_email_text
from ..validators.email_validator import is_plausible_email


# ============================================================================
# COMPILED REGEX PATTERNS
# ============================================================================

# Standard email pattern (RFC 5322 simplified)
_EMAIL_REGEX = re.compile(
    r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b"
)

# Mailto link pattern
_MAILTO_REGEX = re.compile(
    r"mailto:([^\"\'\s<>]+)",
    re.IGNORECASE
)


# ============================================================================
# EMAIL EXTRACTION FROM TEXT
# ============================================================================

def parse_emails_from_text(text: Optional[str], dedupe: bool = True) -> list[str]:
    """
    Extract all email addresses from text content.
    
    This function:
    1. Normalizes text (handles obfuscation, HTML entities, etc.)
    2. Extracts mailto: links
    3. Extracts email patterns using regex
    4. Handles percent-encoding
    5. Validates and filters results
    6. Returns deduplicated list (if dedupe=True)
    
    Args:
        text: Raw text content that may contain emails
        dedupe: If True, deduplicate results while preserving order (default: True)
        
    Returns:
        List of extracted email addresses (lowercase, optionally deduplicated)
        
    Examples:
        >>> parse_emails_from_text("Contact us at info@example.com")
        ['info@example.com']
        
        >>> parse_emails_from_text("Email: contact [at] example [dot] com")
        ['contact@example.com']
        
        >>> parse_emails_from_text('<a href="mailto:sales@company.de">Email</a>')
        ['sales@company.de']
    """
    if not text:
        return []
    
    # Step 1: Normalize text to handle obfuscation
    normalized = normalize_text(text)
    
    # Step 2: Extract candidates from various sources
    candidates: list[str] = []
    
    # Extract from mailto: links (higher priority)
    candidates.extend(extract_mailto_links(normalized))
    
    # Extract using regex pattern
    candidates.extend(_EMAIL_REGEX.findall(normalized) or [])
    
    # Step 3: Clean and validate each candidate
    cleaned: list[str] = []
    
    for candidate in candidates:
        # Basic cleaning
        email = clean_email_text(candidate)
        if not email:
            continue
        
        # Handle percent-encoding
        # If email contains %, decode it and re-extract emails
        if "%" in email:
            decoded = unquote(email)
            # Extract all emails from decoded string
            decoded_emails = _EMAIL_REGEX.findall(decoded) or []
            for decoded_email in decoded_emails:
                decoded_email = clean_email_text(decoded_email)
                if decoded_email and is_plausible_email(decoded_email):
                    cleaned.append(decoded_email)
            continue
        
        # If candidate still contains junk, try to extract email substring
        match = _EMAIL_REGEX.search(email)
        if match:
            email = match.group(0).lower()
        
        # Final validation
        if is_plausible_email(email):
            cleaned.append(email)
    
    # Step 4: Deduplicate while preserving order (if requested)
    if dedupe:
        return _deduplicate_preserve_order(cleaned)
    
    return cleaned


def extract_mailto_links(text: Optional[str]) -> list[str]:
    """
    Extract email addresses from mailto: links in text.
    
    This handles various mailto: formats:
    - Simple: mailto:info@example.com
    - With subject: mailto:info@example.com?subject=Hello
    - Percent-encoded: mailto:info%40example.com
    
    Args:
        text: Text content that may contain mailto: links
        
    Returns:
        List of extracted email addresses
        
    Examples:
        >>> extract_mailto_links('<a href="mailto:info@example.com">Email</a>')
        ['info@example.com']
        
        >>> extract_mailto_links('mailto:contact@example.com?subject=Hello')
        ['contact@example.com']
    """
    if not text:
        return []
    
    results: list[str] = []
    
    for match in _MAILTO_REGEX.finditer(text):
        target = (match.group(1) or "").strip()
        
        # Remove query parameters (subject, body, etc.)
        target = target.split("?", 1)[0].strip()
        
        # Decode percent-encoding
        target = unquote(target).strip()
        
        if not target:
            continue
        
        # Sometimes mailto contains extra text; extract first valid email
        email_match = _EMAIL_REGEX.search(target)
        if email_match:
            results.append(email_match.group(0))
        else:
            # Fallback: keep raw target (might still be a plain email)
            results.append(target)
    
    return results


# ============================================================================
# BATCH PROCESSING
# ============================================================================

def parse_emails_from_texts(texts: list[str]) -> list[str]:
    """
    Extract emails from multiple text blocks.
    
    Args:
        texts: List of text content blocks
        
    Returns:
        Combined list of extracted emails (deduplicated)
        
    Examples:
        >>> texts = ["Contact: info@example.com", "Sales: sales@example.com"]
        >>> parse_emails_from_texts(texts)
        ['info@example.com', 'sales@example.com']
    """
    all_emails: list[str] = []
    
    for text in texts:
        emails = parse_emails_from_text(text)
        all_emails.extend(emails)
    
    return _deduplicate_preserve_order(all_emails)


def parse_emails_with_context(
    text: Optional[str],
    context_chars: int = 50
) -> list[tuple[str, str]]:
    """
    Extract emails along with surrounding context.
    
    This is useful for debugging or understanding where emails were found.
    
    Args:
        text: Text content
        context_chars: Number of characters to include before/after email
        
    Returns:
        List of (email, context) tuples
        
    Examples:
        >>> text = "Please contact us at info@example.com for more information"
        >>> parse_emails_with_context(text, context_chars=20)
        [('info@example.com', 'contact us at info@example.com for more')]
    """
    if not text:
        return []
    
    normalized = normalize_text(text)
    results: list[tuple[str, str]] = []
    
    for match in _EMAIL_REGEX.finditer(normalized):
        email = match.group(0).lower()
        
        # Extract context
        start = max(0, match.start() - context_chars)
        end = min(len(normalized), match.end() + context_chars)
        context = normalized[start:end].strip()
        
        if is_plausible_email(email):
            results.append((email, context))
    
    return results


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _deduplicate_preserve_order(items: list[str]) -> list[str]:
    """
    Remove duplicates from list while preserving order.
    
    Args:
        items: List of strings (may contain duplicates)
        
    Returns:
        List with duplicates removed, order preserved
        
    Examples:
        >>> _deduplicate_preserve_order(['a', 'b', 'a', 'c'])
        ['a', 'b', 'c']
    """
    seen = set()
    result: list[str] = []
    
    for item in items:
        item = (item or "").strip().lower()
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    
    return result


def count_emails_in_text(text: Optional[str]) -> int:
    """
    Count the number of email addresses in text.
    
    This is faster than extracting all emails when you only need the count.
    
    Args:
        text: Text content
        
    Returns:
        Number of email addresses found
        
    Examples:
        >>> count_emails_in_text("Contact: info@example.com, sales@example.com")
        2
    """
    if not text:
        return 0
    
    normalized = normalize_text(text)
    matches = _EMAIL_REGEX.findall(normalized)
    
    # Filter to only plausible emails
    plausible = [m for m in matches if is_plausible_email(m)]
    
    return len(set(plausible))


def has_emails(text: Optional[str]) -> bool:
    """
    Quick check if text contains any email addresses.
    
    Args:
        text: Text content
        
    Returns:
        True if text contains at least one email
        
    Examples:
        >>> has_emails("Contact us at info@example.com")
        True
        
        >>> has_emails("No emails here")
        False
    """
    return count_emails_in_text(text) > 0


def extract_email_domains(text: Optional[str]) -> list[str]:
    """
    Extract unique domains from all emails in text.
    
    Args:
        text: Text content
        
    Returns:
        List of unique domains
        
    Examples:
        >>> extract_email_domains("info@example.com, sales@example.com, admin@other.com")
        ['example.com', 'other.com']
    """
    from ..utils.domain_utils import extract_domain_from_email
    
    emails = parse_emails_from_text(text)
    domains = [extract_domain_from_email(email) for email in emails]
    
    # Remove empty and deduplicate
    return list(set(d for d in domains if d))


# ============================================================================
# ADVANCED EXTRACTION
# ============================================================================

def extract_emails_near_keywords(
    text: Optional[str],
    keywords: list[str],
    max_distance: int = 100
) -> list[str]:
    """
    Extract emails that appear near specific keywords.
    
    This is useful for finding emails in specific contexts, like
    "contact", "email", "reach us", etc.
    
    Args:
        text: Text content
        keywords: List of keywords to search for
        max_distance: Maximum character distance from keyword
        
    Returns:
        List of emails found near keywords
        
    Examples:
        >>> text = "For support, email us at support@example.com"
        >>> extract_emails_near_keywords(text, ["support", "email"])
        ['support@example.com']
    """
    if not text or not keywords:
        return []
    
    normalized = normalize_text(text).lower()
    results: list[str] = []
    
    # Find all email positions
    email_positions = [
        (match.group(0), match.start(), match.end())
        for match in _EMAIL_REGEX.finditer(normalized)
    ]
    
    # Find all keyword positions
    keyword_positions = []
    for keyword in keywords:
        keyword_lower = keyword.lower()
        start = 0
        while True:
            pos = normalized.find(keyword_lower, start)
            if pos == -1:
                break
            keyword_positions.append((pos, pos + len(keyword_lower)))
            start = pos + 1
    
    # Find emails near keywords
    for email, email_start, email_end in email_positions:
        for kw_start, kw_end in keyword_positions:
            # Calculate distance
            if email_start >= kw_end:
                distance = email_start - kw_end
            elif kw_start >= email_end:
                distance = kw_start - email_end
            else:
                distance = 0  # Overlapping
            
            if distance <= max_distance:
                if is_plausible_email(email):
                    results.append(email.lower())
                break
    
    return _deduplicate_preserve_order(results)