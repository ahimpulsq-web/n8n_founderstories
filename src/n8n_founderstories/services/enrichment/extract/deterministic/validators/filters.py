"""
Email Filtering Module

This module provides filtering logic for email addresses based on deny lists,
patterns, and business rules. It helps remove:
- System/automated emails (noreply@, bounce@, etc.)
- Asset-related false positives (logo@2x.png, style@import.css, etc.)
- Spam traps and honeypots
- Low-quality email addresses

IMPORTANT: This is the SINGLE SOURCE OF TRUTH for all business policy filtering.
The validator module (email_validator.py) handles ONLY structural validation.

Author: N8N FounderStories Team
Last Modified: 2026-02-19
"""

from __future__ import annotations

import re
from typing import Optional

from ..config import config
from ..utils.domain_utils import extract_domain_from_email, extract_localpart_from_email


# ============================================================================
# COMPILED REGEX PATTERNS
# ============================================================================

# Pattern to detect image density indicators (e.g., "logo2x", "icon-2x", "image@2x")
# Matches patterns like: 2x, 3x, @2x, -2x, _2x (with or without word boundaries)
_IMAGE_DENSITY_PATTERN = re.compile(r"[@\-_]?\d+x\b", re.IGNORECASE)


# ============================================================================
# SYSTEM EMAIL DETECTION
# ============================================================================

def is_system_email(email: Optional[str]) -> bool:
    """
    Check if an email is a system/automated email address.
    
    System emails include:
    - noreply@, no-reply@
    - bounce@, bounces@
    - mailer-daemon@
    - postmaster@
    - abuse@, spam@
    
    These are typically not useful for business contact purposes.
    
    Args:
        email: Email address to check
        
    Returns:
        True if email is a system email, False otherwise
        
    Examples:
        >>> is_system_email("noreply@example.com")
        True
        
        >>> is_system_email("contact@example.com")
        False
        
        >>> is_system_email("bounce@mail.example.com")
        True
    """
    if not email:
        return False
    
    local_part = extract_localpart_from_email(email)
    if not local_part:
        return False
    
    # Check against deny list from config
    return local_part in config.validation.deny_localpart_prefixes


def is_asset_email(email: Optional[str]) -> bool:
    """
    Check if an email is actually a file asset reference.
    
    This is the SINGLE SOURCE OF TRUTH for asset detection.
    It detects false positives like:
    - logo@2x.png (asset TLD)
    - icon-2x.svg (asset TLD)
    - style@import.css (asset TLD)
    - image@2x.com (image density pattern in domain)
    - logo2x@example.com (image density pattern in local part)
    
    Args:
        email: Email address to check
        
    Returns:
        True if email appears to be an asset reference, False otherwise
        
    Examples:
        >>> is_asset_email("logo@2x.png")
        True
        
        >>> is_asset_email("contact@example.com")
        False
        
        >>> is_asset_email("image@2x.svg")
        True
        
        >>> is_asset_email("logo2x@example.com")
        True
        
        >>> is_asset_email("icon-2x@company.com")
        True
        
        >>> is_asset_email("image@2x.com")
        True
    """
    if not email:
        return False
    
    # Extract parts
    local_part = extract_localpart_from_email(email)
    domain_part = extract_domain_from_email(email)
    
    if not local_part or not domain_part:
        return False
    
    # Check 1: Image density patterns in local part
    # Examples: "logo2x", "icon-2x"
    if _IMAGE_DENSITY_PATTERN.search(local_part):
        return True
    
    # Check 2: Image density patterns in domain
    # Examples: "image@2x.com" where domain is "2x.com"
    if _IMAGE_DENSITY_PATTERN.search(domain_part):
        return True
    
    # Check 3: Domain ends with asset extension
    for suffix in config.validation.deny_domain_suffixes:
        if domain_part.endswith(suffix):
            return True
    
    # Check 4: TLD is an asset extension
    domain_parts = domain_part.split(".")
    if domain_parts:
        tld = domain_parts[-1]
        if tld in config.validation.asset_tlds:
            return True
    
    return False


def is_generic_email(email: Optional[str]) -> bool:
    """
    Check if an email uses a generic/free email provider.
    
    This can be useful for filtering out personal emails when looking
    for business contacts.
    
    Args:
        email: Email address to check
        
    Returns:
        True if email uses a generic provider, False otherwise
        
    Examples:
        >>> is_generic_email("user@gmail.com")
        True
        
        >>> is_generic_email("contact@company.com")
        False
    """
    if not email:
        return False
    
    domain_part = extract_domain_from_email(email)
    if not domain_part:
        return False
    
    # Common free email providers
    generic_providers = {
        "gmail.com", "googlemail.com",
        "yahoo.com", "yahoo.de",
        "hotmail.com", "hotmail.de",
        "outlook.com", "outlook.de",
        "aol.com",
        "icloud.com", "me.com",
        "protonmail.com", "proton.me",
        "gmx.de", "gmx.net",
        "web.de",
        "t-online.de",
        "freenet.de",
    }
    
    return domain_part in generic_providers


# ============================================================================
# COMPREHENSIVE FILTERING
# ============================================================================

def should_filter_email(
    email: Optional[str],
    filter_system: bool = True,
    filter_assets: bool = True,
    filter_generic: bool = False,
) -> bool:
    """
    Determine if an email should be filtered out based on various criteria.
    
    This is the main filtering function that combines multiple checks.
    
    Args:
        email: Email address to check
        filter_system: Whether to filter system emails
        filter_assets: Whether to filter asset references
        filter_generic: Whether to filter generic email providers
        
    Returns:
        True if email should be filtered out, False if it should be kept
        
    Examples:
        >>> should_filter_email("noreply@example.com")
        True
        
        >>> should_filter_email("contact@example.com")
        False
        
        >>> should_filter_email("user@gmail.com", filter_generic=True)
        True
    """
    if not email:
        return True
    
    # Check system emails
    if filter_system and is_system_email(email):
        return True
    
    # Check asset references
    if filter_assets and is_asset_email(email):
        return True
    
    # Check generic providers
    if filter_generic and is_generic_email(email):
        return True
    
    return False


def filter_email_list(
    emails: list[str],
    filter_system: bool = True,
    filter_assets: bool = True,
    filter_generic: bool = False,
) -> list[str]:
    """
    Filter a list of emails based on various criteria.
    
    Args:
        emails: List of email addresses
        filter_system: Whether to filter system emails
        filter_assets: Whether to filter asset references
        filter_generic: Whether to filter generic email providers
        
    Returns:
        Filtered list of email addresses
        
    Examples:
        >>> emails = ["contact@example.com", "noreply@example.com", "logo@2x.png"]
        >>> filter_email_list(emails)
        ['contact@example.com']
    """
    return [
        email for email in emails
        if not should_filter_email(email, filter_system, filter_assets, filter_generic)
    ]


# ============================================================================
# DOMAIN-BASED FILTERING
# ============================================================================

def filter_by_domain(
    emails: list[str],
    allowed_domains: Optional[list[str]] = None,
    blocked_domains: Optional[list[str]] = None,
) -> list[str]:
    """
    Filter emails based on domain allow/block lists.
    
    Args:
        emails: List of email addresses
        allowed_domains: If provided, only keep emails from these domains
        blocked_domains: If provided, remove emails from these domains
        
    Returns:
        Filtered list of email addresses
        
    Examples:
        >>> emails = ["user@example.com", "admin@other.com", "info@example.com"]
        >>> filter_by_domain(emails, allowed_domains=["example.com"])
        ['user@example.com', 'info@example.com']
        
        >>> filter_by_domain(emails, blocked_domains=["other.com"])
        ['user@example.com', 'info@example.com']
    """
    from ..utils.domain_utils import normalize_domain
    
    result = []
    
    # Normalize domain lists
    allowed = {normalize_domain(d) for d in (allowed_domains or [])} if allowed_domains else None
    blocked = {normalize_domain(d) for d in (blocked_domains or [])} if blocked_domains else set()
    
    for email in emails:
        domain = extract_domain_from_email(email)
        if not domain:
            continue
        
        # Check blocked list first
        if domain in blocked:
            continue
        
        # Check allowed list if provided
        if allowed is not None and domain not in allowed:
            continue
        
        result.append(email)
    
    return result


def filter_by_company_domain(
    emails: list[str],
    company_domain: str,
    prefer_company_domain: bool = True,
) -> list[str]:
    """
    Filter emails based on company domain matching.
    
    Args:
        emails: List of email addresses
        company_domain: The company's domain
        prefer_company_domain: If True, prioritize emails from company domain
        
    Returns:
        Filtered list of email addresses
        
    Examples:
        >>> emails = ["info@example.com", "contact@other.com", "sales@example.com"]
        >>> filter_by_company_domain(emails, "example.com", prefer_company_domain=True)
        ['info@example.com', 'sales@example.com']
    """
    from ..utils.domain_utils import email_domain_matches, normalize_domain
    
    company_domain_norm = normalize_domain(company_domain)
    
    if not prefer_company_domain:
        return emails
    
    # Separate emails by domain match
    matching = []
    non_matching = []
    
    for email in emails:
        if email_domain_matches(email, company_domain_norm):
            matching.append(email)
        else:
            non_matching.append(email)
    
    # If we have matching emails, return only those
    # Otherwise, return all emails
    return matching if matching else non_matching


# ============================================================================
# QUALITY-BASED FILTERING
# ============================================================================

def filter_by_quality_score(
    emails: list[str],
    min_score: int = 50,
) -> list[str]:
    """
    Filter emails based on quality score threshold.
    
    Args:
        emails: List of email addresses
        min_score: Minimum quality score (0-100)
        
    Returns:
        Filtered list of email addresses
    """
    from .email_validator import calculate_email_quality_score
    
    return [
        email for email in emails
        if calculate_email_quality_score(email) >= min_score
    ]


def filter_duplicates(emails: list[str], preserve_order: bool = True) -> list[str]:
    """
    Remove duplicate emails while optionally preserving order.
    
    Args:
        emails: List of email addresses (may contain duplicates)
        preserve_order: Whether to preserve the original order
        
    Returns:
        List of unique email addresses
        
    Examples:
        >>> filter_duplicates(["a@x.com", "b@x.com", "a@x.com"])
        ['a@x.com', 'b@x.com']
    """
    if not preserve_order:
        return list(set(emails))
    
    seen = set()
    result = []
    
    for email in emails:
        email_lower = email.lower().strip()
        if email_lower not in seen:
            seen.add(email_lower)
            result.append(email)
    
    return result


# ============================================================================
# COMBINED FILTERING PIPELINE
# ============================================================================

def apply_standard_filters(
    emails: list[str],
    company_domain: Optional[str] = None,
) -> list[str]:
    """
    Apply standard filtering pipeline to email list.
    
    This applies the most common filters in the recommended order:
    1. Remove duplicates
    2. Filter system emails
    3. Filter asset references
    4. Filter by quality score
    5. Optionally prefer company domain
    
    Args:
        emails: List of email addresses
        company_domain: Optional company domain for domain matching
        
    Returns:
        Filtered list of email addresses
        
    Examples:
        >>> emails = ["info@example.com", "noreply@example.com", "info@example.com"]
        >>> apply_standard_filters(emails)
        ['info@example.com']
    """
    # Step 1: Remove duplicates
    filtered = filter_duplicates(emails)
    
    # Step 2: Apply standard filters
    filtered = filter_email_list(
        filtered,
        filter_system=True,
        filter_assets=True,
        filter_generic=False,
    )
    
    # Step 3: Filter by quality
    filtered = filter_by_quality_score(filtered, min_score=50)
    
    # Step 4: Prefer company domain if provided
    if company_domain:
        filtered = filter_by_company_domain(
            filtered,
            company_domain,
            prefer_company_domain=True,
        )
    
    return filtered