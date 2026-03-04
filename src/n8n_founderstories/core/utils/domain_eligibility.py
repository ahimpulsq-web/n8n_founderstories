"""
Domain eligibility utilities for filtering unwanted domains.

This module provides utilities to determine if a domain should be
included in the lead generation system (DB storage and Sheets export).
"""

from typing import Optional
from .domain import normalize_domain


def is_social_domain(domain_or_url: Optional[str]) -> bool:
    """
    Returns True if the domain_or_url belongs to Facebook or Instagram
    (including any subdomain).
    
    This function is used to filter out social media domains from:
    - Database storage
    - Google Sheets exports
    
    The check is performed after domain normalization, so it works with
    any input format (full URLs, domains with www, etc.).
    
    Examples:
        >>> is_social_domain("facebook.com")
        True
        >>> is_social_domain("www.facebook.com")
        True
        >>> is_social_domain("m.facebook.com")
        True
        >>> is_social_domain("https://instagram.com/p/123")
        True
        >>> is_social_domain("example.com")
        False
        >>> is_social_domain(None)
        False
        >>> is_social_domain("")
        False
    
    Args:
        domain_or_url: Domain or URL to check (can be None or empty)
        
    Returns:
        True if the domain is Facebook or Instagram (including subdomains),
        False otherwise (including when normalization fails)
    """
    # Normalize the domain first
    normalized = normalize_domain(domain_or_url)
    
    # If normalization failed, it's not a valid domain
    if not normalized:
        return False
    
    # Check if it's Facebook or Instagram (including subdomains)
    # After normalization, www. is already removed, so we only need to check:
    # 1. Exact match: "facebook.com" or "instagram.com"
    # 2. Subdomain: "*.facebook.com" or "*.instagram.com"
    
    if normalized == "facebook.com" or normalized.endswith(".facebook.com"):
        return True
    
    if normalized == "instagram.com" or normalized.endswith(".instagram.com"):
        return True
    
    return False