"""
Domain Utility Functions for Email Extraction

This module provides utilities for working with domain names in the context
of email extraction and validation. It includes functions for:
- Domain normalization
- Domain extraction from emails
- Domain matching and comparison
- URL parsing and domain extraction

Author: N8N FounderStories Team
Last Modified: 2026-02-18
"""

from __future__ import annotations

from typing import Optional
from urllib.parse import urlparse


# ============================================================================
# DOMAIN NORMALIZATION
# ============================================================================

def normalize_domain(domain: Optional[str]) -> str:
    """
    Normalize a domain name to a canonical form.
    
    This function:
    1. Strips whitespace
    2. Converts to lowercase
    3. Removes protocol prefixes (http://, https://)
    4. Removes path components
    5. Removes 'www.' prefix
    6. Removes trailing dots
    
    Args:
        domain: Raw domain string (may include protocol, path, etc.)
        
    Returns:
        Normalized domain name
        
    Examples:
        >>> normalize_domain("https://www.example.com/path")
        'example.com'
        
        >>> normalize_domain("HTTP://EXAMPLE.COM")
        'example.com'
        
        >>> normalize_domain("www.example.com.")
        'example.com'
        
        >>> normalize_domain("  Example.COM  ")
        'example.com'
    """
    if not domain:
        return ""
    
    # Step 1: Basic cleanup
    normalized = domain.strip().lower()
    
    # Step 2: Remove protocol prefixes
    normalized = normalized.replace("https://", "").replace("http://", "")
    
    # Step 3: Remove path components (everything after first /)
    normalized = normalized.split("/", 1)[0]
    
    # Step 4: Remove 'www.' prefix
    if normalized.startswith("www."):
        normalized = normalized[4:]  # Remove "www." (4 characters)
    
    # Step 5: Remove trailing dots
    normalized = normalized.rstrip(".")
    
    return normalized


def extract_domain_from_url(url: Optional[str]) -> str:
    """
    Extract and normalize domain from a URL.
    
    This uses urllib.parse for robust URL parsing and then normalizes
    the extracted domain.
    
    Args:
        url: Full URL string
        
    Returns:
        Normalized domain name
        
    Examples:
        >>> extract_domain_from_url("https://www.example.com/contact")
        'example.com'
        
        >>> extract_domain_from_url("http://subdomain.example.com:8080/path?query=1")
        'subdomain.example.com'
    """
    if not url:
        return ""
    
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        return normalize_domain(domain)
    except Exception:
        # Fallback to simple normalization if parsing fails
        return normalize_domain(url)


def extract_domain_from_email(email: Optional[str]) -> str:
    """
    Extract and normalize the domain part from an email address.
    
    Args:
        email: Email address
        
    Returns:
        Normalized domain name
        
    Examples:
        >>> extract_domain_from_email("user@example.com")
        'example.com'
        
        >>> extract_domain_from_email("contact@SUBDOMAIN.EXAMPLE.COM")
        'subdomain.example.com'
        
        >>> extract_domain_from_email("invalid-email")
        ''
    """
    if not email or "@" not in email:
        return ""
    
    try:
        # Split on @ and take the domain part
        parts = email.split("@")
        if len(parts) != 2:
            return ""
        
        domain = parts[1]
        return normalize_domain(domain)
    except Exception:
        return ""


def extract_localpart_from_email(email: Optional[str]) -> str:
    """
    Extract the local part (username) from an email address.
    
    Args:
        email: Email address
        
    Returns:
        Local part in lowercase
        
    Examples:
        >>> extract_localpart_from_email("user@example.com")
        'user'
        
        >>> extract_localpart_from_email("Contact@Example.COM")
        'contact'
        
        >>> extract_localpart_from_email("invalid-email")
        ''
    """
    if not email or "@" not in email:
        return ""
    
    try:
        parts = email.split("@")
        if len(parts) != 2:
            return ""
        
        return parts[0].strip().lower()
    except Exception:
        return ""


# ============================================================================
# DOMAIN MATCHING AND COMPARISON
# ============================================================================

def domains_match(domain1: Optional[str], domain2: Optional[str]) -> bool:
    """
    Check if two domains match after normalization.
    
    This performs exact matching after normalization, so subdomains
    are considered different domains.
    
    Args:
        domain1: First domain
        domain2: Second domain
        
    Returns:
        True if domains match, False otherwise
        
    Examples:
        >>> domains_match("example.com", "EXAMPLE.COM")
        True
        
        >>> domains_match("www.example.com", "example.com")
        True
        
        >>> domains_match("sub.example.com", "example.com")
        False
    """
    if not domain1 or not domain2:
        return False
    
    norm1 = normalize_domain(domain1)
    norm2 = normalize_domain(domain2)
    
    return norm1 == norm2


def email_domain_matches(email: Optional[str], domain: Optional[str]) -> bool:
    """
    Check if an email's domain matches a given domain.
    
    This supports both exact matching and subdomain matching.
    For example, "user@mail.example.com" matches "example.com".
    
    Args:
        email: Email address
        domain: Domain to match against
        
    Returns:
        True if email's domain matches, False otherwise
        
    Examples:
        >>> email_domain_matches("user@example.com", "example.com")
        True
        
        >>> email_domain_matches("user@mail.example.com", "example.com")
        True
        
        >>> email_domain_matches("user@other.com", "example.com")
        False
    """
    if not email or not domain:
        return False
    
    email_domain = extract_domain_from_email(email)
    target_domain = normalize_domain(domain)
    
    if not email_domain or not target_domain:
        return False
    
    # Exact match
    if email_domain == target_domain:
        return True
    
    # Subdomain match (e.g., mail.example.com matches example.com)
    if email_domain.endswith("." + target_domain):
        return True
    
    return False


def get_domain_parts(domain: Optional[str]) -> list[str]:
    """
    Split a domain into its component parts.
    
    Args:
        domain: Domain name
        
    Returns:
        List of domain parts (from right to left: TLD, domain, subdomains)
        
    Examples:
        >>> get_domain_parts("mail.example.com")
        ['com', 'example', 'mail']
        
        >>> get_domain_parts("example.co.uk")
        ['uk', 'co', 'example']
    """
    if not domain:
        return []
    
    normalized = normalize_domain(domain)
    if not normalized:
        return []
    
    # Split on dots and reverse (TLD first)
    parts = normalized.split(".")
    return list(reversed(parts))


def get_root_domain(domain: Optional[str]) -> str:
    """
    Extract the root domain (domain + TLD) from a potentially subdomain.
    
    Note: This is a simple implementation that assumes the last two parts
    are the root domain. For complex TLDs (like .co.uk), this may not work
    perfectly without a TLD list.
    
    Args:
        domain: Full domain (may include subdomains)
        
    Returns:
        Root domain (domain + TLD)
        
    Examples:
        >>> get_root_domain("mail.example.com")
        'example.com'
        
        >>> get_root_domain("www.subdomain.example.com")
        'example.com'
        
        >>> get_root_domain("example.com")
        'example.com'
    """
    if not domain:
        return ""
    
    normalized = normalize_domain(domain)
    if not normalized:
        return ""
    
    parts = normalized.split(".")
    
    # If only one or two parts, return as-is
    if len(parts) <= 2:
        return normalized
    
    # Return last two parts (domain + TLD)
    return ".".join(parts[-2:])


def is_subdomain(domain: Optional[str], parent_domain: Optional[str]) -> bool:
    """
    Check if a domain is a subdomain of another domain.
    
    Args:
        domain: Potential subdomain
        parent_domain: Parent domain
        
    Returns:
        True if domain is a subdomain of parent_domain
        
    Examples:
        >>> is_subdomain("mail.example.com", "example.com")
        True
        
        >>> is_subdomain("example.com", "example.com")
        False
        
        >>> is_subdomain("other.com", "example.com")
        False
    """
    if not domain or not parent_domain:
        return False
    
    norm_domain = normalize_domain(domain)
    norm_parent = normalize_domain(parent_domain)
    
    if not norm_domain or not norm_parent:
        return False
    
    # Can't be subdomain if they're equal
    if norm_domain == norm_parent:
        return False
    
    # Check if domain ends with .parent_domain
    return norm_domain.endswith("." + norm_parent)


# ============================================================================
# DOMAIN VALIDATION
# ============================================================================

def is_valid_domain(domain: Optional[str]) -> bool:
    """
    Perform basic validation on a domain name.
    
    This checks:
    - Domain is not empty
    - Domain contains at least one dot
    - Domain parts are not empty
    - Domain doesn't contain invalid characters
    
    Args:
        domain: Domain to validate
        
    Returns:
        True if domain appears valid, False otherwise
        
    Examples:
        >>> is_valid_domain("example.com")
        True
        
        >>> is_valid_domain("subdomain.example.com")
        True
        
        >>> is_valid_domain("invalid")
        False
        
        >>> is_valid_domain("example..com")
        False
    """
    if not domain:
        return False
    
    normalized = normalize_domain(domain)
    
    # Must contain at least one dot
    if "." not in normalized:
        return False
    
    # Split into parts
    parts = normalized.split(".")
    
    # All parts must be non-empty
    if any(not part for part in parts):
        return False
    
    # Must have at least 2 parts (domain + TLD)
    if len(parts) < 2:
        return False
    
    # Basic character validation (alphanumeric, hyphens, dots)
    import re
    if not re.match(r'^[a-z0-9.-]+$', normalized):
        return False
    
    return True


def get_tld(domain: Optional[str]) -> str:
    """
    Extract the top-level domain (TLD) from a domain.
    
    Args:
        domain: Domain name
        
    Returns:
        TLD (e.g., 'com', 'de', 'co.uk')
        
    Examples:
        >>> get_tld("example.com")
        'com'
        
        >>> get_tld("example.de")
        'de'
        
        >>> get_tld("subdomain.example.com")
        'com'
    """
    if not domain:
        return ""
    
    normalized = normalize_domain(domain)
    if not normalized:
        return ""
    
    parts = normalized.split(".")
    if not parts:
        return ""
    
    return parts[-1]