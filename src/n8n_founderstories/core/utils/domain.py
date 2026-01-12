"""
Domain normalization utilities for deduplication.

This module provides robust domain normalization for case-insensitive,
scheme-agnostic deduplication across different data sources.
"""

import re
from typing import Optional
from urllib.parse import urlparse


def normalize_domain(domain: Optional[str]) -> Optional[str]:
    """
    Normalize domain for case-insensitive, scheme-agnostic deduplication.
    
    Normalization rules:
    1. Strip whitespace
    2. Lowercase
    3. Remove scheme (http://, https://, ftp://, etc.)
    4. Remove www. prefix
    5. Remove path, query, fragment
    6. Remove trailing slash
    7. Remove port number
    8. Handle empty/None
    
    Examples:
        'Example.com' -> 'example.com'
        'https://www.Example.com/' -> 'example.com'
        'HTTP://Example.com:8080/path?q=1' -> 'example.com'
        'www.example.com' -> 'example.com'
        'example.com/' -> 'example.com'
        '' -> None
        None -> None
    
    Args:
        domain: Raw domain string
        
    Returns:
        Normalized domain or None if invalid
    """
    if not domain:
        return None
    
    # Strip whitespace
    domain = domain.strip()
    if not domain:
        return None
    
    # Lowercase
    domain = domain.lower()
    
    # Try to parse as URL first
    if '://' in domain or domain.startswith('//'):
        try:
            parsed = urlparse(domain if '://' in domain else f'http:{domain}')
            domain = parsed.netloc or parsed.path
        except Exception:
            pass
    
    # Remove scheme if still present (e.g., "http:example.com")
    domain = re.sub(r'^[a-z][a-z0-9+.-]*:', '', domain)
    
    # Remove leading //
    domain = domain.lstrip('/')
    
    # Remove www. prefix
    if domain.startswith('www.'):
        domain = domain[4:]
    
    # Remove port
    domain = re.sub(r':\d+$', '', domain)
    
    # Remove path, query, fragment
    domain = re.sub(r'[/?#].*$', '', domain)
    
    # Remove trailing dots
    domain = domain.rstrip('.')
    
    # Final validation - must have at least one dot and valid characters
    if not domain or '.' not in domain:
        return None
    
    # Check for valid domain characters
    if not re.match(r'^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$', domain):
        return None
    
    return domain


def extract_domain_from_url(url: Optional[str]) -> Optional[str]:
    """
    Extract and normalize domain from URL.
    
    This is an alias for normalize_domain() for clarity when working with URLs.
    
    Args:
        url: Full URL or domain
        
    Returns:
        Normalized domain or None
    """
    return normalize_domain(url)