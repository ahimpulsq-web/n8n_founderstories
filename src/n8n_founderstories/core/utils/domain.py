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
    8. Convert IDN (Unicode) domains to punycode
    9. Handle empty/None
    
    Examples:
        'Example.com' -> 'example.com'
        'https://www.Example.com/' -> 'example.com'
        'HTTP://Example.com:8080/path?q=1' -> 'example.com'
        'www.example.com' -> 'example.com'
        'example.com/' -> 'example.com'
        'http://www.böhler-hörimarkt.de/' -> 'xn--bhler-hrimarkt-9kb.de'
        'https://www.superimmunität.de' -> 'xn--superimmunitt-pmb.de'
        '' -> None
        None -> None
    
    Args:
        domain: Raw domain string (may contain Unicode characters)
        
    Returns:
        Normalized domain (ASCII/punycode) or None if invalid
    """
    if not domain:
        return None
    
    # Strip whitespace
    domain = domain.strip()
    if not domain:
        return None
    
    # Try to parse as URL first (before lowercasing to preserve URL parsing)
    if '://' in domain or domain.startswith('//'):
        try:
            parsed = urlparse(domain if '://' in domain else f'http:{domain}')
            domain = parsed.netloc or parsed.path
        except Exception:
            pass
    
    # Lowercase after URL parsing
    domain = domain.lower()
    
    # Remove www. prefix (before port removal to handle www.example.com:8080)
    if domain.startswith('www.'):
        domain = domain[4:]
    
    # Remove port (must be done before scheme removal to avoid confusion)
    domain = re.sub(r':\d+$', '', domain)
    
    # Remove scheme if still present (e.g., "http:example.com")
    # Only match actual schemes, not domain:port patterns
    domain = re.sub(r'^[a-z][a-z0-9+.-]*:', '', domain)
    
    # Remove leading //
    domain = domain.lstrip('/')
    
    # Remove path, query, fragment
    domain = re.sub(r'[/?#].*$', '', domain)
    
    # Remove trailing dots
    domain = domain.rstrip('.')
    
    # Final validation - must have at least one dot
    if not domain or '.' not in domain:
        return None
    
    # Convert IDN (Unicode) domains to punycode (ASCII)
    # This must happen after all other normalization but before final validation
    try:
        # Check if domain contains non-ASCII characters
        domain.encode('ascii')
    except UnicodeEncodeError:
        # Domain contains Unicode characters - convert to punycode
        try:
            domain = domain.encode('idna').decode('ascii')
        except (UnicodeError, UnicodeDecodeError):
            # Invalid IDN domain
            return None
    
    # Check for valid domain characters (now all ASCII after punycode conversion)
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