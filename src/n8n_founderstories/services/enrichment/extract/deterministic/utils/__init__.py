"""
Utility modules for deterministic email extraction.

This package contains utility functions for text processing, domain handling,
and other helper functions used throughout the extraction pipeline.

Modules:
    text_normalizer: Text cleaning and normalization utilities
    domain_utils: Domain-related utility functions
"""

from .text_normalizer import normalize_text, clean_email_text
from .domain_utils import normalize_domain, extract_domain_from_email, domains_match

__all__ = [
    "normalize_text",
    "clean_email_text",
    "normalize_domain",
    "extract_domain_from_email",
    "domains_match",
]