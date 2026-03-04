"""
Core extraction modules for deterministic email extraction.

This package contains the main extraction logic including:
- Email parsing from text
- Email prioritization and ranking
- Main extraction orchestration

Modules:
    parser: Email parsing from text content
    prioritizer: Email ranking and sorting logic
    extractor: Main extraction orchestration
"""

from .parser import parse_emails_from_text, extract_mailto_links
from .prioritizer import prioritize_emails, EmailPriority
from .extractor import extract_emails_from_pages, DeterministicExtractor

__all__ = [
    "parse_emails_from_text",
    "extract_mailto_links",
    "prioritize_emails",
    "EmailPriority",
    "extract_emails_from_pages",
    "DeterministicExtractor",
]