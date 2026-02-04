"""
Web Search database persistence layer.

This module provides database operations for web search results.
"""

from .models import WebSearchResultRow
from .repos import WebSearchResultsRepository, convert_db_results_to_sheets_format
from .safe_insert import safe_insert_web_search_results
from .mapper import build_web_search_rows

__all__ = [
    "WebSearchResultRow",
    "WebSearchResultsRepository",
    "convert_db_results_to_sheets_format",
    "safe_insert_web_search_results",
    "build_web_search_rows",
]