"""
Safe insert wrapper for web search results.

This module provides a safe wrapper for inserting web search results
using the shared bulk writer utility.
"""

from __future__ import annotations

from typing import List

from ...database.utils.bulk_writer import safe_bulk_insert
from ...database.config import db_config

from .models import WebSearchResultRow
from .repos import WebSearchResultsRepository


def safe_insert_web_search_results(
    job_id: str | None,
    request_id: str,
    rows: List[WebSearchResultRow]
) -> None:
    """
    Safe wrapper for inserting web search results to PostgreSQL.
    
    This function uses the shared bulk writer utility to provide:
    - Feature flag handling
    - Error handling (never raises)
    - Structured logging with context
    - Metrics tracking
    
    Args:
        job_id: Job identifier (optional, for logging context)
        request_id: Request identifier
        rows: List of WebSearchResultRow instances to insert
    
    Returns:
        None - This function never raises exceptions
    """
    repo = WebSearchResultsRepository()
    safe_bulk_insert(
        repo=repo,
        rows=rows,
        job_id=job_id or "unknown",
        request_id=request_id,
        feature_enabled=db_config.is_web_search_results_enabled,
        log_prefix="Web search results",
    )