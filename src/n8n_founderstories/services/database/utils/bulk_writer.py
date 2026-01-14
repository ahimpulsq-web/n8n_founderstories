"""
Shared database bulk writer utility.

This module provides a standardized, safe database write helper that:
- Never raises exceptions
- Handles feature flags consistently
- Provides standardized logging with structured context
- Supports metrics tracking
- Works with any repository that follows the standard pattern
"""

from __future__ import annotations

import logging
from typing import Any, Callable, List, Optional, Protocol, TypeVar

logger = logging.getLogger(__name__)

# Type variables for generic repository operations
T = TypeVar('T')


class BulkRepository(Protocol[T]):
    """
    Protocol for repositories that support bulk insert operations.
    
    Any repository implementing this protocol can be used with safe_bulk_insert.
    """
    
    def insert_many(self, rows: List[T]) -> tuple[bool, Optional[str], int]:
        """
        Insert multiple rows in a single transaction.
        
        Args:
            rows: List of data objects to insert
            
        Returns:
            Tuple of (success, error_message, inserted_count)
        """
        ...


def safe_bulk_insert(
    *,
    repo: BulkRepository[T],
    rows: List[T],
    job_id: str,
    request_id: str,
    feature_enabled: bool,
    log_prefix: str,
) -> None:
    """
    Generic safe DB insert wrapper.
    
    This function provides a standardized way to perform bulk database inserts
    across all tools (HunterIO, Google Maps, etc.) with consistent:
    - Feature flag handling
    - Error handling (never raises)
    - Structured logging with context
    - Metrics tracking
    
    Args:
        repo: Repository instance implementing BulkRepository protocol
        rows: List of data objects to insert (must match repo's expected type)
        job_id: Job identifier for logging context
        request_id: Request identifier for logging context
        feature_enabled: Whether the database feature is enabled
        log_prefix: Prefix for log messages (e.g., "Hunter.io results", "Google Maps results")
    
    Returns:
        None - This function never raises exceptions
    """
    # Structured logging context
    log_context = {
        "job_id": job_id,
        "request_id": request_id,
        "tool": log_prefix.lower().replace(" ", "_"),
    }
    
    # Check if feature is enabled
    if not feature_enabled:
        logger.debug(
            "%s PostgreSQL integration is disabled",
            log_prefix,
            extra=log_context
        )
        return
    
    if not rows:
        logger.debug(
            "No %s to insert",
            log_prefix.lower(),
            extra=log_context
        )
        return
    
    # Add row count to context
    log_context["rows_attempted"] = len(rows)
    
    try:
        # Perform bulk insert
        success, error, inserted_count = repo.insert_many(rows)
        
        # Add results to context
        log_context["rows_inserted"] = inserted_count
        log_context["success"] = success
        
        if success:
            logger.debug(
                "%s PostgreSQL write successful: %d rows inserted "
                "(job_id=%s, request_id=%s)",
                log_prefix,
                inserted_count,
                job_id,
                request_id,
                extra=log_context
            )
        else:
            log_context["error"] = error
            logger.warning(
                "%s PostgreSQL write failed: %s "
                "(job_id=%s, request_id=%s)",
                log_prefix,
                error,
                job_id,
                request_id,
                extra=log_context
            )
            
    except Exception as e:
        log_context["error"] = str(e)
        log_context["rows_inserted"] = 0
        log_context["success"] = False
        
        logger.error(
            "Unexpected error in %s PostgreSQL write: %s "
            "(job_id=%s, request_id=%s)",
            log_prefix,
            e,
            job_id,
            request_id,
            extra=log_context
        )


def safe_bulk_insert_with_converter(
    *,
    repo: BulkRepository[T],
    raw_data: List[Any],
    converter: Callable[[Any], T],
    job_id: str,
    request_id: str,
    feature_enabled: bool,
    log_prefix: str,
) -> None:
    """
    Safe bulk insert with data conversion.
    
    This is a convenience wrapper around safe_bulk_insert that handles
    converting raw data (like sheets rows) to repository objects.
    
    Args:
        repo: Repository instance implementing BulkRepository protocol
        raw_data: List of raw data objects to convert and insert
        converter: Function to convert raw data to repository objects
        job_id: Job identifier for logging context
        request_id: Request identifier for logging context
        feature_enabled: Whether the database feature is enabled
        log_prefix: Prefix for log messages
    
    Returns:
        None - This function never raises exceptions
    """
    if not raw_data:
        return
    
    # Structured logging context
    log_context = {
        "job_id": job_id,
        "request_id": request_id,
        "tool": log_prefix.lower().replace(" ", "_"),
        "raw_data_count": len(raw_data),
    }
    
    try:
        # Convert raw data to repository objects
        converted_rows = []
        conversion_errors = 0
        
        for item in raw_data:
            try:
                converted_row = converter(item)
                converted_rows.append(converted_row)
            except Exception as e:
                conversion_errors += 1
                logger.debug(
                    "Failed to convert %s data item: %s",
                    log_prefix.lower(),
                    e,
                    extra={**log_context, "conversion_error": str(e)}
                )
        
        log_context["conversion_errors"] = conversion_errors
        log_context["converted_rows"] = len(converted_rows)
        
        if conversion_errors > 0:
            logger.warning(
                "%s data conversion had %d errors out of %d items "
                "(job_id=%s, request_id=%s)",
                log_prefix,
                conversion_errors,
                len(raw_data),
                job_id,
                request_id,
                extra=log_context
            )
        
        # Perform bulk insert with converted data
        safe_bulk_insert(
            repo=repo,
            rows=converted_rows,
            job_id=job_id,
            request_id=request_id,
            feature_enabled=feature_enabled,
            log_prefix=log_prefix,
        )
        
    except Exception as e:
        log_context["error"] = str(e)
        logger.error(
            "Unexpected error in %s data conversion: %s "
            "(job_id=%s, request_id=%s)",
            log_prefix,
            e,
            job_id,
            request_id,
            extra=log_context
        )