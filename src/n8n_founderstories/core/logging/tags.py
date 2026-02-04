"""
Structured logging helpers for DB and Sheets operations.

Provides consistent, single-line logs that match the existing HunterIO
live status log style.
"""

from __future__ import annotations

import logging
from typing import Any


def log_db(logger: logging.Logger, *, service: str, level: str = "info", **fields: Any) -> None:
    """
    Log a database operation with structured fields.
    
    Formats logs as: DB | <SERVICE> | key=value | key=value | ...
    
    Args:
        logger: Logger instance to use
        service: Service name (e.g., "HUNTERIOV2")
        level: Log level - "info", "debug", or "error" (default: "info")
        **fields: Additional fields to log (e.g., job_id, action, rows, table, state, err)
    
    Examples:
        >>> log_db(logger, service="HUNTERIOV2", job_id="htrio__abc123", action="UPSERT", rows=84)
        DB | HUNTERIOV2 | job_id=htrio__abc123 | action=UPSERT | rows=84
        
        >>> log_db(logger, service="HUNTERIOV2", job_id="htrio__abc123", table="hunterio_results", state="READY")
        DB | HUNTERIOV2 | job_id=htrio__abc123 | table=hunterio_results | state=READY
    """
    # Build message parts
    parts = ["DATABASE", service]
    
    # Add fields in deterministic order
    # Priority order: job_id, table, action, rows, state, err
    # Then remaining fields sorted alphabetically
    priority_keys = ["job_id", "table", "action", "rows", "state", "err"]
    
    # Process priority keys first
    for key in priority_keys:
        if key in fields:
            value = fields[key]
            if value is not None:
                parts.append(f"{key}={value}")
    
    # Process remaining keys alphabetically
    remaining_keys = sorted(set(fields.keys()) - set(priority_keys))
    for key in remaining_keys:
        value = fields[key]
        if value is not None:
            parts.append(f"{key}={value}")
    
    # Join with " | " separator
    message = " | ".join(parts)
    
    # Log at appropriate level
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(message)


def log_sheets(logger: logging.Logger, *, service: str, level: str = "info", **fields: Any) -> None:
    """
    Log a Google Sheets operation with structured fields.
    
    Formats logs as: SHEETS | <SERVICE> | key=value | key=value | ...
    
    Args:
        logger: Logger instance to use
        service: Service name (e.g., "HUNTERIOV2")
        level: Log level - "info", "debug", or "error" (default: "info")
        **fields: Additional fields to log (e.g., job_id, tab, rows, state, reason, err)
    
    Examples:
        >>> log_sheets(logger, service="HUNTERIOV2", job_id="htrio__abc123", tab="hunterio_results", state="START")
        SHEETS | HUNTERIOV2 | job_id=htrio__abc123 | tab=hunterio_results | state=START
        
        >>> log_sheets(logger, service="HUNTERIOV2", job_id="htrio__abc123", tab="hunterio_results", rows=84, state="COMPLETED")
        SHEETS | HUNTERIOV2 | job_id=htrio__abc123 | tab=hunterio_results | rows=84 | state=COMPLETED
        
        >>> log_sheets(logger, service="HUNTERIOV2", job_id="htrio__abc123", state="SKIPPED", reason="no rows")
        SHEETS | HUNTERIOV2 | job_id=htrio__abc123 | state=SKIPPED | reason=no rows
    """
    # Build message parts
    parts = ["SHEETS", service]
    
    # Add fields in deterministic order
    # Priority order: job_id, tab, rows, state, reason, err
    # Then remaining fields sorted alphabetically
    priority_keys = ["job_id", "tab", "rows", "state", "reason", "err"]
    
    # Process priority keys first
    for key in priority_keys:
        if key in fields:
            value = fields[key]
            if value is not None:
                parts.append(f"{key}={value}")
    
    # Process remaining keys alphabetically
    remaining_keys = sorted(set(fields.keys()) - set(priority_keys))
    for key in remaining_keys:
        value = fields[key]
        if value is not None:
            parts.append(f"{key}={value}")
    
    # Join with " | " separator
    message = " | ".join(parts)
    
    # Log at appropriate level
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(message)