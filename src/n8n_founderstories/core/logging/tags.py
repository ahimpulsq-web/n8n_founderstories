"""
Structured logging helpers for DB and Sheets operations.

Provides consistent, single-line logs that match the existing HunterIO
live status log style.
"""

from __future__ import annotations

import logging
from typing import Any


def log_db(logger: logging.Logger, *, service: str, table: str, rows: int, source: str | None = None, level: str = "info") -> None:
    """
    Log a database operation with structured fields.
    
    Format: INFO DATABASE | <SERVICE> | <TABLE_NAME> | rows=<N>
    Format (with source): INFO DATABASE | <SERVICE> | <TABLE_NAME> | source=<SOURCE> | rows=<N>
    
    Args:
        logger: Logger instance to use
        service: Service name (e.g., "HUNTERIOV2", "GOOGLEMAPSV2", "MASTER")
        table: Table name (e.g., "htr_results", "gmaps_results", "mstr_results")
        rows: Deduped row count
        source: Optional source name for MASTER service (e.g., "hunter", "google_maps")
        level: Log level - "info", "debug", or "error" (default: "info")
    
    Examples:
        >>> log_db(logger, service="HUNTERIOV2", table="htr_results", rows=1584)
        INFO DATABASE | HUNTERIOV2 | htr_results | rows=1584
        
        >>> log_db(logger, service="GOOGLEMAPSV2", table="gmaps_results", rows=412)
        INFO DATABASE | GOOGLEMAPSV2 | gmaps_results | rows=412
        
        >>> log_db(logger, service="MASTER", table="mstr_results", source="hunter", rows=419)
        INFO DATABASE | MASTER | mstr_results | source=hunter | rows=419
    """
    if source:
        message = f"DATABASE | {service} | {table} | source={source} | rows={rows}"
    else:
        message = f"DATABASE | {service} | {table} | rows={rows}"
    
    # Log at appropriate level
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(message)


def log_sheets(logger: logging.Logger, *, service: str, tab: str, mode: str, rows: int, level: str = "info") -> None:
    """
    Log a Google Sheets operation with structured fields.
    
    Format: INFO SHEETS | <SERVICE> | <TAB_NAME> | mode=<append|replace> | rows=<N>
    
    Args:
        logger: Logger instance to use
        service: Service name (e.g., "HUNTERIOV2", "GOOGLEMAPSV2", "MASTER")
        tab: Tab/sheet name (e.g., "htr_results", "gmaps_results", "master")
        mode: Write mode - "append" or "replace"
        rows: Deduped row count
        level: Log level - "info", "debug", or "error" (default: "info")
    
    Examples:
        >>> log_sheets(logger, service="HUNTERIOV2", tab="htr_results", mode="replace", rows=1584)
        INFO SHEETS | HUNTERIOV2 | htr_results | mode=replace | rows=1584
        
        >>> log_sheets(logger, service="GOOGLEMAPSV2", tab="gmaps_results", mode="replace", rows=412)
        INFO SHEETS | GOOGLEMAPSV2 | gmaps_results | mode=replace | rows=412
        
        >>> log_sheets(logger, service="TOOL_STATUS", tab="Tool_Status", mode="append", rows=1)
        INFO SHEETS | TOOL_STATUS | Tool_Status | mode=append | rows=1
    """
    message = f"SHEETS | {service} | {tab} | mode={mode} | rows={rows}"
    
    # Log at appropriate level
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(message)