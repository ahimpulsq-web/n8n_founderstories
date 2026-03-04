"""
Global Mail Tracking export orchestrator for Google Sheets.

Glue layer that coordinates Global Mail Tracking data export to Google Sheets.
Combines data fetching, sheet specification, and writer to produce
a complete export operation.

Classification:
- Role: Orchestration (glue between fetcher, spec, and writer)
- No business logic
- No direct database queries
- No direct Google Sheets API calls
- Pure coordination of existing components

Architecture:
    ┌─────────────────────────────────────────────────────┐
    │  services/sheets/exports/global_mail_tracking.py    │
    │  (THIS MODULE - Orchestration)                      │
    └─────────────────────────────────────────────────────┘
                    │
                    ├──> data_fetchers.global_mail_tracking.fetch_rows_for_sheet()
                    │    (Database queries + formatting)
                    │
                    ├──> specs.global_mail_tracking
                    │    (Sheet layout + formatting)
                    │
                    └──> writer.write_rows()
                         (Google Sheets API calls)

Usage:
    from services.sheets.exports import global_mail_tracking
    
    count = global_mail_tracking.export_to_sheet(
        sheet_id="1bGJotWtJT17Od34Ff62o7BArqEQgZOlgYI-bznDolZA",
    )
"""

from __future__ import annotations

import logging

from ..data_fetchers import global_mail_tracking as data_fetcher
from ..specs import global_mail_tracking as spec
from ..writer import write_rows
from n8n_founderstories.core.db import get_conn
from n8n_founderstories.core.config import settings

logger = logging.getLogger(__name__)

# ============================================================================
# EXPORT ORCHESTRATOR
# ============================================================================

def export_to_sheet(
    *,
    sheet_id: str | None = None,
    tab_name: str | None = None,
    request_id: str | None = None,
    mode: str = "replace",
    suppress_log: bool = False,
) -> dict:
    """
    Export Global Mail Tracking results to Google Sheets.
    
    This function orchestrates the complete export process:
    1. Opens database connection
    2. Fetches mail tracking data using data_fetcher
    3. Writes to Google Sheets using writer with spec configuration
    4. Returns export stats
    
    No business logic is performed here - this is pure orchestration
    of existing components.
    
    Args:
        sheet_id: Google Sheets spreadsheet ID (optional, uses GLOBAL_MAIL_TRACKING_SHEET_ID from settings)
        tab_name: Tab name (optional, uses default "Global Mail Tracking")
        request_id: Request ID to filter results (optional, if None exports all results)
        mode: Write mode - "append" (default) or "replace"
        suppress_log: Suppress SHEETS log output (default: False)
        
    Returns:
        Dict with export stats: {
            "sheet_id": str,
            "tab_name": str,
            "rows_exported": int
        }
        
    Raises:
        ValueError: If sheet_id is not provided and GLOBAL_MAIL_TRACKING_SHEET_ID is not set
        Exception: If database or Sheets API operation fails
        
    Example:
        >>> result = export_to_sheet(
        ...     sheet_id="1bGJotWtJT17Od34Ff62o7BArqEQgZOlgYI-bznDolZA",
        ...     request_id="req_abc"
        ... )
        >>> print(f"Exported {result['rows_exported']} tracking records")
        Exported 42 tracking records
    """
    # ========================================================================
    # STEP 1: Get configuration
    # ========================================================================
    
    # Get sheet_id from parameter or settings
    sheet_id = (sheet_id or settings.global_mail_tracking_sheet_id or "").strip()
    if not sheet_id:
        raise ValueError(
            "sheet_id must be provided or GLOBAL_MAIL_TRACKING_SHEET_ID environment variable must be set"
        )
    
    # Get tab name (with optional override)
    tab_name = (tab_name or spec.TAB_NAME).strip()
    
    logger.debug(
        f"Starting Global Mail Tracking export | sheet_id={sheet_id} | tab_name={tab_name} | "
        f"request_id={request_id} | mode={mode}"
    )
    
    # ========================================================================
    # STEP 2: Fetch data from database
    # ========================================================================
    
    conn = get_conn()
    try:
        rows = data_fetcher.fetch_rows_for_sheet(
            conn,
            request_id=request_id,
        )
        
        logger.debug(f"Fetched {len(rows)} mail tracking rows for Global Mail Tracking export")
        
    finally:
        conn.close()
    
    # ========================================================================
    # STEP 3: Write to Google Sheets
    # ========================================================================
    
    write_rows(
        sheet_id=sheet_id,
        tab_name=tab_name,
        headers=spec.HEADERS,
        rows=rows,
        mode=mode,
        service="GLOBAL_MAIL_TRACKING",
        format=True,  # Apply formatting on every replace to ensure consistency
        suppress_log=suppress_log,
    )
    
    logger.debug(
        f"Successfully exported {len(rows)} tracking records to sheet {sheet_id}, "
        f"tab '{tab_name}' (mode={mode})"
    )
    
    return {
        "sheet_id": sheet_id,
        "tab_name": tab_name,
        "rows_exported": len(rows)
    }