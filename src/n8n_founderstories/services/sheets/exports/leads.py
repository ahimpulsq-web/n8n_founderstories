"""
Leads export orchestrator for Google Sheets.

Glue layer that coordinates Leads data export to Google Sheets.
Combines data fetching, sheet specification, and writer to produce
a complete export operation.

Classification:
- Role: Orchestration (glue between fetcher, spec, and writer)
- No business logic
- No direct database queries
- No direct Google Sheets API calls
- Pure coordination of existing components

Architecture:
    ┌─────────────────────────────────────────┐
    │  services/sheets/exports/leads.py       │
    │  (THIS MODULE - Orchestration)          │
    └─────────────────────────────────────────┘
                    │
                    ├──> data_fetchers.leads.fetch_rows_for_sheet()
                    │    (Database queries + formatting)
                    │
                    ├──> specs.leads
                    │    (Sheet layout + formatting)
                    │
                    └──> writer.write_rows()
                         (Google Sheets API calls)

Usage:
    from services.sheets.exports import leads
    
    count = leads.export_to_sheet(
        sheet_id="1A2B3C...",
        request_id="req_abc",
    )
"""

from __future__ import annotations

import logging
import os

from ..data_fetchers import leads as data_fetcher
from ..specs import leads as spec
from ..writer import write_rows
from n8n_founderstories.core.db import get_conn

logger = logging.getLogger(__name__)

# ============================================================================
# EXPORT ORCHESTRATOR
# ============================================================================

def export_to_sheet(
    *,
    sheet_id: str | None = None,
    tab_name: str | None = None,
    job_id: str | None = None,
    request_id: str | None = None,
    suppress_log: bool = False,
) -> dict:
    """
    Export Leads results to Google Sheets.
    
    This function orchestrates the complete export process:
    1. Opens database connection
    2. Fetches enriched results using data_fetcher
    3. Writes to Google Sheets using writer with spec configuration
    4. Returns export stats
    
    No business logic is performed here - this is pure orchestration
    of existing components.
    
    Args:
        sheet_id: Google Sheets spreadsheet ID (optional, uses MASTER_SHEET_ID env var)
        tab_name: Tab name (optional, uses default "Leads")
        job_id: Job ID to filter results (optional, if None exports all results)
        request_id: Request ID to filter results (optional, takes precedence over job_id)
        suppress_log: Suppress SHEETS log output (default: False)
        
    Returns:
        Dict with export stats: {
            "sheet_id": str,
            "tab_name": str,
            "rows_exported": int
        }
        
    Raises:
        ValueError: If sheet_id is not provided and MASTER_SHEET_ID is not set
        Exception: If database or Sheets API operation fails
        
    Example:
        >>> result = export_to_sheet(
        ...     sheet_id="1A2B3C4D5E6F...",
        ...     request_id="req_abc"
        ... )
        >>> print(f"Exported {result['rows_exported']} leads")
        Exported 42 leads
    """
    # ========================================================================
    # STEP 1: Get configuration
    # ========================================================================
    
    # Get sheet_id from parameter or environment
    sheet_id = (sheet_id or os.getenv("MASTER_SHEET_ID", "")).strip()
    if not sheet_id:
        raise ValueError(
            "sheet_id must be provided or MASTER_SHEET_ID environment variable must be set"
        )
    
    # Get tab name (with optional override)
    tab_name = (tab_name or spec.TAB_NAME).strip()
    
    logger.debug(
        f"Starting Leads export | sheet_id={sheet_id} | tab_name={tab_name} | "
        f"job_id={job_id} | request_id={request_id}"
    )
    
    # ========================================================================
    # STEP 2: Fetch data from database
    # ========================================================================
    
    conn = get_conn()
    try:
        rows = data_fetcher.fetch_rows_for_sheet(
            conn,
            job_id=job_id,
            request_id=request_id,
        )
        
        logger.debug(f"Fetched {len(rows)} enriched results for Leads export")
        
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
        mode="replace",
        service="LEADS",
        job_id=job_id,
        format=True,  # Always apply formatting for Leads tab
        suppress_log=suppress_log,
    )
    
    logger.debug(
        f"Successfully exported {len(rows)} leads to sheet {sheet_id}, "
        f"tab '{tab_name}'"
    )
    
    return {
        "sheet_id": sheet_id,
        "tab_name": tab_name,
        "rows_exported": len(rows)
    }