"""
Google Maps Places export orchestrator for Google Sheets.

Glue layer that coordinates Google Maps Places data export to Google Sheets.
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
    │  services/sheets/exports/google_maps.py │
    │  (THIS MODULE - Orchestration)          │
    └─────────────────────────────────────────┘
                    │
                    ├──> data_fetchers.google_maps.fetch_rows_for_sheet()
                    │    (Database queries + sorting)
                    │
                    ├──> specs.google_maps_leads
                    │    (Sheet layout + formatting)
                    │
                    └──> writer.write_rows()
                         (Google Sheets API calls)

Usage:
    from services.sheets.exports import google_maps
    
    count = google_maps.export_to_sheet(
        job_id="gmp__abc123",
        sheet_id="1A2B3C...",
        context={
            "query_order": ["restaurants", "cafes"],
            "country_order": ["FR", "DE"],
        }
    )
"""

from __future__ import annotations

import logging
from typing import Any

from ..data_fetchers import google_maps as data_fetcher
from ..specs import google_maps_leads as spec
from ..writer import write_rows
from n8n_founderstories.core.db import get_conn

logger = logging.getLogger(__name__)

# ============================================================================
# EXPORT ORCHESTRATOR
# ============================================================================

def export_to_sheet(
    *,
    job_id: str,
    sheet_id: str,
    context: dict[str, Any],
) -> int:
    """
    Export Google Maps Places results to Google Sheets.
    
    This function orchestrates the complete export process:
    1. Opens database connection
    2. Fetches and sorts data using data_fetcher
    3. Writes to Google Sheets using writer with spec configuration
    4. Returns row count
    
    No business logic is performed here - this is pure orchestration
    of existing components.
    
    Args:
        job_id: Job identifier to fetch results for
        sheet_id: Target Google Sheet ID
        context: Export context containing:
            - query_order: List of search queries in desired order
            - country_order: List of country codes in desired order
            
    Returns:
        Number of rows exported to the sheet
        
    Raises:
        Exception: If database connection, data fetching, or sheet writing fails
        
    Example:
        >>> count = export_to_sheet(
        ...     job_id="gmp__abc123",
        ...     sheet_id="1A2B3C4D5E6F...",
        ...     context={
        ...         "query_order": ["restaurants", "cafes"],
        ...         "country_order": ["FR", "DE"],
        ...     }
        ... )
        >>> print(f"Exported {count} rows")
        Exported 42 rows
    """
    # ========================================================================
    # STEP 1: Extract context parameters
    # ========================================================================
    
    query_order = context.get("query_order", [])
    country_order = context.get("country_order", [])
    
    logger.debug(
        f"Starting Google Maps export | job_id={job_id} | sheet_id={sheet_id} | "
        f"queries={len(query_order)} | countries={len(country_order)}"
    )
    
    # ========================================================================
    # STEP 2: Fetch data from database
    # ========================================================================
    
    conn = get_conn()
    try:
        rows = data_fetcher.fetch_rows_for_sheet(
            conn,
            job_id=job_id,
            query_order=query_order,
            country_order=country_order,
        )
        
        logger.debug(f"Fetched {len(rows)} rows for export")
        
    finally:
        conn.close()
    
    # ========================================================================
    # STEP 3: Write to Google Sheets
    # ========================================================================
    
    write_rows(
        sheet_id=sheet_id,
        tab_name=spec.TAB_NAME,
        headers=spec.HEADERS,
        rows=rows,
        mode="replace",
        service="GOOGLE_MAPS",
        job_id=job_id,
    )
    
    logger.debug(
        f"Successfully exported {len(rows)} rows to sheet {sheet_id}, "
        f"tab '{spec.TAB_NAME}'"
    )
    
    return len(rows)