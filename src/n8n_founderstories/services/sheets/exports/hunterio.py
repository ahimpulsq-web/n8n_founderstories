"""
Hunter.io export orchestrator for Google Sheets.

Glue layer that coordinates Hunter.io data export to Google Sheets.
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
    │  services/sheets/exports/hunterio.py    │
    │  (THIS MODULE - Orchestration)          │
    └─────────────────────────────────────────┘
                    │
                    ├──> data_fetchers.hunterio.fetch_rows_for_sheet()
                    │    (Database queries + sorting)
                    │
                    ├──> specs.hunter_leads
                    │    (Sheet layout + formatting)
                    │
                    └──> writer.write_rows()
                         (Google Sheets API calls)

Usage:
    from services.sheets.exports import hunterio
    
    count = hunterio.export_to_sheet(
        job_id="htrio__abc123",
        sheet_id="1A2B3C...",
        context={
            "term_order": ["keyword1", "keyword2"],
            "country_order": ["DE", "US"],
        }
    )
"""

from __future__ import annotations

import logging
from typing import Any

from ..data_fetchers import hunterio as data_fetcher
from ..specs import hunter_leads as spec
from ..writer import write_rows
from n8n_founderstories.core.db import get_conn

logger = logging.getLogger(__name__)

# ============================================================================
# EXPORT ORCHESTRATOR
# ============================================================================

def export_to_sheet(
    *,
    request_id: str,
    sheet_id: str,
    context: dict[str, Any],
    job_id: str | None = None,
) -> int:
    """
    Export Hunter.io results to Google Sheets.
    
    This function orchestrates the complete export process:
    1. Opens database connection
    2. Fetches and sorts data using data_fetcher
    3. Writes to Google Sheets using writer with spec configuration
    4. Returns row count
    
    No business logic is performed here - this is pure orchestration
    of existing components.
    
    Args:
        request_id: Request identifier to fetch results for (primary key component)
        sheet_id: Target Google Sheet ID
        context: Export context containing:
            - term_order: List of search queries in desired order
            - country_order: List of country codes in desired order
        job_id: Optional job identifier for logging purposes only
            
    Returns:
        Number of rows exported to the sheet
        
    Raises:
        Exception: If database connection, data fetching, or sheet writing fails
        
    Example:
        >>> count = export_to_sheet(
        ...     request_id="req_abc123",
        ...     sheet_id="1A2B3C4D5E6F...",
        ...     context={
        ...         "term_order": ["SaaS", "AI"],
        ...         "country_order": ["DE", "US"],
        ...     },
        ...     job_id="htrio__abc123"
        ... )
        >>> print(f"Exported {count} rows")
        Exported 42 rows
    """
    # ========================================================================
    # STEP 1: Extract context parameters
    # ========================================================================
    
    term_order = context.get("term_order", [])
    country_order = context.get("country_order", [])
    
    logger.debug(
        f"Starting Hunter.io export | request_id={request_id} | sheet_id={sheet_id} | "
        f"job_id={job_id} | terms={len(term_order)} | countries={len(country_order)}"
    )
    
    # ========================================================================
    # STEP 2: Fetch data from database
    # ========================================================================
    
    conn = get_conn()
    try:
        rows = data_fetcher.fetch_rows_for_sheet(
            conn,
            request_id=request_id,
            term_order=term_order,
            country_order=country_order,
        )
        
        logger.debug(f"Fetched {len(rows)} rows for export (request_id={request_id})")
        
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
        service="HUNTER",
        job_id=job_id,
    )
    
    logger.debug(
        f"Successfully exported {len(rows)} rows to sheet {sheet_id}, "
        f"tab '{spec.TAB_NAME}'"
    )
    
    return len(rows)