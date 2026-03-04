"""
Jobs Tool Status export orchestrator for Google Sheets.

Glue layer that coordinates Tool Status data export to Google Sheets.
Combines data fetching, sheet specification, and writer to produce
a complete export operation.

Classification:
- Role: Orchestration (glue between fetcher, spec, and writer)
- No business logic
- No direct store queries
- No direct Google Sheets API calls
- Pure coordination of existing components

Architecture:
    ┌─────────────────────────────────────────────┐
    │  services/sheets/exports/jobs_tool_status.py│
    │  (THIS MODULE - Orchestration)              │
    └─────────────────────────────────────────────┘
                    │
                    ├──> data_fetchers.jobs_tool_status.fetch_rows_for_sheet()
                    │    (Jobs store queries + formatting)
                    │
                    ├──> specs.jobs_tool_status
                    │    (Sheet layout + formatting)
                    │
                    └──> writer.write_rows()
                         (Google Sheets API calls)

Usage:
    from services.sheets.exports import jobs_tool_status
    
    count = jobs_tool_status.export_to_sheet(
        sheet_id="1A2B3C...",
    )
"""

from __future__ import annotations

import logging

from ..data_fetchers import jobs_tool_status as data_fetcher
from ..specs import jobs_tool_status as spec
from ..writer import write_rows

logger = logging.getLogger(__name__)

# ============================================================================
# EXPORT ORCHESTRATOR
# ============================================================================

def export_to_sheet(
    *,
    sheet_id: str,
    job_id: str | None = None,
    request_id: str | None = None,
    suppress_log: bool = False,
) -> int:
    """
    Export Tool Status data to Google Sheets.
    
    This function orchestrates the complete export process:
    1. Fetches job records from the jobs store (optionally filtered by request_id)
    2. Writes to Google Sheets using writer with spec configuration
    3. Returns row count
    
    No business logic is performed here - this is pure orchestration
    of existing components.
    
    Args:
        sheet_id: Target Google Sheet ID
        job_id: Optional job ID for logging (not used for filtering)
        request_id: Optional request ID to filter jobs. If provided, only jobs
                   matching this request_id will be exported.
            
    Returns:
        Number of rows exported to the sheet
        
    Raises:
        Exception: If data fetching or sheet writing fails
        
    Example:
        >>> count = export_to_sheet(
        ...     sheet_id="1A2B3C4D5E6F...",
        ...     request_id="req_abc123",
        ... )
        >>> print(f"Exported {count} job status rows")
        Exported 2 job status rows
    """
    logger.debug(f"Starting Tool Status export | sheet_id={sheet_id} | request_id={request_id}")
    
    # ========================================================================
    # STEP 1: Fetch data from jobs store (optionally filtered)
    # ========================================================================
    
    rows = data_fetcher.fetch_rows_for_sheet(request_id=request_id)
    
    logger.debug(f"Fetched {len(rows)} job records for export")
    
    # ========================================================================
    # STEP 2: Write to Google Sheets
    # ========================================================================
    
    write_rows(
        sheet_id=sheet_id,
        tab_name=spec.TAB_NAME,
        headers=spec.HEADERS,
        rows=rows,
        mode="replace",
        service="JOBS",
        job_id=job_id,
        suppress_log=suppress_log,
    )
    
    logger.debug(
        f"Successfully exported {len(rows)} rows to sheet {sheet_id}, "
        f"tab '{spec.TAB_NAME}'"
    )
    
    return len(rows)


def write_single_job_status(
    *,
    sheet_id: str,
    job_id: str,
    tool: str,
    request_id: str,
    state: str,
) -> None:
    """
    Write Tool Status for jobs matching the given request_id.
    
    This performs a filtered sheet rewrite (replace mode) showing only jobs
    that match the provided request_id. This ensures each search plan has its
    own isolated Tool Status view.
    
    Note: This does a full sheet rewrite on every call. Do not call this
    function too frequently (e.g., on every progress update). It's designed
    for state transitions (RUNNING, SUCCEEDED, FAILED) only.
    
    Args:
        sheet_id: Target Google Sheet ID
        job_id: Job identifier (for logging only)
        tool: Tool name (for logging only)
        request_id: Request correlation ID - used to filter which jobs to show
        state: Job state (for logging only)
    """
    # Export only jobs matching this request_id
    # This ensures each search plan has its own isolated Tool Status view
    export_to_sheet(sheet_id=sheet_id, job_id=job_id, request_id=request_id)