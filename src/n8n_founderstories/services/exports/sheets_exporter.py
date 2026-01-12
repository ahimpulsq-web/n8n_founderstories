"""
Lightweight Google Sheets exporter for DB-first workflow.

This module provides minimal, fast export functionality:
- Only exports data from PostgreSQL to Sheets at job completion
- Uses shared schema constants from sheets_schema.py
- No formatting operations
- No setup operations during job runs
- Minimal API calls (tab ensure + batch write)
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

from .sheets import SheetsClient
from .sheets_schema import (
    TAB_GOOGLE_MAPS_MAIN,
    TAB_GOOGLE_MAPS_AUDIT,
    HEADERS_GOOGLE_MAPS_MAIN,
    HEADERS_GOOGLE_MAPS_AUDIT,
    TAB_HUNTER_MAIN,
    TAB_HUNTER_AUDIT,
    HEADERS_HUNTER_MAIN,
    HEADERS_HUNTER_AUDIT,
    TAB_MASTER_MAIN,
    TAB_MASTER_AUDIT,
    HEADERS_MASTER_MAIN,
    HEADERS_MASTER_AUDIT,
)
from ...core.utils.text import norm

logger = logging.getLogger(__name__)


def _col_index_to_letter(col_index: int) -> str:
    """Convert 0-based column index to Excel-style letter (A, B, ..., Z, AA, AB, ...)."""
    if col_index < 0:
        raise ValueError("col_index must be >= 0")
    
    n = col_index + 1
    letters: list[str] = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(ord("A") + rem))
    return "".join(reversed(letters))


def export_table_to_sheet(
    client: SheetsClient,
    tab_name: str,
    headers: list[str],
    rows: list[list[Any]],
    *,
    clear_first: bool = True
) -> None:
    """
    Export a single table to a sheet with standardized behavior.
    
    This is the core export function used by all tool exporters.
    
    Args:
        client: SheetsClient instance
        tab_name: Name of the tab to export to
        headers: List of header strings (from sheets_schema.py)
        rows: List of data rows
        clear_first: If True, overwrites existing data (default behavior)
        
    Behavior:
        1. Ensures the tab exists (creates if missing)
        2. Always writes headers explicitly to row 1 (prevents drift)
        3. Writes data rows starting at row 2
        4. Uses single batch write operation (reduces API calls)
        
    This function is the single source of truth for export behavior.
    All tool-specific exporters MUST use this function.
    """
    t = norm(tab_name)
    if not t or not headers:
        logger.warning(f"SHEETS_EXPORT_INVALID | tab={t} | headers={bool(headers)}")
        return
    
    try:
        # 1. Ensure tab exists (creates if missing)
        client.ensure_tab(t)
        
        # 2. Prepare data: headers + rows
        all_data = [headers]
        if rows:
            # Convert all values to strings
            cleaned_rows = [[norm(str(cell)) for cell in row] for row in rows]
            all_data.extend(cleaned_rows)
        
        # 3. Calculate range
        num_rows = len(all_data)
        num_cols = len(headers)
        end_col = _col_index_to_letter(num_cols - 1)
        range_str = f"{t}!A1:{end_col}{num_rows}"
        
        # 4. Write all data in one batch call (headers + data)
        client.values_batch_update(
            data=[{
                "range": range_str,
                "values": all_data
            }],
            value_input_option="RAW"
        )
        
        logger.info(f"SHEETS_EXPORT_TABLE | tab={t} | rows={len(rows)} | cols={num_cols}")
        
    except Exception as e:
        logger.error(f"SHEETS_EXPORT_TABLE_FAILED | tab={t} | error={e}")
        raise


def export_multiple_tables(
    client: SheetsClient,
    exports: list[dict[str, Any]]
) -> None:
    """
    Export multiple tables in a single batch operation.
    
    This is the most efficient way to export multiple tabs, minimizing API calls.
    
    Args:
        client: SheetsClient instance
        exports: List of export specs, each containing:
            - tab: Tab name
            - headers: List of header strings
            - rows: List of data rows
            
    Example:
        export_multiple_tables(client, [
            {"tab": "GoogleMaps_v2", "headers": [...], "rows": [...]},
            {"tab": "GoogleMaps_Audit_v2", "headers": [...], "rows": [...]},
        ])
        
    Behavior:
        1. Ensures all tabs exist (single batch call)
        2. Writes headers + data for all tabs (single batch call)
        3. Minimizes API calls to reduce quota errors
    """
    if not exports:
        return
    
    try:
        # 1. Ensure all tabs exist (single batch call)
        tab_names = [norm(e.get("tab", "")) for e in exports if norm(e.get("tab", ""))]
        if not tab_names:
            logger.warning("SHEETS_EXPORT_MULTIPLE_NO_TABS")
            return
        
        client.ensure_tabs(tab_names)
        
        # 2. Prepare batch data for all tabs
        data = []
        for export_spec in exports:
            tab_name = norm(export_spec.get("tab", ""))
            headers = export_spec.get("headers", [])
            rows = export_spec.get("rows", [])
            
            if not tab_name or not headers:
                continue
            
            # Prepare data: headers + rows
            all_data = [headers]
            if rows:
                # Convert all values to strings
                cleaned_rows = [[norm(str(cell)) for cell in row] for row in rows]
                all_data.extend(cleaned_rows)
            
            # Calculate range
            num_rows = len(all_data)
            num_cols = len(headers)
            end_col = _col_index_to_letter(num_cols - 1)
            range_str = f"{tab_name}!A1:{end_col}{num_rows}"
            
            data.append({
                "range": range_str,
                "values": all_data
            })
        
        # 3. Write all tabs in one batch call
        if data:
            client.values_batch_update(data=data, value_input_option="RAW")
        
        logger.info(f"SHEETS_EXPORT_MULTIPLE | tabs={len(data)}")
        
    except Exception as e:
        logger.error(f"SHEETS_EXPORT_MULTIPLE_FAILED | error={e}")
        raise


def export_google_maps_results(
    *,
    client: SheetsClient,
    job_id: str,
    request_id: str,
    results_rows: list[list[str]],
    audit_rows: list[list[str]] | None = None,
) -> None:
    """
    Export Google Maps results from DB to Sheets (v2 schema).
    
    Creates tabs only at export time with versioned names:
    - GoogleMaps_v2 (main results)
    - GoogleMaps_Audit_v2 (optional audit)
    
    Headers are always written explicitly from sheets_schema.py to prevent drift.
    
    Args:
        client: SheetsClient instance
        job_id: Job ID
        request_id: Request ID
        results_rows: Main results (already formatted for Sheets)
        audit_rows: Optional audit rows
    """
    # Prepare exports using schema constants
    exports = [
        {
            "tab": TAB_GOOGLE_MAPS_MAIN,
            "headers": HEADERS_GOOGLE_MAPS_MAIN,
            "rows": results_rows
        }
    ]
    
    if audit_rows:
        exports.append({
            "tab": TAB_GOOGLE_MAPS_AUDIT,
            "headers": HEADERS_GOOGLE_MAPS_AUDIT,
            "rows": audit_rows
        })
    
    # Export all tabs (tabs created only at export time)
    export_multiple_tables(client, exports)
    logger.info(f"GOOGLE_MAPS_EXPORT_COMPLETE | job_id={job_id} | tabs={len(exports)}")


def export_hunter_results(
    *,
    client: SheetsClient,
    job_id: str,
    request_id: str,
    results_rows: list[list[str]],
    audit_rows: list[list[str]] | None = None,
) -> None:
    """
    Export Hunter.io results from DB to Sheets (v2 schema).
    
    Creates tabs only at export time with versioned names:
    - HunterIO_v2 (main results)
    - HunterIO_Audit_v2 (optional audit)
    
    Headers are always written explicitly from sheets_schema.py to prevent drift.
    
    Args:
        client: SheetsClient instance
        job_id: Job ID
        request_id: Request ID
        results_rows: Main results (already formatted for Sheets)
        audit_rows: Optional audit rows
    """
    # Prepare exports using schema constants
    exports = [
        {
            "tab": TAB_HUNTER_MAIN,
            "headers": HEADERS_HUNTER_MAIN,
            "rows": results_rows
        }
    ]
    
    if audit_rows:
        exports.append({
            "tab": TAB_HUNTER_AUDIT,
            "headers": HEADERS_HUNTER_AUDIT,
            "rows": audit_rows
        })
    
    # Export all tabs (tabs created only at export time)
    export_multiple_tables(client, exports)
    logger.info(f"HUNTER_EXPORT_COMPLETE | job_id={job_id} | tabs={len(exports)}")


def export_master_results(
    *,
    client: SheetsClient,
    job_id: str,
    request_id: str,
    results_rows: list[list[str]],
    audit_rows: list[list[str]] | None = None,
) -> None:
    """
    Export Master results from DB to Sheets (v2 schema).
    
    Creates tabs only at export time with versioned names:
    - Master_v2 (main results)
    - Master_Audit_v2 (optional audit)
    
    Headers are always written explicitly from sheets_schema.py to prevent drift.
    
    Args:
        client: SheetsClient instance
        job_id: Job ID
        request_id: Request ID
        results_rows: Main results (already formatted for Sheets)
        audit_rows: Optional audit rows
    """
    # Prepare exports using schema constants
    exports = [
        {
            "tab": TAB_MASTER_MAIN,
            "headers": HEADERS_MASTER_MAIN,
            "rows": results_rows
        }
    ]
    
    if audit_rows:
        exports.append({
            "tab": TAB_MASTER_AUDIT,
            "headers": HEADERS_MASTER_AUDIT,
            "rows": audit_rows
        })
    
    # Export all tabs (tabs created only at export time)
    export_multiple_tables(client, exports)
    logger.info(f"MASTER_EXPORT_COMPLETE | job_id={job_id} | tabs={len(exports)}")


__all__ = [
    "export_table_to_sheet",
    "export_multiple_tables",
    "export_google_maps_results",
    "export_hunter_results",
    "export_master_results",
]