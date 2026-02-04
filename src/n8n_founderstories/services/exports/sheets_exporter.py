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

# Track which tabs have been formatted to avoid redundant formatting
_FORMATTED_TABS: set[str] = set()


def _format_guard_key(spreadsheet_id: str, tab_name: str) -> str:
    """Generate unique key for tracking formatted tabs."""
    from ...core.utils.text import norm
    return f"{norm(spreadsheet_id)}::{norm(tab_name)}"


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


def format_hunter_tabs_once(client: SheetsClient) -> None:
    """
    One-time formatting for HunterIO tabs using a single batchUpdate.
    
    Formatting specs:
    - Column widths: 320, 320, 140, 140, 320, 200 (Organisation, Domain, Location, Headcount, Search Query, Debug Filters)
    - Row heights: Header 35px, Data rows 30px
    - Header: Bold, Grey (#F1F3F4)
    - Data rows: No coloring (white/default)
    - Hide columns G onwards
    """
    spreadsheet_id = client._spreadsheet_id
    
    # Check if already formatted
    main_key = _format_guard_key(spreadsheet_id, TAB_HUNTER_MAIN)
    if main_key in _FORMATTED_TABS:
        return
    
    try:
        # Get sheet IDs for both tabs
        main_sheet_id = client.get_sheet_id(TAB_HUNTER_MAIN)
        
        if main_sheet_id is None:
            logger.warning("HUNTER_FORMAT_SKIP | main_sheet_not_found")
            return
        
        # Build all requests for a single batchUpdate
        requests = []
        
        # === Format HunterIO_v2 (Main) ===
        # Column widths: 320, 320, 140, 140, 320, 200
        column_widths = [320, 320, 140, 140, 320, 200]
        for col_idx, width in enumerate(column_widths):
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": main_sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": col_idx,
                        "endIndex": col_idx + 1,
                    },
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize",
                }
            })
        
        # Header row height: 35px
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": main_sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": 1,
                },
                "properties": {"pixelSize": 35},
                "fields": "pixelSize",
            }
        })
        
        # Data rows height: 30px (rows 2-1000)
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": main_sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 1,
                    "endIndex": 1000,
                },
                "properties": {"pixelSize": 30},
                "fields": "pixelSize",
            }
        })
        
        # Format header row: Bold, Grey (#F1F3F4), Centered
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": main_sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(HEADERS_HUNTER_MAIN),
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 0.945,
                            "green": 0.953,
                            "blue": 0.957
                        },
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontSize": 10,
                            "bold": True
                        },
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)",
            }
        })
        
        # Hide columns G onwards (column index 6+)
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": main_sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 6,  # Column G (0-indexed)
                    "endIndex": 26,   # Hide through column Z
                },
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser",
            }
        })
        
        # Execute single batchUpdate
        client._service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()
        
        _FORMATTED_TABS.add(main_key)
        logger.debug("HUNTER_FORMAT_SUCCESS | tab=%s | requests=%d", TAB_HUNTER_MAIN, len(requests))
        
    except Exception as e:
        logger.warning("HUNTER_FORMAT_FAILED | error=%s", e)


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
    
    Note: Audit tabs are no longer exported as per requirements.
    
    Headers are always written explicitly from sheets_schema.py to prevent drift.
    
    Args:
        client: SheetsClient instance
        job_id: Job ID
        request_id: Request ID
        results_rows: Main results (already formatted for Sheets)
        audit_rows: Optional audit rows (ignored - no longer exported)
    """
    # Prepare exports using schema constants - NO AUDIT EXPORT
    exports = [
        {
            "tab": TAB_GOOGLE_MAPS_MAIN,
            "headers": HEADERS_GOOGLE_MAPS_MAIN,
            "rows": results_rows
        }
    ]
    
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
    
    Note: Audit tabs are no longer exported as per requirements.
    
    Headers are always written explicitly from sheets_schema.py to prevent drift.
    Applies one-time formatting for professional appearance.
    
    Args:
        client: SheetsClient instance
        job_id: Job ID
        request_id: Request ID
        results_rows: Main results (already formatted for Sheets)
        audit_rows: Optional audit rows (ignored - no longer exported)
    """
    # Prepare exports using schema constants - NO AUDIT EXPORT
    exports = [
        {
            "tab": TAB_HUNTER_MAIN,
            "headers": HEADERS_HUNTER_MAIN,
            "rows": results_rows
        }
    ]
    
    # Export all tabs (tabs created only at export time)
    export_multiple_tables(client, exports)
    
    # Apply one-time formatting (only runs once per spreadsheet)
    format_hunter_tabs_once(client)
    
    logger.info(f"HUNTER_EXPORT_COMPLETE | job_id={job_id} | tabs={len(exports)}")


def format_master_tabs_once(client: SheetsClient) -> None:
    """
    One-time formatting for Master_v2 tab using a single batchUpdate.
    
    Formatting specs (matching HunterIO style):
    - Column widths:
        * master_result_id (A): Hidden
        * Organisation (B): 320px
        * Domain (C): 320px
        * Source (D): 100px
        * Company Name (E): 320px
        * E-mail ID (F): 320px
        * Contact Names (G): 320px
        * Short Company Description (H): 500px (wrapped)
        * Long Company Description (I): 1000px (wrapped)
    - Row heights: Header 35px, Data rows 50px
    - Header: Bold, Grey (#F1F3F4), Font size 10
    - Data rows: No coloring (white/default)
    """
    spreadsheet_id = client._spreadsheet_id
    
    # Check if already formatted
    main_key = _format_guard_key(spreadsheet_id, TAB_MASTER_MAIN)
    if main_key in _FORMATTED_TABS:
        return
    
    try:
        # Get sheet ID for Master_v2
        main_sheet_id = client.get_sheet_id(TAB_MASTER_MAIN)
        
        if main_sheet_id is None:
            logger.warning("MASTER_FORMAT_SKIP | main_sheet_not_found")
            return
        
        # Build all requests for a single batchUpdate
        requests = []
        
        # === Format Master_v2 (Main) ===
        # Column widths: master_result_id will be hidden, others as specified
        column_widths = [0, 320, 320, 100, 320, 320, 320, 500, 1000]  # A-I
        for col_idx, width in enumerate(column_widths):
            if width > 0:  # Skip column A (master_result_id) for now, will hide it
                requests.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": main_sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": col_idx,
                            "endIndex": col_idx + 1,
                        },
                        "properties": {"pixelSize": width},
                        "fields": "pixelSize",
                    }
                })
        
        # Hide column A (master_result_id)
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": main_sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,  # Column A
                    "endIndex": 1,
                },
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser",
            }
        })
        
        # Header row height: 35px
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": main_sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": 1,
                },
                "properties": {"pixelSize": 35},
                "fields": "pixelSize",
            }
        })
        
        # Data rows height: 50px (rows 2-1000)
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": main_sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 1,
                    "endIndex": 1000,
                },
                "properties": {"pixelSize": 50},
                "fields": "pixelSize",
            }
        })
        
        # Format header row: Bold, Grey (#F1F3F4), Centered, Font size 10
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": main_sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(HEADERS_MASTER_MAIN),
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 0.945,
                            "green": 0.953,
                            "blue": 0.957
                        },
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontSize": 10,
                            "bold": True
                        },
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)",
            }
        })
        
        # Set text wrapping for Short Company Description (column H, index 7)
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": main_sheet_id,
                    "startRowIndex": 1,  # Data rows only
                    "endRowIndex": 1000,
                    "startColumnIndex": 7,  # Column H
                    "endColumnIndex": 8,
                },
                "cell": {
                    "userEnteredFormat": {
                        "wrapStrategy": "WRAP"
                    }
                },
                "fields": "userEnteredFormat.wrapStrategy",
            }
        })
        
        # Set text wrapping for Long Company Description (column I, index 8)
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": main_sheet_id,
                    "startRowIndex": 1,  # Data rows only
                    "endRowIndex": 1000,
                    "startColumnIndex": 8,  # Column I
                    "endColumnIndex": 9,
                },
                "cell": {
                    "userEnteredFormat": {
                        "wrapStrategy": "WRAP"
                    }
                },
                "fields": "userEnteredFormat.wrapStrategy",
            }
        })
        
        # Execute single batchUpdate
        client._service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()
        
        _FORMATTED_TABS.add(main_key)
        logger.debug("MASTER_FORMAT_SUCCESS | tab=%s | requests=%d", TAB_MASTER_MAIN, len(requests))
        
    except Exception as e:
        logger.warning("MASTER_FORMAT_FAILED | error=%s", e)


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
    
    Note: Audit tabs are no longer exported as per requirements.
    
    Headers are always written explicitly from sheets_schema.py to prevent drift.
    Applies formatting with hidden master_result_id column and specific column widths.
    
    Args:
        client: SheetsClient instance
        job_id: Job ID
        request_id: Request ID
        results_rows: Main results (already formatted for Sheets)
        audit_rows: Optional audit rows (ignored - no longer exported)
    """
    # Prepare exports using schema constants - NO AUDIT EXPORT
    exports = [
        {
            "tab": TAB_MASTER_MAIN,
            "headers": HEADERS_MASTER_MAIN,
            "rows": results_rows
        }
    ]
    
    # Export all tabs (tabs created only at export time)
    export_multiple_tables(client, exports)
    
    # Apply one-time formatting (only runs once per spreadsheet)
    format_master_tabs_once(client)
    
    logger.info(f"MASTER_EXPORT_COMPLETE | job_id={job_id} | tabs={len(exports)}")


__all__ = [
    "export_table_to_sheet",
    "export_multiple_tables",
    "export_google_maps_results",
    "export_hunter_results",
    "export_master_results",
]