"""
Generic Google Sheets formatting primitives.

Classification:
- Role: Reusable batchUpdate helpers only
- No Tool_Status knowledge, no service logic, no tab names
- Pure Google Sheets API primitives
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def get_sheet_id(service: Any, spreadsheet_id: str, tab_name: str) -> Optional[int]:
    """
    Get the sheet ID for a specific tab.
    
    Args:
        service: Google Sheets API service
        spreadsheet_id: Spreadsheet ID
        tab_name: Tab name to find
        
    Returns:
        Sheet ID (int) or None if not found
    """
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in spreadsheet.get("sheets", []):
            props = sheet.get("properties", {})
            if props.get("title") == tab_name:
                return props.get("sheetId")
    except Exception:
        pass
    return None


def batch_update(service: Any, spreadsheet_id: str, requests: List[Dict[str, Any]]) -> None:
    """
    Execute a batch update with multiple requests.
    
    Args:
        service: Google Sheets API service
        spreadsheet_id: Spreadsheet ID
        requests: List of batchUpdate request dicts
    """
    if not requests:
        return
    
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests}
    ).execute()


def set_column_widths(sheet_id: int, widths: Dict[int, int]) -> List[Dict[str, Any]]:
    """
    Build requests to set column widths.
    
    Args:
        sheet_id: Sheet ID (not spreadsheet ID)
        widths: Dict mapping column index (0-based) to width in pixels
        
    Returns:
        List of batchUpdate requests
    """
    requests = []
    for col_idx, width_px in widths.items():
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": col_idx,
                    "endIndex": col_idx + 1,
                },
                "properties": {"pixelSize": width_px},
                "fields": "pixelSize",
            }
        })
    return requests


def set_row_heights(sheet_id: int, start_row: int, end_row: int, height_px: int) -> Dict[str, Any]:
    """
    Build request to set row heights for a range.
    
    Args:
        sheet_id: Sheet ID (not spreadsheet ID)
        start_row: Start row index (0-based)
        end_row: End row index (0-based, exclusive)
        height_px: Height in pixels
        
    Returns:
        batchUpdate request dict
    """
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "ROWS",
                "startIndex": start_row,
                "endIndex": end_row,
            },
            "properties": {"pixelSize": height_px},
            "fields": "pixelSize",
        }
    }


def freeze_rows(sheet_id: int, num_rows: int) -> Dict[str, Any]:
    """
    Build request to freeze top rows.
    
    Args:
        sheet_id: Sheet ID (not spreadsheet ID)
        num_rows: Number of rows to freeze
        
    Returns:
        batchUpdate request dict
    """
    return {
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {"frozenRowCount": num_rows},
            },
            "fields": "gridProperties.frozenRowCount",
        }
    }


def format_header_row(
    sheet_id: int,
    num_cols: int,
    *,
    bold: bool = True,
    bg_color: Optional[Dict[str, float]] = None,
    h_align: str = "CENTER",
    v_align: str = "MIDDLE",
) -> Dict[str, Any]:
    """
    Build request to format header row (row 0).
    
    Args:
        sheet_id: Sheet ID (not spreadsheet ID)
        num_cols: Number of columns to format
        bold: Whether to make text bold
        bg_color: Background color dict with red, green, blue keys (0-1 range)
        h_align: Horizontal alignment (LEFT, CENTER, RIGHT)
        v_align: Vertical alignment (TOP, MIDDLE, BOTTOM)
        
    Returns:
        batchUpdate request dict
    """
    if bg_color is None:
        bg_color = {"red": 0.9, "green": 0.9, "blue": 0.9}
    
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 1,
                "startColumnIndex": 0,
                "endColumnIndex": num_cols,
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": bg_color,
                    "textFormat": {"bold": bold},
                    "horizontalAlignment": h_align,
                    "verticalAlignment": v_align,
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)",
        }
    }


def set_alignment(
    sheet_id: int,
    start_row: int,
    end_row: int,
    start_col: int,
    end_col: int,
    *,
    h_align: Optional[str] = None,
    v_align: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build request to set cell alignment.
    
    Args:
        sheet_id: Sheet ID (not spreadsheet ID)
        start_row: Start row index (0-based)
        end_row: End row index (0-based, exclusive)
        start_col: Start column index (0-based)
        end_col: End column index (0-based, exclusive)
        h_align: Horizontal alignment (LEFT, CENTER, RIGHT)
        v_align: Vertical alignment (TOP, MIDDLE, BOTTOM)
        
    Returns:
        batchUpdate request dict
    """
    cell_format = {}
    fields = []
    
    if h_align:
        cell_format["horizontalAlignment"] = h_align
        fields.append("horizontalAlignment")
    
    if v_align:
        cell_format["verticalAlignment"] = v_align
        fields.append("verticalAlignment")
    
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col,
            },
            "cell": {"userEnteredFormat": cell_format},
            "fields": f"userEnteredFormat({','.join(fields)})",
        }
    }


def set_wrap_strategy(
    sheet_id: int,
    start_row: int,
    end_row: int,
    start_col: int,
    end_col: int,
    strategy: str,
) -> Dict[str, Any]:
    """
    Build request to set wrap strategy.
    
    Args:
        sheet_id: Sheet ID (not spreadsheet ID)
        start_row: Start row index (0-based)
        end_row: End row index (0-based, exclusive)
        start_col: Start column index (0-based)
        end_col: End column index (0-based, exclusive)
        strategy: Wrap strategy (OVERFLOW_CELL, CLIP, WRAP)
        
    Returns:
        batchUpdate request dict
    """
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col,
            },
            "cell": {"userEnteredFormat": {"wrapStrategy": strategy}},
            "fields": "userEnteredFormat(wrapStrategy)",
        }
    }


def clear_conditional_format_rules(sheet_id: int) -> Dict[str, Any]:
    """
    Build request to clear all conditional formatting rules from a sheet.
    
    Args:
        sheet_id: Sheet ID (not spreadsheet ID)
        
    Returns:
        batchUpdate request dict
    """
    return {
        "deleteConditionalFormatRule": {
            "sheetId": sheet_id,
            "index": 0
        }
    }


def add_conditional_format_rule(
    sheet_id: int,
    start_row: int,
    end_row: int,
    start_col: int,
    end_col: int,
    *,
    condition_type: str,
    formula: Optional[str] = None,
    values: Optional[List[Dict[str, str]]] = None,
    bg_color: Dict[str, float],
    priority: int = 0,
) -> Dict[str, Any]:
    """
    Build request to add conditional formatting rule.
    
    Args:
        sheet_id: Sheet ID (not spreadsheet ID)
        start_row: Start row index (0-based)
        end_row: End row index (0-based, exclusive)
        start_col: Start column index (0-based)
        end_col: End column index (0-based, exclusive)
        condition_type: Condition type (CUSTOM_FORMULA, TEXT_EQ, etc.)
        formula: Formula for CUSTOM_FORMULA type
        values: List of condition value dicts for non-formula types
        bg_color: Background color dict with red, green, blue keys (0-1 range)
        priority: Rule priority (lower = higher priority)
        
    Returns:
        batchUpdate request dict
    """
    condition: Dict[str, Any] = {"type": condition_type}
    
    if condition_type == "CUSTOM_FORMULA" and formula:
        condition["values"] = [{"userEnteredValue": formula}]
    elif values:
        condition["values"] = values
    
    return {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [
                    {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "endRowIndex": end_row,
                        "startColumnIndex": start_col,
                        "endColumnIndex": end_col,
                    }
                ],
                "booleanRule": {
                    "condition": condition,
                    "format": {"backgroundColor": bg_color},
                },
            },
            "index": priority,
        }
    }