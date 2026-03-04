"""
Generic Google Sheets formatting primitives.

Classification:
- Role: Reusable batchUpdate helpers only
- No Tool_Status knowledge, no service logic, no tab names
- Pure Google Sheets API primitives
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from n8n_founderstories.services.sheets.rate_limiter import get_rate_limiter


def batch_update(service: Any, spreadsheet_id: str, requests: List[Dict[str, Any]]) -> None:
    """
    Execute a batch update with multiple requests using rate limiting.
    
    Args:
        service: Google Sheets API service
        spreadsheet_id: Spreadsheet ID
        requests: List of batchUpdate request dicts
    """
    if not requests:
        return
    
    rate_limiter = get_rate_limiter()
    rate_limiter.execute_with_retry(
        lambda: service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute(),
        operation="spreadsheets.batchUpdate"
    )


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
    text_color: Optional[Dict[str, float]] = None,
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
        text_color: Text/foreground color dict with red, green, blue keys (0-1 range, optional)
        priority: Rule priority (lower = higher priority)
        
    Returns:
        batchUpdate request dict
    """
    condition: Dict[str, Any] = {"type": condition_type}
    
    if condition_type == "CUSTOM_FORMULA" and formula:
        condition["values"] = [{"userEnteredValue": formula}]
    elif values:
        condition["values"] = values
    
    # Build format dict with background color and optional text color
    format_dict: Dict[str, Any] = {"backgroundColor": bg_color}
    if text_color:
        format_dict["foregroundColor"] = text_color
    
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
                    "format": format_dict,
                },
            },
            "index": priority,
        }
    }


def hide_columns(sheet_id: int, start_col: int, end_col: int = 50) -> Dict[str, Any]:
    """
    Build request to hide columns.
    
    Args:
        sheet_id: Sheet ID (not spreadsheet ID)
        start_col: Start column index (0-based, inclusive)
        end_col: End column index (0-based, exclusive)
        
    Returns:
        batchUpdate request dict
    """
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "COLUMNS",
                "startIndex": start_col,
                "endIndex": end_col,
            },
            "properties": {"hiddenByUser": True},
            "fields": "hiddenByUser",
        }
    }


def hide_rows(sheet_id: int, start_row: int, end_row: int = 3000) -> Dict[str, Any]:
    """
    Build request to hide rows.
    
    Args:
        sheet_id: Sheet ID (not spreadsheet ID)
        start_row: Start row index (0-based, inclusive)
        end_row: End row index (0-based, exclusive)
        
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
            "properties": {"hiddenByUser": True},
            "fields": "hiddenByUser",
        }
    }


def unhide_rows(sheet_id: int, start_row: int, end_row: int) -> Dict[str, Any]:
    """
    Build request to unhide rows.
    
    Args:
        sheet_id: Sheet ID (not spreadsheet ID)
        start_row: Start row index (0-based, inclusive)
        end_row: End row index (0-based, exclusive)
        
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
            "properties": {"hiddenByUser": False},
            "fields": "hiddenByUser",
        }
    }


def add_borders(
    sheet_id: int,
    start_row: int,
    end_row: int,
    start_col: int,
    end_col: int,
    *,
    border_style: str = "SOLID",
    border_width: int = 1,
    border_color: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """
    Build request to add borders to a range of cells.
    
    Args:
        sheet_id: Sheet ID (not spreadsheet ID)
        start_row: Start row index (0-based)
        end_row: End row index (0-based, exclusive)
        start_col: Start column index (0-based)
        end_col: End column index (0-based, exclusive)
        border_style: Border style (SOLID, DASHED, DOTTED, etc.)
        border_width: Border width in pixels
        border_color: Border color dict with red, green, blue keys (0-1 range)
        
    Returns:
        batchUpdate request dict
    """
    if border_color is None:
        # Default to black border
        border_color = {"red": 0.0, "green": 0.0, "blue": 0.0}
    
    border = {
        "style": border_style,
        "width": border_width,
        "color": border_color,
    }
    
    return {
        "updateBorders": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col,
            },
            "top": border,
            "bottom": border,
            "left": border,
            "right": border,
            "innerHorizontal": border,
            "innerVertical": border,
        }
    }


def hex_to_rgb(hex_color: str) -> Dict[str, float]:
    """
    Convert hex color to Google Sheets RGB dict.
    
    Args:
        hex_color: Hex color string (e.g., "#F1F3F4")
        
    Returns:
        Dict with red, green, blue keys (0-1 range)
    """
    hex_color = hex_color.lstrip("#")
    r = int(hex_color[0:2], 16) / 255.0
    g = int(hex_color[2:4], 16) / 255.0
    b = int(hex_color[4:6], 16) / 255.0
def protect_range(
    sheet_id: int,
    start_row: int,
    end_row: int,
    start_col: int,
    end_col: int,
    *,
    description: str = "Protected cells",
    warning_only: bool = True,
) -> Dict[str, Any]:
    """
    Build request to protect a range of cells.
    
    Protected ranges show a warning when users try to edit the cells.
    When using service account authentication, we must use warning_only mode
    to avoid locking out the service account itself.
    
    Args:
        sheet_id: Sheet ID (not spreadsheet ID)
        start_row: Start row index (0-based)
        end_row: End row index (0-based, exclusive)
        start_col: Start column index (0-based)
        end_col: End column index (0-based, exclusive)
        description: Description of the protected range
        warning_only: If True, shows warning but allows editing. Must be True for service accounts.
        
    Returns:
        batchUpdate request dict
        
    Example:
        >>> # Protect cells K2:K10 (send_status column for rows 2-10)
        >>> protect_range(sheet_id=123, start_row=1, end_row=10, start_col=10, end_col=11,
        ...               description="SENT status cells - locked")
    """
    protected_range = {
        "range": {
            "sheetId": sheet_id,
            "startRowIndex": start_row,
            "endRowIndex": end_row,
            "startColumnIndex": start_col,
            "endColumnIndex": end_col,
        },
        "description": description,
        "warningOnly": warning_only,
    }
    
    # Note: We don't set "editors" field because:
    # 1. With service accounts, setting editors={} would lock out the service account
    # 2. warningOnly mode provides user-friendly protection without hard locks
    # 3. Users will see a warning dialog when trying to edit protected cells
    
    return {
        "addProtectedRange": {
            "protectedRange": protected_range
        }
    }


def unprotect_range(protected_range_id: int) -> Dict[str, Any]:
    """
    Build request to remove protection from a range.
    
    Args:
        protected_range_id: ID of the protected range to remove
        
    Returns:
        batchUpdate request dict
    """
    return {
        "deleteProtectedRange": {
            "protectedRangeId": protected_range_id
        }
    }
    return {"red": r, "green": g, "blue": b}