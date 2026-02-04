"""
Tool_Status sheet specification.

Declarative configuration for the Tool_Status dashboard tab.

Classification:
- Role: WHAT the sheet looks like (declarative spec only)
- No Google API calls, no batchUpdate execution, no business logic
"""

from __future__ import annotations

TAB_NAME = "Tool_Status"

HEADERS = ["Tool", "State", "Request ID", "Job ID"]

# Column widths in pixels (A..D)
COLUMN_WIDTHS_PX = [140, 120, 320, 320]

# Row heights in pixels
HEADER_ROW_HEIGHT_PX = 36
DATA_ROW_HEIGHT_PX = 32

# Freeze first row
FROZEN_ROWS = 1

# Alignment (apply to ALL cells A:D, header + data)
H_ALIGN = "CENTER"
V_ALIGN = "MIDDLE"

# Wrap strategy
HEADER_WRAP = "WRAP"   # header row only
DATA_WRAP = "CLIP"     # data rows only (fast, no wrapping of IDs)

# Header styling
HEADER_STYLE = {
    "bold": True,
    "background_hex": "#F1F3F4",
}

# Conditional formatting rules (apply to range A2:D)
# Priority: FAILED > SUCCEEDED > DEFAULT
# Only color rows that have data (check if Tool column is not empty)
CONDITIONAL_RULES = [
    {
        "name": "FAILED",
        "formula": '=AND($A2<>"", $B2="FAILED")',
        "background_hex": "#FADADD",  # light red
        "priority": 0,
    },
    {
        "name": "SUCCEEDED",
        "formula": '=AND($A2<>"", $B2="SUCCEEDED")',
        "background_hex": "#DFF2BF",  # light green
        "priority": 1,
    },
    {
        "name": "DEFAULT",
        "formula": '=AND($A2<>"", $B2<>"FAILED", $B2<>"SUCCEEDED")',
        "background_hex": "#FFF6CC",  # light yellow (RUNNING/other)
        "priority": 2,
    },
]

# Range definitions (writer will translate to GridRange)
# Data region includes header + data for alignment, widths apply to columns
FORMAT_RANGES = {
    "header_a1": "A1:D1",
    "data_a1": "A2:D1000",   # safe default; writer can choose a larger bound
    "all_a1": "A1:D1000",
}


def hex_to_rgb(hex_color: str) -> dict[str, float]:
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
    return {"red": r, "green": g, "blue": b}