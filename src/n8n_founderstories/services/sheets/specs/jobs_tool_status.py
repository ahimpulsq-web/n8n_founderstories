"""
Tool Status sheet specification.

Declarative configuration for the Tool Status Google Sheets tab.
Defines headers, column widths, formatting, and styling rules.

Classification:
- Role: WHAT the sheet looks like (declarative spec only)
- No Google API calls, no batchUpdate execution, no business logic
- Pure data structure defining sheet layout and appearance

Usage:
    from services.sheets.specs import jobs_tool_status
    
    # Access configuration
    headers = jobs_tool_status.HEADERS
    tab_name = jobs_tool_status.TAB_NAME
"""

from __future__ import annotations

# ============================================================================
# SHEET IDENTITY
# ============================================================================

TAB_NAME = "Tool Status"
"""Name of the Google Sheets tab for job status tracking."""

# ============================================================================
# COLUMN CONFIGURATION
# ============================================================================

HEADERS = ["Tool", "State", "Request ID", "Job ID"]
"""
Column headers for the Tool Status sheet.

Columns:
- Tool: Tool name (e.g., "hunteriov2", "googlemapsv2", "crawl (X/Y)")
- State: Job state (QUEUED, RUNNING, SUCCEEDED, FAILED)
- Request ID: Request correlation ID
- Job ID: Unique job identifier (empty for crawler)

Note: Crawler row shows progress as "crawl (completed/total)" in Tool column
"""

COLUMN_WIDTHS_PX = [180, 120, 320, 320]
"""Column widths in pixels for columns A through D."""

# ============================================================================
# ROW CONFIGURATION
# ============================================================================

HEADER_ROW_HEIGHT_PX = 36
"""Height of the header row in pixels."""

DATA_ROW_HEIGHT_PX = 32
"""Height of data rows in pixels."""

FROZEN_ROWS = 1
"""Number of rows to freeze (header row)."""

# ============================================================================
# ALIGNMENT CONFIGURATION
# ============================================================================

H_ALIGN_PER_COLUMN = ["CENTER", "CENTER", "CENTER", "CENTER"]
"""
Horizontal alignment per column for data rows.

All columns are CENTER aligned for dashboard consistency.
"""

V_ALIGN = "MIDDLE"
"""Vertical alignment for all cells."""

# ============================================================================
# TEXT WRAPPING CONFIGURATION
# ============================================================================

HEADER_WRAP = "WRAP"
"""Text wrap strategy for header row."""

DATA_WRAP_PER_COLUMN = ["CLIP", "CLIP", "CLIP", "CLIP"]
"""
Text wrap strategy per column for data rows.

All columns use CLIP to prevent row height expansion and keep IDs readable.
"""

# ============================================================================
# STYLING CONFIGURATION
# ============================================================================

HEADER_STYLE = {
    "bold": True,
    "background_hex": "#F1F3F4",
}
"""
Header row styling configuration.

Matches other sheet specs for visual consistency.
"""

# ============================================================================
# CONDITIONAL FORMATTING
# ============================================================================

CONDITIONAL_FORMAT_COLUMNS = (0, 4)
"""
Column range for conditional formatting (start_col, end_col).

Applies formatting to columns A through D (0-based, end exclusive).
This ensures the entire row is highlighted based on the State column.
"""

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
        "background_hex": "#C6EFCE",  # light green (same as Master tab succeeded)
        "priority": 1,
    },
    {
        "name": "DEFAULT",
        "formula": '=AND($A2<>"", $B2<>"FAILED", $B2<>"SUCCEEDED")',
        "background_hex": "#FFF6CC",  # light yellow (RUNNING/QUEUED)
        "priority": 2,
    },
]
"""
Conditional formatting rules for this sheet.

Rules are applied in priority order:
1. FAILED jobs: light red background
2. SUCCEEDED jobs: light green background (#C6EFCE - same as Master tab)
3. Other states (RUNNING/QUEUED): light yellow background
"""

# ============================================================================
# RANGE DEFINITIONS
# ============================================================================

FORMAT_RANGES = {
    "header_a1": "A1:D1",
    "data_a1": "A2:D1000",
    "all_a1": "A1:D1000",
}
"""
Named ranges for formatting operations.

Ranges use A1 notation and will be converted to GridRange by the writer.
Default data range supports up to 1000 rows.
"""

# ============================================================================
# DIMENSION HIDING
# ============================================================================

HIDE_COLUMNS_FROM = 4
"""
Hide columns starting from this index (0-based).

Hides column E onward (A=0, B=1, C=2, D=3, E=4).
"""

# Note: Row hiding is dynamic based on used_rows in the writer
# No static HIDE_ROWS_FROM needed

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def hex_to_rgb(hex_color: str) -> dict[str, float]:
    """
    Convert hex color to Google Sheets RGB dict.
    
    Google Sheets API requires RGB values in 0-1 range.
    
    Args:
        hex_color: Hex color string (e.g., "#F1F3F4" or "F1F3F4")
        
    Returns:
        Dict with red, green, blue keys, values in 0-1 range
        
    Example:
        >>> hex_to_rgb("#F1F3F4")
        {'red': 0.945, 'green': 0.953, 'blue': 0.957}
    """
    hex_color = hex_color.lstrip("#")
    r = int(hex_color[0:2], 16) / 255.0
    g = int(hex_color[2:4], 16) / 255.0
    b = int(hex_color[4:6], 16) / 255.0
    return {"red": r, "green": g, "blue": b}