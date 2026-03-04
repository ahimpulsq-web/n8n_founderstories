"""
Hunter Leads sheet specification.

Declarative configuration for the Hunter Leads Google Sheets tab.
Defines headers, column widths, formatting, and styling rules.

Classification:
- Role: WHAT the sheet looks like (declarative spec only)
- No Google API calls, no batchUpdate execution, no business logic
- Pure data structure defining sheet layout and appearance

Usage:
    from services.sheets.specs import hunter_leads
    
    # Access configuration
    headers = hunter_leads.HEADERS
    tab_name = hunter_leads.TAB_NAME
"""

from __future__ import annotations

# ============================================================================
# SHEET IDENTITY
# ============================================================================

TAB_NAME = "Hunter Leads"
"""Name of the Google Sheets tab for Hunter.io lead data."""

# ============================================================================
# COLUMN CONFIGURATION
# ============================================================================

HEADERS = ["Organization", "Domain", "Location", "Headcount", "Search Query"]
"""
Column headers for the Hunter Leads sheet.

Columns:
- Organization: Company name
- Domain: Company website domain
- Location: Geographic location (country/city)
- Headcount: Employee count bucket (e.g., "1-10", "11-50")
- Search Query: Search term that found this lead
"""

COLUMN_WIDTHS_PX = [320, 320, 240, 120, 320]
"""Column widths in pixels for columns A through E."""

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

H_ALIGN_PER_COLUMN = ["LEFT", "LEFT", "CENTER", "CENTER", "CENTER"]
"""
Horizontal alignment per column for data rows.

- Organization & Domain: LEFT aligned (text-heavy)
- Location, Headcount, Search Query: CENTER aligned (categorical)
"""

V_ALIGN = "MIDDLE"
"""Vertical alignment for all cells."""

# ============================================================================
# TEXT WRAPPING CONFIGURATION
# ============================================================================

HEADER_WRAP = "WRAP"
"""Text wrap strategy for header row."""

DATA_WRAP_PER_COLUMN = ["CLIP", "CLIP", "CLIP", "CLIP", "CLIP"]
"""
Text wrap strategy per column for data rows.

All columns use CLIP to prevent row height expansion.
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

Matches Tool_Status sheet styling for visual consistency.
"""

# ============================================================================
# CONDITIONAL FORMATTING
# ============================================================================

CONDITIONAL_RULES = []
"""
Conditional formatting rules for this sheet.

Currently empty - no conditional formatting applied.
"""

# ============================================================================
# RANGE DEFINITIONS
# ============================================================================

FORMAT_RANGES = {
    "header_a1": "A1:E1",
    "data_a1": "A2:E2000",
    "all_a1": "A1:E2000",
}
"""
Named ranges for formatting operations.

Ranges use A1 notation and will be converted to GridRange by the writer.
Default data range supports up to 2000 rows.
"""

# ============================================================================
# DIMENSION HIDING
# ============================================================================

HIDE_COLUMNS_FROM = 5
"""
Hide columns starting from this index (0-based).

Hides column F onward (A=0, B=1, C=2, D=3, E=4, F=5).
"""

HIDE_ROWS_FROM = 2001
"""
Hide rows starting from this index (1-based).

Hides rows 2001 onward to keep sheet clean.
"""

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