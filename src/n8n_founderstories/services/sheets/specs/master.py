"""
Master Results sheet specification.

Declarative configuration for the Master Results tab.
Stage 1: Clean status table with 5 columns only.

Classification:
- Role: WHAT the sheet looks like (declarative spec only)
- No Google API calls, no batchUpdate execution, no business logic
- Pure data structure defining sheet layout and appearance

Usage:
    from services.sheets.specs import master
    
    # Access configuration
    headers = master.HEADERS
    tab_name = master.TAB_NAME
"""

from __future__ import annotations

# ============================================================================
# SHEET IDENTITY
# ============================================================================

TAB_NAME = "Master"
"""Name of the Google Sheets tab for Master consolidated data."""

# ============================================================================
# COLUMN CONFIGURATION
# ============================================================================

HEADERS = [
    "request_id",
    "job_id",
    "organization",
    "source",
    "domain",
    "crawl_status",
    "extract_status",
    "enrichment_status",
    "mail_write_status",
    "mail_send_status",
]
"""
Column headers for the Master Results sheet.

Columns (in exact order):
- request_id: Request identifier
- job_id: Job identifier
- organization: Company name
- source: Data source (hunter, google_maps, or "hunter, google_maps")
- domain: Company website domain
- crawl_status: Crawl status (NULL=not started, "processing", "succeeded", "failed", "reused")
- extract_status: Extraction status (NULL=not started, "processing", "succeeded", "failed", "reused")
- enrichment_status: Enrichment status (NULL=not started, "processing", "succeeded", "failed")
- mail_write_status: Mail content write status (NULL=not started, "succeeded", "failed")
- mail_send_status: Mail send status (NULL=not started, "contacted")
"""

COLUMN_WIDTHS_PX = [200, 200, 320, 180, 320, 150, 150, 150, 150, 150]
"""
Column widths in pixels for columns A through H.

Column widths adjusted for swapped organization and source positions.
"""

HIDDEN_COLUMNS = [0, 1]
"""
Hidden columns (0-based indices).

Columns A (request_id) and B (job_id) are hidden as they are internal identifiers.
Only organization, source, domain, crawl_status, and extract_status are visible to users.
"""

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

H_ALIGN_PER_COLUMN = ["LEFT", "LEFT", "LEFT", "CENTER", "LEFT", "CENTER", "CENTER", "CENTER", "CENTER", "CENTER"]
"""
Horizontal alignment per column for data rows.

- request_id (A): LEFT (text)
- job_id (B): LEFT (text)
- organization (C): LEFT (text-heavy)
- source (D): CENTER (categorical)
- domain (E): LEFT (text-heavy)
- crawl_status (F): CENTER (categorical)
- extract_status (G): CENTER (categorical)
- enrichment_status (H): CENTER (categorical)
- mail_write_status (I): CENTER (categorical)
- mail_send_status (J): CENTER (categorical)
"""

V_ALIGN = "MIDDLE"
"""Vertical alignment for all cells."""

# ============================================================================
# TEXT WRAPPING CONFIGURATION
# ============================================================================

HEADER_WRAP = "WRAP"
"""Text wrap strategy for header row."""

DATA_WRAP_PER_COLUMN = ["CLIP", "CLIP", "CLIP", "CLIP", "CLIP", "CLIP", "CLIP", "CLIP", "CLIP", "CLIP"]
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

# Note: We don't use CONDITIONAL_FORMAT_COLUMNS because we need separate rules per column
# Each rule specifies its own column range via the "columns" key

CONDITIONAL_RULES = [
    # Crawl Status (Column F, index 5) - Same colors as Extract Status
    {
        "name": "CRAWL_FAILED",
        "formula": '=$F2="failed"',
        "background_hex": "#FFC7CE",  # Red background
        "text_hex": "#9C0006",  # Red text
        "priority": 0,
        "columns": (5, 6),  # Column F only (0-based, end exclusive)
    },
    {
        "name": "CRAWL_SUCCEEDED",
        "formula": '=$F2="succeeded"',
        "background_hex": "#C6EFCE",  # Green background
        "text_hex": "#006100",  # Green text
        "priority": 1,
        "columns": (5, 6),  # Column F only
    },
    {
        "name": "CRAWL_PROCESSING",
        "formula": '=$F2="processing"',
        "background_hex": "#FFEB9C",  # Yellow background
        "text_hex": "#9C6500",  # Yellow text
        "priority": 2,
        "columns": (5, 6),  # Column F only
    },
    {
        "name": "CRAWL_REUSED",
        "formula": '=$F2="reused"',
        "background_hex": "#DDEBF7",  # Blue background
        "text_hex": "#1F4E79",  # Blue text
        "priority": 3,
        "columns": (5, 6),  # Column F only
    },
    {
        "name": "CRAWL_NOT_STARTED",
        "formula": '=ISBLANK($F2)',
        "background_hex": "#F1F3F4",  # Same as header (grey)
        "priority": 4,
        "columns": (5, 6),  # Column F only
    },
    # Extract Status (Column G, index 6) - Same colors as Crawl Status
    {
        "name": "EXTRACT_FAILED",
        "formula": '=$G2="failed"',
        "background_hex": "#FFC7CE",  # Red background
        "text_hex": "#9C0006",  # Red text
        "priority": 5,
        "columns": (6, 7),  # Column G only (0-based, end exclusive)
    },
    {
        "name": "EXTRACT_SUCCEEDED",
        "formula": '=$G2="succeeded"',
        "background_hex": "#C6EFCE",  # Green background
        "text_hex": "#006100",  # Green text
        "priority": 6,
        "columns": (6, 7),  # Column G only
    },
    {
        "name": "EXTRACT_PROCESSING",
        "formula": '=$G2="processing"',
        "background_hex": "#FFEB9C",  # Yellow background
        "text_hex": "#9C6500",  # Yellow text
        "priority": 7,
        "columns": (6, 7),  # Column G only
    },
    {
        "name": "EXTRACT_REUSED",
        "formula": '=$G2="reused"',
        "background_hex": "#DDEBF7",  # Blue background
        "text_hex": "#1F4E79",  # Blue text
        "priority": 8,
        "columns": (6, 7),  # Column G only
    },
    {
        "name": "EXTRACT_NOT_STARTED",
        "formula": '=ISBLANK($G2)',
        "background_hex": "#F1F3F4",  # Same as header (grey)
        "priority": 9,
        "columns": (6, 7),  # Column G only
    },
    # Enrichment Status (Column H, index 7) - Same colors as Extract Status
    {
        "name": "ENRICHMENT_FAILED",
        "formula": '=$H2="failed"',
        "background_hex": "#FFC7CE",  # Red background
        "text_hex": "#9C0006",  # Red text
        "priority": 10,
        "columns": (7, 8),  # Column H only (0-based, end exclusive)
    },
    {
        "name": "ENRICHMENT_SUCCEEDED",
        "formula": '=$H2="succeeded"',
        "background_hex": "#C6EFCE",  # Green background
        "text_hex": "#006100",  # Green text
        "priority": 11,
        "columns": (7, 8),  # Column H only
    },
    {
        "name": "ENRICHMENT_PROCESSING",
        "formula": '=$H2="processing"',
        "background_hex": "#FFEB9C",  # Yellow background
        "text_hex": "#9C6500",  # Yellow text
        "priority": 12,
        "columns": (7, 8),  # Column H only
    },
    {
        "name": "ENRICHMENT_NOT_STARTED",
        "formula": '=ISBLANK($H2)',
        "background_hex": "#F1F3F4",  # Same as header (grey)
        "priority": 13,
        "columns": (7, 8),  # Column H only
    },
    # Mail Write Status (Column I, index 8) - Same colors as other status columns
    {
        "name": "MAIL_WRITE_FAILED",
        "formula": '=$I2="failed"',
        "background_hex": "#FFC7CE",  # Red background
        "text_hex": "#9C0006",  # Red text
        "priority": 14,
        "columns": (8, 9),  # Column I only (0-based, end exclusive)
    },
    {
        "name": "MAIL_WRITE_SUCCEEDED",
        "formula": '=$I2="succeeded"',
        "background_hex": "#C6EFCE",  # Green background
        "text_hex": "#006100",  # Green text
        "priority": 15,
        "columns": (8, 9),  # Column I only
    },
    {
        "name": "MAIL_WRITE_NOT_STARTED",
        "formula": '=ISBLANK($I2)',
        "background_hex": "#F1F3F4",  # Same as header (grey)
        "priority": 16,
        "columns": (8, 9),  # Column I only
    },
    # Mail Send Status (Column J, index 9) - Green for contacted, red for failed, grey for not started
    {
        "name": "MAIL_SEND_FAILED",
        "formula": '=$J2="FAILED"',
        "background_hex": "#FFC7CE",  # Red background
        "text_hex": "#9C0006",  # Red text
        "priority": 17,
        "columns": (9, 10),  # Column J only (0-based, end exclusive)
    },
    {
        "name": "MAIL_SEND_CONTACTED",
        "formula": '=$J2="CONTACTED"',
        "background_hex": "#C6EFCE",  # Green background
        "text_hex": "#006100",  # Green text
        "priority": 18,
        "columns": (9, 10),  # Column J only
    },
    {
        "name": "MAIL_SEND_NOT_STARTED",
        "formula": '=ISBLANK($J2)',
        "background_hex": "#F1F3F4",  # Same as header (grey)
        "priority": 19,
        "columns": (9, 10),  # Column J only
    },
]
"""
Conditional formatting rules for crawl_status, extract_status, and enrichment_status columns.

All three columns use the SAME colors based on cell value:
- succeeded: Green (#C6EFCE background, #006100 text)
- failed: Red (#FFC7CE background, #9C0006 text)
- processing: Yellow (#FFEB9C background, #9C6500 text) - all status columns
- reused: Blue (#DDEBF7 background, #1F4E79 text) - crawl_status and extract_status only
- NOT_STARTED (blank): Grey (#F1F3F4) - same as header

Each rule specifies its own column range to apply formatting correctly.
"""

# ============================================================================
# RANGE DEFINITIONS
# ============================================================================

FORMAT_RANGES = {
    "header_a1": "A1:J1",
    "data_a1": "A2:J2000",
    "all_a1": "A1:J2000",
}
"""
Named ranges for formatting operations.

Ranges use A1 notation and will be converted to GridRange by the writer.
Default data range supports up to 2000 rows.
"""

# ============================================================================
# DIMENSION HIDING
# ============================================================================

HIDE_COLUMNS_FROM = 10
"""
Hide columns starting from this index (0-based).

Hides column K onward (A=0...E=4, F=5, G=6, H=7, I=8, J=9, K=10).
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