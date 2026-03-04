"""
Mails sheet specification.

Declarative configuration for the Mails tab.
Exports mail content data from mail_content table.

Classification:
- Role: WHAT the sheet looks like (declarative spec only)
- No Google API calls, no batchUpdate execution, no business logic
- Pure data structure defining sheet layout and appearance

Usage:
    from services.sheets.specs import mails
    
    # Access configuration
    headers = mails.HEADERS
    tab_name = mails.TAB_NAME
"""

from __future__ import annotations

# ============================================================================
# SHEET IDENTITY
# ============================================================================

TAB_NAME = "Mails"
"""Name of the Google Sheets tab for Mails data."""

# ============================================================================
# COLUMN CONFIGURATION
# ============================================================================

HEADERS = [
    "request_id",
    "job_id",
    "organisation",
    "domain",
    "company",
    "email",
    "contacts",
    "test_recipient",
    "subject",
    "content",
    "send_status",
    "comments",
]
"""
Column headers for the Mails sheet.

Columns (in exact order):
- request_id: Request identifier (hidden)
- job_id: Job identifier (hidden)
- organisation: Company name from mail_content
- domain: Company website domain
- company: Company name from mail_content
- email: Contact email from mail_content
- contacts: Contact persons from mail_content
- test_recipient: Test email recipient (blank, user fills)
- subject: Email subject line (from mail_content)
- content: Email content/body (from mail_content)
- send_status: Send status (from mail_content)
- comments: User comments/notes (from mail_content)
"""

COLUMN_WIDTHS_PX = [200, 200, 320, 240, 260, 260, 260, 260, 320, 500, 150, 300]
"""
Column widths in pixels for columns A through L.

Widths optimized for readability:
- request_id, job_id: 200px (hidden)
- organisation: 320px
- domain: 240px
- company: 260px
- email: 260px
- contacts: 260px
- test_recipient: 260px
- subject: 320px
- content: 500px (wider for wrapped text)
- send_status: 150px
- comments: 300px
"""

HIDDEN_COLUMNS = [0, 1]
"""
Hidden columns (0-based indices).

Columns A (request_id) and B (job_id) are hidden.
"""

# ============================================================================
# ROW CONFIGURATION
# ============================================================================

HEADER_ROW_HEIGHT_PX = 36
"""Height of the header row in pixels."""

DATA_ROW_HEIGHT_PX = 50
"""Height of data rows in pixels."""

FROZEN_ROWS = 1
"""Number of rows to freeze (header row)."""

# ============================================================================
# ALIGNMENT CONFIGURATION
# ============================================================================

H_ALIGN_PER_COLUMN = [
    "LEFT",    # request_id
    "LEFT",    # job_id
    "LEFT",    # organisation
    "LEFT",    # domain
    "LEFT",    # company
    "LEFT",    # email
    "LEFT",    # contacts
    "LEFT",    # test_recipient
    "LEFT",    # subject
    "LEFT",    # content
    "CENTER",  # send_status
    "LEFT",    # comments
]
"""
Horizontal alignment per column for data rows.

Most columns are LEFT-aligned for text readability.
send_status is CENTER-aligned as it is a categorical field.
"""

V_ALIGN = "MIDDLE"
"""Vertical alignment for all cells."""

# ============================================================================
# TEXT WRAPPING CONFIGURATION
# ============================================================================

HEADER_WRAP = "WRAP"
"""Text wrap strategy for header row."""

DATA_WRAP_PER_COLUMN = [
    "CLIP",  # request_id
    "CLIP",  # job_id
    "CLIP",  # organisation
    "CLIP",  # domain
    "CLIP",  # company
    "CLIP",  # email
    "WRAP",  # contacts (wrapped)
    "CLIP",  # test_recipient
    "WRAP",  # subject (wrapped)
    "WRAP",  # content (wrapped)
    "CLIP",  # send_status
    "WRAP",  # comments (wrapped)
]
"""
Text wrap strategy per column for data rows.

Wrapped columns: contacts, subject, content, comments
All others use CLIP to prevent row height expansion.
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

Matches other sheet styling for visual consistency.
"""

APPLY_BORDERS = True
"""
Apply borders to all cells in the sheet.

Borders are applied to header and data rows (excluding buffer rows).
Uses solid black borders with 1px width.
"""

# ============================================================================
# CONDITIONAL FORMATTING
# ============================================================================

CONDITIONAL_RULES = [
    # Send_status column (Column K, index 10)
    {
        "name": "SEND_STATUS_VERIFY",
        "formula": '=$K2="VERIFY"',
        "background_hex": "#FFEB9C",  # Yellow background
        "text_hex": "#9C6500",  # Yellow text
        "priority": 0,
        "columns": (10, 11),  # Column K only (0-based, end exclusive)
    },
    {
        "name": "SEND_STATUS_READY",
        "formula": '=$K2="READY"',
        "background_hex": "#DDEBF7",  # Blue background
        "text_hex": "#1F4E79",  # Blue text
        "priority": 1,
        "columns": (10, 11),  # Column K only
    },
    {
        "name": "SEND_STATUS_SENT",
        "formula": '=$K2="SENT"',
        "background_hex": "#E4DFEC",  # Purple/Lavender background
        "text_hex": "#5B2C6F",  # Purple text
        "priority": 2,
        "columns": (10, 11),  # Column K only
    },
    {
        "name": "SEND_STATUS_CONTACTED",
        "formula": '=$K2="CONTACTED"',
        "background_hex": "#E4DFEC",  # Purple/Lavender background (same as SENT)
        "text_hex": "#5B2C6F",  # Purple text (same as SENT)
        "priority": 3,
        "columns": (10, 11),  # Column K only
    },
    {
        "name": "SEND_STATUS_OK",
        "formula": '=$K2="OK"',
        "background_hex": "#C6EFCE",  # Green background
        "text_hex": "#006100",  # Green text
        "priority": 4,
        "columns": (10, 11),  # Column K only
    },
    {
        "name": "SEND_STATUS_FAILED",
        "formula": '=$K2="FAILED"',
        "background_hex": "#FFC7CE",  # Red background
        "text_hex": "#9C0006",  # Red text
        "priority": 5,
        "columns": (10, 11),  # Column K only
    },
]
"""
Conditional formatting rules for send_status column.

Send_status column (K):
- Yellow (#FFEB9C background, #9C6500 text): "VERIFY" status (needs verification)
- Blue (#DDEBF7 background, #1F4E79 text): "READY" status (ready to send)
- Purple (#E4DFEC background, #5B2C6F text): "SENT" status (email has been sent)
- Purple (#E4DFEC background, #5B2C6F text): "CONTACTED" status (email was sent in a previous request)
- Green (#C6EFCE background, #006100 text): "OK" status (sent successfully and confirmed)
- Red (#FFC7CE background, #9C0006 text): "FAILED" status (send failed)
"""

# ============================================================================
# RANGE DEFINITIONS
# ============================================================================

FORMAT_RANGES = {
    "header_a1": "A1:L1",
    "data_a1": "A2:L2000",
    "all_a1": "A1:L2000",
}
"""
Named ranges for formatting operations.

Ranges use A1 notation and will be converted to GridRange by the writer.
Default data range supports up to 2000 rows.
"""

# ============================================================================
# DIMENSION HIDING
# ============================================================================

HIDE_COLUMNS_FROM = 12
"""
Hide columns starting from this index (0-based).

Hides column M onward (A=0...L=11, M=12).
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