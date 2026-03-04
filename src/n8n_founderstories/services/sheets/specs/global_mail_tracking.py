"""
Global Mail Tracking sheet specification.

Declarative configuration for the Mail Tracker tab.
Tracks ALL sent emails across ALL requests for global tracking.

Classification:
- Role: WHAT the sheet looks like (declarative spec only)
- No Google API calls, no batchUpdate execution, no business logic
- Pure data structure defining sheet layout and appearance

Usage:
    from services.sheets.specs import global_mail_tracking
    
    headers = global_mail_tracking.HEADERS
    tab_name = global_mail_tracking.TAB_NAME
"""

from __future__ import annotations

# ============================================================================
# SHEET IDENTITY
# ============================================================================

TAB_NAME = "Mail Tracker"
"""Name of the Google Sheets tab for Mail Tracker data."""

# ============================================================================
# COLUMN CONFIGURATION
# ============================================================================

HEADERS = [
    "request_id",
    "thread_id",
    "Company",
    "Domain",
    "Contacts",
    "E-Mail",
    "Send Status",
    "Sent At",
    "Reply Status",
    "Received At",
    "Action",
    "comments",
]
"""
Column headers for the Mail Tracker sheet.

Columns (in exact order):
- request_id: Request identifier (hidden)
- thread_id: Unique email send ID from email service
- Company: Company name
- Domain: Company domain
- Contacts: Contact names (hidden)
- E-Mail: Email address
- Send Status: Send status (SENT, FAILED, etc.)
- Sent At: Timestamp when email was sent (human-friendly format)
- Reply Status: Reply status (RECEIVED, REPLIED, NO_REPLY, etc.)
- Received At: Timestamp when reply was received (human-friendly format)
- Action: Action buttons or links
- comments: Additional comments
"""

COLUMN_WIDTHS_PX = [150, 200, 320, 320, 150, 320, 120, 180, 120, 180, 120, 250]
"""
Column widths in pixels for columns A through L.

Widths optimized for readability:
- request_id: 150px (hidden)
- thread_id: 200px
- Company: 320px
- Domain: 320px
- Contacts: 150px (hidden)
- E-Mail: 320px
- Send Status: 120px
- Sent At: 180px
- Reply Status: 120px
- Received At: 180px
- Action: 120px
- comments: 250px
"""

HIDDEN_COLUMNS = [0, 4]
"""
Hidden columns (0-based indices).

Hidden columns:
- 0: request_id (internal use only)
- 4: Contacts (not needed in tracker view)
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

FROZEN_COLUMNS = 0
"""Number of columns to freeze (none)."""

# ============================================================================
# ALIGNMENT CONFIGURATION
# ============================================================================

H_ALIGN_PER_COLUMN = [
    "LEFT",    # request_id (hidden)
    "LEFT",    # thread_id
    "LEFT",    # Company
    "LEFT",    # Domain
    "LEFT",    # Contacts (hidden)
    "LEFT",    # E-Mail
    "CENTER",  # Send Status
    "CENTER",  # Sent At
    "CENTER",  # Reply Status
    "CENTER",  # Received At
    "CENTER",  # Action
    "LEFT",    # comments
]
"""
Horizontal alignment per column for data rows.

Most columns are LEFT-aligned for text readability.
Status, timestamp, and action columns are CENTER-aligned.
"""

V_ALIGN = "MIDDLE"
"""Vertical alignment for all cells."""

# ============================================================================
# TEXT WRAPPING CONFIGURATION
# ============================================================================

HEADER_WRAP = "WRAP"
"""Text wrap strategy for header row."""

DATA_WRAP_PER_COLUMN = [
    "CLIP",  # request_id (hidden)
    "CLIP",  # thread_id
    "CLIP",  # Company
    "CLIP",  # Domain
    "WRAP",  # Contacts (wrapped, hidden)
    "CLIP",  # E-Mail
    "CLIP",  # Send Status
    "CLIP",  # Sent At
    "CLIP",  # Reply Status
    "CLIP",  # Received At
    "CLIP",  # Action
    "WRAP",  # comments (wrapped)
]
"""
Text wrap strategy per column for data rows.

Wrapped columns: Contacts, comments
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

Borders are applied to header and data rows.
Uses solid black borders with 1px width.
"""

# ============================================================================
# CONDITIONAL FORMATTING
# ============================================================================

CONDITIONAL_RULES = [
    # Send Status column (Column G, index 6)
    {
        "name": "SEND_STATUS_SENT",
        "formula": '=$G2="SENT"',
        "background_hex": "#C6EFCE",  # Green background
        "text_hex": "#006100",  # Green text
        "priority": 0,
        "columns": (6, 7),  # Column G only (0-based, end exclusive)
    },
    {
        "name": "SEND_STATUS_FAILED",
        "formula": '=$G2="FAILED"',
        "background_hex": "#FFC7CE",  # Red background
        "text_hex": "#9C0006",  # Red text
        "priority": 1,
        "columns": (6, 7),  # Column G only
    },
    {
        "name": "SEND_STATUS_PENDING",
        "formula": '=$G2="PENDING"',
        "background_hex": "#FFEB9C",  # Yellow background
        "text_hex": "#9C6500",  # Yellow text
        "priority": 2,
        "columns": (6, 7),  # Column G only
    },
    # Reply Status column (Column I, index 8)
    {
        "name": "REPLY_STATUS_RECEIVED",
        "formula": '=$I2="RECEIVED"',
        "background_hex": "#C6EFCE",  # Green background
        "text_hex": "#006100",  # Green text
        "priority": 3,
        "columns": (8, 9),  # Column I only
    },
    {
        "name": "REPLY_STATUS_REPLIED",
        "formula": '=$I2="REPLIED"',
        "background_hex": "#C6EFCE",  # Green background
        "text_hex": "#006100",  # Green text
        "priority": 4,
        "columns": (8, 9),  # Column I only
    },
    {
        "name": "REPLY_STATUS_BOUNCED",
        "formula": '=$I2="BOUNCED"',
        "background_hex": "#FFC7CE",  # Red background
        "text_hex": "#9C0006",  # Red text
        "priority": 5,
        "columns": (8, 9),  # Column I only
    },
    {
        "name": "REPLY_STATUS_NO_REPLY",
        "formula": '=$I2="NO_REPLY"',
        "background_hex": "#FFEB9C",  # Yellow background
        "text_hex": "#9C6500",  # Yellow text
        "priority": 6,
        "columns": (8, 9),  # Column I only
    },
]
"""
Conditional formatting rules for Send Status and Reply Status columns.

Send Status column (G):
- Green (#C6EFCE background, #006100 text): "SENT" status
- Red (#FFC7CE background, #9C0006 text): "FAILED" status
- Yellow (#FFEB9C background, #9C6500 text): "PENDING" status

Reply Status column (I):
- Green (#C6EFCE background, #006100 text): "RECEIVED" or "REPLIED" status
- Red (#FFC7CE background, #9C0006 text): "BOUNCED" status
- Yellow (#FFEB9C background, #9C6500 text): "NO_REPLY" status
"""

# ============================================================================
# RANGE DEFINITIONS
# ============================================================================

FORMAT_RANGES = {
    "header_a1": "A1:L1",
    "data_a1": "A2:L5000",
    "all_a1": "A1:L5000",
}
"""
Named ranges for formatting operations.

Ranges use A1 notation and will be converted to GridRange by the writer.
Default data range supports up to 5000 rows (more than Mails sheet since this is global).
Now includes column L for the Action column.
"""

# ============================================================================
# DIMENSION HIDING
# ============================================================================

HIDE_COLUMNS_FROM = 12
"""
Hide columns starting from this index (0-based).

Hides column M onward (A=0...L=11, M=12).
"""

HIDE_ROWS_FROM = 5001
"""
Hide rows starting from this index (1-based).

Hides rows 5001 onward to keep sheet clean.
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