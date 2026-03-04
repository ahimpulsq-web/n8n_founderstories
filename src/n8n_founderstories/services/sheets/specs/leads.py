"""
Leads sheet specification.

Declarative configuration for the Leads tab.
Exports enriched results with email verification and send status tracking.

Classification:
- Role: WHAT the sheet looks like (declarative spec only)
- No Google API calls, no batchUpdate execution, no business logic
- Pure data structure defining sheet layout and appearance

Usage:
    from services.sheets.specs import leads
    
    # Access configuration
    headers = leads.HEADERS
    tab_name = leads.TAB_NAME
"""

from __future__ import annotations

# ============================================================================
# SHEET IDENTITY
# ============================================================================

TAB_NAME = "Leads"
"""Name of the Google Sheets tab for Leads data."""

# ============================================================================
# COLUMN CONFIGURATION
# ============================================================================

HEADERS = [
    "request_id",
    "job_id",
    "organisation",
    "domain",
    "company",
    "company_score",
    "email",
    "email_score",
    "contacts",
    "description",
    "verification_link",
    "send_status",
    "comments",
]
"""
Column headers for the Leads sheet.

Columns (in exact order):
- request_id: Request identifier (hidden)
- job_id: Job identifier (hidden)
- organisation: Company name from mstr_results
- domain: Company website domain
- company: Company name from enrichment (company JSON - name field)
- company_score: Company confidence score (hidden, used for conditional formatting)
- email: Best email from enrichment (email JSON - email field)
- email_score: Email confidence score (hidden, used for conditional formatting)
- contacts: Contact persons from enrichment (wrapped)
- description: Short description from enrichment (wrapped)
- verification_link: First evidence URL from email
- send_status: Send status (default: "VERIFY")
- comments: User comments/notes (blank, user fills)
"""

COLUMN_WIDTHS_PX = [200, 200, 320, 240, 260, 100, 260, 100, 260, 500, 320, 150, 300]
"""
Column widths in pixels for columns A through P.

Widths optimized for readability:
- request_id, job_id: 200px (hidden)
- organisation: 320px
- domain: 240px
- company: 260px
- company_score: 100px (hidden)
- email: 260px
- email_score: 100px (hidden)
- contacts: 260px
- description: 500px (wider for wrapped text)
- verification_link: 320px
- test_recipient: 260px
- subject: 320px
- content: 500px (wider for wrapped text)
- send_status: 150px
- comments: 300px
"""

HIDDEN_COLUMNS = [0, 1, 5, 7]
"""
Hidden columns (0-based indices).

Columns A (request_id), B (job_id), F (company_score), and H (email_score) are hidden.
Scores are hidden but used for conditional formatting.
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
    "CENTER",  # company_score
    "LEFT",    # email
    "CENTER",  # email_score
    "LEFT",    # contacts
    "LEFT",    # description
    "LEFT",    # verification_link
    "CENTER",  # send_status
    "LEFT",    # comments
]
"""
Horizontal alignment per column for data rows.

Most columns are LEFT-aligned for text readability.
company_score, email_score, and send_status are CENTER-aligned as they are numeric/categorical fields.
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
    "CLIP",  # company_score
    "CLIP",  # email
    "CLIP",  # email_score
    "WRAP",  # contacts (wrapped)
    "WRAP",  # description (wrapped)
    "CLIP",  # verification_link
    "CLIP",  # send_status
    "WRAP",  # comments (wrapped)
]
"""
Text wrap strategy per column for data rows.

Wrapped columns: contacts, description, comments
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

Matches Tool_Status and Master sheet styling for visual consistency.
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
    # CONTACTED status - Orange for entire row (highest priority)
    # This must be first to ensure it applies to the whole row before other rules
    {
        "name": "ROW_CONTACTED",
        "formula": '=$L2="CONTACTED"',
        "background_hex": "#FFE4CC",  # Light orange background
        "text_hex": "#000000",  # Black text
        "priority": 0,
        "columns": (0, 13),  # All columns A through M (entire row)
    },
    # Company column (Column E, index 4) - Green if score > 0.75, Red if score <= 0.75
    # Uses hidden company_score column (F) for score-based formatting
    # Only apply if company name is not empty
    {
        "name": "COMPANY_LOW_SCORE",
        "formula": '=AND($F2<=0.75, $F2>0, LEN($E2)>0)',
        "background_hex": "#FFC7CE",  # Red background (same as Master failed)
        "text_hex": "#9C0006",  # Red text
        "priority": 1,
        "columns": (4, 5),  # Column E only (0-based, end exclusive)
    },
    {
        "name": "COMPANY_HIGH_SCORE",
        "formula": '=AND($F2>0.75, LEN($E2)>0)',
        "background_hex": "#C6EFCE",  # Green background (same as Master succeeded)
        "text_hex": "#006100",  # Green text
        "priority": 2,
        "columns": (4, 5),  # Column E only (0-based, end exclusive)
    },
    # Email column (Column G, index 6) - Green if score > 0.75, Red if score <= 0.75
    # Uses hidden email_score column (H) for score-based formatting
    # Only apply if email is not empty
    {
        "name": "EMAIL_LOW_SCORE",
        "formula": '=AND($H2<=0.75, $H2>0, LEN($G2)>0)',
        "background_hex": "#FFC7CE",  # Red background (same as Master failed)
        "text_hex": "#9C0006",  # Red text
        "priority": 3,
        "columns": (6, 7),  # Column G only (0-based, end exclusive)
    },
    {
        "name": "EMAIL_HIGH_SCORE",
        "formula": '=AND($H2>0.75, LEN($G2)>0)',
        "background_hex": "#C6EFCE",  # Green background (same as Master succeeded)
        "text_hex": "#006100",  # Green text
        "priority": 4,
        "columns": (6, 7),  # Column G only (0-based, end exclusive)
    },
    # Send_status column (Column L, index 11) - Color coding for different statuses
    {
        "name": "SEND_STATUS_VERIFY",
        "formula": '=$L2="VERIFY"',
        "background_hex": "#FFEB9C",  # Yellow background
        "text_hex": "#9C6500",  # Yellow text
        "priority": 5,
        "columns": (11, 12),  # Column L only (0-based, end exclusive)
    },
    {
        "name": "SEND_STATUS_READY",
        "formula": '=$L2="READY"',
        "background_hex": "#DDEBF7",  # Blue background (same as Mails tab)
        "text_hex": "#1F4E79",  # Blue text (same as Mails tab)
        "priority": 6,
        "columns": (11, 12),  # Column L only
    },
    {
        "name": "SEND_STATUS_SENT",
        "formula": '=$L2="SENT"',
        "background_hex": "#E4DFEC",  # Purple/Lavender background (same as Mails tab)
        "text_hex": "#5B2C6F",  # Purple text (same as Mails tab)
        "priority": 7,
        "columns": (11, 12),  # Column L only
    },
    {
        "name": "SEND_STATUS_OK",
        "formula": '=$L2="OK"',
        "background_hex": "#C6EFCE",  # Green background (same as Master succeeded)
        "text_hex": "#006100",  # Green text
        "priority": 8,
        "columns": (11, 12),  # Column L only
    },
    {
        "name": "SEND_STATUS_FAILED",
        "formula": '=$L2="FAILED"',
        "background_hex": "#FFC7CE",  # Red background (same as Mails tab)
        "text_hex": "#9C0006",  # Red text (same as Mails tab)
        "priority": 9,
        "columns": (11, 12),  # Column L only
    },
]
"""
Conditional formatting rules for entire rows, company, email, and send_status columns.

Entire Row (A-M):
- Light Orange (#FFE4CC background, #000000 text): "CONTACTED" status (email was previously contacted)
  - Applied to entire row when send_status = "CONTACTED"
  - Highest priority to ensure visibility
  - Indicates this contact was already reached in a previous campaign

Company column (E):
- Green (#C6EFCE background, #006100 text): Score > 0.75 (high confidence)
- Red (#FFC7CE background, #9C0006 text): Score <= 0.75 (low confidence)
- Uses hidden company_score column (F) for score comparison
- Same colors as Master tab "succeeded" (green) and "failed" (red) statuses

Email column (G):
- Green (#C6EFCE background, #006100 text): Score > 0.75 (high confidence)
- Red (#FFC7CE background, #9C0006 text): Score <= 0.75 (low confidence)
- Uses hidden email_score column (H) for score comparison
- Same colors as Master tab "succeeded" (green) and "failed" (red) statuses

Send_status column (L):
- Yellow (#FFEB9C background, #9C6500 text): "VERIFY" status (needs verification)
- Blue (#DDEBF7 background, #1F4E79 text): "READY" status (ready to send)
- Purple (#E4DFEC background, #5B2C6F text): "SENT" status (email has been sent)
- Green (#C6EFCE background, #006100 text): "OK" status (sent successfully and confirmed)
- Red (#FFC7CE background, #9C0006 text): "FAILED" status (send failed)
- Same colors as Mails tab for consistency

Note: CONTACTED status formatting applies to the entire row and takes precedence over other formatting rules.
"""

# ============================================================================
# RANGE DEFINITIONS
# ============================================================================

FORMAT_RANGES = {
    "header_a1": "A1:M1",
    "data_a1": "A2:M2000",
    "all_a1": "A1:M2000",
}
"""
Named ranges for formatting operations.

Ranges use A1 notation and will be converted to GridRange by the writer.
Default data range supports up to 2000 rows.
"""

# ============================================================================
# DIMENSION HIDING
# ============================================================================

HIDE_COLUMNS_FROM = 13
"""
Hide columns starting from this index (0-based).

Hides column N onward (A=0...M=12, N=13).
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