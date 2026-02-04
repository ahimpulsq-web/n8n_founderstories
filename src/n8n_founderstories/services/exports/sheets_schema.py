"""
Shared Sheets schema constants and headers.

This module defines the single source of truth for:
- Tab names (versioned for tool outputs)
- Column headers for all export tabs
- Schema versioning to prevent drift

All exporters MUST use these constants to ensure consistency.

Note: Tool_Status schema has been moved to services.jobs.sheets_spec
"""

from __future__ import annotations

# =============================================================================
# GOOGLE MAPS TABS (v2 - created only at export time)
# =============================================================================

TAB_GOOGLE_MAPS_MAIN = "GoogleMaps_v2"
TAB_GOOGLE_MAPS_AUDIT = "GoogleMaps_Audit_v2"

HEADERS_GOOGLE_MAPS_MAIN = [
    "Organisation",
    "Domain",
    "Phone Number",
    "Location",
    "Address",
    "Type",
    "Google Maps URL",
    "Search Query",
]

HEADERS_GOOGLE_MAPS_AUDIT = [
    "Job ID",
    "Request ID",
    "Phase",
    "Country (ISO2)",
    "HL (Plan)",
    "Language Used",
    "Location Label",
    "Base Query",
    "Final Query",
    "Region Param",
    "Take N",
    "Returned Count",
    "Eligible After Dedupe",
    "Appended Rows",
    "Unique Places (Job)",
    "Stop Reason",
    "Error",
    "Timestamp",
    "Raw Meta (JSON)",
]

# =============================================================================
# HUNTER.IO TABS (v2 - created only at export time)
# =============================================================================

TAB_HUNTER_MAIN = "HunterIO_v2"
TAB_HUNTER_AUDIT = "HunterIO_Audit_v2"

HEADERS_HUNTER_MAIN = [
    "Organisation",
    "Domain",
    "Location",
    "Headcount",
    "Search Query",
    "Debug Filters",
]

HEADERS_HUNTER_AUDIT = [
    "Job ID",
    "Request ID",
    "Query Type",
    "Intended Location",
    "Intended Headcount",
    "Applied Location",
    "Applied Headcount",
    "Query Text",
    "Keywords",
    "Keyword Match",
    "Total Results",
    "Returned Count",
    "Appended Rows",
    "Applied Filters (JSON)",
]

# =============================================================================
# WEB ENRICHMENT TABS (legacy - for append-only sync)
# =============================================================================

TAB_WEB_ENRICHMENT_MAIN = "WebEnrichment"

HEADERS_WEB_ENRICHMENT_MAIN = [
    "master_result_id",
    "Organisation",
    "Domain",
    "Source",
    "Company Name",
    "E-Mail ID",
    "Contact Name",
    "Company Description",
    "Extraction Status",
]

# =============================================================================
# MASTER TABS (v2 - created only at export time)
# =============================================================================

TAB_MASTER_MAIN = "Master_v2"
TAB_MASTER_AUDIT = "Master_Audit_v2"

HEADERS_MASTER_MAIN = [
    "master_result_id",                # Column 0 (A) - Row key for matching
    "Organisation",                    # Column 1 (B) - Master field
    "Domain",                          # Column 2 (C) - Master field
    "Source",                          # Column 3 (D) - Master field
    "Company Name",                    # Column 4 (E) - Web enrichment
    "E-mail ID",                       # Column 5 (F) - Web enrichment
    "Contact Names",                   # Column 6 (G) - Web enrichment
    "Short Company Description",       # Column 7 (H) - Web enrichment
    "Long Company Description",        # Column 8 (I) - Web enrichment
]

# =============================================================================
# MAIL CONTENT TAB (v2 - created only at export time)
# =============================================================================

TAB_MAIL_CONTENT = "Mail_Content_v2"

HEADERS_MAIL_CONTENT = [
    "master_result_id",                # Column 0 (A) - Row key for matching (hidden)
    "Organisation",                    # Column 1 (B) - From Master
    "Domain",                          # Column 2 (C) - From Master
    "Company Name",                    # Column 3 (D) - From combine (highest confidence)
    "E-mail ID",                       # Column 4 (E) - From combine (highest confidence, single)
    "Test Recipient",                  # Column 4 (E) - From combine (highest confidence, single)
    "Contact Names",                   # Column 5 (F) - From combine (all with roles)
    "Subject",                         # Column 6 (G) - Empty for now
    "Content",                         # Column 7 (H) - Empty for now
    "Mail Status",                     # Column 8 (H) - Empty for now
    "Send Status",                     # Column 9 (H) - Empty for now
    "Notes",                           # Column 10 (H) - Empty for now

]

HEADERS_MASTER_AUDIT = [
    "Request ID",
    "Source Tool",
    "Total Rows",
    "Unique Domains",
    "Duplicates",
    "Last Updated",
]

# =============================================================================
# LEGACY TAB NAMES (Read-only, do not write to these)
# =============================================================================

# These tabs may exist in older spreadsheets but should not be written to
LEGACY_TABS = {
    "GoogleMaps",
    "GoogleMaps_Audit",
    "HunterIO",
    "HunterIO_Audit",
}

# =============================================================================
# SCHEMA VERSION TRACKING
# =============================================================================

SCHEMA_VERSION = "v2"

# Map of tool names to their current tab names
TOOL_TAB_MAP = {
    "google_maps": {
        "main": TAB_GOOGLE_MAPS_MAIN,
        "audit": TAB_GOOGLE_MAPS_AUDIT,
    },
    "hunter": {
        "main": TAB_HUNTER_MAIN,
        "audit": TAB_HUNTER_AUDIT,
    },
    "master": {
        "main": TAB_MASTER_MAIN,
        "audit": TAB_MASTER_AUDIT,
    },
    "mail_content": {
        "main": TAB_MAIL_CONTENT,
    },
}

# Map of tab names to their headers
TAB_HEADERS_MAP = {
    TAB_GOOGLE_MAPS_MAIN: HEADERS_GOOGLE_MAPS_MAIN,
    TAB_GOOGLE_MAPS_AUDIT: HEADERS_GOOGLE_MAPS_AUDIT,
    TAB_HUNTER_MAIN: HEADERS_HUNTER_MAIN,
    TAB_HUNTER_AUDIT: HEADERS_HUNTER_AUDIT,
    TAB_MASTER_MAIN: HEADERS_MASTER_MAIN,
    TAB_MASTER_AUDIT: HEADERS_MASTER_AUDIT,
    TAB_MAIL_CONTENT: HEADERS_MAIL_CONTENT,
}


def get_tool_tabs(tool_name: str) -> dict[str, str]:
    """
    Get tab names for a tool.
    
    Args:
        tool_name: Tool identifier (e.g., "google_maps", "hunter")
        
    Returns:
        Dict with 'main' and 'audit' tab names
        
    Raises:
        ValueError: If tool_name is not recognized
    """
    if tool_name not in TOOL_TAB_MAP:
        raise ValueError(f"Unknown tool: {tool_name}. Available: {list(TOOL_TAB_MAP.keys())}")
    return TOOL_TAB_MAP[tool_name]


def get_tab_headers(tab_name: str) -> list[str]:
    """
    Get headers for a tab.
    
    Args:
        tab_name: Tab name
        
    Returns:
        List of header strings
        
    Raises:
        ValueError: If tab_name is not recognized
    """
    if tab_name not in TAB_HEADERS_MAP:
        raise ValueError(f"Unknown tab: {tab_name}. Available: {list(TAB_HEADERS_MAP.keys())}")
    return list(TAB_HEADERS_MAP[tab_name])


__all__ = [
    # Google Maps tabs (export-time only)
    "TAB_GOOGLE_MAPS_MAIN",
    "TAB_GOOGLE_MAPS_AUDIT",
    "HEADERS_GOOGLE_MAPS_MAIN",
    "HEADERS_GOOGLE_MAPS_AUDIT",
    # Hunter tabs (export-time only)
    "TAB_HUNTER_MAIN",
    "TAB_HUNTER_AUDIT",
    "HEADERS_HUNTER_MAIN",
    "HEADERS_HUNTER_AUDIT",
    # Master tabs (export-time only)
    "TAB_MASTER_MAIN",
    "TAB_MASTER_AUDIT",
    "HEADERS_MASTER_MAIN",
    "HEADERS_MASTER_AUDIT",
    # Mail Content tab (export-time only)
    "TAB_MAIL_CONTENT",
    "HEADERS_MAIL_CONTENT",
    # Legacy tabs (read-only)
    "LEGACY_TABS",
    # Schema metadata
    "SCHEMA_VERSION",
    "TOOL_TAB_MAP",
    "TAB_HEADERS_MAP",
    # Helper functions
    "get_tool_tabs",
    "get_tab_headers",
]