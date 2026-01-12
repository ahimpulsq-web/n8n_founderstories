"""
Shared Sheets schema constants and headers.

This module defines the single source of truth for:
- Tab names (versioned for tool outputs)
- Column headers for all export tabs
- Schema versioning to prevent drift

All exporters MUST use these constants to ensure consistency.
"""

from __future__ import annotations

# =============================================================================
# STATUS TAB (Exception: created at runtime for live progress updates)
# =============================================================================

TAB_STATUS = "Tool_Status"

HEADERS_STATUS = [
    "Job ID",
    "Tool",
    "Request ID",
    "State",
    "Phase",
    "Current",
    "Total",
    "Percent",
    "Message",
    "Updated At",
    "Spreadsheet ID",
    "Meta (JSON)",
]

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
# MASTER TABS (v2 - created only at export time)
# =============================================================================

TAB_MASTER_MAIN = "Master_v2"
TAB_MASTER_AUDIT = "Master_Audit_v2"

HEADERS_MASTER_MAIN = [
    "Organisation",
    "Domain",
    "Source",
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
}

# Map of tab names to their headers
TAB_HEADERS_MAP = {
    TAB_STATUS: HEADERS_STATUS,
    TAB_GOOGLE_MAPS_MAIN: HEADERS_GOOGLE_MAPS_MAIN,
    TAB_GOOGLE_MAPS_AUDIT: HEADERS_GOOGLE_MAPS_AUDIT,
    TAB_HUNTER_MAIN: HEADERS_HUNTER_MAIN,
    TAB_HUNTER_AUDIT: HEADERS_HUNTER_AUDIT,
    TAB_MASTER_MAIN: HEADERS_MASTER_MAIN,
    TAB_MASTER_AUDIT: HEADERS_MASTER_AUDIT,
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
    # Status tab (runtime exception)
    "TAB_STATUS",
    "HEADERS_STATUS",
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