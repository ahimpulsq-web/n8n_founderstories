"""
Hunter.io data fetcher for Google Sheets export.

Handles database queries and data preparation for Hunter.io lead export.
Fetches data from the htr_results table and applies custom sorting logic.

Classification:
- Role: HOW to fetch and sort data from database
- No Google Sheets API calls
- No sheet layout definitions
- Pure data retrieval and transformation

Usage:
    from services.sheets.data_fetchers import hunterio
    
    rows = hunterio.fetch_rows_for_sheet(
        conn=db_connection,
        job_id="htrio__abc123",
        term_order=["keyword1", "keyword2"],
        country_order=["DE", "US"],
    )
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

# ============================================================================
# SORTING CONFIGURATION
# ============================================================================

HEADCOUNT_ORDER = {
    "1-10": 0,
    "11-50": 1,
    "51-200": 2,
    "201-500": 3,
}
"""
Headcount bucket sort order (small to large).

Maps headcount bucket strings to numeric sort keys.
Smaller companies appear first in the sheet.
"""

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def _s(x: Any) -> str:
    """
    Safely convert any value to a string, handling None gracefully.
    
    This defensive helper prevents NoneType errors if database constraints
    are loosened in the future or if left joins introduce NULL values.
    
    Args:
        x: Any value to convert to string
        
    Returns:
        Stripped string representation, or empty string if None
        
    Examples:
        >>> _s("hello")
        'hello'
        >>> _s("  world  ")
        'world'
        >>> _s(None)
        ''
        >>> _s(123)
        '123'
    """
    return str(x).strip() if x is not None else ""


def _extract_country(location: str) -> str:
    """
    Extract country code from location string.
    
    Location strings can be:
    - "-" (no location)
    - "DE" (country only)
    - "DE/Berlin" (country/city)
    
    Args:
        location: Location string from database
        
    Returns:
        Country code or empty string if no location
        
    Examples:
        >>> _extract_country("-")
        ''
        >>> _extract_country("DE")
        'DE'
        >>> _extract_country("DE/Berlin")
        'DE'
    """
    if not location or location == "-":
        return ""
    
    # Split on "/" and take first part (country code)
    parts = location.split("/")
    return parts[0].strip()

# ============================================================================
# DATA FETCHER
# ============================================================================

def fetch_rows_for_sheet(
    conn: psycopg.Connection[Any],
    *,
    request_id: str,
    term_order: list[str],
    country_order: list[str],
) -> list[list[str]]:
    """
    Fetch and sort Hunter.io results for Google Sheets export.
    
    Retrieves data from the htr_results table and applies multi-level sorting
    to produce a clean, organized sheet layout.
    
    Sorting order (highest to lowest priority):
    1. Headcount (small→large: 1-10, 11-50, 51-200, 201-500)
    2. Search query (custom order from term_order list)
    3. Country (custom order from country_order list)
    4. Organization (A→Z, case-insensitive)
    5. Domain (A→Z, for stability)
    
    Args:
        conn: Active psycopg database connection
        request_id: Request identifier to filter results (primary key component)
        term_order: List of search queries in desired order (case-insensitive matching)
        country_order: List of country codes in desired order
        
    Returns:
        List of rows, where each row is [organization, domain, location, headcount, query].
        Rows are sorted according to the multi-level sorting logic.
        
    Example:
        >>> rows = fetch_rows_for_sheet(
        ...     conn=db_conn,
        ...     request_id="req_abc123",
        ...     term_order=["SaaS", "AI"],
        ...     country_order=["DE", "US"],
        ... )
        >>> rows[0]
        ['Acme Corp', 'acme.com', 'DE/Berlin', '1-10', 'SaaS']
    """
    # ========================================================================
    # STEP 1: Fetch raw data from database
    # ========================================================================
    
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT organization, domain, location, headcount, query
            FROM htr_results
            WHERE request_id = %s
            """,
            (request_id,)
        )
        raw_rows = cur.fetchall()
    
    if not raw_rows:
        logger.debug(f"No rows found for request_id={request_id}")
        return []
    
    # ========================================================================
    # STEP 2: Build sort index maps
    # ========================================================================
    
    # Build term index map (case-insensitive)
    term_index_map = {}
    for idx, term in enumerate(term_order):
        term_index_map[term.lower().strip()] = idx
    
    # Build country index map
    country_index_map = {}
    for idx, country in enumerate(country_order):
        country_index_map[country.strip()] = idx
    
    # ========================================================================
    # STEP 3: Convert rows to sortable format
    # ========================================================================
    
    sortable_rows = []
    for row in raw_rows:
        organization, domain, location, headcount, query = row
        
        # Extract country from location (defensive normalization)
        country = _extract_country(_s(location))
        
        # Get sort indexes (use large number for items not in order lists)
        # Defensive normalization prevents NoneType errors if schema changes
        query_idx = term_index_map.get(_s(query).lower(), 999999)
        country_idx = country_index_map.get(country, 999999)
        headcount_idx = HEADCOUNT_ORDER.get(_s(headcount), 999)
        
        # Create multi-level sort key
        sort_key = (
            headcount_idx,              # 1. Headcount (small to large)
            query_idx,                  # 2. Query (custom order)
            country_idx,                # 3. Country (custom order)
            _s(organization).lower(),   # 4. Organization (A-Z)
            _s(domain).lower(),         # 5. Domain (stability)
        )
        
        sortable_rows.append((sort_key, [organization, domain, location, headcount, query]))
    
    # ========================================================================
    # STEP 4: Sort and extract row data
    # ========================================================================
    
    sortable_rows.sort(key=lambda x: x[0])
    sorted_rows = [row_data for _, row_data in sortable_rows]
    
    logger.debug(f"Fetched and sorted {len(sorted_rows)} rows for request_id={request_id}")
    
    return sorted_rows