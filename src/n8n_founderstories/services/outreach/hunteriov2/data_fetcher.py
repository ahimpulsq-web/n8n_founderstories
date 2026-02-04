"""
HunterIO data fetcher for Google Sheets export.

Handles database queries and data preparation for sheet export.
Separated from sheets_spec.py for clean separation of concerns.
"""

import logging
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

# Headcount ordering (small to large)
HEADCOUNT_ORDER = {
    "1-10": 0,
    "11-50": 1,
    "51-200": 2,
    "201-500": 3,
}


def _extract_country(location: str) -> str:
    """
    Extract country code from location string.
    
    Examples:
        "-" -> ""
        "DE" -> "DE"
        "DE/Berlin" -> "DE"
    
    Args:
        location: Location string from database
        
    Returns:
        Country code or empty string
    """
    if not location or location == "-":
        return ""
    
    # Split on "/" and take first part
    parts = location.split("/")
    return parts[0].strip()


def fetch_rows_for_sheet(
    conn: psycopg.Connection[Any],
    *,
    job_id: str,
    term_order: list[str],
    country_order: list[str],
) -> list[list[str]]:
    """
    Fetch and sort HunterIO results for Google Sheets export.
    
    Returns rows in the exact order/format expected by Google Sheets:
    [organization, domain, location, headcount, term]
    
    Sorting order:
    1. term (custom order from term_order list)
    2. country (custom order from country_order list)
    3. headcount (small→large: 1-10, 11-50, 51-200, 201-500)
    4. organization (A→Z, case-insensitive)
    5. domain (A→Z, for stability)
    
    Args:
        conn: Active psycopg connection
        job_id: Job identifier to filter results
        term_order: List of terms in desired order (case-insensitive matching)
        country_order: List of country codes in desired order
        
    Returns:
        List of rows, where each row is [organization, domain, location, headcount, term]
    """
    # Fetch raw rows from database
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT organization, domain, location, headcount, term
            FROM hunterio_results
            WHERE job_id = %s
            """,
            (job_id,)
        )
        raw_rows = cur.fetchall()
    
    if not raw_rows:
        logger.debug(f"No rows found for job_id={job_id}")
        return []
    
    # Build term index map (case-insensitive)
    term_index_map = {}
    for idx, term in enumerate(term_order):
        term_index_map[term.lower().strip()] = idx
    
    # Build country index map
    country_index_map = {}
    for idx, country in enumerate(country_order):
        country_index_map[country.strip()] = idx
    
    # Convert rows to sortable format
    sortable_rows = []
    for row in raw_rows:
        organization, domain, location, headcount, term = row
        
        # Extract country from location
        country = _extract_country(location)
        
        # Get sort indexes
        term_idx = term_index_map.get(term.lower().strip(), 999999)
        country_idx = country_index_map.get(country, 999999)
        headcount_idx = HEADCOUNT_ORDER.get(headcount, 999)
        
        # Create sort key
        sort_key = (
            term_idx,
            country_idx,
            headcount_idx,
            organization.lower(),
            domain.lower(),
        )
        
        sortable_rows.append((sort_key, [organization, domain, location, headcount, term]))
    
    # Sort by the sort key
    sortable_rows.sort(key=lambda x: x[0])
    
    # Extract just the row data
    sorted_rows = [row_data for _, row_data in sortable_rows]
    
    logger.debug(f"Fetched and sorted {len(sorted_rows)} rows for job_id={job_id}")
    
    return sorted_rows