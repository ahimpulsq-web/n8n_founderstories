"""
Google Maps Places data fetcher for Google Sheets export.

Handles database queries and data preparation for sheet export.
Separated from sheets_spec.py for clean separation of concerns.
"""

import logging
from typing import Any

import psycopg

logger = logging.getLogger(__name__)


def _extract_country(location: str) -> str:
    """
    Extract country code from location string.
    
    Examples:
        "" -> ""
        "FR" -> "FR"
        "FR/Île-de-France" -> "FR"
        "FR/Île-de-France/Paris" -> "FR"
    
    Args:
        location: Location string from database
        
    Returns:
        Country code or empty string
    """
    if not location or not isinstance(location, str):
        return ""
    
    # Split on "/" and take first part
    parts = location.split("/")
    return parts[0].strip() if parts else ""


def fetch_rows_for_sheet(
    conn: psycopg.Connection[Any],
    *,
    job_id: str,
    query_order: list[str],
    country_order: list[str],
) -> list[list[str]]:
    """
    Fetch and sort Google Maps Places results for Google Sheets export.
    
    Returns rows in the exact order/format expected by Google Sheets:
    [organization, website, location, description, text_query]
    
    Sorting order:
    1. text_query (custom order from query_order list)
    2. country (custom order from country_order list, extracted from location)
    3. organization (A→Z, case-insensitive)
    4. website (A→Z, for stability)
    
    Args:
        conn: Active psycopg connection
        job_id: Job identifier to filter results
        query_order: List of text queries in desired order (case-insensitive matching)
        country_order: List of country codes in desired order
        
    Returns:
        List of rows, where each row is [organization, website, location, description, text_query]
    """
    # Fetch raw rows from database
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT organization, website, location, description, text_query
            FROM googlemaps_places_results
            WHERE job_id = %s
            """,
            (job_id,)
        )
        raw_rows = cur.fetchall()
    
    if not raw_rows:
        logger.debug(f"No rows found for job_id={job_id}")
        return []
    
    # Build query index map (case-insensitive)
    query_index_map = {}
    for idx, query in enumerate(query_order):
        query_index_map[query.lower().strip()] = idx
    
    # Build country index map
    country_index_map = {}
    for idx, country in enumerate(country_order):
        country_index_map[country.strip()] = idx
    
    # Convert rows to sortable format
    sortable_rows = []
    for row in raw_rows:
        organization, website, location, description, text_query = row
        
        # Extract country from location
        country = _extract_country(location)
        
        # Get sort indexes
        query_idx = query_index_map.get(text_query.lower().strip(), 999999)
        country_idx = country_index_map.get(country, 999999)
        
        # Create sort key
        sort_key = (
            query_idx,
            country_idx,
            organization.lower(),
            website.lower(),
        )
        
        sortable_rows.append((sort_key, [organization, website, location, description, text_query]))
    
    # Sort by the sort key
    sortable_rows.sort(key=lambda x: x[0])
    
    # Extract just the row data
    sorted_rows = [row_data for _, row_data in sortable_rows]
    
    logger.debug(f"Fetched and sorted {len(sorted_rows)} rows for job_id={job_id}")
    
    return sorted_rows