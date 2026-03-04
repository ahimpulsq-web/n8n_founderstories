"""
Google Maps Places data fetcher for Google Sheets export.

Handles database queries and data preparation for Google Maps Places export.
Fetches data from the gmaps_results table and applies custom sorting logic.

Classification:
- Role: HOW to fetch and sort data from database
- No Google Sheets API calls
- No sheet layout definitions
- Pure data retrieval and transformation

Usage:
    from services.sheets.data_fetchers import google_maps
    
    rows = google_maps.fetch_rows_for_sheet(
        conn=db_connection,
        job_id="gmp__abc123",
        query_order=["restaurants", "cafes"],
        country_order=["FR", "DE"],
    )
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

from n8n_founderstories.core.utils.domain import extract_domain_from_url

logger = logging.getLogger(__name__)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def _extract_country(location: str) -> str:
    """
    Extract country code from location string.
    
    Location strings can be:
    - "" (no location)
    - "FR" (country only)
    - "FR/Île-de-France" (country/region)
    - "FR/Île-de-France/Paris" (country/region/city)
    
    Args:
        location: Location string from database
        
    Returns:
        Country code or empty string if no location
        
    Examples:
        >>> _extract_country("")
        ''
        >>> _extract_country("FR")
        'FR'
        >>> _extract_country("FR/Île-de-France/Paris")
        'FR'
    """
    if not location or not isinstance(location, str):
        return ""
    
    # Split on "/" and take first part (country code)
    parts = location.split("/")
    return parts[0].strip() if parts else ""

# ============================================================================
# DATA FETCHER
# ============================================================================

def fetch_rows_for_sheet(
    conn: psycopg.Connection[Any],
    *,
    job_id: str,
    query_order: list[str],
    country_order: list[str],
) -> list[list[str]]:
    """
    Fetch and sort Google Maps Places results for Google Sheets export.
    
    Retrieves data from the gmaps_results table and applies multi-level sorting
    to produce a clean, organized sheet layout.
    
    Sorting order (highest to lowest priority):
    1. Search query (custom order from query_order list)
    2. Country (custom order from country_order list)
    3. Organization (A→Z, case-insensitive)
    4. Domain (A→Z, for stability)
    
    Args:
        conn: Active psycopg database connection
        job_id: Job identifier to filter results
        query_order: List of search queries in desired order (case-insensitive matching)
        country_order: List of country codes in desired order
        
    Returns:
        List of rows, where each row is [organization, domain, location, query].
        Rows are sorted according to the multi-level sorting logic.
        
    Example:
        >>> rows = fetch_rows_for_sheet(
        ...     conn=db_conn,
        ...     job_id="gmp__abc123",
        ...     query_order=["restaurants", "cafes"],
        ...     country_order=["FR", "DE"],
        ... )
        >>> rows[0]
        ['Le Bistro', 'lebistro.fr', 'FR/Île-de-France/Paris', 'restaurants']
    """
    # ========================================================================
    # STEP 1: Fetch raw data from database
    # ========================================================================
    
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT organization, domain, location, query
            FROM gmaps_results
            WHERE job_id = %s
            """,
            (job_id,)
        )
        raw_rows = cur.fetchall()
    
    if not raw_rows:
        logger.debug(f"No rows found for job_id={job_id}")
        return []
    
    # ========================================================================
    # STEP 2: Build sort index maps
    # ========================================================================
    
    # Build query index map (case-insensitive)
    query_index_map = {}
    for idx, query in enumerate(query_order):
        query_index_map[query.lower().strip()] = idx
    
    # Build country index map
    country_index_map = {}
    for idx, country in enumerate(country_order):
        country_index_map[country.strip()] = idx
    
    # ========================================================================
    # STEP 3: Convert rows to sortable format
    # ========================================================================
    
    sortable_rows = []
    for row in raw_rows:
        organization, domain, location, query = row
        
        # Domain is already stored in the table, no need to extract
        domain = domain if domain else ""
        
        # Extract country from location
        country = _extract_country(location)
        
        # Get sort indexes (use large number for items not in order lists)
        query_idx = query_index_map.get(query.lower().strip(), 999999)
        country_idx = country_index_map.get(country, 999999)
        
        # Create multi-level sort key
        sort_key = (
            query_idx,               # 1. Query (custom order)
            country_idx,             # 2. Country (custom order)
            organization.lower(),    # 3. Organization (A-Z)
            domain.lower(),          # 4. Domain (stability)
        )
        
        sortable_rows.append((sort_key, [organization, domain, location, query]))
    
    # ========================================================================
    # STEP 4: Sort and extract row data
    # ========================================================================
    
    sortable_rows.sort(key=lambda x: x[0])
    sorted_rows = [row_data for _, row_data in sortable_rows]
    
    logger.debug(f"Fetched and sorted {len(sorted_rows)} rows for job_id={job_id}")
    
    return sorted_rows