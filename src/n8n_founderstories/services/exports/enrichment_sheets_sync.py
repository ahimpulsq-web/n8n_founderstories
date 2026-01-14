"""
Enrichment Sheets Sync Helper - DB-first incremental updates with deterministic row matching.

This module provides incremental Sheets updates for enrichment data:
- Reads enrichment data from DB (never from extractor output)
- Updates Google Sheets in batch after enrichment completes
- Never extracts data or writes to DB
- Handles partial updates without overwriting unrelated rows
- Uses master_result_id for deterministic row matching
- Uses header-based column mapping (no hard-coded column letters)

Architecture:
    Extractor → DB → Sheets (this module)
    
Never:
    Extractor → DB + Sheets
"""

from __future__ import annotations

import logging
from typing import List, Optional, Dict
from uuid import UUID

from .sheets import SheetsClient
from .sheets_schema import TAB_MASTER_MAIN
from ..company_enrichment.repos import CompanyEnrichmentResultsRepository
from ...core.utils.text import norm

logger = logging.getLogger(__name__)


def _read_header_row(client: SheetsClient, tab_name: str) -> Dict[str, int]:
    """
    Read the header row from a sheet and build a mapping of header name to column index.
    
    Args:
        client: SheetsClient instance
        tab_name: Name of the tab to read from
        
    Returns:
        Dict mapping lowercased header name to 0-based column index
        
    Example:
        {"master_result_id": 0, "organisation": 1, "domain": 2, ...}
    """
    try:
        # Read first row (header row)
        header_values = client.read_range(tab_name=tab_name, a1_range="1:1")
        
        if not header_values or not header_values[0]:
            logger.warning("ENRICHMENT_SHEETS_SYNC | HEADER_ROW_EMPTY | tab=%s", tab_name)
            return {}
        
        # Build header map: lowercased header -> 0-based column index
        header_map = {}
        for idx, header in enumerate(header_values[0]):
            header_normalized = norm(str(header)).lower()
            if header_normalized:
                header_map[header_normalized] = idx
        
        logger.debug(
            "ENRICHMENT_SHEETS_SYNC | HEADER_MAP_BUILT | tab=%s | headers=%s",
            tab_name,
            list(header_map.keys())
        )
        
        return header_map
        
    except Exception as e:
        logger.error(
            "ENRICHMENT_SHEETS_SYNC | HEADER_READ_FAILED | tab=%s | error=%s",
            tab_name,
            e,
            exc_info=True
        )
        return {}


def _read_master_result_id_column(
    client: SheetsClient,
    tab_name: str,
    master_id_col_idx: int,
    max_rows: int = 10000,
) -> Dict[str, int]:
    """
    Read the master_result_id column and build a mapping to sheet row numbers.
    
    Args:
        client: SheetsClient instance
        tab_name: Name of the tab to read from
        master_id_col_idx: 0-based column index of master_result_id
        max_rows: Maximum number of rows to read
        
    Returns:
        Dict mapping master_result_id (str) to 1-based sheet row number
        
    Example:
        {"uuid-1": 2, "uuid-2": 3, "uuid-3": 4, ...}
    """
    try:
        # Convert column index to letter (A, B, C, ...)
        col_letter = _col_index_to_letter(master_id_col_idx)
        
        # Read column from row 2 onwards (row 1 is header)
        range_str = f"{col_letter}2:{col_letter}{max_rows + 1}"
        values = client.read_range(tab_name=tab_name, a1_range=range_str)
        
        if not values:
            logger.warning(
                "ENRICHMENT_SHEETS_SYNC | MASTER_ID_COLUMN_EMPTY | tab=%s | col=%s",
                tab_name,
                col_letter
            )
            return {}
        
        # Build map: master_result_id -> sheet row number (1-based)
        row_map = {}
        for idx, row in enumerate(values):
            if row and row[0]:  # Check if cell has value
                master_id = norm(str(row[0]))
                if master_id:
                    sheet_row = idx + 2  # +2 because: idx is 0-based, and we start from row 2
                    row_map[master_id] = sheet_row
        
        logger.debug(
            "ENRICHMENT_SHEETS_SYNC | ROW_MAP_BUILT | tab=%s | rows=%d",
            tab_name,
            len(row_map)
        )
        
        return row_map
        
    except Exception as e:
        logger.error(
            "ENRICHMENT_SHEETS_SYNC | ROW_MAP_BUILD_FAILED | tab=%s | error=%s",
            tab_name,
            e,
            exc_info=True
        )
        return {}


def _col_index_to_letter(col_index: int) -> str:
    """Convert 0-based column index to Excel-style letter (A, B, ..., Z, AA, AB, ...)."""
    if col_index < 0:
        raise ValueError("col_index must be >= 0")
    
    n = col_index + 1
    letters: list[str] = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(ord("A") + rem))
    return "".join(reversed(letters))


def sync_enrichment_to_sheets(
    *,
    client: SheetsClient,
    request_id: str,
    master_result_ids: List[UUID],
) -> None:
    """
    Sync enrichment data from DB to Google Sheets for specific master_result_ids.
    
    This function uses deterministic row matching via master_result_id and header-based
    column mapping to ensure safe, accurate updates.
    
    Process:
    1. Read header row to locate column indices by name
    2. Validate required headers exist (master_result_id, emails, contacts, status, debug)
    3. Read master_result_id column to build row mapping
    4. Read enrichment data from DB for affected master_result_ids
    5. Build batch update for only the affected rows
    6. Update Sheets in single batch operation
    
    Row Matching:
    - Uses master_result_id as stable row key (not domain)
    - Deterministic: same master_result_id always maps to same row
    - Safe: skips missing IDs without crashing
    
    Column Mapping:
    - Locates columns by header name (no hard-coded letters)
    - Required headers: master_result_id, emails, contacts, status, debug
    - Validates headers exist before attempting update
    
    Args:
        client: SheetsClient instance
        request_id: Request identifier
        master_result_ids: List of master_result_ids that were enriched
        
    Raises:
        Exception: If Sheets update fails (logged, does not break enrichment)
    """
    rid = norm(request_id)
    if not rid or not master_result_ids:
        logger.warning("ENRICHMENT_SHEETS_SYNC_SKIP | reason=empty_input")
        return
    
    try:
        # Step 1: Read header row and build column index map
        header_map = _read_header_row(client, TAB_MASTER_MAIN)
        
        if not header_map:
            logger.error(
                "ENRICHMENT_SHEETS_SYNC_FAILED | reason=header_read_failed | request_id=%s",
                rid
            )
            return
        
        # Step 2: Validate required headers exist
        # Note: We do NOT update debug_message column - it's DB-only, not user-facing
        required_headers = ["master_result_id", "emails", "contacts", "extraction status"]
        missing_headers = []
        
        for header in required_headers:
            if header not in header_map:
                missing_headers.append(header)
        
        if missing_headers:
            logger.error(
                "ENRICHMENT_SHEETS_SYNC_FAILED | reason=missing_headers | headers=%s | request_id=%s",
                missing_headers,
                rid
            )
            return
        
        # Get column indices for required columns (excluding debug)
        master_id_col_idx = header_map["master_result_id"]
        emails_col_idx = header_map["emails"]
        contacts_col_idx = header_map["contacts"]
        status_col_idx = header_map["extraction status"]
        
        logger.info(
            "ENRICHMENT_SHEETS_SYNC | COLUMN_INDICES | master_id=%d | emails=%d | contacts=%d | status=%d",
            master_id_col_idx,
            emails_col_idx,
            contacts_col_idx,
            status_col_idx
        )
        
        # Step 3: Read master_result_id column to build row mapping
        row_map = _read_master_result_id_column(
            client=client,
            tab_name=TAB_MASTER_MAIN,
            master_id_col_idx=master_id_col_idx,
        )
        
        if not row_map:
            logger.error(
                "ENRICHMENT_SHEETS_SYNC_FAILED | reason=row_map_empty | request_id=%s",
                rid
            )
            return
        
        # Step 4: Read enrichment data from DB
        enrichment_repo = CompanyEnrichmentResultsRepository()
        enrichment_results = enrichment_repo.get_all_by_request(request_id=rid)
        
        # Build map: master_result_id -> enrichment data
        enrichment_map = {
            str(enrich.master_result_id): enrich
            for enrich in enrichment_results
        }
        
        # Step 5: Build batch update data for affected rows
        batch_data = []
        updated_count = 0
        skipped_count = 0
        
        for master_result_id in master_result_ids:
            mrid_str = str(master_result_id)
            
            # Find sheet row number for this master_result_id
            sheet_row = row_map.get(mrid_str)
            
            if sheet_row is None:
                logger.warning(
                    "ENRICHMENT_SHEETS_SYNC_ROW_NOT_FOUND | master_result_id=%s",
                    mrid_str
                )
                skipped_count += 1
                continue
            
            # Get enrichment data (may be None if not yet enriched)
            enrichment = enrichment_map.get(mrid_str)
            
            # Build update for each enrichment column
            # We update each column separately to avoid overwriting other columns
            # Note: We do NOT update debug_message - it's DB-only, not user-facing
            
            # Update emails column - DB already stores in correct format: (email: url),(email2: url2)
            emails_col_letter = _col_index_to_letter(emails_col_idx)
            batch_data.append({
                "range": f"{TAB_MASTER_MAIN}!{emails_col_letter}{sheet_row}",
                "values": [[norm(enrichment.emails if enrichment else '')]]
            })
            
            # Update contacts column
            contacts_col_letter = _col_index_to_letter(contacts_col_idx)
            batch_data.append({
                "range": f"{TAB_MASTER_MAIN}!{contacts_col_letter}{sheet_row}",
                "values": [[norm(enrichment.contacts if enrichment else '')]]
            })
            
            # Update status column
            status_col_letter = _col_index_to_letter(status_col_idx)
            batch_data.append({
                "range": f"{TAB_MASTER_MAIN}!{status_col_letter}{sheet_row}",
                "values": [[norm(enrichment.extraction_status if enrichment else '')]]
            })
            
            # Debug column is NOT updated - it remains DB-only
            
            updated_count += 1
        
        if not batch_data:
            logger.info(
                "ENRICHMENT_SHEETS_SYNC_SKIP | reason=no_updates | request_id=%s | skipped=%d",
                rid,
                skipped_count
            )
            return
        
        # Step 6: Perform batch update to Sheets
        logger.info(
            "ENRICHMENT_SHEETS_SYNC_START | request_id=%s | rows=%d | skipped=%d | updates=%d",
            rid,
            updated_count,
            skipped_count,
            len(batch_data)
        )
        
        client.values_batch_update(
            data=batch_data,
            value_input_option="RAW"
        )
        
        logger.info(
            "ENRICHMENT_SHEETS_SYNC_COMPLETE | request_id=%s | rows=%d | skipped=%d",
            rid,
            updated_count,
            skipped_count
        )
        
    except Exception as e:
        # Log error but do not raise - Sheets sync failure should not break enrichment
        logger.error(
            "ENRICHMENT_SHEETS_SYNC_FAILED | request_id=%s | error=%s",
            rid,
            e,
            exc_info=True
        )


__all__ = [
    "sync_enrichment_to_sheets",
]