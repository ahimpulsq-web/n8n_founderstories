"""
Delta Sheets writer for incremental enrichment updates.

This module provides efficient batch updates to enrichment columns in Google Sheets
without rewriting the entire sheet. Used by company_enrichment runner to update
results as they complete.
"""

from __future__ import annotations

import logging
from typing import List, Tuple

from .sheets import SheetsClient
from .sheets_schema import TAB_MASTER_MAIN

logger = logging.getLogger(__name__)


def update_enrichment_columns_batch(
    client: SheetsClient,
    spreadsheet_id: str,
    updates: List[Tuple[int, str, str, str, str]]
) -> None:
    """
    Update enrichment columns (D-G) for multiple rows in a single batch operation.
    
    This function performs delta updates to only the enrichment columns without
    rewriting the entire sheet. It uses a single batchUpdate API call for efficiency.
    
    Args:
        client: SheetsClient instance
        spreadsheet_id: Google Sheets spreadsheet ID
        updates: List of tuples, each containing:
            - row_index: 1-based row number in sheet (e.g., 2 for first data row)
            - emails: Formatted emails string (or empty string)
            - contacts: Formatted contacts string (or empty string)
            - extraction_status: Status string (or empty string)
            - debug_message: Debug message string (or empty string)
    
    Example:
        updates = [
            (2, "(email@example.com:https://example.com)", "John Doe:", "ok", ""),
            (3, "", "", "not_found", "No contact page found"),
        ]
        update_enrichment_columns_batch(client, spreadsheet_id, updates)
    
    Notes:
        - Uses columns D (Emails), E (Contacts), F (Extraction Status), G (Debug Message)
        - Empty strings are written as-is (not converted to None)
        - All updates are performed in a single API call for efficiency
        - Row 1 is assumed to be the header row
    """
    if not updates:
        logger.debug("SHEETS_DELTA_SKIP | reason=no_updates")
        return
    
    if not spreadsheet_id:
        raise ValueError("spreadsheet_id must not be empty")
    
    # Build batch update data
    # Columns: D=3, E=4, F=5, G=6 (0-indexed)
    batch_data = []
    
    for row_index, emails, contacts, status, debug in updates:
        if row_index < 2:
            logger.warning(
                "SHEETS_DELTA_INVALID_ROW | row=%d | skipping (must be >= 2)",
                row_index
            )
            continue
        
        # Build range for this row's enrichment columns (D:G)
        # Format: "Master_v2!D{row}:G{row}"
        range_str = f"{TAB_MASTER_MAIN}!D{row_index}:G{row_index}"
        
        # Build values array - ensure all values are strings
        values = [
            str(emails) if emails else "",
            str(contacts) if contacts else "",
            str(status) if status else "",
            str(debug) if debug else "",
        ]
        
        batch_data.append({
            "range": range_str,
            "values": [values]  # Single row
        })
    
    if not batch_data:
        logger.warning("SHEETS_DELTA_NO_VALID_UPDATES | total_updates=%d", len(updates))
        return
    
    # Execute batch update
    try:
        logger.info(
            "SHEETS_DELTA_UPDATE_START | spreadsheet=%s | rows=%d | ranges=%s",
            spreadsheet_id,
            len(batch_data),
            ", ".join([d["range"] for d in batch_data[:5]]) + ("..." if len(batch_data) > 5 else "")
        )
        
        result = client.values_batch_update(
            data=batch_data,
            value_input_option="RAW"
        )
        
        total_updated = result.get("totalUpdatedRows", 0) if result else 0
        logger.info(
            "SHEETS_DELTA_UPDATE_SUCCESS | spreadsheet=%s | batch_size=%d | updated_rows=%d",
            spreadsheet_id,
            len(batch_data),
            total_updated
        )
        
    except Exception as e:
        logger.error(
            "SHEETS_DELTA_UPDATE_FAILED | spreadsheet=%s | batch_size=%d | error=%s",
            spreadsheet_id,
            len(batch_data),
            str(e),
            exc_info=True
        )
        raise RuntimeError(f"Failed to update enrichment columns: {e}") from e


__all__ = [
    "update_enrichment_columns_batch",
]