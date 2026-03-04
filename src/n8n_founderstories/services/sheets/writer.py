"""
Lightweight Google Sheets writer.

Provides a simple interface to write tabular data (headers + rows) to Google Sheets
using service account authentication with rate limiting.
"""

import logging
import os
import time
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

from n8n_founderstories.core.logging.tags import log_sheets
from n8n_founderstories.services.sheets.rate_limiter import get_rate_limiter
from n8n_founderstories.services.sheets.request_queue import get_request_queue

logger = logging.getLogger(__name__)

# Sheets API scope
SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"

# Chunk size for large exports to avoid API limits
# Google Sheets API has request size limits, so we chunk large datasets
CHUNK_SIZE = 1000  # rows per chunk

# Tail buffer size - number of empty rows to write after data
# This clears stale data from previous exports when formatting is skipped
# Reduced to 2 rows since unused rows are now hidden by formatting
TAIL_BUFFER_ROWS = 2  # empty rows to clear stale data (rest are hidden)

# Spreadsheet metadata cache to avoid repeated spreadsheets.get() calls
# Cache structure: {sheet_id: {"metadata": dict, "timestamp": float}}
_SPREADSHEET_CACHE = {}
_CACHE_TTL = 60  # Cache for 60 seconds


def _get_sheets_service():
    """
    Build and return a Google Sheets API v4 service client.
    
    Authenticates using the service account file specified in the
    GOOGLE_SERVICE_ACCOUNT_FILE environment variable.
    
    Returns:
        Google Sheets API service resource
        
    Raises:
        ValueError: If GOOGLE_SERVICE_ACCOUNT_FILE is not set
    """
    service_account_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    if not service_account_file:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_FILE environment variable is required")
    
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file,
        scopes=[SHEETS_SCOPE]
    )
    
    service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    return service


def _get_cached_spreadsheet_metadata(service, sheet_id: str, force_refresh: bool = False) -> dict:
    """
    Get spreadsheet metadata with caching to reduce API calls.
    
    Args:
        service: Google Sheets API service
        sheet_id: The spreadsheet ID
        force_refresh: If True, bypass cache and fetch fresh data
        
    Returns:
        Spreadsheet metadata dict
    """
    current_time = time.time()
    
    # Check cache if not forcing refresh
    if not force_refresh and sheet_id in _SPREADSHEET_CACHE:
        cached = _SPREADSHEET_CACHE[sheet_id]
        if current_time - cached["timestamp"] < _CACHE_TTL:
            logger.debug(f"Using cached metadata for sheet {sheet_id[:8]}...")
            return cached["metadata"]
    
    # Fetch fresh metadata
    rate_limiter = get_rate_limiter()
    spreadsheet = rate_limiter.execute_with_retry(
        lambda: service.spreadsheets().get(spreadsheetId=sheet_id).execute(),
        operation="spreadsheets.get"
    )
    
    # Update cache
    _SPREADSHEET_CACHE[sheet_id] = {
        "metadata": spreadsheet,
        "timestamp": current_time
    }
    
    logger.debug(f"Fetched and cached metadata for sheet {sheet_id[:8]}...")
    return spreadsheet


def _ensure_tab(service, sheet_id: str, tab_name: str) -> tuple[bool, dict]:
    """
    Ensure the specified tab exists in the spreadsheet.
    
    If the tab doesn't exist, it will be created.
    Master tab is always positioned as the last tab.
    
    Args:
        service: Google Sheets API service
        sheet_id: The spreadsheet ID
        tab_name: The name of the tab to ensure exists
        
    Returns:
        Tuple of (tab_was_created: bool, spreadsheet_metadata: dict)
        - tab_was_created: True if the tab was just created, False if it already existed
        - spreadsheet_metadata: The spreadsheet metadata from spreadsheets().get()
          This can be reused by formatting to avoid duplicate API calls
    """
    # Get existing sheets using cached metadata
    spreadsheet = _get_cached_spreadsheet_metadata(service, sheet_id)
    sheets = spreadsheet.get("sheets", [])
    
    # Check if tab already exists and get its current index
    existing_sheet_id = None
    existing_sheet_index = None
    for idx, sheet in enumerate(sheets):
        if sheet["properties"]["title"] == tab_name:
            existing_sheet_id = sheet["properties"]["sheetId"]
            existing_sheet_index = idx
            logger.debug(f"Tab '{tab_name}' already exists at index {idx}")
            break
    
    # If Master tab exists, move it to the end (if not already there)
    if existing_sheet_id is not None and tab_name == "Master":
        # Target index should be the total number of sheets (which will place it last)
        # When moving a sheet, indices shift, so we use len(sheets) - 1 as the target
        # But we need to check if it's already last by comparing with the actual last index
        last_index = len(sheets) - 1
        
        # Skip move if Master is already in the last position
        if existing_sheet_index == last_index:
            logger.debug(f"Tab '{tab_name}' already at last position (index {last_index}), skipping move")
            return False, spreadsheet  # Tab already existed and positioned correctly
        
        # Move Master tab to the last position
        # Use len(sheets) - 1 as target because when we move the sheet,
        # it will end up at the last position
        request_body = {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": existing_sheet_id,
                            "index": len(sheets)  # Move to position after all current sheets
                        },
                        "fields": "index"
                    }
                }
            ]
        }
        rate_limiter = get_rate_limiter()
        rate_limiter.execute_with_retry(
            lambda: service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body=request_body
            ).execute(),
            operation="spreadsheets.batchUpdate"
        )
        logger.debug(f"Moved tab '{tab_name}' from index {existing_sheet_index} to last position")
        
        # Invalidate cache and refresh metadata after move
        if sheet_id in _SPREADSHEET_CACHE:
            del _SPREADSHEET_CACHE[sheet_id]
        spreadsheet = _get_cached_spreadsheet_metadata(service, sheet_id, force_refresh=True)
        return False, spreadsheet  # Tab already existed
    elif existing_sheet_id is not None:
        # Tab exists and is not Master, no need to move
        return False, spreadsheet  # Tab already existed
    
    # Create the tab
    # For Master tab, create it at the end
    # For Tool Status tab, also delete Sheet1 if it exists (after creation)
    requests = []
    
    if tab_name == "Master":
        requests.append({
            "addSheet": {
                "properties": {
                    "title": tab_name,
                    "index": len(sheets)  # Append after last sheet
                }
            }
        })
    else:
        requests.append({
            "addSheet": {
                "properties": {
                    "title": tab_name
                }
            }
        })
    
    # For Tool Status tab, also delete Sheet1 if it exists
    # This must be done AFTER creating Tool Status, as at least one tab must exist
    if tab_name == "Tool Status":
        # Check if Sheet1 exists
        sheet1_id = None
        for sheet in sheets:
            if sheet["properties"]["title"] == "Sheet1":
                sheet1_id = sheet["properties"]["sheetId"]
                logger.debug("Found default 'Sheet1' tab, will delete it after creating Tool Status")
                break
        
        # Add delete request if Sheet1 exists
        if sheet1_id is not None:
            requests.append({
                "deleteSheet": {
                    "sheetId": sheet1_id
                }
            })
    
    # Execute batch update with all requests
    # Handle race condition where multiple sources try to create the same tab
    request_body = {"requests": requests}
    try:
        rate_limiter = get_rate_limiter()
        rate_limiter.execute_with_retry(
            lambda: service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body=request_body
            ).execute(),
            operation="spreadsheets.batchUpdate"
        )
        
        if tab_name == "Tool Status" and len(requests) > 1:
            logger.debug(f"Created tab '{tab_name}' and deleted default 'Sheet1' tab")
        else:
            logger.debug(f"Created tab '{tab_name}'" + (" at last position" if tab_name == "Master" else ""))
    
    except Exception as e:
        # Check if error is due to tab already existing (race condition)
        error_msg = str(e)
        if "already exists" in error_msg and tab_name in error_msg:
            logger.debug(f"Tab '{tab_name}' already exists (created by another source), continuing...")
            # Invalidate cache and refresh metadata
            if sheet_id in _SPREADSHEET_CACHE:
                del _SPREADSHEET_CACHE[sheet_id]
            spreadsheet = _get_cached_spreadsheet_metadata(service, sheet_id, force_refresh=True)
            return False, spreadsheet  # Tab already existed (race condition)
        else:
            # Re-raise if it's a different error
            raise
    
    # Invalidate cache and refresh metadata after tab creation
    if sheet_id in _SPREADSHEET_CACHE:
        del _SPREADSHEET_CACHE[sheet_id]
    spreadsheet = _get_cached_spreadsheet_metadata(service, sheet_id, force_refresh=True)
    
    return True, spreadsheet  # Tab was just created, return refreshed metadata


def _normalize_rows(rows: list[list[str]], expected_width: int | None = None) -> list[list[str]]:
    """
    Normalize rows by converting None values to empty strings and ensuring consistent width.
    
    Args:
        rows: List of rows to normalize
        expected_width: Expected number of columns. If provided, rows will be:
            - Truncated if they have more columns
            - Padded with "" if they have fewer columns
            This ensures deterministic sheet shape and prevents data spillover.
        
    Returns:
        Normalized rows with None values replaced by "" and consistent width
    """
    normalized = []
    for row in rows:
        # Convert to list first (defensive against tuple rows from DB)
        # Then convert None to ""
        normalized_row = list(row)
        normalized_row = [cell if cell is not None else "" for cell in normalized_row]
        
        # Adjust width if expected_width is specified
        if expected_width is not None:
            if len(normalized_row) > expected_width:
                # Truncate extra columns
                normalized_row = normalized_row[:expected_width]
            elif len(normalized_row) < expected_width:
                # Pad with empty strings
                normalized_row.extend([""] * (expected_width - len(normalized_row)))
        
        normalized.append(normalized_row)
    return normalized


def _ensure_sheet_size(
    service,
    sheet_id: str,
    tab_name: str,
    required_rows: int,
    required_cols: int
) -> None:
    """
    Ensure the sheet has enough rows and columns to accommodate the data.
    
    If the sheet is too small, it will be expanded using appendDimension.
    This prevents "exceeds grid limits" errors when writing large datasets.
    
    Args:
        service: Google Sheets API service
        sheet_id: The spreadsheet ID
        tab_name: The name of the tab to check/expand
        required_rows: Minimum number of rows needed
        required_cols: Minimum number of columns needed
    """
    # Get current sheet properties
    rate_limiter = get_rate_limiter()
    spreadsheet = rate_limiter.execute_with_retry(
        lambda: service.spreadsheets().get(spreadsheetId=sheet_id).execute(),
        operation="spreadsheets.get"
    )
    sheets = spreadsheet.get("sheets", [])
    
    # Find the target sheet
    target_sheet = None
    for sheet in sheets:
        if sheet["properties"]["title"] == tab_name:
            target_sheet = sheet
            break
    
    if not target_sheet:
        logger.warning(f"Sheet '{tab_name}' not found, skipping size check")
        return
    
    # Get current dimensions
    sheet_id_int = target_sheet["properties"]["sheetId"]
    current_rows = target_sheet["properties"]["gridProperties"]["rowCount"]
    current_cols = target_sheet["properties"]["gridProperties"]["columnCount"]
    
    # Calculate how many rows/cols to add
    rows_to_add = max(0, required_rows - current_rows)
    cols_to_add = max(0, required_cols - current_cols)
    
    if rows_to_add == 0 and cols_to_add == 0:
        logger.debug(f"Sheet '{tab_name}' already has sufficient size ({current_rows}x{current_cols})")
        return
    
    # Build expansion requests
    requests = []
    
    if rows_to_add > 0:
        requests.append({
            "appendDimension": {
                "sheetId": sheet_id_int,
                "dimension": "ROWS",
                "length": rows_to_add
            }
        })
    
    if cols_to_add > 0:
        requests.append({
            "appendDimension": {
                "sheetId": sheet_id_int,
                "dimension": "COLUMNS",
                "length": cols_to_add
            }
        })
    
    # Execute expansion
    if requests:
        rate_limiter = get_rate_limiter()
        rate_limiter.execute_with_retry(
            lambda: service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={"requests": requests}
            ).execute(),
            operation="spreadsheets.batchUpdate"
        )
        logger.debug(
            f"Expanded sheet '{tab_name}' from {current_rows}x{current_cols} "
            f"to {current_rows + rows_to_add}x{current_cols + cols_to_add}"
        )


def _batch_write_values(
    service,
    sheet_id: str,
    tab_name: str,
    headers: list[str],
    rows: list[list[str]]
) -> None:
    """
    Write headers and rows to a sheet using batchUpdate with chunking for large datasets.
    
    This is the optimized write path that uses spreadsheets.values.batchUpdate
    to write headers (to A1) and data rows (chunked if needed) in a single API call.
    The batchUpdate overwrites existing values, so no clear operation is needed.
    
    For large datasets (>CHUNK_SIZE rows), rows are split into chunks to avoid
    API request size limits. Each chunk is written to a separate range in the
    same batchUpdate call.
    
    Args:
        service: Google Sheets API service
        sheet_id: The spreadsheet ID
        tab_name: The name of the tab to write to
        headers: List of column headers
        rows: List of data rows (already normalized)
    """
    # Ensure sheet has enough space for data + headers + tail buffer
    required_rows = 1 + len(rows) + TAIL_BUFFER_ROWS  # header + data + buffer
    required_cols = len(headers)
    _ensure_sheet_size(service, sheet_id, tab_name, required_rows, required_cols)
    # Prepare batch update data starting with headers
    data = [
        {
            "range": f"{tab_name}!A1",
            "values": [headers]
        }
    ]
    
    # Add data rows in chunks if present
    last_data_row = 1  # Start after headers
    if rows:
        # Split rows into chunks to avoid API limits
        for chunk_idx in range(0, len(rows), CHUNK_SIZE):
            chunk = rows[chunk_idx:chunk_idx + CHUNK_SIZE]
            # Calculate starting row: A2 for first chunk, A1002 for second chunk (if CHUNK_SIZE=1000), etc.
            start_row = 2 + chunk_idx  # +2 because row 1 is headers, rows are 1-indexed
            
            data.append({
                "range": f"{tab_name}!A{start_row}",
                "values": chunk
            })
            last_data_row = start_row + len(chunk) - 1
    
    # Add tail buffer of empty rows to clear stale data from previous exports
    # This ensures old data doesn't remain visible when formatting is skipped
    tail_start_row = last_data_row + 1
    empty_row = [""] * len(headers)  # Empty row matching header width
    tail_rows = [empty_row[:] for _ in range(TAIL_BUFFER_ROWS)]
    
    data.append({
        "range": f"{tab_name}!A{tail_start_row}",
        "values": tail_rows
    })
    
    # Execute batch update - this overwrites existing values
    body = {
        "valueInputOption": "RAW",
        "data": data
    }
    
    rate_limiter = get_rate_limiter()
    rate_limiter.execute_with_retry(
        lambda: service.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id,
            body=body
        ).execute(),
        operation="spreadsheets.values.batchUpdate"
    )
    
    num_chunks = (len(rows) + CHUNK_SIZE - 1) // CHUNK_SIZE if rows else 0
    logger.debug(f"Batch wrote headers, {len(rows)} rows, and {TAIL_BUFFER_ROWS} tail buffer rows to '{tab_name}' in 1 API call ({num_chunks} data chunks)")


def write_rows(
    *,
    sheet_id: str,
    tab_name: str,
    headers: list[str],
    rows: list[list[str]],
    mode: str = "replace",
    service: str | None = None,
    job_id: str | None = None,
    format: bool = False,
    suppress_log: bool = False,
) -> None:
    """
    Writes rows to Google Sheets.

    Args:
        sheet_id: The Google Sheets spreadsheet ID
        tab_name: The name of the tab/sheet to write to
        headers: List of column headers
        rows: List of rows, where each row is a list of cell values
        mode: Write mode - either "replace" or "append" (default: "replace")
            - "replace" (RECOMMENDED): Overwrites the entire tab with headers and rows
              in a single optimized API call. Treats exports as immutable snapshots.
              Always writes headers even with 0 rows to show empty result state.
            - "append": Appends rows to existing data. Only use for event log style tabs.
              Not deterministic (can reorder if sheet is edited) and slower than replace.
        service: Optional service name for logging (e.g., "GOOGLE_MAPS", "HUNTER")
        job_id: Optional job ID for logging
        format: Whether to apply formatting. If False (default), formatting is only
            applied when the tab is first created. If True, formatting is always applied.
            This avoids expensive formatting operations on every write.

    Raises:
        ValueError: If GOOGLE_SERVICE_ACCOUNT_FILE is not set or mode is invalid
        Exception: If Google API calls fail
        
    Examples:
        >>> # Standard export (recommended) - fast, deterministic, 1 API call
        >>> write_rows(
        ...     sheet_id="1abc...",
        ...     tab_name="Google Maps Leads",
        ...     headers=["Name", "Email", "Company"],
        ...     rows=[["John Doe", "john@example.com", "Acme Inc"]],
        ...     service="GOOGLE_MAPS"
        ... )
        
        >>> # Event log style (rare use case) - 2 API calls, non-deterministic
        >>> write_rows(
        ...     sheet_id="1abc...",
        ...     tab_name="Activity Log",
        ...     headers=["Timestamp", "Event"],
        ...     rows=[["2024-01-01", "User login"]],
        ...     mode="append"
        ... )
    """
    if mode not in ("append", "replace"):
        raise ValueError(f"mode must be 'append' or 'replace', got: {mode}")
    
    # Check for skip conditions based on mode
    # Replace mode: Always write headers even with 0 rows (shows empty result state)
    # Append mode: Skip if no rows to append
    if mode == "append" and not rows:
        logger.debug(f"Skipping Sheets append: no rows for {service}/{tab_name}")
        return
    
    sheets_service = _get_sheets_service()
    request_queue = get_request_queue()
    
    # Wrap the entire operation in the request queue to prevent concurrent writes
    # to the same sheet and enforce minimum delays between operations
    def _execute_write():
        # Ensure the tab exists and get metadata for potential reuse
        # Returns (tab_was_created, spreadsheet_metadata)
        tab_was_created, spreadsheet_metadata = _ensure_tab(sheets_service, sheet_id, tab_name)
        
        # Normalize rows (convert None to "" and ensure width matches headers)
        normalized_rows = _normalize_rows(rows, expected_width=len(headers))
        
        if mode == "replace":
            # Use optimized batch write: overwrites headers and rows in 1 API call
            # No clear needed - batchUpdate overwrites existing values
            _batch_write_values(
                sheets_service,
                sheet_id,
                tab_name,
                headers,
                normalized_rows
            )
        
        elif mode == "append":
            # Optimized append: always overwrite headers (A1) + append data
            # This avoids the expensive _tab_is_empty() read call
            # Overwriting headers is harmless and ensures consistency
            
            # Use batch update to write headers and append data in 2 API calls
            # (Could be 1 call but append semantics require separate append operation)
            
            # 1. Overwrite headers at A1
            rate_limiter = get_rate_limiter()
            rate_limiter.execute_with_retry(
                lambda: sheets_service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=f"{tab_name}!A1",
                    valueInputOption="RAW",
                    body={"values": [headers]}
                ).execute(),
                operation="spreadsheets.values.update"
            )
            
            # 2. Append data rows
            if normalized_rows:
                rate_limiter.execute_with_retry(
                    lambda: sheets_service.spreadsheets().values().append(
                        spreadsheetId=sheet_id,
                        range=f"{tab_name}!A2",  # Start from A2 to append after headers
                        valueInputOption="RAW",
                        insertDataOption="OVERWRITE",  # OVERWRITE appends after existing data
                        body={"values": normalized_rows}
                    ).execute(),
                    operation="spreadsheets.values.append"
                )
                logger.debug(f"Appended {len(normalized_rows)} rows to '{tab_name}'")
        
        # Log completion if service is provided (unless suppressed)
        if service and not suppress_log:
            log_sheets(logger, service=service, tab=tab_name, mode=mode, rows=len(normalized_rows))
        
        # Apply formatting only if:
        # 1. Explicitly requested via format=True, OR
        # 2. Tab was just created (first-time setup)
        # This avoids expensive formatting operations on every write
        should_format = format or tab_was_created
        
        if service and should_format:
            # Calculate used rows: 1 header + data rows
            used_rows = 1 + len(normalized_rows)
            # Pass spreadsheet_metadata to avoid duplicate API call
            _apply_sheet_formatting(
                sheets_service,
                sheet_id,
                tab_name,
                service,
                used_rows,
                spreadsheet_metadata=spreadsheet_metadata
            )
            logger.debug(f"Applied formatting to '{tab_name}' (format={format}, tab_was_created={tab_was_created})")
    
    # Execute the write operation with request queue throttling
    try:
        request_queue.execute_with_queue(
            sheet_id=sheet_id,
            tab_name=tab_name,
            operation=mode,
            func=_execute_write,
            priority=1 if mode == "replace" else 0
        )
    except Exception as e:
        logger.error(f"Failed to write to Sheets {service}/{tab_name}: {e}")
        raise


def _apply_sheet_formatting(
    service,
    sheet_id: str,
    tab_name: str,
    service_name: str,
    used_rows: int,
    spreadsheet_metadata: dict | None = None
) -> None:
    """
    Apply formatting to a sheet based on the service's sheets_spec.
    
    Args:
        service: Google Sheets API service
        sheet_id: The spreadsheet ID
        tab_name: The name of the tab to format
        service_name: The service name (e.g., "GOOGLE_MAPS", "HUNTER")
        used_rows: Number of rows actually used (header + data rows)
        spreadsheet_metadata: Optional pre-fetched spreadsheet metadata from _ensure_tab().
            If provided, avoids an additional API call. If None, will fetch metadata.
    """
    try:
        # Import formatting helpers
        from . import formatting
        
        # CRITICAL: Always fetch fresh metadata to get accurate row/column counts
        # The spreadsheet_metadata parameter may be stale if _ensure_sheet_size()
        # expanded the sheet after the metadata was fetched. We need the current
        # dimensions to apply formatting correctly to all rows.
        rate_limiter = get_rate_limiter()
        spreadsheet = rate_limiter.execute_with_retry(
            lambda: service.spreadsheets().get(spreadsheetId=sheet_id).execute(),
            operation="spreadsheets.get"
        )
        
        # Find the target sheet and extract all needed data in one pass
        internal_sheet_id = None
        sheet_props = None
        existing_rules_count = 0
        
        for sheet in spreadsheet.get("sheets", []):
            props = sheet.get("properties", {})
            if props.get("title") == tab_name:
                internal_sheet_id = props.get("sheetId")
                sheet_props = props
                # Get conditional formats count while we're here
                existing_rules_count = len(sheet.get("conditionalFormats", []))
                break
        
        if internal_sheet_id is None:
            logger.warning(f"Could not find sheet ID for tab '{tab_name}', skipping formatting")
            return
        
        if not sheet_props:
            logger.warning(f"Could not find sheet properties for tab '{tab_name}'")
            return
        
        grid_props = sheet_props.get("gridProperties", {})
        actual_row_count = grid_props.get("rowCount", 1000)
        actual_col_count = grid_props.get("columnCount", 26)
        
        logger.debug(f"Formatting '{tab_name}': used_rows={used_rows}, actual_row_count={actual_row_count}, actual_col_count={actual_col_count}")
        
        # CRITICAL: Unhide rows that might contain data before applying new formatting
        # This ensures that when multiple services write to the same sheet,
        # previously hidden rows become visible again for new data
        # Only unhide up to used_rows + small buffer to avoid huge API requests
        requests = []
        unhide_up_to = min(used_rows + 10, actual_row_count)
        if unhide_up_to > 1:  # Don't unhide header row
            requests.append(formatting.unhide_rows(internal_sheet_id, 1, unhide_up_to))
        
        # Load the appropriate sheets_spec based on service
        sheets_spec = None
        if service_name == "GOOGLE_MAPS":
            from n8n_founderstories.services.sheets.specs import google_maps_leads as sheets_spec
        elif service_name == "HUNTER":
            from n8n_founderstories.services.sheets.specs import hunter_leads as sheets_spec
        elif service_name == "JOBS":
            from n8n_founderstories.services.sheets.specs import jobs_tool_status as sheets_spec
        elif service_name == "MASTER":
            from n8n_founderstories.services.sheets.specs import master as sheets_spec
        elif service_name == "LEADS":
            from n8n_founderstories.services.sheets.specs import leads as sheets_spec
        elif service_name == "MAILS":
            from n8n_founderstories.services.sheets.specs import mails as sheets_spec
        elif service_name == "GLOBAL_MAIL_TRACKING":
            from n8n_founderstories.services.sheets.specs import global_mail_tracking as sheets_spec
        else:
            # Unknown service, skip formatting
            logger.debug(f"No formatting spec for service '{service_name}'")
            return
        
        # Continue building formatting requests
        
        # 1. Set column widths - build dict once and call set_column_widths once
        widths_dict = {i: width for i, width in enumerate(sheets_spec.COLUMN_WIDTHS_PX)}
        requests.extend(formatting.set_column_widths(internal_sheet_id, widths_dict))
        
        # 2. Set row heights (with small buffer of 2 rows beyond used rows)
        row_format_end = min(used_rows + 2, actual_row_count)
        requests.append(formatting.set_row_heights(
            internal_sheet_id, 0, 1, sheets_spec.HEADER_ROW_HEIGHT_PX
        ))
        requests.append(formatting.set_row_heights(
            internal_sheet_id, 1, row_format_end, sheets_spec.DATA_ROW_HEIGHT_PX
        ))
        
        # 3. Freeze header row
        requests.append(formatting.freeze_rows(internal_sheet_id, sheets_spec.FROZEN_ROWS))
        
        # 4. Format header row (always CENTER aligned)
        bg_color = formatting.hex_to_rgb(sheets_spec.HEADER_STYLE["background_hex"])
        requests.append(formatting.format_header_row(
            internal_sheet_id,
            len(sheets_spec.HEADERS),
            bold=sheets_spec.HEADER_STYLE["bold"],
            bg_color=bg_color,
            h_align="CENTER",
            v_align=sheets_spec.V_ALIGN,
        ))
        
        # 5. Set header wrap strategy
        requests.append(formatting.set_wrap_strategy(
            internal_sheet_id, 0, 1, 0, len(sheets_spec.HEADERS), sheets_spec.HEADER_WRAP
        ))
        
        # 6. Format data rows with per-column alignment and wrap strategy (with small buffer)
        for col_idx, wrap_strategy in enumerate(sheets_spec.DATA_WRAP_PER_COLUMN):
            # Get horizontal alignment for this column
            if hasattr(sheets_spec, 'H_ALIGN_PER_COLUMN'):
                h_align = sheets_spec.H_ALIGN_PER_COLUMN[col_idx]
            else:
                h_align = getattr(sheets_spec, 'H_ALIGN', 'CENTER')
            
            requests.append(formatting.set_alignment(
                internal_sheet_id, 1, row_format_end, col_idx, col_idx + 1,
                h_align=h_align, v_align=sheets_spec.V_ALIGN
            ))
            requests.append(formatting.set_wrap_strategy(
                internal_sheet_id, 1, row_format_end, col_idx, col_idx + 1, wrap_strategy
            ))
        
        # 7. Apply conditional formatting rules (if CONDITIONAL_RULES is defined)
        if hasattr(sheets_spec, 'CONDITIONAL_RULES') and sheets_spec.CONDITIONAL_RULES:
            # Clear existing conditional formatting rules by adding delete requests
            # to the main requests list (will be executed in same batch)
            # Delete in reverse order (highest index first) to prevent index shifting
            if existing_rules_count > 0:
                for i in range(existing_rules_count - 1, -1, -1):
                    requests.append({
                        "deleteConditionalFormatRule": {
                            "sheetId": internal_sheet_id,
                            "index": i
                        }
                    })
            
            # Add new conditional formatting rules
            # Sort by priority to ensure correct evaluation order
            # Use sequential insertion indices (0, 1, 2...) regardless of priority values
            
            # Process each rule with its own column range
            for insert_idx, rule in enumerate(sorted(sheets_spec.CONDITIONAL_RULES, key=lambda r: r["priority"])):
                bg_color = sheets_spec.hex_to_rgb(rule["background_hex"])
                
                # Note: Google Sheets API does not support text color (foregroundColor) in conditional formatting
                # Text color can only be set through regular cell formatting, not conditional formatting
                # So we only use background color here
                
                # Determine column range for this specific rule
                if "columns" in rule:
                    # Rule specifies its own column range
                    start_col, end_col = rule["columns"]
                elif hasattr(sheets_spec, 'CONDITIONAL_FORMAT_COLUMNS'):
                    # Use spec-defined column range (backward compatibility)
                    start_col, end_col = sheets_spec.CONDITIONAL_FORMAT_COLUMNS
                else:
                    # Default: apply to all columns (for backward compatibility)
                    start_col = 0
                    end_col = actual_col_count
                
                requests.append(formatting.add_conditional_format_rule(
                    internal_sheet_id,
                    start_row=1,  # Row 2 (0-indexed)
                    end_row=row_format_end,
                    start_col=start_col,
                    end_col=end_col,
                    condition_type="CUSTOM_FORMULA",
                    formula=rule["formula"],
                    bg_color=bg_color,
                    text_color=None,  # Not supported by Google Sheets API
                    priority=insert_idx,  # Use sequential index, not spec priority value
                ))
        
        # 8. Hide specific columns (if HIDDEN_COLUMNS is defined)
        if hasattr(sheets_spec, 'HIDDEN_COLUMNS'):
            for col_idx in sheets_spec.HIDDEN_COLUMNS:
                if col_idx < actual_col_count:
                    requests.append(formatting.hide_columns(
                        internal_sheet_id, col_idx, col_idx + 1
                    ))
        
        # 9. Hide unused columns (only if they exist)
        if hasattr(sheets_spec, 'HIDE_COLUMNS_FROM'):
            if sheets_spec.HIDE_COLUMNS_FROM < actual_col_count:
                requests.append(formatting.hide_columns(
                    internal_sheet_id, sheets_spec.HIDE_COLUMNS_FROM, actual_col_count
                ))
        
        # 10. Add borders to all cells (if APPLY_BORDERS is defined)
        # Apply borders to header + data rows (excluding buffer rows)
        if hasattr(sheets_spec, 'APPLY_BORDERS') and sheets_spec.APPLY_BORDERS:
            # used_rows is 1-based count (header + data rows)
            # Convert to 0-based for API: 0 to used_rows (exclusive)
            # This includes header (row 0) and all data rows up to used_rows-1
            requests.append(formatting.add_borders(
                internal_sheet_id,
                start_row=0,  # Include header
                end_row=used_rows,  # Exclude buffer rows (used_rows is already exclusive)
                start_col=0,
                end_col=len(sheets_spec.HEADERS),
                border_style="SOLID",
                border_width=1,
            ))
        
        # 11. Hide unused rows dynamically (hide all rows after used_rows)
        # Keep header + data + 2 blank buffer rows visible
        # used_rows is 1-based count (header + data rows)
        # hide_rows() takes 0-based startIndex, so used_rows + 2 means:
        #   - used_rows converts to 0-based last data row
        #   - +2 leaves 2 blank rows visible as buffer
        # Example: used_rows=1 (header only) → hide from 0-based row 3 (4th row)
        #          keeping rows 1 (header), 2-3 (blank buffer) visible
        if used_rows + 2 < actual_row_count:
            requests.append(formatting.hide_rows(
                internal_sheet_id, used_rows + 2, actual_row_count
            ))
        
        # Apply all formatting in a single batch update
        if requests:
            formatting.batch_update(service, sheet_id, requests)
            logger.debug(f"Applied formatting to tab '{tab_name}' ({len(requests)} requests)")
        
        # 12. Apply cell protection for SENT status cells (Mails sheet only)
        if service_name == "MAILS":
            _apply_cell_protection_for_mails(
                service,
                sheet_id,
                tab_name,
                internal_sheet_id
            )
    
    except Exception as e:
        # Log but don't fail - formatting is nice-to-have
        logger.warning(f"Failed to apply formatting to tab '{tab_name}': {e}")


def _apply_cell_protection_for_mails(
    service,
    sheet_id: str,
    tab_name: str,
    internal_sheet_id: int,
) -> None:
    """
    Apply cell protection to send_status column cells with "SENT" value.
    
    This function reads the current data from the Mails sheet, identifies rows
    where send_status is "SENT", and protects those cells from editing.
    
    Args:
        service: Google Sheets API service
        sheet_id: The spreadsheet ID
        tab_name: The name of the tab (should be "Mails")
        internal_sheet_id: The internal sheet ID for the tab
    """
    try:
        from . import formatting
        
        # Read the send_status column (column K, index 10) to find SENT rows
        # Start from row 2 (index 1) to skip header
        range_to_read = f"{tab_name}!K2:K"
        
        rate_limiter = get_rate_limiter()
        result = rate_limiter.execute_with_retry(
            lambda: service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=range_to_read
            ).execute(),
            operation="spreadsheets.values.get"
        )
        
        values = result.get('values', [])
        
        if not values:
            logger.debug(f"No data in send_status column for '{tab_name}', skipping protection")
            return
        
        # Clear existing protected ranges for this sheet first
        # Get current protected ranges
        spreadsheet = rate_limiter.execute_with_retry(
            lambda: service.spreadsheets().get(spreadsheetId=sheet_id).execute(),
            operation="spreadsheets.get"
        )
        protected_ranges = []
        
        for sheet in spreadsheet.get("sheets", []):
            if sheet["properties"]["sheetId"] == internal_sheet_id:
                protected_ranges = sheet.get("protectedRanges", [])
                break
        
        # Build requests to remove old protections and add new ones
        requests = []
        
        # Remove existing protected ranges for this sheet
        for protected_range in protected_ranges:
            requests.append(formatting.unprotect_range(protected_range["protectedRangeId"]))
        
        # Find rows with "SENT" status and protect them
        # values is 0-indexed but represents rows starting from row 2 (0-indexed row 1)
        sent_rows = []
        for idx, row in enumerate(values):
            if row and len(row) > 0 and row[0] == "SENT":
                # idx is 0-based in values array, but represents row 2+ in sheet
                # So actual row index is idx + 1 (0-based), or idx + 2 (1-based)
                sent_rows.append(idx + 1)  # Convert to 0-based row index
        
        if not sent_rows:
            logger.debug(f"No SENT status rows found in '{tab_name}', skipping protection")
            # Still apply the batch update to remove old protections
            if requests:
                formatting.batch_update(service, sheet_id, requests)
            return
        
        # Group consecutive rows for efficient protection
        # This reduces the number of protected ranges we create
        row_groups = []
        current_group_start = sent_rows[0]
        current_group_end = sent_rows[0] + 1
        
        for row_idx in sent_rows[1:]:
            if row_idx == current_group_end:
                # Consecutive row, extend current group
                current_group_end = row_idx + 1
            else:
                # Gap found, save current group and start new one
                row_groups.append((current_group_start, current_group_end))
                current_group_start = row_idx
                current_group_end = row_idx + 1
        
        # Don't forget the last group
        row_groups.append((current_group_start, current_group_end))
        
        # Create protection requests for each group
        # Column K is index 10 (0-based)
        send_status_col = 10
        
        for start_row, end_row in row_groups:
            requests.append(formatting.protect_range(
                internal_sheet_id,
                start_row=start_row,
                end_row=end_row,
                start_col=send_status_col,
                end_col=send_status_col + 1,
                description="This email has been sent - editing not recommended",
                warning_only=True,  # Show warning when editing (required for service accounts)
            ))
        
        # Apply all protection changes in a single batch
        if requests:
            formatting.batch_update(service, sheet_id, requests)
            logger.debug(
                f"Applied cell protection to {len(sent_rows)} SENT status cells "
                f"in '{tab_name}' ({len(row_groups)} protected ranges)"
            )
    
    except Exception as e:
        # Log but don't fail - protection is nice-to-have
        logger.warning(f"Failed to apply cell protection to tab '{tab_name}': {e}")
