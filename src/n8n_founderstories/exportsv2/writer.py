"""
Lightweight Google Sheets writer.

Provides a simple interface to write tabular data (headers + rows) to Google Sheets
using service account authentication.
"""

import logging
import os
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

from n8n_founderstories.core.logging.tags import log_sheets

logger = logging.getLogger(__name__)

# Sheets API scope
SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"


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


def _ensure_tab(service, sheet_id: str, tab_name: str) -> None:
    """
    Ensure the specified tab exists in the spreadsheet.
    
    If the tab doesn't exist, it will be created.
    
    Args:
        service: Google Sheets API service
        sheet_id: The spreadsheet ID
        tab_name: The name of the tab to ensure exists
    """
    # Get existing sheets
    spreadsheet = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheets = spreadsheet.get("sheets", [])
    
    # Check if tab already exists
    for sheet in sheets:
        if sheet["properties"]["title"] == tab_name:
            logger.debug(f"Tab '{tab_name}' already exists")
            return
    
    # Create the tab
    request_body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": tab_name
                    }
                }
            }
        ]
    }
    
    service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body=request_body
    ).execute()
    
    logger.debug(f"Created tab '{tab_name}'")


def _tab_is_empty(service, sheet_id: str, tab_name: str) -> bool:
    """
    Check if a tab is empty (has no values).
    
    Args:
        service: Google Sheets API service
        sheet_id: The spreadsheet ID
        tab_name: The name of the tab to check
        
    Returns:
        True if the tab is empty, False otherwise
    """
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"{tab_name}!A1:Z1"
        ).execute()
        
        values = result.get("values", [])
        return len(values) == 0
    except Exception:
        # If we can't read, assume empty
        return True


def _normalize_rows(rows: list[list[str]]) -> list[list[str]]:
    """
    Normalize rows by converting None values to empty strings.
    
    Args:
        rows: List of rows to normalize
        
    Returns:
        Normalized rows with None values replaced by ""
    """
    normalized = []
    for row in rows:
        normalized.append([cell if cell is not None else "" for cell in row])
    return normalized


def write_rows(
    *,
    sheet_id: str,
    tab_name: str,
    headers: list[str],
    rows: list[list[str]],
    mode: str = "append",
    service: str | None = None,
    job_id: str | None = None,
) -> None:
    """
    Writes rows to Google Sheets.

    Args:
        sheet_id: The Google Sheets spreadsheet ID
        tab_name: The name of the tab/sheet to write to
        headers: List of column headers
        rows: List of rows, where each row is a list of cell values
        mode: Write mode - either "append" or "replace"
            - "replace": Clears the tab, writes headers, then writes rows
            - "append": Appends rows; if the tab is empty, writes headers first
        service: Optional service name for logging (e.g., "HUNTERIOV2")
        job_id: Optional job ID for logging

    Raises:
        ValueError: If GOOGLE_SERVICE_ACCOUNT_FILE is not set or mode is invalid
        Exception: If Google API calls fail
        
    Examples:
        >>> write_rows(
        ...     sheet_id="1abc...",
        ...     tab_name="results",
        ...     headers=["Name", "Email", "Company"],
        ...     rows=[["John Doe", "john@example.com", "Acme Inc"]],
        ...     mode="replace",
        ...     service="HUNTERIOV2",
        ...     job_id="htrio__abc123"
        ... )
    """
    if mode not in ("append", "replace"):
        raise ValueError(f"mode must be 'append' or 'replace', got: {mode}")
    
    # Log start if service is provided
    if service:
        log_fields = {"tab": tab_name, "state": "START"}
        if job_id:
            log_fields["job_id"] = job_id
        log_sheets(logger, service=service, level="debug", **log_fields)
    
    # Check for skip conditions
    if not rows:
        if service:
            log_fields = {"state": "SKIPPED", "reason": "no rows"}
            if job_id:
                log_fields["job_id"] = job_id
            log_sheets(logger, service=service, **log_fields)
        return
    
    sheets_service = _get_sheets_service()
    
    try:
        # Ensure the tab exists
        _ensure_tab(sheets_service, sheet_id, tab_name)
        
        # Normalize rows (convert None to "")
        normalized_rows = _normalize_rows(rows)
        
        if mode == "replace":
            # Clear the tab first
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=sheet_id,
                range=f"{tab_name}!A:Z"
            ).execute()
            logger.debug(f"Cleared tab '{tab_name}'")
            
            # Prepare data: headers + rows
            all_data = [headers] + normalized_rows
            
            # Write all data at once
            if all_data:
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=f"{tab_name}!A1",
                    valueInputOption="RAW",
                    body={"values": all_data}
                ).execute()
                logger.debug(f"Wrote headers and {len(normalized_rows)} rows to '{tab_name}'")
        
        elif mode == "append":
            # Check if tab is empty
            is_empty = _tab_is_empty(sheets_service, sheet_id, tab_name)
            
            if is_empty:
                # Write headers first
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=f"{tab_name}!A1",
                    valueInputOption="RAW",
                    body={"values": [headers]}
                ).execute()
                logger.debug(f"Wrote headers to '{tab_name}'")
            
            # Append rows if there are any
            if normalized_rows:
                sheets_service.spreadsheets().values().append(
                    spreadsheetId=sheet_id,
                    range=f"{tab_name}!A1",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": normalized_rows}
                ).execute()
                logger.debug(f"Appended {len(normalized_rows)} rows to '{tab_name}'")
        
        # Log completion if service is provided
        if service:
            log_fields = {"tab": tab_name, "rows": len(normalized_rows), "state": "COMPLETED"}
            if job_id:
                log_fields["job_id"] = job_id
            log_sheets(logger, service=service, **log_fields)
    
    except Exception as e:
        # Log error if service is provided
        if service:
            log_fields = {"tab": tab_name, "err": str(e)}
            if job_id:
                log_fields["job_id"] = job_id
            log_sheets(logger, service=service, level="error", **log_fields)
        raise
