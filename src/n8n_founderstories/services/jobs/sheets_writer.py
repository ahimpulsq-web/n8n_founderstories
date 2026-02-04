"""
Tool_Status sheet writer.

Applies formatting spec and handles row upserts using exportsv2.
Migrated to use only exportsv2 components for clean separation of concerns.
"""

from __future__ import annotations

import os
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build

from ...exportsv2 import formatting
from . import sheets_spec

SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"


class JobsSheetWriter:
    """
    Writer for Tool_Status sheet with spec-based formatting and upsert logic.
    
    Maintains one row per job_id by finding and updating existing rows.
    Uses only exportsv2 components - no dependency on old exports/sheets.py.
    """
    
    def __init__(self, *, sheet_id: str):
        """
        Initialize writer for a specific spreadsheet.
        
        Args:
            sheet_id: Google Spreadsheet ID
        """
        if not sheet_id or not sheet_id.strip():
            raise ValueError("sheet_id must not be empty")
        
        self._sheet_id = sheet_id.strip()
        self._service = self._get_sheets_service()
        self._row_cache: dict[str, int] = {}  # job_id -> row_number
        
        # Ensure tab exists with headers and apply formatting
        self._ensure_ready()
    
    def _get_sheets_service(self):
        """Build and return Google Sheets API service."""
        service_account_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
        if not service_account_file:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_FILE environment variable is required")
        
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=[SHEETS_SCOPE]
        )
        
        return build("sheets", "v4", credentials=credentials, cache_discovery=False)
    
    def _ensure_ready(self) -> None:
        """Ensure Tool_Status tab exists with headers and formatting."""
        try:
            self._ensure_tab_with_header(sheets_spec.TAB_NAME, sheets_spec.HEADERS)
            self._apply_formatting()
        except Exception:
            # Best effort - tab might already exist
            pass
    
    def _ensure_tab_with_header(self, tab_name: str, headers: list[str]) -> None:
        """Ensure tab exists and has headers in row 1."""
        # Check if tab exists
        spreadsheet = self._service.spreadsheets().get(spreadsheetId=self._sheet_id).execute()
        sheets = spreadsheet.get("sheets", [])
        
        tab_exists = any(sheet["properties"]["title"] == tab_name for sheet in sheets)
        
        if not tab_exists:
            # Create tab
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
            self._service.spreadsheets().batchUpdate(
                spreadsheetId=self._sheet_id,
                body=request_body
            ).execute()
        
        # Write headers
        self._service.spreadsheets().values().update(
            spreadsheetId=self._sheet_id,
            range=f"{tab_name}!A1",
            valueInputOption="RAW",
            body={"values": [headers]}
        ).execute()
    
    def _apply_formatting(self) -> None:
        """
        Apply formatting spec to Tool_Status sheet.
        
        Reads configuration from sheets_spec and applies via batchUpdate.
        This method clears existing conditional formatting rules before applying
        new ones to ensure the formatting is always up-to-date with the spec.
        """
        sheet_id = formatting.get_sheet_id(
            self._service,
            self._sheet_id,
            sheets_spec.TAB_NAME
        )
        
        if sheet_id is None:
            return
        
        try:
            requests = []
            num_cols = len(sheets_spec.HEADERS)
            
            # Clear existing conditional formatting rules first to avoid duplicates
            try:
                for _ in range(10):  # Max 10 rules to clear (safety limit)
                    clear_request = formatting.clear_conditional_format_rules(sheet_id)
                    formatting.batch_update(self._service, self._sheet_id, [clear_request])
            except Exception:
                # Expected - no more rules to delete
                pass
            
            # Column widths
            for col_idx, width_px in enumerate(sheets_spec.COLUMN_WIDTHS_PX):
                requests.extend(formatting.set_column_widths(sheet_id, {col_idx: width_px}))
            
            # Row heights
            requests.append(formatting.set_row_heights(
                sheet_id, 0, 1, sheets_spec.HEADER_ROW_HEIGHT_PX
            ))
            requests.append(formatting.set_row_heights(
                sheet_id, 1, 1000, sheets_spec.DATA_ROW_HEIGHT_PX
            ))
            
            # Freeze header row
            requests.append(formatting.freeze_rows(sheet_id, sheets_spec.FROZEN_ROWS))
            
            # Header formatting
            header_bg = sheets_spec.hex_to_rgb(sheets_spec.HEADER_STYLE["background_hex"])
            requests.append(formatting.format_header_row(
                sheet_id,
                num_cols,
                bold=sheets_spec.HEADER_STYLE["bold"],
                bg_color=header_bg,
                h_align=sheets_spec.H_ALIGN,
                v_align=sheets_spec.V_ALIGN,
            ))
            
            # Header wrap strategy
            requests.append(formatting.set_wrap_strategy(
                sheet_id, 0, 1, 0, num_cols,
                sheets_spec.HEADER_WRAP
            ))
            
            # Data rows alignment (all cells)
            requests.append(formatting.set_alignment(
                sheet_id, 1, 1000, 0, num_cols,
                h_align=sheets_spec.H_ALIGN,
                v_align=sheets_spec.V_ALIGN,
            ))
            
            # Data rows wrap strategy
            requests.append(formatting.set_wrap_strategy(
                sheet_id, 1, 1000, 0, num_cols,
                sheets_spec.DATA_WRAP
            ))
            
            # Conditional formatting rules (in priority order)
            for rule in sheets_spec.CONDITIONAL_RULES:
                bg_color = sheets_spec.hex_to_rgb(rule["background_hex"])
                requests.append(formatting.add_conditional_format_rule(
                    sheet_id,
                    start_row=1,      # Row 2 (0-indexed)
                    end_row=1000,
                    start_col=0,      # Column A
                    end_col=num_cols, # Column D (exclusive)
                    condition_type="CUSTOM_FORMULA",
                    formula=rule["formula"],
                    bg_color=bg_color,
                    priority=rule["priority"],
                ))
            
            # Execute batch update
            formatting.batch_update(self._service, self._sheet_id, requests)
            
        except Exception:
            # Best effort - don't fail if formatting fails
            pass
    
    def _find_row(self, job_id: str) -> Optional[int]:
        """
        Find the row number for a job_id.
        
        Args:
            job_id: Job identifier to find
            
        Returns:
            Row number (1-based) or None if not found
        """
        # Check cache first
        if job_id in self._row_cache:
            return self._row_cache[job_id]
        
        # Search in sheet (column D = job_id, starting from row 2)
        try:
            result = self._service.spreadsheets().values().get(
                spreadsheetId=self._sheet_id,
                range=f"{sheets_spec.TAB_NAME}!D2:D1000"
            ).execute()
            
            values = result.get("values", [])
            for i, row in enumerate(values, start=2):
                if row and len(row) > 0 and row[0].strip() == job_id:
                    self._row_cache[job_id] = i
                    return i
        except Exception:
            pass
        
        return None
    
    def write(
        self,
        *,
        job_id: str,
        tool: str,
        request_id: str,
        state: str,
        current: Optional[int] = None,
        total: Optional[int] = None,
    ) -> None:
        """
        Write or update job status (upsert by job_id).
        
        Args:
            job_id: Unique job identifier
            tool: Tool name (e.g., "google_maps", "hunter")
            request_id: Request correlation ID
            state: Job state (QUEUED, RUNNING, SUCCEEDED, FAILED)
            current: Ignored (kept for backward compatibility)
            total: Ignored (kept for backward compatibility)
        """
        # Build row
        row = [
            tool.strip() if tool else "",
            state.strip().upper() if state else "",
            request_id.strip() if request_id else "",
            job_id.strip() if job_id else "",
        ]
        
        # Find existing row or append new one
        row_num = self._find_row(job_id)
        
        try:
            if row_num:
                # Update existing row
                self._service.spreadsheets().values().update(
                    spreadsheetId=self._sheet_id,
                    range=f"{sheets_spec.TAB_NAME}!A{row_num}",
                    valueInputOption="RAW",
                    body={"values": [row]}
                ).execute()
            else:
                # Append new row
                result = self._service.spreadsheets().values().append(
                    spreadsheetId=self._sheet_id,
                    range=f"{sheets_spec.TAB_NAME}!A1",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [row]}
                ).execute()
                
                # Try to extract row number from response and cache it
                if result and "updates" in result:
                    updated_range = result["updates"].get("updatedRange", "")
                    # Parse range like "Tool_Status!A3:D3" to get row number
                    if "!" in updated_range:
                        range_part = updated_range.split("!")[1]
                        if ":" in range_part:
                            start_cell = range_part.split(":")[0]
                            # Extract row number from cell like "A3"
                            row_str = "".join(c for c in start_cell if c.isdigit())
                            if row_str:
                                new_row = int(row_str)
                                self._row_cache[job_id] = new_row
        except Exception:
            # Best effort - don't fail the job if status write fails
            pass