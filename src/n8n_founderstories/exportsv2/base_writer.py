"""
Base writer for Google Sheets with formatting support.

Provides a reusable pattern for services to write to Google Sheets
with automatic formatting based on their sheets_spec.py configuration.
"""

import logging
import os
from typing import Any, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build

from . import formatting
from .writer import write_rows

logger = logging.getLogger(__name__)

SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"


class BaseSheetWriter:
    """
    Base writer for Google Sheets with spec-based formatting.
    
    Services should subclass this and provide their sheets_spec module.
    """
    
    def __init__(
        self,
        *,
        sheet_id: str,
        sheets_spec: Any,
        service_name: Optional[str] = None,
    ):
        """
        Initialize writer for a specific spreadsheet.
        
        Args:
            sheet_id: Google Spreadsheet ID
            sheets_spec: Module containing TAB_NAME, HEADERS, and formatting config
            service_name: Optional service name for logging
        """
        if not sheet_id or not sheet_id.strip():
            raise ValueError("sheet_id must not be empty")
        
        self._sheet_id = sheet_id.strip()
        self._spec = sheets_spec
        self._service_name = service_name
        self._service = self._get_sheets_service()
        
        # Ensure tab exists and apply formatting
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
        """Ensure tab exists with headers and formatting."""
        try:
            # Ensure tab exists
            self._ensure_tab(self._spec.TAB_NAME)
            
            # Apply formatting if spec provides it
            if hasattr(self._spec, 'COLUMN_WIDTHS_PX'):
                self._apply_formatting()
        except Exception as e:
            logger.warning(f"Failed to ensure tab ready: {e}")
    
    def _ensure_tab(self, tab_name: str) -> None:
        """Ensure the specified tab exists."""
        spreadsheet = self._service.spreadsheets().get(spreadsheetId=self._sheet_id).execute()
        sheets = spreadsheet.get("sheets", [])
        
        # Check if tab exists
        for sheet in sheets:
            if sheet["properties"]["title"] == tab_name:
                return
        
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
    
    def _apply_formatting(self) -> None:
        """
        Apply formatting spec to sheet.
        
        Override this method in subclasses for custom formatting logic.
        """
        sheet_id = formatting.get_sheet_id(
            self._service,
            self._sheet_id,
            self._spec.TAB_NAME
        )
        
        if sheet_id is None:
            return
        
        try:
            requests = []
            num_cols = len(self._spec.HEADERS)
            
            # Column widths
            if hasattr(self._spec, 'COLUMN_WIDTHS_PX'):
                for col_idx, width_px in enumerate(self._spec.COLUMN_WIDTHS_PX):
                    requests.extend(formatting.set_column_widths(sheet_id, {col_idx: width_px}))
            
            # Row heights
            if hasattr(self._spec, 'HEADER_ROW_HEIGHT_PX'):
                requests.append(formatting.set_row_heights(
                    sheet_id, 0, 1, self._spec.HEADER_ROW_HEIGHT_PX
                ))
            
            if hasattr(self._spec, 'DATA_ROW_HEIGHT_PX'):
                requests.append(formatting.set_row_heights(
                    sheet_id, 1, 1000, self._spec.DATA_ROW_HEIGHT_PX
                ))
            
            # Freeze rows
            if hasattr(self._spec, 'FROZEN_ROWS'):
                requests.append(formatting.freeze_rows(sheet_id, self._spec.FROZEN_ROWS))
            
            # Header formatting
            if hasattr(self._spec, 'HEADER_STYLE'):
                header_bg = self._spec.hex_to_rgb(self._spec.HEADER_STYLE["background_hex"])
                h_align = getattr(self._spec, 'H_ALIGN', 'CENTER')
                v_align = getattr(self._spec, 'V_ALIGN', 'MIDDLE')
                
                requests.append(formatting.format_header_row(
                    sheet_id,
                    num_cols,
                    bold=self._spec.HEADER_STYLE.get("bold", True),
                    bg_color=header_bg,
                    h_align=h_align,
                    v_align=v_align,
                ))
            
            # Execute batch update
            if requests:
                formatting.batch_update(self._service, self._sheet_id, requests)
                
        except Exception as e:
            logger.warning(f"Failed to apply formatting: {e}")
    
    @property
    def service(self):
        """Access to underlying Google Sheets service."""
        return self._service
    
    @property
    def sheet_id(self) -> str:
        """Spreadsheet ID."""
        return self._sheet_id
    
    @property
    def tab_name(self) -> str:
        """Tab name from spec."""
        return self._spec.TAB_NAME
    
    @property
    def headers(self) -> list[str]:
        """Headers from spec."""
        return self._spec.HEADERS
    
    def write(
        self,
        *,
        rows: list[list[str]],
        mode: str = "append",
        job_id: Optional[str] = None,
    ) -> None:
        """
        Write rows to sheet using exportsv2.writer.
        
        Args:
            rows: List of rows to write
            mode: Write mode - "append" or "replace"
            job_id: Optional job ID for logging
        """
        write_rows(
            sheet_id=self._sheet_id,
            tab_name=self._spec.TAB_NAME,
            headers=self._spec.HEADERS,
            rows=rows,
            mode=mode,
            service=self._service_name,
            job_id=job_id,
        )