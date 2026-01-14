from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from ..exports.sheets import SheetsClient
from ..exports.sheets_manager import GoogleSheetsManager
from ..exports.sheets_schema import TAB_STATUS, HEADERS_STATUS
from ...core.utils.text import norm
from .store import load_job

logger = logging.getLogger(__name__)

# Note: TAB_STATUS and HEADERS_STATUS imported from sheets_schema.py


def _progress_format(current: int | None, total: int | None) -> str:
    """Format progress as '50/100' or empty string if not available."""
    if current is None or total is None or total <= 0:
        return ""
    try:
        return f"{current}/{total}"
    except Exception:
        return ""


_FORMAT_APPLIED: set[str] = set()


def _format_guard_key(spreadsheet_id: str) -> str:
    return f"{norm(spreadsheet_id)}::{TAB_STATUS}"


# Tool name display mapping
TOOL_DISPLAY_NAMES = {
    "hunter": "HunterIO",
    "master": "Master",
    "company_enrichment": "Enrichment",
    "gmaps": "GMaps",
    "google_maps": "GMaps",
    "web_scraper": "WebScraper",
    "serp": "SERP",
}


def _normalize_tool_name(tool: str) -> str:
    """Normalize internal tool names to display names."""
    normalized = norm(tool).lower()
    return TOOL_DISPLAY_NAMES.get(normalized, tool.title() if tool else "")


@dataclass
class ToolStatusWriter:
    """
    Unified Tool_Status writer.

    Improvements:
    - First-time allocation appends a FULL status row (State=RUNNING), so the row is colored immediately.
    - Formatting applied best-effort and retriable.
    - Bounded scan once per job_id to find existing row.
    - Optional buffering via GoogleSheetsManager.
    """

    sheets: SheetsClient
    spreadsheet_id: str
    manager: Optional[GoogleSheetsManager] = None

    _is_ready: bool = False
    _row_by_job_id: dict[str, int] = field(default_factory=dict)

    _last_write_ts_by_job: dict[str, float] = field(default_factory=dict)
    _last_signature_by_job: dict[str, str] = field(default_factory=dict)

    min_running_update_interval_s: float = 3.0

    scan_start_row: int = 2
    scan_max_rows: int = 2000

    def header(self) -> list[str]:
        return list(HEADERS_STATUS)

    # ---------------------------------------------------------------------
    # Ready + formatting
    # ---------------------------------------------------------------------

    def ensure_ready(self, *, force_format: bool = False) -> None:
        """Ensure tab is ready with one-time setup."""
        if self._is_ready:
            return

        sid = norm(self.spreadsheet_id)
        if not sid:
            return

        key = _format_guard_key(sid)
        if key in _FORMAT_APPLIED:
            self._is_ready = True
            return

        try:
            # One-time setup: create tab, write headers, set formatting, add conditional rules
            self._setup_tab_once()
            
            # Clean up any existing duplicates on first initialization
            self.cleanup_duplicates()
            
            _FORMAT_APPLIED.add(key)
            self._is_ready = True

        except Exception:
            logger.exception("TOOL_STATUS_SETUP_FAILED")
            self._is_ready = True  # Don't retry on every write

    def _setup_tab_once(self) -> None:
        """
        One-time setup using a single batchUpdate call.
        Sets column widths, row heights, header formatting, conditional formatting, and hides extra columns.
        """
        sheet_id = self.sheets.get_sheet_id(TAB_STATUS)
        
        # If tab doesn't exist, create it first
        if sheet_id is None:
            self.sheets.ensure_tab(TAB_STATUS)
            time.sleep(0.2)  # Brief pause after creation
            sheet_id = self.sheets.get_sheet_id(TAB_STATUS)
            if sheet_id is None:
                logger.warning("Failed to get sheet_id for Tool_Status")
                return

        # Build all requests for a single batchUpdate
        requests = []

        # 1. Set fixed column widths: Tool/State/Progress = 140px, Request ID/Job ID = 320px
        column_widths = [140, 140, 140, 320, 320]  # Tool, State, Progress, Request ID, Job ID
        for col_idx, width in enumerate(column_widths):
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": col_idx,
                        "endIndex": col_idx + 1,
                    },
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize",
                }
            })

        # 2. Set row heights: header = 35px, data rows = 30px
        # Header row (row 1)
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": 1,
                },
                "properties": {"pixelSize": 35},
                "fields": "pixelSize",
            }
        })
        
        # Data rows (rows 2-1000)
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 1,
                    "endIndex": 1000,
                },
                "properties": {"pixelSize": 30},
                "fields": "pixelSize",
            }
        })

        # 3. Format header row: bold, light gray (#F1F3F4), centered
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(HEADERS_STATUS),
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 0.945,    # #F1F3F4
                            "green": 0.953,
                            "blue": 0.957
                        },
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontSize": 10,
                            "bold": True
                        },
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)",
            }
        })

        # 4. Hide columns F onwards (column index 5+)
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 5,  # Column F (0-indexed)
                    "endIndex": 26,   # Hide through column Z
                },
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser",
            }
        })

        # 5. Add conditional formatting rules
        # Rule 1 (lowest priority): Yellow for all data rows (default for RUNNING and other states)
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(HEADERS_STATUS),
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": "=TRUE"}]  # Always true = default yellow
                        },
                        "format": {
                            "backgroundColor": {"red": 1.0, "green": 0.95, "blue": 0.70}  # Yellow
                        }
                    }
                },
                "index": 2  # Lowest priority
            }
        })

        # Rule 2 (medium priority): Green for SUCCEEDED
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(HEADERS_STATUS),
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": '=$B2="SUCCEEDED"'}]
                        },
                        "format": {
                            "backgroundColor": {"red": 0.80, "green": 0.92, "blue": 0.80}  # Green
                        }
                    }
                },
                "index": 1  # Medium priority
            }
        })

        # Rule 3 (highest priority): Red for FAILED
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(HEADERS_STATUS),
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": '=$B2="FAILED"'}]
                        },
                        "format": {
                            "backgroundColor": {"red": 0.96, "green": 0.80, "blue": 0.80}  # Red
                        }
                    }
                },
                "index": 0  # Highest priority
            }
        })

        # Execute single batchUpdate with all formatting
        try:
            self.sheets._service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": requests}
            ).execute()
            logger.debug("TOOL_STATUS_BATCH_FORMAT_SUCCESS | requests=%d", len(requests))
        except Exception as e:
            logger.warning("TOOL_STATUS_BATCH_FORMAT_FAILED | error=%s", e)

        # Write header row values separately (values API)
        try:
            self.sheets.write_range(
                tab_name=TAB_STATUS,
                start_cell="A1",
                values=[list(HEADERS_STATUS)]
            )
        except Exception as e:
            logger.warning("TOOL_STATUS_HEADER_WRITE_FAILED | error=%s", e)

    # ---------------------------------------------------------------------
    # Row allocation
    # ---------------------------------------------------------------------

    def _find_existing_row(self, *, job_id: str) -> int | None:
        jid = norm(job_id)
        if not jid:
            return None

        start = int(self.scan_start_row)
        end = start + int(self.scan_max_rows) - 1

        try:
            # Job ID is now in column E (index 4)
            values = self.sheets.read_range(tab_name=TAB_STATUS, a1_range=f"E{start}:E{end}")
        except Exception:
            return None

        target = jid.lower()
        row = start
        for r in values:
            v = norm(r[0]).lower() if r else ""
            if v == target:
                return row
            row += 1
        return None

    def cleanup_duplicates(self) -> None:
        """
        Clean up any duplicate job entries in the Tool_Status sheet.
        This is a maintenance operation to fix any existing duplicates.
        New format: Tool | State | Progress | Request ID | Job ID
        """
        try:
            start = int(self.scan_start_row)
            end = start + int(self.scan_max_rows) - 1
            
            # Read all data from the sheet (now only 5 columns: A-E)
            all_data = self.sheets.read_range(tab_name=TAB_STATUS, a1_range=f"A{start}:E{end}")
            
            if not all_data:
                return
            
            # Track seen job IDs and their row data
            seen_jobs: dict[str, tuple[int, list[str]]] = {}  # job_id -> (row_index, row_data)
            rows_to_keep: list[list[str]] = []
            
            for i, row in enumerate(all_data):
                if not row or len(row) < 5:
                    continue
                
                # Job ID is now in column E (index 4)
                job_id = norm(row[4]) if len(row) > 4 else ""
                if not job_id:
                    continue
                
                current_row_index = start + i
                
                if job_id in seen_jobs:
                    # Duplicate found - keep the most recent one
                    # Since we don't have timestamps anymore, keep the first occurrence
                    existing_row_index, existing_row_data = seen_jobs[job_id]
                    logger.debug("CLEANUP_DUPLICATE | job_id=%s | keeping_first_occurrence", job_id)
                else:
                    # First occurrence of this job ID
                    seen_jobs[job_id] = (current_row_index, row)
                    rows_to_keep.append(row)
            
            # If we found duplicates, rewrite the sheet with cleaned data
            if len(rows_to_keep) < len([r for r in all_data if r and len(r) >= 5 and r[4].strip()]):
                logger.debug("CLEANUP_DUPLICATES | original_rows=%d | cleaned_rows=%d",
                          len(all_data), len(rows_to_keep))
                
                # Clear the data area and rewrite with cleaned data
                if rows_to_keep:
                    self.sheets.write_range(tab_name=TAB_STATUS, start_cell=f"A{start}", values=rows_to_keep)
                
                # Clear the row cache since row numbers have changed
                self._row_by_job_id.clear()
                
        except Exception as exc:
            logger.warning("CLEANUP_DUPLICATES_FAILED | error=%s", exc)

    def allocate_row(self, *, job_id: str, initial_row: Optional[list[str]] = None) -> int:
        """
        Allocate a dedicated row for job_id.

        If new, append `initial_row` (must match HEADERS_STATUS length) so State is set immediately.
        New format: Tool | State | Progress | Request ID | Job ID
        """
        jid = norm(job_id)
        if not jid:
            raise ValueError("job_id must not be empty.")

        if jid in self._row_by_job_id:
            return self._row_by_job_id[jid]

        found = self._find_existing_row(job_id=jid)
        if found is not None and found > 0:
            self._row_by_job_id[jid] = found
            return found

        row_to_append: list[str]
        if initial_row and len(initial_row) == len(HEADERS_STATUS):
            row_to_append = initial_row
        else:
            # fallback placeholder: Tool | State | Progress | Request ID | Job ID
            row_to_append = ["", "", "", "", jid]

        resp = self.sheets.append_rows(tab_name=TAB_STATUS, rows=[row_to_append])
        row_idx = SheetsClient.parse_row_from_append_response(resp)
        if row_idx is None or row_idx <= 0:
            found2 = self._find_existing_row(job_id=jid)
            row_idx = found2 if found2 else -1

        self._row_by_job_id[jid] = row_idx
        return row_idx

    # ---------------------------------------------------------------------
    # Write-light policy
    # ---------------------------------------------------------------------

    def _signature(
        self,
        *,
        state: str,
        current: int | None,
        total: int | None,
    ) -> str:
        """Generate signature for change detection. Simplified for new format."""
        return "|".join(
            [
                norm(state),
                str(current if current is not None else ""),
                str(total if total is not None else ""),
            ]
        )

    def _should_write(self, *, job_id: str, state: str, signature: str, force: bool) -> bool:
        if force:
            return True

        jid = norm(job_id)
        last_sig = self._last_signature_by_job.get(jid)
        if last_sig == signature:
            return False

        if norm(state).upper() == "RUNNING":
            now = time.time()
            last_ts = self._last_write_ts_by_job.get(jid, 0.0)
            if (now - last_ts) < float(self.min_running_update_interval_s):
                return False

        return True

    def write(
        self,
        *,
        job_id: str,
        tool: str,
        request_id: str,
        state: str,
        phase: str | None,
        current: int | None,
        total: int | None,
        message: str | None,
        meta: dict[str, Any] | None = None,
        force: bool = False,
    ) -> None:
        """
        Write status to sheet using values-only updates.
        New format: Tool | State | Progress | Request ID | Job ID
        Note: phase, message, and meta are ignored in the new simplified format
        """
        self.ensure_ready(force_format=force)

        jid = norm(job_id)

        # Normalize tool name for display
        tool_display = _normalize_tool_name(tool)
        progress = _progress_format(current, total)

        row = [
            tool_display,
            norm(state),
            progress,
            norm(request_id),
            jid,
        ]

        sig = self._signature(
            state=state,
            current=current,
            total=total,
        )
        if not self._should_write(job_id=jid, state=state, signature=sig, force=force):
            return

        # Allocate row; for first-time allocation append FULL row so it is colored immediately
        row_idx = self.allocate_row(job_id=jid, initial_row=row)

        # Values-only update (no formatting calls)
        if self.manager and row_idx > 0:
            self.manager.queue_values_update(tab_name=TAB_STATUS, a1_range=f"A{row_idx}", values=[row])
        else:
            if row_idx > 0:
                self.sheets.write_range(tab_name=TAB_STATUS, start_cell=f"A{row_idx}", values=[row])
            else:
                self.sheets.append_rows(tab_name=TAB_STATUS, rows=[row])

        now = time.time()
        self._last_write_ts_by_job[jid] = now
        self._last_signature_by_job[jid] = sig
