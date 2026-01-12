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


def _percent(current: int | None, total: int | None) -> str:
    if current is None or not total or total <= 0:
        return ""
    try:
        return f"{round((current / total) * 100.0, 2)}"
    except Exception:
        return ""


_FORMAT_APPLIED: set[str] = set()


def _format_guard_key(spreadsheet_id: str) -> str:
    return f"{norm(spreadsheet_id)}::{TAB_STATUS}"


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
        if self._is_ready:
            if force_format:
                self._apply_formatting_best_effort(force=True)
            return

        self.sheets.ensure_tab_with_header(TAB_STATUS, HEADERS_STATUS)
        
        # Clean up any existing duplicates on first initialization
        self.cleanup_duplicates()
        
        self._apply_formatting_best_effort(force=True)  # Always force on first setup
        self._is_ready = True

    def _apply_formatting_best_effort(self, *, force: bool) -> None:
        sid = norm(self.spreadsheet_id)
        if not sid:
            return

        key = _format_guard_key(sid)
        if (not force) and (key in _FORMAT_APPLIED):
            return

        try:
            time.sleep(0.15)  # reduce race immediately after tab creation

            sheet_id = self.sheets.get_sheet_id(TAB_STATUS)
            if sheet_id is None:
                return

            # Apply enhanced table formatting
            self.sheets.format_table_layout(
                tab_name=TAB_STATUS,
                n_cols=len(HEADERS_STATUS),
                last_row=1000,  # Format up to 1000 rows
                header_row=True,
                auto_resize=True,
                wrap_strategy_body="CLIP",
                header_height=35,  # Slightly taller header
                body_row_height=25,  # Slightly taller body rows for better readability
            )

            start_row_index = 1  # row 2
            start_col_index = 0  # A
            end_col_index = len(HEADERS_STATUS)  # A..L
            state_col_in_range = 3  # D within A..L

            # Enhanced conditional formatting rules with better colors
            rules = [
                self.sheets.build_conditional_format_rule_text_equals(
                    sheet_id=sheet_id,
                    start_row_index=start_row_index,
                    start_col_index=start_col_index,
                    end_col_index=end_col_index,
                    eval_col_index_in_range=state_col_in_range,
                    equals_text="FAILED",
                    background_rgb=(0.96, 0.80, 0.80),  # Softer red
                ),
                self.sheets.build_conditional_format_rule_text_equals(
                    sheet_id=sheet_id,
                    start_row_index=start_row_index,
                    start_col_index=start_col_index,
                    end_col_index=end_col_index,
                    eval_col_index_in_range=state_col_in_range,
                    equals_text="RUNNING",
                    background_rgb=(1.0, 0.95, 0.70),  # Warmer yellow
                ),
                self.sheets.build_conditional_format_rule_text_equals(
                    sheet_id=sheet_id,
                    start_row_index=start_row_index,
                    start_col_index=start_col_index,
                    end_col_index=end_col_index,
                    eval_col_index_in_range=state_col_in_range,
                    equals_text="SUCCEEDED",
                    background_rgb=(0.80, 0.92, 0.80),  # Softer green
                ),
                self.sheets.build_conditional_format_rule_text_equals(
                    sheet_id=sheet_id,
                    start_row_index=start_row_index,
                    start_col_index=start_col_index,
                    end_col_index=end_col_index,
                    eval_col_index_in_range=state_col_in_range,
                    equals_text="QUEUED",
                    background_rgb=(0.90, 0.90, 0.95),  # Light blue-gray
                ),
            ]

            self.sheets.replace_conditional_format_rules(tab_name=TAB_STATUS, rules=rules)

            # Apply additional formatting for better readability
            self._apply_column_specific_formatting(sheet_id)
            
            _FORMAT_APPLIED.add(key)

        except Exception:
            logger.exception("TOOL_STATUS_FORMATTING_FAILED")

    def _apply_column_specific_formatting(self, sheet_id: int) -> None:
        """
        Apply column-specific formatting for better readability.
        """
        try:
            # Format Job ID column (A) - left align, monospace-like
            self.sheets.format_cells(
                tab_name=TAB_STATUS,
                start_row_index=1,
                end_row_index=1000,
                start_col_index=0,  # Job ID
                end_col_index=1,
                horizontal_align="LEFT",
                vertical_align="MIDDLE",
            )

            # Format Tool column (B) - center align
            self.sheets.format_cells(
                tab_name=TAB_STATUS,
                start_row_index=1,
                end_row_index=1000,
                start_col_index=1,  # Tool
                end_col_index=2,
                horizontal_align="CENTER",
                vertical_align="MIDDLE",
            )

            # Format State column (D) - center align, bold
            self.sheets.format_cells(
                tab_name=TAB_STATUS,
                start_row_index=1,
                end_row_index=1000,
                start_col_index=3,  # State
                end_col_index=4,
                horizontal_align="CENTER",
                vertical_align="MIDDLE",
                bold=True,
            )

            # Format Phase column (E) - center align
            self.sheets.format_cells(
                tab_name=TAB_STATUS,
                start_row_index=1,
                end_row_index=1000,
                start_col_index=4,  # Phase
                end_col_index=5,
                horizontal_align="CENTER",
                vertical_align="MIDDLE",
            )

            # Format Current/Total/Percent columns (F,G,H) - right align for numbers
            self.sheets.format_cells(
                tab_name=TAB_STATUS,
                start_row_index=1,
                end_row_index=1000,
                start_col_index=5,  # Current
                end_col_index=8,    # Through Percent
                horizontal_align="RIGHT",
                vertical_align="MIDDLE",
            )

            # Format Message column (I) - left align, wrap text
            self.sheets.format_cells(
                tab_name=TAB_STATUS,
                start_row_index=1,
                end_row_index=1000,
                start_col_index=8,  # Message
                end_col_index=9,
                horizontal_align="LEFT",
                vertical_align="MIDDLE",
                wrap_strategy="WRAP",
            )

            # Format Updated At column (J) - center align
            self.sheets.format_cells(
                tab_name=TAB_STATUS,
                start_row_index=1,
                end_row_index=1000,
                start_col_index=9,  # Updated At
                end_col_index=10,
                horizontal_align="CENTER",
                vertical_align="MIDDLE",
            )

        except Exception as exc:
            logger.warning("COLUMN_SPECIFIC_FORMATTING_FAILED | error=%s", exc)

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
            values = self.sheets.read_range(tab_name=TAB_STATUS, a1_range=f"A{start}:A{end}")
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
        """
        try:
            start = int(self.scan_start_row)
            end = start + int(self.scan_max_rows) - 1
            
            # Read all data from the sheet
            all_data = self.sheets.read_range(tab_name=TAB_STATUS, a1_range=f"A{start}:L{end}")
            
            if not all_data:
                return
            
            # Track seen job IDs and their row data
            seen_jobs: dict[str, tuple[int, list[str]]] = {}  # job_id -> (row_index, row_data)
            rows_to_keep: list[list[str]] = []
            
            for i, row in enumerate(all_data):
                if not row or not row[0].strip():
                    continue
                    
                job_id = norm(row[0])
                if not job_id:
                    continue
                
                current_row_index = start + i
                
                if job_id in seen_jobs:
                    # Duplicate found - keep the one with more recent data
                    existing_row_index, existing_row_data = seen_jobs[job_id]
                    
                    # Compare "Updated At" timestamps (column J, index 9)
                    current_updated = row[9] if len(row) > 9 else ""
                    existing_updated = existing_row_data[9] if len(existing_row_data) > 9 else ""
                    
                    # Keep the row with the more recent timestamp
                    if current_updated > existing_updated:
                        # Replace the existing entry with current one
                        seen_jobs[job_id] = (current_row_index, row)
                        # Remove the old entry from rows_to_keep
                        rows_to_keep = [r for r in rows_to_keep if r != existing_row_data]
                        rows_to_keep.append(row)
                        logger.info("CLEANUP_DUPLICATE | job_id=%s | kept_newer=%s | removed_older=%s",
                                  job_id, current_updated, existing_updated)
                    else:
                        # Keep the existing entry, skip current
                        logger.info("CLEANUP_DUPLICATE | job_id=%s | kept_existing=%s | skipped_older=%s",
                                  job_id, existing_updated, current_updated)
                else:
                    # First occurrence of this job ID
                    seen_jobs[job_id] = (current_row_index, row)
                    rows_to_keep.append(row)
            
            # If we found duplicates, rewrite the sheet with cleaned data
            if len(rows_to_keep) < len([r for r in all_data if r and r[0].strip()]):
                logger.info("CLEANUP_DUPLICATES | original_rows=%d | cleaned_rows=%d",
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
            # fallback placeholder (should be rare now)
            row_to_append = [jid] + [""] * (len(HEADERS_STATUS) - 1)

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
        phase: str | None,
        current: int | None,
        total: int | None,
        message: str | None,
        meta: dict[str, Any] | None,
    ) -> str:
        return "|".join(
            [
                norm(state),
                norm(phase),
                str(current if current is not None else ""),
                str(total if total is not None else ""),
                norm(message),
                json.dumps(meta or {}, ensure_ascii=False, sort_keys=True),
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
        self.ensure_ready(force_format=force)

        jid = norm(job_id)
        sid = norm(self.spreadsheet_id)

        updated_at = datetime.utcnow().isoformat()
        pct = _percent(current, total)

        row = [
            jid,
            norm(tool),
            norm(request_id),
            norm(state),
            norm(phase),
            str(current if current is not None else ""),
            str(total if total is not None else ""),
            pct,
            norm(message),
            updated_at,
            sid,
            json.dumps(meta or {}, ensure_ascii=False),
        ]

        sig = self._signature(
            state=state,
            phase=phase,
            current=current,
            total=total,
            message=message,
            meta=meta,
        )
        if not self._should_write(job_id=jid, state=state, signature=sig, force=force):
            return

        # Allocate row; for first-time allocation append FULL row so it is colored immediately
        row_idx = self.allocate_row(job_id=jid, initial_row=row)

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
