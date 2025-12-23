from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from ..exports.sheets import SheetsClient
from ..exports.sheets_manager import GoogleSheetsManager
from ...core.utils.text import norm

TAB_STATUS = "Tool_Status"
logger = logging.getLogger(__name__)

HEADERS_STATUS = [
    "Job ID",
    "Tool",
    "Request ID",
    "State",
    "Phase",
    "Current",
    "Total",
    "Percent",
    "Message",
    "Updated At",
    "Spreadsheet ID",
    "Meta (JSON)",
]


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
        self._apply_formatting_best_effort(force=force_format)
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

            sheet_id = self.sheets._get_sheet_id_by_title(TAB_STATUS)
            if sheet_id is None:
                return

            start_row_index = 1  # row 2
            start_col_index = 0  # A
            end_col_index = len(HEADERS_STATUS)  # A..L
            state_col_in_range = 3  # D within A..L

            rules = [
                self.sheets.build_conditional_format_rule_text_equals(
                    sheet_id=sheet_id,
                    start_row_index=start_row_index,
                    start_col_index=start_col_index,
                    end_col_index=end_col_index,
                    eval_col_index_in_range=state_col_in_range,
                    equals_text="FAILED",
                    background_rgb=(1.0, 0.85, 0.85),
                ),
                self.sheets.build_conditional_format_rule_text_equals(
                    sheet_id=sheet_id,
                    start_row_index=start_row_index,
                    start_col_index=start_col_index,
                    end_col_index=end_col_index,
                    eval_col_index_in_range=state_col_in_range,
                    equals_text="RUNNING",
                    background_rgb=(1.0, 0.97, 0.80),
                ),
                self.sheets.build_conditional_format_rule_text_equals(
                    sheet_id=sheet_id,
                    start_row_index=start_row_index,
                    start_col_index=start_col_index,
                    end_col_index=end_col_index,
                    eval_col_index_in_range=state_col_in_range,
                    equals_text="SUCCEEDED",
                    background_rgb=(0.85, 0.95, 0.85),
                ),
            ]

            self.sheets.replace_conditional_format_rules(tab_name=TAB_STATUS, rules=rules)
            _FORMAT_APPLIED.add(key)

        except Exception:
            logger.exception("TOOL_STATUS_FORMATTING_FAILED")

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
