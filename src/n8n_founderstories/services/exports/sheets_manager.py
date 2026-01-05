from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Dict, List, Sequence, Tuple, Optional

from .sheets import SheetsClient
from ...core.config import settings
from ...core.utils.text import norm

logger = logging.getLogger(__name__)

# Process-local lock per spreadsheet (prevents repeated setup within same process).
_SPREADSHEET_LOCKS: Dict[str, Lock] = {}
_SPREADSHEET_LOCKS_GUARD = Lock()


def _get_spreadsheet_lock(spreadsheet_id: str) -> Lock:
    sid = norm(spreadsheet_id)
    with _SPREADSHEET_LOCKS_GUARD:
        lock = _SPREADSHEET_LOCKS.get(sid)
        if lock is None:
            lock = Lock()
            _SPREADSHEET_LOCKS[sid] = lock
        return lock


class _RequestBudget:
    """
    Simple per-minute budgeting (in-process only).

    Notes:
    - Complements SheetsClient's retry + shared throttling.
    - If you run multiple processes, budgets are per-process.
    """
    def __init__(self, *, per_minute: int) -> None:
        self._limit = int(per_minute)
        self._lock = Lock()
        self._ts: List[float] = []

    def wait_for_slot(self) -> None:
        if self._limit <= 0:
            return

        while True:
            with self._lock:
                now = time.time()
                window_start = now - 60.0
                self._ts = [t for t in self._ts if t >= window_start]

                if len(self._ts) < self._limit:
                    self._ts.append(now)
                    return

                oldest = self._ts[0] if self._ts else now
                sleep_s = max(0.05, (oldest + 60.0) - now)

            time.sleep(sleep_s)


@dataclass(frozen=True)
class _BufferedValuesUpdate:
    tab_name: str
    a1_range: str
    values: Sequence[Sequence[str]]


@dataclass
class GoogleSheetsManager:
    """
    Tool-agnostic sheets manager:
    - buffers and batches writes
    - enforces request budgets
    - provides interval-based flushing
    - buffers appends and flushes as 1 append per tab per flush
    """

    client: SheetsClient

    # budgets (keep below your project limits)
    write_budget_per_minute: int = field(
        default_factory=lambda: int(getattr(settings, "google_sheets_write_budget_per_minute", 55))
    )
    read_budget_per_minute: int = field(
        default_factory=lambda: int(getattr(settings, "google_sheets_read_budget_per_minute", 45))
    )

    # flush policy (size-based)
    flush_max_values_updates: int = field(
        default_factory=lambda: int(getattr(settings, "google_sheets_flush_max_values_updates", 50))
    )
    flush_max_batch_requests: int = field(
        default_factory=lambda: int(getattr(settings, "google_sheets_flush_max_batch_requests", 50))
    )

    # append buffering knobs
    flush_max_append_tabs: int = field(
        default_factory=lambda: int(getattr(settings, "google_sheets_flush_max_append_tabs", 10))
    )
    flush_max_append_rows_total: int = field(
        default_factory=lambda: int(getattr(settings, "google_sheets_flush_max_append_rows_total", 1200))
    )

    # flush policy (time-based)
    flush_interval_seconds: float = field(
        default_factory=lambda: float(getattr(settings, "google_sheets_flush_interval_seconds", 2.0))
    )

    _lock: Lock = field(default_factory=Lock, init=False)
    _values_updates: Dict[Tuple[str, str], _BufferedValuesUpdate] = field(default_factory=dict, init=False)
    _batch_requests: List[dict] = field(default_factory=list, init=False)

    _append_rows_by_tab: Dict[str, List[List[str]]] = field(default_factory=dict, init=False)

    _last_flush_ts: float = field(default_factory=lambda: 0.0, init=False)

    _write_budget: _RequestBudget = field(init=False)
    _read_budget: _RequestBudget = field(init=False)

    def __post_init__(self) -> None:
        self._write_budget = _RequestBudget(per_minute=self.write_budget_per_minute)
        self._read_budget = _RequestBudget(per_minute=self.read_budget_per_minute)

    # -------------------------------------------------------------------------
    # Budgeted read wrappers
    # -------------------------------------------------------------------------

    def read_range(self, *, tab_name: str, a1_range: str) -> List[List[str]]:
        self._read_budget.wait_for_slot()
        return self.client.read_range(tab_name=tab_name, a1_range=a1_range)

    def batch_get(self, *, tab_ranges: Sequence[Tuple[str, str]]) -> List[List[List[str]]]:
        """
        Batch read multiple ranges with ONE API call.

        tab_ranges: [(tab, "A2:Z200"), (tab, "B2:B2"), ...]
        Returns a list aligned with tab_ranges; each element is a 2D list (rows x cols).
        """
        ranges: List[str] = []
        for t, r in (tab_ranges or []):
            tn = norm(t)
            rn = norm(r)
            if tn and rn:
                ranges.append(f"{tn}!{rn}")

        self._read_budget.wait_for_slot()
        resp = self.client.values_batch_get(ranges=ranges)
        value_ranges = (resp or {}).get("valueRanges") or []

        out: List[List[List[str]]] = []
        for vr in value_ranges:
            values = (vr or {}).get("values") or []
            cleaned: List[List[str]] = []
            for row in values:
                if isinstance(row, list):
                    cleaned.append([norm(str(c)) for c in row])
            out.append(cleaned)

        # Ensure alignment even if API returns fewer ranges.
        while len(out) < len(ranges):
            out.append([])

        return out

    def read_cell(self, *, tab_name: str, cell: str) -> str:
        vals = self.read_range(tab_name=tab_name, a1_range=cell)
        if not vals or not vals[0]:
            return ""
        return norm(vals[0][0])

    # -------------------------------------------------------------------------
    # Queue APIs (buffering + flush triggers)
    # -------------------------------------------------------------------------

    def queue_values_update(self, *, tab_name: str, a1_range: str, values: Sequence[Sequence[str]]) -> None:
        t = norm(tab_name)
        r = norm(a1_range)
        if not t or not r or not values:
            return

        should_flush = False
        now = time.time()

        with self._lock:
            self._values_updates[(t, r)] = _BufferedValuesUpdate(tab_name=t, a1_range=r, values=values)

            if len(self._values_updates) >= self.flush_max_values_updates:
                should_flush = True
            elif self.flush_interval_seconds > 0 and (now - self._last_flush_ts) >= self.flush_interval_seconds:
                should_flush = True

        if should_flush:
            try:
                self.flush()
            except Exception:
                logger.exception("SHEETS_MANAGER_FLUSH_FAILED (values)")

    def delete_default_sheet_best_effort(self) -> None:
        try:
            if self.client.delete_tab(tab_name="Sheet1"):
                logger.info("SHEETS_DELETE_DEFAULT | Sheet1")
        except Exception:
            logger.exception("SHEETS_DELETE_DEFAULT_FAILED")

    def queue_batch_update_request(self, request: dict) -> None:
        if not isinstance(request, dict):
            return

        should_flush = False
        now = time.time()

        with self._lock:
            self._batch_requests.append(request)

            if len(self._batch_requests) >= self.flush_max_batch_requests:
                should_flush = True
            elif self.flush_interval_seconds > 0 and (now - self._last_flush_ts) >= self.flush_interval_seconds:
                should_flush = True

        if should_flush:
            try:
                self.flush()
            except Exception:
                logger.exception("SHEETS_MANAGER_FLUSH_FAILED (batchUpdate)")

    def queue_append_rows(self, *, tab_name: str, rows: Sequence[Sequence[str]]) -> None:
        t = norm(tab_name)
        if not t or not rows:
            return

        cleaned: List[List[str]] = [[norm(str(c)) for c in r] for r in rows if r]
        if not cleaned:
            return

        should_flush = False
        now = time.time()

        with self._lock:
            bucket = self._append_rows_by_tab.setdefault(t, [])
            bucket.extend(cleaned)

            total_rows = sum(len(v) for v in self._append_rows_by_tab.values())

            if len(self._append_rows_by_tab) >= self.flush_max_append_tabs:
                should_flush = True
            elif total_rows >= self.flush_max_append_rows_total:
                should_flush = True
            elif self.flush_interval_seconds > 0 and (now - self._last_flush_ts) >= self.flush_interval_seconds:
                should_flush = True

        if should_flush:
            try:
                self.flush()
            except Exception:
                logger.exception("SHEETS_MANAGER_FLUSH_FAILED (append)")

    # -------------------------------------------------------------------------
    # Flush (the ONLY flush implementation; performs I/O)
    # -------------------------------------------------------------------------

    def flush(self) -> None:
        with self._lock:
            values_items = list(self._values_updates.values())
            batch_reqs = list(self._batch_requests)
            append_by_tab = {k: list(v) for k, v in self._append_rows_by_tab.items()}

            self._values_updates.clear()
            self._batch_requests.clear()
            self._append_rows_by_tab.clear()

        if not values_items and not batch_reqs and not append_by_tab:
            return

        try:
            # 1) values.batchUpdate (single call)
            if values_items:
                data: list[dict] = []
                for u in values_items:
                    data.append(
                        {
                            "range": f"{u.tab_name}!{u.a1_range}",
                            "values": [[norm(str(c)) for c in row] for row in u.values],
                        }
                    )

                self._write_budget.wait_for_slot()
                self.client.values_batch_update(data=data, value_input_option="RAW")

            # 2) spreadsheets.batchUpdate (single call)
            if batch_reqs:
                self._write_budget.wait_for_slot()
                self.client.batch_update_requests(requests=batch_reqs)

            # 3) appends (one call per tab)
            for tab, rows in append_by_tab.items():
                if not rows:
                    continue
                self._write_budget.wait_for_slot()
                self.client.append_rows(tab_name=tab, rows=rows)

            with self._lock:
                self._last_flush_ts = time.time()

        except Exception:
            # Re-queue on failure (best-effort)
            with self._lock:
                for u in values_items:
                    self._values_updates[(u.tab_name, u.a1_range)] = u
                if batch_reqs:
                    self._batch_requests = batch_reqs + self._batch_requests
                for tab, rows in append_by_tab.items():
                    if not rows:
                        continue
                    self._append_rows_by_tab.setdefault(tab, []).extend(rows)
            raise

    # -------------------------------------------------------------------------
    # Initialization marker (prevents repeated setup in same process)
    # -------------------------------------------------------------------------

    def is_initialized(self, *, tab_name: str = "Dashboard", cell: str = "A1", marker_prefix: str = "__INIT_DONE__") -> bool:
        try:
            v = self.read_cell(tab_name=tab_name, cell=cell)
            return v.startswith(marker_prefix)
        except Exception:
            return False

    def write_init_marker(self, *, tab_name: str = "Dashboard", cell: str = "A1", marker: str | None = None) -> None:
        if marker is None:
            marker = f"__INIT_DONE__:{int(time.time())}"
        self.queue_values_update(tab_name=tab_name, a1_range=cell, values=[[marker]])

    # -------------------------------------------------------------------------
    # High-level convenience ops
    # -------------------------------------------------------------------------

    def setup_tabs(
        self,
        *,
        headers: Sequence[Tuple[str, Sequence[str]]],
        hide_tabs: Sequence[str] = (),
        tab_order: Sequence[str] = (),
        tab_colors: Optional[Dict[str, Tuple[float, float, float]]] = None,
        overwrite_headers_for_owned_tabs: bool = True,
        init_marker_tab: str = "Dashboard",
        init_marker_cell: str = "A1",
        init_marker_prefix: str = "__INIT_DONE__",
    ) -> None:
        """
        Standard workbook setup.

        Key production behaviors:
        - Bulk tab creation (one sheet-map read, one batchUpdate write).
        - Optional init marker to avoid repeated setup.
        - Avoid ensure_header (reads) by overwriting headers in one batch update.

        Note:
        - This lock is process-local. If you run multiple processes, add a distributed lock (Redis).
        """
        spreadsheet_lock = _get_spreadsheet_lock(self.client.spreadsheet_id)

        with spreadsheet_lock:
            # Skip if already initialized (one read)
            if self.is_initialized(tab_name=init_marker_tab, cell=init_marker_cell, marker_prefix=init_marker_prefix):
                return

            tabs = [t for t, _ in headers]
            # Ensure marker tab exists too
            if norm(init_marker_tab) and init_marker_tab not in tabs:
                tabs = [init_marker_tab] + tabs

            # 1) Ensure tabs exist in bulk
            self.client.ensure_tabs(tabs)

            # 2) Headers
            if overwrite_headers_for_owned_tabs:
                for tab, hdr in headers:
                    self.queue_values_update(tab_name=tab, a1_range="A1", values=[list(hdr)])
            else:
                # Avoid this in hot paths; it reads row 1
                for tab, hdr in headers:
                    self.client.ensure_header(tab, hdr)

            # 3) Hide tabs
            for t in hide_tabs:
                sid = self.client.get_sheet_id(t)
                if sid is None:
                    continue
                self.queue_batch_update_request(
                    {"updateSheetProperties": {"properties": {"sheetId": sid, "hidden": True}, "fields": "hidden"}}
                )

            # 4) Reorder tabs
            for idx, t in enumerate(tab_order):
                sid = self.client.get_sheet_id(t)
                if sid is None:
                    continue
                self.queue_batch_update_request(
                    {"updateSheetProperties": {"properties": {"sheetId": sid, "index": int(idx)}, "fields": "index"}}
                )

            # 5) Tab colors
            if tab_colors:
                for t, (r, g, b) in tab_colors.items():
                    sid = self.client.get_sheet_id(t)
                    if sid is None:
                        continue
                    self.queue_batch_update_request(
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": sid,
                                    "tabColor": {"red": float(r), "green": float(g), "blue": float(b)},
                                },
                                "fields": "tabColor",
                            }
                        }
                    )

            self.delete_default_sheet_best_effort()

            # 6) Write init marker last, then flush once
            self.write_init_marker(tab_name=init_marker_tab, cell=init_marker_cell)
            self.flush()
