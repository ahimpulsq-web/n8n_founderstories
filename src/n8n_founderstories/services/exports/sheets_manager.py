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

    NEW:
    - buffers appends and flushes as 1 append per tab per flush
    """

    client: SheetsClient

    # budgets (keep below your project limits)
    write_budget_per_minute: int = field(
        default_factory=lambda: int(getattr(settings, "google_sheets_write_budget_per_minute", 55))
    )
    read_budget_per_minute: int = field(
        default_factory=lambda: int(getattr(settings, "google_sheets_read_budget_per_minute", 55))
    )

    # flush policy (size-based)
    flush_max_values_updates: int = field(
        default_factory=lambda: int(getattr(settings, "google_sheets_flush_max_values_updates", 50))
    )
    flush_max_batch_requests: int = field(
        default_factory=lambda: int(getattr(settings, "google_sheets_flush_max_batch_requests", 50))
    )

    # NEW: append buffering knobs
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

    # NEW: append buffer per tab
    _append_rows_by_tab: Dict[str, List[List[str]]] = field(default_factory=dict, init=False)

    _last_flush_ts: float = field(default_factory=lambda: 0.0, init=False)

    _write_budget: _RequestBudget = field(init=False)
    _read_budget: _RequestBudget = field(init=False)

    def __post_init__(self) -> None:
        self._write_budget = _RequestBudget(per_minute=self.write_budget_per_minute)
        self._read_budget = _RequestBudget(per_minute=self.read_budget_per_minute)

    # -------------------------------------------------------------------------
    # Budgeted read wrapper
    # -------------------------------------------------------------------------

    def read_range(self, *, tab_name: str, a1_range: str) -> List[List[str]]:
        self._read_budget.wait_for_slot()
        return self.client.read_range(tab_name=tab_name, a1_range=a1_range)

    # -------------------------------------------------------------------------
    # Queue APIs (buffering + flush triggers)
    # -------------------------------------------------------------------------

    def queue_values_update(self, *, tab_name: str, a1_range: str, values: Sequence[Sequence[str]]) -> None:
        """
        Buffer a values update. If same (tab, range) is queued multiple times,
        last one wins.
        """
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
        """
        Delete default 'Sheet1' tab if it exists.
        Safe to call multiple times.
        """
        try:
            if self.client.delete_tab(tab_name="Sheet1"):
                logger.info("SHEETS_DELETE_DEFAULT | Sheet1")
        except Exception:
            logger.exception("SHEETS_DELETE_DEFAULT_FAILED")


    def queue_batch_update_request(self, request: dict) -> None:
        """
        Buffer a single spreadsheets.batchUpdate request object.
        """
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
        """
        Buffer append rows. Rows are appended in-order per tab on flush.

        Policy:
        - 1 append per tab per flush (best effort).
        """
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
        """
        Flush buffered writes in as few API calls as possible.

        Order:
        1) values.batchUpdate (single call)
        2) spreadsheets.batchUpdate (single call)
        3) values.append (one call per tab with buffered rows)
        """
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
    ) -> None:
        """
        Standard workbook setup.
        - Ensures tabs exist.
        - Writes headers (either overwrite in one batch, or ensure_header with reads).
        - Buffers structural changes and flushes once.
        """
        for tab, _hdr in headers:
            self.client.ensure_tab(tab)

        # Headers
        if overwrite_headers_for_owned_tabs:
            for tab, hdr in headers:
                self.queue_values_update(tab_name=tab, a1_range="A1", values=[list(hdr)])
        else:
            for tab, hdr in headers:
                self.client.ensure_header(tab, hdr)

        # Hide tabs
        for t in hide_tabs:
            sid = self.client._get_sheet_id_by_title(t)
            if sid is None:
                continue
            self.queue_batch_update_request(
                {"updateSheetProperties": {"properties": {"sheetId": sid, "hidden": True}, "fields": "hidden"}}
            )

        # Reorder tabs
        for idx, t in enumerate(tab_order):
            sid = self.client._get_sheet_id_by_title(t)
            if sid is None:
                continue
            self.queue_batch_update_request(
                {"updateSheetProperties": {"properties": {"sheetId": sid, "index": int(idx)}, "fields": "index"}}
            )

        # Tab colors
        if tab_colors:
            for t, (r, g, b) in tab_colors.items():
                sid = self.client._get_sheet_id_by_title(t)
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
        self.flush()
