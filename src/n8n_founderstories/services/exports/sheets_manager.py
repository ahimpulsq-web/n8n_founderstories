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
    # Overwrite mode operations (for DB-first exports)
    # -------------------------------------------------------------------------

