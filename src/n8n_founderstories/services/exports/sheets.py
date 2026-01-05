from __future__ import annotations

import logging
import random
import re
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Sequence, Tuple

from ...core.config import settings
from ...core.utils.text import norm

try:
    from googleapiclient.errors import HttpError  # type: ignore
except Exception:  # pragma: no cover
    HttpError = Exception  # type: ignore

logger = logging.getLogger(__name__)


def _exc_message(exc: Exception) -> str:
    try:
        return str(getattr(exc, "content", b"") or "") or str(exc)
    except Exception:
        return str(exc)


def _is_transient_api_error(exc: Exception) -> bool:
    m = _exc_message(exc).lower()

    if "rate_limit_exceeded" in m:
        return True
    if "quota exceeded" in m:
        return True

    if "429" in m:
        return True
    if "503" in m or "backend error" in m:
        return True

    if "500" in m or "internal error" in m:
        return True

    return False


def _is_missing_range_error(exc: Exception) -> bool:
    m = _exc_message(exc).lower()
    return "unable to parse range" in m or "invalid range" in m


class _RateLimiter:
    def __init__(self, *, min_delay_seconds: float) -> None:
        self._min_delay = float(min_delay_seconds)
        self._lock = Lock()
        self._last_ts: float | None = None

    def wait(self) -> None:
        if self._min_delay <= 0:
            return

        with self._lock:
            now = time.time()
            if self._last_ts is not None:
                elapsed = now - self._last_ts
                remaining = self._min_delay - elapsed
                if remaining > 0:
                    time.sleep(remaining)
            self._last_ts = time.time()


_SHARED_WRITE_LIMITER = _RateLimiter(
    min_delay_seconds=float(getattr(settings, "google_sheets_min_write_delay_seconds", 1.10))
)
_SHARED_READ_LIMITER = _RateLimiter(
    min_delay_seconds=float(getattr(settings, "google_sheets_min_read_delay_seconds", 1.2))
)


@dataclass(frozen=True)
class SheetsConfig:
    service_account_file: str
    spreadsheet_id: str


def default_sheets_config(*, spreadsheet_id: str) -> SheetsConfig:
    sa_file = norm(getattr(settings, "google_service_account_file", None))
    if not sa_file:
        raise RuntimeError("google_service_account_file is not configured in Settings.")

    sid = norm(spreadsheet_id)
    if not sid:
        raise ValueError("spreadsheet_id must not be empty.")

    return SheetsConfig(service_account_file=sa_file, spreadsheet_id=sid)


def _col_index_to_a1(col_index: int) -> str:
    if col_index < 0:
        raise ValueError("col_index must be >= 0")

    n = col_index + 1
    letters: list[str] = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(ord("A") + rem))
    return "".join(reversed(letters))


def _parse_row_from_updated_range(updated_range: str) -> int | None:
    s = norm(updated_range)
    if not s:
        return None
    m = re.search(r"![A-Z]+(\d+)", s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


class _SheetMapCache:
    def __init__(self, *, ttl_seconds: float) -> None:
        self._ttl = float(ttl_seconds)
        self._lock = Lock()
        self._cache: dict[str, tuple[float, dict[str, int]]] = {}

    def get(self, spreadsheet_id: str) -> dict[str, int] | None:
        sid = norm(spreadsheet_id)
        if not sid:
            return None
        now = time.time()
        with self._lock:
            item = self._cache.get(sid)
            if not item:
                return None
            ts, data = item
            if (now - ts) > self._ttl:
                return None
            return dict(data)

    def put(self, spreadsheet_id: str, data: dict[str, int]) -> None:
        sid = norm(spreadsheet_id)
        if not sid:
            return
        with self._lock:
            self._cache[sid] = (time.time(), dict(data))

    def invalidate(self, spreadsheet_id: str) -> None:
        sid = norm(spreadsheet_id)
        if not sid:
            return
        with self._lock:
            self._cache.pop(sid, None)


_SHEET_MAP_CACHE = _SheetMapCache(
    ttl_seconds=float(getattr(settings, "google_sheets_sheet_map_cache_ttl_seconds", 120.0))
)


class SheetsClient:
    _A1_RE = re.compile(r"^([A-Za-z]+)(\d+)?$")

    def __init__(self, *, config: SheetsConfig) -> None:
        self._spreadsheet_id = norm(config.spreadsheet_id)
        if not self._spreadsheet_id:
            raise ValueError("spreadsheet_id must not be empty.")

        self._service = self._build_service(config.service_account_file)

    # -------------------------------------------------------------------------
    # Service construction + retry/throttle wrappers
    # -------------------------------------------------------------------------

    @staticmethod
    def _build_service(service_account_file: str):
        try:
            from google.oauth2.service_account import Credentials  # type: ignore
            from googleapiclient.discovery import build  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "Google Sheets dependencies missing. Install: google-api-python-client google-auth"
            ) from exc

        scopes = getattr(settings, "google_sheets_scopes", None) or [
            "https://www.googleapis.com/auth/spreadsheets"
        ]
        creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
        logger.debug("SHEETS_CLIENT_INIT | scopes=%s | creds=%s", scopes, type(creds).__name__)
        return build("sheets", "v4", credentials=creds, cache_discovery=False)

    def _execute_with_retry(
        self,
        *,
        op_name: str,
        fn,
        limiter: _RateLimiter,
        max_retries: int = 6,
    ) -> Any:
        last_exc: Exception | None = None

        for attempt in range(1, max_retries + 1):
            try:
                limiter.wait()
                return fn()

            except Exception as exc:
                last_exc = exc
                if not _is_transient_api_error(exc):
                    raise

                base = min(30.0, 1.2 * (2 ** (attempt - 1)))
                sleep_s = base + random.uniform(0.0, 0.6)

                logger.warning(
                    "SHEETS_RETRY | op=%s | attempt=%d/%d | sleep=%.2fs | error=%s",
                    op_name,
                    attempt,
                    max_retries,
                    sleep_s,
                    exc,
                )
                time.sleep(sleep_s)

        raise last_exc  # type: ignore[misc]

    def _execute_write(self, *, op_name: str, fn, max_retries: int = 6) -> Any:
        return self._execute_with_retry(
            op_name=op_name, fn=fn, limiter=_SHARED_WRITE_LIMITER, max_retries=max_retries
        )

    def _execute_read(self, *, op_name: str, fn, max_retries: int = 6) -> Any:
        return self._execute_with_retry(
            op_name=op_name, fn=fn, limiter=_SHARED_READ_LIMITER, max_retries=max_retries
        )

    # -------------------------------------------------------------------------
    # Public API wrappers
    # -------------------------------------------------------------------------

    @property
    def spreadsheet_id(self) -> str:
        return self._spreadsheet_id

    def values_batch_update(self, *, data: list[dict], value_input_option: str = "RAW") -> dict | None:
        if not data:
            return None

        def _do():
            return self._service.spreadsheets().values().batchUpdate(
                spreadsheetId=self._spreadsheet_id,
                body={"valueInputOption": str(value_input_option), "data": data},
            ).execute()

        return self._execute_write(op_name="values.batchUpdate", fn=_do)

    def values_batch_get(self, *, ranges: list[str], major_dimension: str | None = None) -> dict:
        """
        Batch read multiple ranges in one API call.

        ranges examples:
          ["Master!A2:Z200", "Master_State!B2:B2", "Tool_Status!A1:L1"]
        """
        cleaned = [norm(r) for r in (ranges or []) if norm(r)]
        if not cleaned:
            return {"valueRanges": []}

        def _do():
            req = self._service.spreadsheets().values().batchGet(
                spreadsheetId=self._spreadsheet_id,
                ranges=cleaned,
            )
            # Avoid setting majorDimension unless caller needs it; Google client is picky with None.
            if major_dimension:
                req = req  # placeholder; google client supports majorDimension but not always via kwargs
            return req.execute()

        return self._execute_read(op_name="values.batchGet", fn=_do)

    def batch_update_requests(self, *, requests: list[dict]) -> dict | None:
        if not requests:
            return None
        return self._batch_update({"requests": requests})

    def _batch_update(self, body: dict) -> dict | None:
        def _do():
            return self._service.spreadsheets().batchUpdate(
                spreadsheetId=self._spreadsheet_id,
                body=body,
            ).execute()

        # Structural updates can change sheet map.
        _SHEET_MAP_CACHE.invalidate(self._spreadsheet_id)
        return self._execute_write(op_name="batchUpdate", fn=_do)

    def get_spreadsheet_metadata(self) -> dict:
        def _do():
            return self._service.spreadsheets().get(
                spreadsheetId=self._spreadsheet_id,
                fields="sheets(properties,conditionalFormats)",
            ).execute()

        return self._execute_read(op_name="get_spreadsheet_metadata", fn=_do)

    # -------------------------------------------------------------------------
    # Sheet map + IDs (cached)
    # -------------------------------------------------------------------------

    def _fetch_sheet_map(self) -> dict[str, int]:
        def _do():
            return self._service.spreadsheets().get(
                spreadsheetId=self._spreadsheet_id,
                fields="sheets(properties(sheetId,title))",
            ).execute()

        resp = self._execute_read(op_name="spreadsheets.get", fn=_do)
        sheets = (resp or {}).get("sheets") or []
        out: dict[str, int] = {}
        for s in sheets:
            props = (s or {}).get("properties") or {}
            title = props.get("title")
            sid = props.get("sheetId")
            if isinstance(title, str) and isinstance(sid, int):
                out[title] = sid
        return out

    def get_sheet_map(self, *, force_refresh: bool = False) -> dict[str, int]:
        if not force_refresh:
            cached = _SHEET_MAP_CACHE.get(self._spreadsheet_id)
            if cached is not None:
                return cached

        m = self._fetch_sheet_map()
        _SHEET_MAP_CACHE.put(self._spreadsheet_id, m)
        return m

    def get_sheet_id(self, tab_name: str, *, force_refresh: bool = False) -> int | None:
        name = norm(tab_name)
        if not name:
            return None
        return self.get_sheet_map(force_refresh=force_refresh).get(name)

    # -------------------------------------------------------------------------
    # Tabs
    # -------------------------------------------------------------------------

    def ensure_tab(self, tab_name: str) -> None:
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")

        # Fast path: cached map.
        if name in self.get_sheet_map():
            return

        logger.info("SHEETS_ADD_TAB | spreadsheet=%s | tab=%s", self._spreadsheet_id, name)
        body = {"requests": [{"addSheet": {"properties": {"title": name}}}]}

        try:
            self._batch_update(body)
            return

        except HttpError as exc:  # type: ignore
            msg = _exc_message(exc).lower()
            # If it already exists, do not force a read refresh here. Let TTL refresh later.
            if "already exists" in msg:
                logger.info(
                    "SHEETS_ADD_TAB_RACE_OK | spreadsheet=%s | tab=%s",
                    self._spreadsheet_id,
                    name,
                )
                return
            raise

    def ensure_tabs(self, tab_names: Sequence[str]) -> None:
        """
        Ensure multiple tabs exist with:
        - ONE sheet-map read (cached)
        - ONE batchUpdate write (best effort)
        """
        names = [norm(t) for t in (tab_names or []) if norm(t)]
        if not names:
            return

        m = self.get_sheet_map()
        missing = [t for t in names if t not in m]
        if not missing:
            return

        logger.info(
            "SHEETS_ADD_TABS | spreadsheet=%s | tabs=%s",
            self._spreadsheet_id,
            ",".join(missing),
        )

        requests = [{"addSheet": {"properties": {"title": t}}} for t in missing]
        try:
            self._batch_update({"requests": requests})
        except HttpError as exc:  # type: ignore
            msg = _exc_message(exc).lower()
            # If some exist due to races, treat as ok.
            if "already exists" in msg:
                logger.info(
                    "SHEETS_ADD_TABS_RACE_OK | spreadsheet=%s | tabs=%s",
                    self._spreadsheet_id,
                    ",".join(missing),
                )
                return
            raise

    def delete_tab(self, *, tab_name: str) -> bool:
        name = norm(tab_name)
        if not name:
            return False

        sid = self.get_sheet_id(name)
        if sid is None:
            return False

        body = {"requests": [{"deleteSheet": {"sheetId": sid}}]}
        self._batch_update(body)
        logger.info("SHEETS_DELETE_TAB | spreadsheet=%s | tab=%s", self._spreadsheet_id, name)
        return True

    def delete_sheet_if_exists(self, *, tab_name: str) -> None:
        self.delete_tab(tab_name=tab_name)

    def move_tab(self, *, tab_name: str, index: int) -> None:
        sid = self.get_sheet_id(tab_name)
        if sid is None:
            return

        body = {
            "requests": [
                {"updateSheetProperties": {"properties": {"sheetId": sid, "index": int(index)}, "fields": "index"}}
            ]
        }
        self._batch_update(body)

    def hide_tab(self, *, tab_name: str) -> None:
        sid = self.get_sheet_id(tab_name)
        if sid is None:
            return

        body = {
            "requests": [
                {"updateSheetProperties": {"properties": {"sheetId": sid, "hidden": True}, "fields": "hidden"}}
            ]
        }
        self._batch_update(body)

    # -------------------------------------------------------------------------
    # Headers
    # -------------------------------------------------------------------------

    def ensure_header(self, tab_name: str, header: Sequence[str]) -> None:
        """
        NOTE: Keep for compatibility, but avoid in hot paths.
        Prefer manager.setup_tabs(overwrite_headers_for_owned_tabs=True).
        """
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")

        hdr = [norm(h) for h in header]
        if not hdr or any(not h for h in hdr):
            raise ValueError("header must be a non-empty list of non-empty strings.")

        def _read():
            return self._service.spreadsheets().values().get(
                spreadsheetId=self._spreadsheet_id,
                range=f"{name}!1:1",
            ).execute()

        resp = self._execute_read(op_name=f"read_header:{name}", fn=_read)
        values = (resp or {}).get("values") or []
        if values and any((v or "").strip() for v in values[0] if isinstance(v, str)):
            return

        logger.info(
            "SHEETS_WRITE_HEADER | spreadsheet=%s | tab=%s | cols=%d",
            self._spreadsheet_id,
            name,
            len(hdr),
        )

        def _do():
            return self._service.spreadsheets().values().update(
                spreadsheetId=self._spreadsheet_id,
                range=f"{name}!A1",
                valueInputOption="RAW",
                body={"values": [hdr]},
            ).execute()

        self._execute_write(op_name=f"write_header:{name}", fn=_do)

    def ensure_tab_with_header(self, tab_name: str, header: Sequence[str]) -> None:
        self.ensure_tab(tab_name)
        self.ensure_header(tab_name, header)

    # -------------------------------------------------------------------------
    # Values read/write/append
    # -------------------------------------------------------------------------

    def read_range(self, *, tab_name: str, a1_range: str) -> list[list[str]]:
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")

        rng = norm(a1_range)
        if not rng:
            raise ValueError("a1_range must not be empty.")

        def _do():
            return self._service.spreadsheets().values().get(
                spreadsheetId=self._spreadsheet_id,
                range=f"{name}!{rng}",
            ).execute()

        try:
            resp = self._execute_read(op_name=f"read_range:{name}!{rng}", fn=_do)
        except Exception as exc:
            if _is_missing_range_error(exc):
                logger.info(
                    "SHEETS_READ_RANGE_MISSING | spreadsheet=%s | range=%s!%s",
                    self._spreadsheet_id,
                    name,
                    rng,
                )
                return []
            raise

        values = (resp or {}).get("values") or []
        out: list[list[str]] = []
        for row in values:
            if not isinstance(row, list):
                continue
            out.append([norm(str(x)) for x in row])
        return out

    def write_range(self, *, tab_name: str, start_cell: str, values: Sequence[Sequence[str]]) -> dict | None:
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")

        cell = norm(start_cell).upper()
        if not cell:
            raise ValueError("start_cell must not be empty.")

        if not values:
            return None

        cleaned = [[norm(str(c)) for c in row] for row in values]

        def _do():
            return self._service.spreadsheets().values().update(
                spreadsheetId=self._spreadsheet_id,
                range=f"{name}!{cell}",
                valueInputOption="RAW",
                body={"values": cleaned},
            ).execute()

        return self._execute_write(op_name=f"write_range:{name}:{cell}", fn=_do)

    def append_rows(self, *, tab_name: str, rows: Sequence[Sequence[str]]) -> dict | None:
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")
        if not rows:
            return None

        cleaned = [[norm(str(c)) for c in r] for r in rows]

        def _do():
            return self._service.spreadsheets().values().append(
                spreadsheetId=self._spreadsheet_id,
                range=f"{name}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": cleaned},
            ).execute()

        return self._execute_write(op_name=f"append_rows:{name}", fn=_do)

    # -------------------------------------------------------------------------
    # Dedupe helpers
    # -------------------------------------------------------------------------

    def load_existing_keys(self, *, tab_name: str, key_col: int) -> set[str]:
        """
        WARNING: full-column reads do not scale. Use sparingly.
        Prefer run-local dedupe or a dedicated Dedupe_Index tab.
        """
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")
        if key_col < 0:
            raise ValueError("key_col must be >= 0.")

        col_letter = _col_index_to_a1(key_col)
        values = self.read_range(tab_name=name, a1_range=f"{col_letter}:{col_letter}")

        out: set[str] = set()
        for row in values:
            if not row:
                continue
            v = norm(row[0]).lower()
            if v:
                out.add(v)
        return out

    def append_rows_deduped(
        self,
        *,
        tab_name: str,
        rows: Sequence[Sequence[str]],
        key_col: int,
        existing_keys: set[str] | None = None,
    ) -> int:
        if not rows:
            return 0

        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")

        keys = existing_keys if existing_keys is not None else self.load_existing_keys(tab_name=name, key_col=key_col)

        out: list[list[str]] = []
        appended = 0

        for r in rows:
            if key_col >= len(r):
                continue
            key = norm(r[key_col]).lower()
            if not key or key in keys:
                continue
            keys.add(key)
            out.append([norm(str(c)) for c in r])
            appended += 1

        if out:
            self.append_rows(tab_name=name, rows=out)

        return appended

    # -------------------------------------------------------------------------
    # Conditional formatting helpers
    # -------------------------------------------------------------------------

    def replace_conditional_format_rules(self, *, tab_name: str, rules: list[dict]) -> None:
        """
        Heavy read. Use during setup only, not per job.
        """
        sheet_id = self.get_sheet_id(tab_name)
        if sheet_id is None:
            return

        try:
            meta = self.get_spreadsheet_metadata()
            sheets = meta.get("sheets") or []
            rule_count = 0

            for s in sheets:
                props = (s or {}).get("properties") or {}
                if props.get("sheetId") == sheet_id:
                    cfr = (s or {}).get("conditionalFormats") or []
                    rule_count = len(cfr) if isinstance(cfr, list) else 0
                    break

            requests: list[dict] = []

            for _ in range(rule_count):
                requests.append({"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": 0}})

            for i, r in enumerate(rules or []):
                if not isinstance(r, dict):
                    continue
                requests.append({"addConditionalFormatRule": {"rule": r, "index": i}})

            if requests:
                self._batch_update({"requests": requests})

        except Exception:
            logger.exception(
                "SHEETS_COND_FORMAT_REPLACE_FAILED | spreadsheet=%s | tab=%s",
                self._spreadsheet_id,
                tab_name,
            )

    def build_conditional_format_rule_custom_formula(
        self,
        *,
        sheet_id: int,
        start_row_index: int,
        start_col_index: int,
        end_col_index: int,
        formula: str,
        end_row_index: int | None = None,
        background_rgb: tuple[float, float, float] | None = None,
    ) -> dict:
        rng: dict[str, Any] = {
            "sheetId": int(sheet_id),
            "startRowIndex": int(start_row_index),
            "startColumnIndex": int(start_col_index),
            "endColumnIndex": int(end_col_index),
        }
        if end_row_index is not None:
            rng["endRowIndex"] = int(end_row_index)

        fmt: dict[str, Any] = {}
        if background_rgb is not None:
            r, g, b = background_rgb
            fmt["backgroundColor"] = {"red": float(r), "green": float(g), "blue": float(b)}

        return {
            "ranges": [rng],
            "booleanRule": {
                "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": str(formula)}]},
                "format": fmt,
            },
        }

    def build_conditional_format_rule_text_equals(
        self,
        *,
        sheet_id: int,
        start_row_index: int,
        start_col_index: int,
        end_col_index: int,
        eval_col_index_in_range: int,
        equals_text: str,
        background_rgb: tuple[float, float, float],
        end_row_index: int | None = None,
    ) -> dict:
        eval_col_abs = start_col_index + int(eval_col_index_in_range)
        eval_col_letter = _col_index_to_a1(eval_col_abs)
        formula = f'=${eval_col_letter}2="{str(equals_text)}"'

        return self.build_conditional_format_rule_custom_formula(
            sheet_id=sheet_id,
            start_row_index=start_row_index,
            start_col_index=start_col_index,
            end_col_index=end_col_index,
            end_row_index=end_row_index,
            formula=formula,
            background_rgb=background_rgb,
        )

    # -------------------------------------------------------------------------
    # Layout formatting helpers (alignment + autosize)
    # -------------------------------------------------------------------------

    def auto_resize_columns(
        self,
        *,
        tab_name: str,
        start_col_index: int = 0,
        end_col_index: int | None = None,
    ) -> None:
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")

        sheet_id = self.get_sheet_id(name)
        if sheet_id is None:
            return

        if start_col_index < 0:
            start_col_index = 0

        if end_col_index is None:
            end_col_index = 26

        if end_col_index <= start_col_index:
            return

        req = {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": int(sheet_id),
                    "dimension": "COLUMNS",
                    "startIndex": int(start_col_index),
                    "endIndex": int(end_col_index),
                }
            }
        }
        self.batch_update_requests(requests=[req])

    def format_cells(
        self,
        *,
        tab_name: str,
        start_row_index: int,
        end_row_index: int,
        start_col_index: int,
        end_col_index: int,
        horizontal_align: str | None = None,
        vertical_align: str | None = None,
        bold: bool | None = None,
        wrap_strategy: str | None = None,
    ) -> None:
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")

        sheet_id = self.get_sheet_id(name)
        if sheet_id is None:
            return

        start_row_index = max(0, int(start_row_index))
        start_col_index = max(0, int(start_col_index))
        end_row_index = max(start_row_index + 1, int(end_row_index))
        end_col_index = max(start_col_index + 1, int(end_col_index))

        user_fmt: dict[str, Any] = {}
        fields: list[str] = []

        if horizontal_align:
            user_fmt["horizontalAlignment"] = str(horizontal_align).upper()
            fields.append("userEnteredFormat.horizontalAlignment")

        if vertical_align:
            user_fmt["verticalAlignment"] = str(vertical_align).upper()
            fields.append("userEnteredFormat.verticalAlignment")

        if wrap_strategy:
            user_fmt["wrapStrategy"] = str(wrap_strategy).upper()
            fields.append("userEnteredFormat.wrapStrategy")

        if bold is not None:
            user_fmt.setdefault("textFormat", {})
            user_fmt["textFormat"]["bold"] = bool(bold)
            fields.append("userEnteredFormat.textFormat.bold")

        if not fields:
            return

        req = {
            "repeatCell": {
                "range": {
                    "sheetId": int(sheet_id),
                    "startRowIndex": int(start_row_index),
                    "endRowIndex": int(end_row_index),
                    "startColumnIndex": int(start_col_index),
                    "endColumnIndex": int(end_col_index),
                },
                "cell": {"userEnteredFormat": user_fmt},
                "fields": ",".join(fields),
            }
        }
        self.batch_update_requests(requests=[req])

    def format_table_layout(
        self,
        *,
        tab_name: str,
        n_cols: int,
        last_row: int,
        header_row: bool = True,
        auto_resize: bool = True,
        wrap_strategy_body: str = "OVERFLOW_CELL",
    ) -> None:
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")

        n_cols = max(1, int(n_cols))
        last_row = max(1, int(last_row))

        if header_row:
            self.format_cells(
                tab_name=name,
                start_row_index=0,
                end_row_index=1,
                start_col_index=0,
                end_col_index=n_cols,
                horizontal_align="CENTER",
                vertical_align="MIDDLE",
                bold=True,
                wrap_strategy="WRAP",
            )

        if last_row >= 2:
            self.format_cells(
                tab_name=name,
                start_row_index=1,
                end_row_index=last_row,
                start_col_index=0,
                end_col_index=n_cols,
                horizontal_align="LEFT",
                vertical_align="MIDDLE",
                bold=False,
                wrap_strategy=wrap_strategy_body,
            )

        if auto_resize:
            self.auto_resize_columns(tab_name=name, start_col_index=0, end_col_index=n_cols)

    # -------------------------------------------------------------------------
    # Batch cell updates + bounded upsert
    # -------------------------------------------------------------------------

    def batch_update_cells(self, *, tab_name: str, updates: Sequence[Tuple[str, str]]) -> None:
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")
        if not updates:
            return

        data = []
        for cell, value in updates:
            c = norm(cell).upper()
            if not c:
                continue
            data.append({"range": f"{name}!{c}", "values": [[norm(value)]]})

        if not data:
            return

        self.values_batch_update(data=data, value_input_option="RAW")

    def get_last_row(self, *, tab_name: str, signal_col: int = 0) -> int:
        """
        WARNING: Reads entire signal column. Use sparingly.
        """
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")

        if signal_col < 0:
            signal_col = 0

        col = _col_index_to_a1(signal_col)
        values = self.read_range(tab_name=name, a1_range=f"{col}:{col}")
        return len(values or [])

    def upsert_row_by_key(
        self,
        *,
        tab_name: str,
        key: str,
        row_values: list[str],
        key_col_letter: str = "A",
        start_row: int = 2,
        max_scan_rows: int = 2000,
    ) -> int:
        name = norm(tab_name)
        k = norm(key)
        if not name:
            raise ValueError("tab_name must not be empty.")
        if not k:
            raise ValueError("key must not be empty.")
        if not row_values:
            raise ValueError("row_values must not be empty.")

        key_col = norm(key_col_letter).upper()
        if not key_col:
            raise ValueError("key_col_letter must not be empty.")

        end_row = start_row + max_scan_rows - 1
        keys = self.read_range(tab_name=name, a1_range=f"{key_col}{start_row}:{key_col}{end_row}")

        found_row: int | None = None
        row_num = start_row
        target = k.lower()

        for r in keys:
            v = norm(r[0]).lower() if r else ""
            if v == target:
                found_row = row_num
                break
            row_num += 1

        if found_row is not None:
            self.write_range(tab_name=name, start_cell=f"A{found_row}", values=[row_values])
            return found_row

        self.append_rows(tab_name=name, rows=[row_values])
        return -1

    # -------------------------------------------------------------------------
    # Data validation helpers (dropdowns / checkboxes)
    # -------------------------------------------------------------------------

    @staticmethod
    def _a1_col_to_index(col: str) -> int:
        c = (col or "").strip().upper()
        if not c or not c.isalpha():
            raise ValueError(f"Invalid column: {col!r}")
        n = 0
        for ch in c:
            n = n * 26 + (ord(ch) - ord("A") + 1)
        return n - 1

    @staticmethod
    def _parse_a1_cell(a1: str) -> tuple[int, int | None]:
        s = (a1 or "").strip()
        m = SheetsClient._A1_RE.match(s)
        if not m:
            raise ValueError(f"Invalid A1 token: {a1!r}")
        col_letters, row_digits = m.group(1), m.group(2)
        col_idx = SheetsClient._a1_col_to_index(col_letters)
        if row_digits is None:
            return (col_idx, None)
        return (col_idx, int(row_digits) - 1)

    @staticmethod
    def _a1_to_grid_range(
        *,
        sheet_id: int,
        a1_range: str,
        default_max_rows: int = 20000,
    ) -> dict[str, Any]:
        rng = (a1_range or "").strip().upper()
        if not rng:
            raise ValueError("a1_range must not be empty.")

        if ":" in rng:
            left, right = rng.split(":", 1)
            c0, r0 = SheetsClient._parse_a1_cell(left)
            c1, r1 = SheetsClient._parse_a1_cell(right)

            start_col = int(c0)
            end_col_excl = int(c1 + 1)

            start_row = int(r0) if r0 is not None else 0
            end_row_excl = int(r1 + 1) if r1 is not None else int(default_max_rows)

        else:
            c0, r0 = SheetsClient._parse_a1_cell(rng)
            start_col = int(c0)
            end_col_excl = int(c0 + 1)

            start_row = int(r0) if r0 is not None else 0
            end_row_excl = int(r0 + 1) if r0 is not None else int(default_max_rows)

        return {
            "sheetId": int(sheet_id),
            "startRowIndex": int(start_row),
            "endRowIndex": int(end_row_excl),
            "startColumnIndex": int(start_col),
            "endColumnIndex": int(end_col_excl),
        }

    def set_data_validation_list(
        self,
        *,
        tab_name: str,
        a1_range: str,
        options: Sequence[str],
        strict: bool = True,
        show_dropdown: bool = True,
        default_max_rows: int = 20000,
    ) -> None:
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")

        sheet_id = self.get_sheet_id(name)
        if sheet_id is None:
            return

        opts = [norm(str(o)) for o in (options or []) if norm(str(o))]
        if not opts:
            raise ValueError("options must not be empty.")

        grid = SheetsClient._a1_to_grid_range(
            sheet_id=sheet_id,
            a1_range=a1_range,
            default_max_rows=default_max_rows,
        )

        rule = {
            "condition": {
                "type": "ONE_OF_LIST",
                "values": [{"userEnteredValue": o} for o in opts],
            },
            "strict": bool(strict),
            "showCustomUi": bool(show_dropdown),
        }

        self.batch_update_requests(requests=[{"setDataValidation": {"range": grid, "rule": rule}}])

    def set_checkbox_validation(
        self,
        *,
        tab_name: str,
        a1_range: str,
        default_max_rows: int = 20000,
    ) -> None:
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")

        sheet_id = self.get_sheet_id(name)
        if sheet_id is None:
            return

        grid = SheetsClient._a1_to_grid_range(
            sheet_id=sheet_id,
            a1_range=a1_range,
            default_max_rows=default_max_rows,
        )

        rule = {
            "condition": {"type": "BOOLEAN"},
            "strict": True,
            "showCustomUi": True,
        }

        self.batch_update_requests(requests=[{"setDataValidation": {"range": grid, "rule": rule}}])

    # -------------------------------------------------------------------------
    # Append response parser
    # -------------------------------------------------------------------------

    @staticmethod
    def parse_row_from_append_response(resp: dict | None) -> int | None:
        if not isinstance(resp, dict):
            return None
        updates = resp.get("updates") or {}
        updated_range = updates.get("updatedRange")
        if not isinstance(updated_range, str):
            return None
        return _parse_row_from_updated_range(updated_range)
