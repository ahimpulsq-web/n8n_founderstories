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
    min_delay_seconds=float(getattr(settings, "google_sheets_min_write_delay_seconds", 1.5))
)
_SHARED_READ_LIMITER = _RateLimiter(
    min_delay_seconds=float(getattr(settings, "google_sheets_min_read_delay_seconds", 1.5))
)


class _SimpleCache:
    """Thread-safe cache for sheet maps."""
    
    def __init__(self) -> None:
        self._lock = Lock()
        self._data: dict[str, dict[str, int]] = {}
    
    def get(self, key: str) -> dict[str, int] | None:
        with self._lock:
            return self._data.get(key)
    
    def put(self, key: str, value: dict[str, int]) -> None:
        with self._lock:
            self._data[key] = value
    
    def invalidate(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)


_SHEET_MAP_CACHE = _SimpleCache()


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
        """Ensure a tab exists. Minimal implementation for export-only workflow."""
        name = norm(tab_name)
        if not name:
            raise ValueError("tab_name must not be empty.")

        # Fast path: check cache
        if name in self.get_sheet_map():
            return

        logger.info("SHEETS_ADD_TAB | spreadsheet=%s | tab=%s", self._spreadsheet_id, name)
        body = {"requests": [{"addSheet": {"properties": {"title": name}}}]}

        try:
            self._batch_update(body)
            return

        except HttpError as exc:  # type: ignore
            msg = _exc_message(exc).lower()
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

    def ensure_tab_with_header(self, tab_name: str, headers: list[str]) -> None:
        """
        Ensure tab exists and has headers in row 1.
        Minimal implementation for Tool_Status compatibility.
        """
        name = norm(tab_name)
        if not name or not headers:
            raise ValueError("tab_name and headers must not be empty.")
        
        # Ensure tab exists
        self.ensure_tab(name)
        
        # Write headers to row 1
        self.write_range(tab_name=name, start_cell="A1", values=[headers])

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

    # -------------------------------------------------------------------------
    # REMOVED: delete_sheet_if_exists, move_tab, hide_tab
    # These are not needed for export-only workflow
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # Headers - SIMPLIFIED
    # -------------------------------------------------------------------------
    # NOTE: Header management is now handled by bulk write operations
    # No separate ensure_header needed for export-only workflow

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
    # REMOVED: Dedupe helpers (load_existing_keys, append_rows_deduped)
    # Deduplication is now handled in the database, not in Sheets
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # Conditional formatting helpers - KEPT FOR TOOL_STATUS ONLY
    # -------------------------------------------------------------------------

    def replace_conditional_format_rules(self, *, tab_name: str, rules: list[dict]) -> None:
        """
        Replace conditional formatting rules for a tab.
        Used ONLY for Tool_Status tab coloring. Heavy read - use during setup only.
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
        """Build conditional format rule with custom formula. Used for Tool_Status coloring."""
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
        """Build conditional format rule for text equality. Used for Tool_Status coloring."""
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
    # Minimal formatting helpers - ONLY for Tool_Status
    # -------------------------------------------------------------------------

    def format_table_layout(
        self,
        *,
        tab_name: str,
        n_cols: int,
        last_row: int,
        header_row: bool = True,
        auto_resize: bool = False,
        wrap_strategy_body: str | None = None,
        header_height: int | None = None,
        body_row_height: int | None = None,
    ) -> None:
        """
        Minimal table formatting for Tool_Status.
        Only applies basic formatting, no heavy operations.
        """
        sheet_id = self.get_sheet_id(tab_name)
        if sheet_id is None:
            return

        requests: list[dict] = []

        # Set row heights if specified
        if header_row and header_height:
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": 0,
                        "endIndex": 1,
                    },
                    "properties": {"pixelSize": header_height},
                    "fields": "pixelSize",
                }
            })

        if body_row_height:
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": 1,
                        "endIndex": last_row,
                    },
                    "properties": {"pixelSize": body_row_height},
                    "fields": "pixelSize",
                }
            })

        # Auto-resize columns if requested
        if auto_resize:
            requests.append({
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": n_cols,
                    }
                }
            })

        if requests:
            self.batch_update_requests(requests=requests)

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
        """
        Minimal cell formatting for Tool_Status.
        Only applies basic text formatting.
        """
        sheet_id = self.get_sheet_id(tab_name)
        if sheet_id is None:
            return

        cell_format: dict[str, Any] = {}
        fields: list[str] = []

        if horizontal_align:
            cell_format["horizontalAlignment"] = horizontal_align.upper()
            fields.append("horizontalAlignment")

        if vertical_align:
            cell_format["verticalAlignment"] = vertical_align.upper()
            fields.append("verticalAlignment")

        if bold is not None:
            cell_format["textFormat"] = {"bold": bool(bold)}
            fields.append("textFormat.bold")

        if wrap_strategy:
            cell_format["wrapStrategy"] = wrap_strategy.upper()
            fields.append("wrapStrategy")

        if not cell_format:
            return

        request = {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row_index,
                    "endRowIndex": end_row_index,
                    "startColumnIndex": start_col_index,
                    "endColumnIndex": end_col_index,
                },
                "cell": {"userEnteredFormat": cell_format},
                "fields": ",".join(f"userEnteredFormat.{f}" for f in fields),
            }
        }

        self.batch_update_requests(requests=[request])

    # -------------------------------------------------------------------------
    # REMOVED: Batch cell updates, get_last_row, upsert_row_by_key
    # Not needed for bulk export workflow
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # REMOVED: Data validation helpers (dropdowns/checkboxes)
    # Not needed for export-only workflow
    # -------------------------------------------------------------------------

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
