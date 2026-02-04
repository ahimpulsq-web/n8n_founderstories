from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

_lock = threading.Lock()

_jsonl: Path | None = None
_txt: Path | None = None

_header_written = False

_current_query: str | None = None
_current_country: str | None = None
_current_state: str | None = None
_current_city: str | None = None


def _get_files() -> tuple[Path, Path]:
    """Get or create log files for this run (single file per run)."""
    global _jsonl, _txt
    if _jsonl and _txt:
        return _jsonl, _txt

    logs = Path(__file__).parent / "logs"
    logs.mkdir(exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    _jsonl = logs / f"{ts}.jsonl"
    _txt = logs / f"{ts}.txt"
    return _jsonl, _txt


def _write_header_if_needed(
    *,
    f_txt,
    request_id: str,
    language: str,
    page_size: int,
    max_pages: int,
) -> None:
    """Write header once at the start of the log file."""
    global _header_written
    if _header_written:
        return

    f_txt.write(f"REQUEST ID: {request_id}\n")
    f_txt.write(f"LANGUAGE: {language}\n")
    f_txt.write(f"MAX PAGES: {max_pages}\n")
    f_txt.write(f"PAGE SIZE: {page_size}\n")
    f_txt.write(f"FIELDS: organization | website | location\n\n")
    _header_written = True


def _write_query_header_if_needed(*, f_txt, text_query: str) -> None:
    """Write query header when query changes."""
    global _current_query
    if text_query == _current_query:
        return
    if _current_query is not None:
        f_txt.write("\n" + "=" * 60 + "\n\n")
    f_txt.write(f"QUERY: {text_query}\n")
    f_txt.write("-" * 60 + "\n")
    _current_query = text_query


def _write_group_header_if_needed(*, f_txt, country: str, state: str | None, city: str | None) -> None:
    """
    Write location group headers when they change.
    Groups in TXT like:
      COUNTRY: FR
        STATE: Île-de-France
          CITY: Paris
    state/city are optional and only printed when present.
    """
    global _current_country, _current_state, _current_city

    # If country changes, reset deeper group keys
    if country != _current_country:
        if _current_country is not None:
            f_txt.write("\n")
        f_txt.write(f"COUNTRY: {country}\n")
        _current_country = country
        _current_state = None
        _current_city = None

    # State grouping (optional)
    if state != _current_state:
        if state is not None:
            f_txt.write(f"  STATE: {state}\n")
        _current_state = state
        _current_city = None

    # City grouping (optional)
    if city != _current_city:
        if city is not None:
            f_txt.write(f"    CITY: {city}\n")
        _current_city = city


def _norm_str(x: Any) -> str:
    """Normalize string value."""
    if not isinstance(x, str):
        return ""
    return x.strip()


def _fmt_website(v: Any) -> str:
    """Format website for display."""
    s = _norm_str(v)
    return s or "-"


def _fmt_location(v: Any) -> str:
    """Format location for display."""
    s = _norm_str(v)
    return s or "-"


def _fmt_organization(v: Any) -> str:
    """Format organization name for display."""
    s = _norm_str(v)
    return s or "-"


def append_geocode_result(
    *,
    request_id: str,
    country: str,
    state: str | None,
    city: str | None,
    address: str,
    success: bool,
    error_msg: str | None = None,
) -> None:
    """
    Append one GEOCODE result to the log files.
    - JSONL: full structured payload per line
    - TXT: human-readable geocoding status
    """
    jsonl_path, txt_path = _get_files()
    
    # Build location label
    parts: list[str] = [country]
    if state:
        parts.append(state)
    if city:
        parts.append(city)
    location_label = " | ".join(parts)
    
    with _lock:
        # --- JSONL ---
        with jsonl_path.open("a", encoding="utf-8") as f_jsonl:
            json.dump(
                {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "request_id": request_id,
                    "type": "geocode",
                    "country": country,
                    "state": state,
                    "city": city,
                    "address": address,
                    "success": success,
                    "error_msg": error_msg,
                },
                f_jsonl,
                ensure_ascii=False,
            )
            f_jsonl.write("\n")
        
        # --- TXT ---
        with txt_path.open("a", encoding="utf-8") as f_txt:
            status = "OK" if success else f"FAILED ({error_msg or 'no viewport'})"
            f_txt.write(f"GEOCODE | {location_label} | {address} | {status}\n")


def append_places_page(
    *,
    request_id: str,
    text_query: str,
    language: str,
    page_size: int,
    max_pages: int,
    country: str,
    state: str | None,
    city: str | None,
    page_no: int,
    returned: int,
    kept: int,
    leads_preview: Iterable[dict[str, Any]],
    response: dict[str, Any] | None = None,
    max_preview_txt: int = 5,
) -> None:
    """
    Append one PAGE run to the log files.
    - JSONL: full structured payload per line
    - TXT: human-readable, grouped, shows per-page stats + top N leads
    """

    jsonl_path, txt_path = _get_files()

    # Materialize preview for JSONL + TXT
    preview_list = []
    for item in leads_preview:
        if isinstance(item, dict):
            preview_list.append(
                {
                    "organization": item.get("organization") or "",
                    "website": item.get("website") or "",
                    "location": item.get("location") or "",
                    "description": item.get("description") or "",
                }
            )

    with _lock:
        # --- JSONL ---
        with jsonl_path.open("a", encoding="utf-8") as f_jsonl:
            json.dump(
                {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "request_id": request_id,
                    "text_query": text_query,
                    "language": language,
                    "page_size": page_size,
                    "max_pages": max_pages,
                    "country": country,
                    "state": state,
                    "city": city,
                    "page_no": int(page_no),
                    "returned": int(returned),
                    "kept": int(kept),
                    "preview": preview_list,
                    "response": response,
                },
                f_jsonl,
                ensure_ascii=False,
            )
            f_jsonl.write("\n")

        # --- TXT ---
        with txt_path.open("a", encoding="utf-8") as f_txt:
            _write_header_if_needed(
                f_txt=f_txt,
                request_id=request_id,
                language=language,
                page_size=page_size,
                max_pages=max_pages,
            )

            _write_query_header_if_needed(f_txt=f_txt, text_query=text_query)
            _write_group_header_if_needed(f_txt=f_txt, country=country, state=state, city=city)

            # Build location label
            parts: list[str] = [country]
            if state:
                parts.append(state)
            if city:
                parts.append(city)
            location_label = " | ".join(parts)

            # Per-page line
            f_txt.write(f"    {location_label} | PAGE {int(page_no)}: returned={int(returned)} kept={int(kept)}\n")

            # Show preview leads
            show = preview_list[: max(0, int(max_preview_txt))]
            for idx, lead in enumerate(show, start=1):
                org = _fmt_organization(lead.get("organization"))
                ws = _fmt_website(lead.get("website"))
                loc = _fmt_location(lead.get("location"))
                f_txt.write(f"      {idx}) {org}\n")
                f_txt.write(f"         web: {ws}\n")
                f_txt.write(f"         loc: {loc}\n")

            remaining = max(0, len(preview_list) - len(show))
            if remaining:
                f_txt.write(f"      ... (+{remaining} more)\n")


def append_places_location_summary(
    *,
    request_id: str,
    text_query: str,
    country: str,
    state: str | None,
    city: str | None,
    total_returned: int,
    total_kept: int,
) -> None:
    """
    Write summary line after finishing all pages for a location.
    Writes only to TXT (summary line).
    """
    _, txt_path = _get_files()
    
    # Build location label
    parts: list[str] = [country]
    if state:
        parts.append(state)
    if city:
        parts.append(city)
    location_label = " | ".join(parts)
    
    with _lock:
        with txt_path.open("a", encoding="utf-8") as f_txt:
            _write_query_header_if_needed(f_txt=f_txt, text_query=text_query)
            _write_group_header_if_needed(f_txt=f_txt, country=country, state=state, city=city)
            f_txt.write(f"    {location_label} | TOTAL: returned={int(total_returned)} kept={int(total_kept)}\n\n")
