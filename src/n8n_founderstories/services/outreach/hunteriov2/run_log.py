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

_current_country: str | None = None
_current_city: str | None = None
_current_headcount: str | None = None


def _get_files() -> tuple[Path, Path]:
    global _jsonl, _txt
    if _jsonl and _txt:
        return _jsonl, _txt

    logs = Path(__file__).parent / "logs"
    logs.mkdir(exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    _jsonl = logs / f"{ts}.jsonl"
    _txt = logs / f"{ts}.txt"
    return _jsonl, _txt


def _write_header_if_needed(*, f_txt, request_id: str, target: str) -> None:
    global _header_written
    if _header_written:
        return

    f_txt.write(f"REQUEST ID: {request_id}\n")
    f_txt.write(f"TARGET: {target}\n")
    f_txt.write("\n")
    _header_written = True


def _write_group_header_if_needed(
    *,
    f_txt,
    country: str,
    city: str | None,
    headcount: str | None,
) -> None:
    """
    Groups in TXT like:
      COUNTRY: DE
        CITY: Berlin
          HEADCOUNT: 51-200
    City/headcount are optional and only printed when present.
    """
    global _current_country, _current_city, _current_headcount

    # If country changes, reset deeper group keys
    if country != _current_country:
        if _current_country is not None:
            f_txt.write("\n" + "-" * 60 + "\n\n")
        f_txt.write(f"COUNTRY: {country}\n")
        _current_country = country
        _current_city = None
        _current_headcount = None

    # City grouping (optional)
    if city != _current_city:
        # Only print city block if city is provided
        if city is not None:
            f_txt.write(f"  CITY: {city}\n")
        _current_city = city
        _current_headcount = None

    # Headcount grouping (optional)
    if headcount != _current_headcount:
        if headcount is not None:
            f_txt.write(f"    HEADCOUNT: {headcount}\n")
        _current_headcount = headcount


def append_hunter_run(
    *,
    request_id: str,
    target: str,
    country: str,
    city: str | None = None,
    headcount: str | None = None,
    keyword: str,
    total_results: int,
    domains: Iterable[str],
    response: dict[str, Any] | None = None,
    max_domains_txt: int = 5,
) -> None:
    """
    Appends one run to:
      - JSONL: full structured payload per line
      - TXT: human-readable grouped output (country -> city -> headcount)

    City/headcount are optional; if None they won't be required and won't be printed as blocks.
    """

    # Materialize + normalize domains
    domains_list = [d.strip().lower() for d in domains if isinstance(d, str) and d.strip()]
    total_results_int = int(total_results) if total_results is not None else 0

    jsonl_path, txt_path = _get_files()

    with _lock:
        # --- JSONL ---
        with jsonl_path.open("a", encoding="utf-8") as f_jsonl:
            json.dump(
                {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "request_id": request_id,
                    "target": target,
                    "country": country,
                    "city": city,
                    "headcount": headcount,
                    "keyword": keyword,
                    "total_results": total_results_int,
                    "domains": domains_list,
                    "response": response,
                },
                f_jsonl,
                ensure_ascii=False,
            )
            f_jsonl.write("\n")

        # --- TXT ---
        with txt_path.open("a", encoding="utf-8") as f_txt:
            _write_header_if_needed(f_txt=f_txt, request_id=request_id, target=target)

            _write_group_header_if_needed(
                f_txt=f_txt,
                country=country,
                city=city,
                headcount=headcount,
            )

            # Build label like: "DE | Berlin | 51-200"
            parts: list[str] = [country]
            if city:
                parts.append(city)
            if headcount:
                parts.append(headcount)
            scope = " | ".join(parts)

            f_txt.write(f"{scope} | {keyword} | results={total_results_int}\n")

            show = domains_list[: max(0, int(max_domains_txt))]
            for dom in show:
                f_txt.write(f"  - {dom}\n")

            remaining = max(0, len(domains_list) - len(show))
            if remaining:
                f_txt.write(f"  ... (+{remaining} more)\n")
