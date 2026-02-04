from __future__ import annotations

import json
import threading
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

_lock = threading.Lock()
_jsonl: Path | None = None
_txt: Path | None = None


def _get_files() -> tuple[Path, Path]:
    global _jsonl, _txt
    if _jsonl and _txt:
        return _jsonl, _txt

    with _lock:
        logs = Path(__file__).parent / "logs"
        logs.mkdir(exist_ok=True)

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        _jsonl = logs / f"{ts}.jsonl"
        _txt = logs / f"{ts}.txt"

        print("[GOOGLE SEARCH RUN LOG]")
        print(f"  JSONL: {_jsonl}")
        print(f"  TXT:   {_txt}")

        return _jsonl, _txt


def append_query_page_result(
    *,
    query: str,
    country: str,
    location: str,
    language: str,
    domain: str,
    page: int,
    hits: Iterable[Any],
    reason: str = "ok",
) -> None:
    jsonl, txt = _get_files()
    hits_list = list(hits)

    payload = {
        "query": query,
        "country": country,
        "location": location,
        "page": page,
        "results_found": len(hits_list),
        "hits": [
            (asdict(h) if is_dataclass(h) else dict(h))
            for h in hits_list
        ],
        "reason": reason,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
    }

    with _lock, open(jsonl, "a", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
        f.write("\n")

        lines = [
        f"QUERY: {query}",
        f"LOCATION: {location}",
        f"LANGUAGE: {language}",
        f"COUNTRY: {country}",
        f"GOOGLE DOMAIN: {domain}",
        f"PAGE: {page + 1}",
        "",
        f"RESULTS FOUND: {len(hits_list)}",
        "",
    ]

    for i, h in enumerate(hits_list, start=1):
        d = asdict(h) if is_dataclass(h) else dict(h)
        lines.append(f"  {i}. {d.get('title')}")
        lines.append(f"     {d.get('link')}")
        if d.get("snippet"):
            lines.append(f"     {d.get('snippet')}")
        lines.append("")

    lines += [
        f"REASON: {reason}",
        "-" * 60,
        "",
    ]

    with _lock, open(txt, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))
