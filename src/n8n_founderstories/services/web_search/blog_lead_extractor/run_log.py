from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any

_lock = threading.Lock()
_jsonl: Path | None = None
_txt: Path | None = None


def _get_files():
    global _jsonl, _txt
    if _jsonl and _txt:
        return _jsonl, _txt

    with _lock:
        logs = Path(__file__).parent / "logs"
        logs.mkdir(exist_ok=True)

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        _jsonl = logs / f"{ts}.jsonl"
        _txt = logs / f"{ts}.txt"

        print("[BLOG LEAD EXTRACTOR RUN LOG]")
        print(" JSONL:", _jsonl)
        print(" TXT:  ", _txt)

        return _jsonl, _txt


def append_blog_extraction_result(
    *,
    source_url: str,
    search_intent: str,
    companies: List[Dict[str, Any]],
    model: str,
    status: str = "ok",
) -> None:
    jsonl, txt = _get_files()

    record = {
        "source_url": source_url,
        "search_intent": search_intent,
        "companies": companies,
        "model": model,
        "status": status,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
    }

    # JSONL
    with _lock, open(jsonl, "a", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False)
        f.write("\n")

    # TXT
    lines = [
        "SOURCE URL:",
        source_url,
        "",
        "SEARCH INTENT:",
        search_intent,
        "",
        f"EXTRACTED COMPANIES: {len(companies)}",
        "",
    ]

    for i, c in enumerate(companies, start=1):
        lines += [
            f"{i}. COMPANY",
            f"   Name: {c.get('name')}",
            f"   Website: {c.get('website') or '-'}",
            f"   Evidence: {c.get('evidence')}",
            "",
        ]

    lines += [
        "-" * 60,
        f"MODEL: {model}",
        f"STATUS: {status}",
        "",
    ]

    with _lock, open(txt, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))
