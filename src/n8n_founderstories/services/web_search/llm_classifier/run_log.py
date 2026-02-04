from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

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

        print("[LLM CLASSIFIER RUN LOG]")
        print(" JSONL:", _jsonl)
        print(" TXT:  ", _txt)

        return _jsonl, _txt


def append_classification_result(
    *,
    url: str,
    title: str | None,
    snippet: str | None,
    classification: Dict,
    model: str,
) -> None:
    jsonl, txt = _get_files()

    record = {
        "url": url,
        "title": title,
        "snippet": snippet,
        "classification": classification,
        "model": model,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
    }

    # JSONL
    with _lock, open(jsonl, "a", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False)
        f.write("\n")

    # TXT
    company_name = classification.get('company_name') or '-'
    lines = [
        f"URL: {url}",
        f"COMPANY NAME: {company_name}",
        f"TITLE: {title or '-'}",
        f"TYPE: {classification.get('type')}",
        f"CONFIDENCE: {classification.get('confidence')}",
        f"MODEL: {model}",
        f"REASON: {classification.get('reason')}",
        "-" * 60,
        "",
    ]

    with _lock, open(txt, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))
