# src/n8n_founderstories/services/web_scrapers/company_enrichment/extract/deterministic/run_log.py
from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import List

from ...models import DeterministicEmail

_lock = threading.Lock()
_jsonl: Path | None = None
_txt: Path | None = None


def _get_files():
    """
    Create one JSONL + one TXT log per run.

    Mirrors crawl/run_log.py behavior:
      - single timestamped JSONL + TXT per run
      - append one record per domain
    """
    global _jsonl, _txt
    if _jsonl and _txt:
        return _jsonl, _txt

    with _lock:
        logs = Path(__file__).parent / "logs"
        logs.mkdir(exist_ok=True)

        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        _jsonl = logs / f"{ts}.jsonl"
        _txt = logs / f"{ts}.txt"

        print("[DETERMINISTIC RUN LOG]")
        print(f"  JSONL: {_jsonl}")
        print(f"  TXT:   {_txt}")

        return _jsonl, _txt


def append_domain_result(
    *,
    domain: str,
    emails: List[DeterministicEmail],
    pages_used: int,
    pages_scanned: List[str],
    reason: str,
) -> None:
    """
    Append one deterministic extraction result.

    JSONL fields:
      - domain
      - emails_found
      - emails: [{email, source_url}]
      - pages_used
      - pages_scanned: [url, ...]  (ordered as scanned)
      - reason

    TXT formatting rules (aligned with your crawl logs):
      - blank line after DOMAIN
      - compact sections
      - include PAGES SCANNED only when pages_used > 1
    """
    jsonl, txt = _get_files()

    pages_scanned = [str(u).strip() for u in (pages_scanned or []) if str(u).strip()]

    # -----------------
    # JSONL (machine-readable, stable)
    # -----------------
    with _lock, open(jsonl, "a", encoding="utf-8") as f:
        json.dump(
            {
                "domain": domain,
                "emails_found": len(emails),
                "emails": [
                    {
                        "email": str(e.email),
                        "source_url": str(e.source_url) if e.source_url else None,
                    }
                    for e in emails
                ],
                "pages_used": pages_used,
                "pages_scanned": pages_scanned,
                "reason": reason,
            },
            f,
            ensure_ascii=False,
        )
        f.write("\n")

    # -----------------
    # TXT (human-readable)
    # -----------------
    lines = [
        f"DOMAIN: {domain}",
        "",
        f"EMAILS FOUND ({len(emails)}):",
    ]

    for e in emails:
        src = str(e.source_url) if e.source_url else "-"
        lines.append(f"  - {e.email} | {src}")

    lines += [
        "",
        f"PAGES USED: {pages_used}",
    ]

    # Only show scanned pages when more than one page was used (reduces noise).
    if pages_used > 1 and pages_scanned:
        lines.append("PAGES SCANNED:")
        for u in pages_scanned:
            lines.append(f"  - {u}")

    lines += [
        "",
        f"REASON: {reason}",
        "-" * 60,
        "",
    ]

    with _lock, open(txt, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))
