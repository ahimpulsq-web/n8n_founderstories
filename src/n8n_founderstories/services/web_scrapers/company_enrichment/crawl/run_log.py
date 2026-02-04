# src/n8n_founderstories/services/web_scrapers/company_enrichment/crawl/run_log.py
from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

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
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        _jsonl = logs / f"{ts}.jsonl"
        _txt = logs / f"{ts}.txt"
        return _jsonl, _txt


def append_domain_result(
    *,
    domain: str,
    contact_case: str,
    contact_links: List[str],
    about_case: Optional[str],
    about_links: List[str],
    contact_typed_links: Optional[List[Dict[str, str]]] = None,
) -> None:
    jsonl, txt = _get_files()

    # ---- JSONL ----
    rec: Dict[str, Any] = {
        "domain": domain,
        "contact_case": contact_case,
        "selected_links": contact_links,
        "about_case": about_case,
        "about_links": about_links,
    }
    if contact_typed_links is not None:
        rec["contact_typed_links"] = contact_typed_links

    with _lock, open(jsonl, "a", encoding="utf-8") as f:
        json.dump(rec, f, ensure_ascii=False)
        f.write("\n")

    # ---- TXT ----
    lines = [
        f"DOMAIN: {domain}",
        "",
        f"CONTACT CASE: {contact_case}",
        f"CONTACT SELECTED ({len(contact_links)}):",
    ]
    for u in contact_links:
        lines.append(f"  - {u}")

    if contact_typed_links:
        lines.append("")
        lines.append(f"CONTACT TYPED ({len(contact_typed_links)}):")
        for item in contact_typed_links:
            u = item.get("url", "")
            k = item.get("kind", "")
            lines.append(f"  - [{k}] {u}")

    lines += [
        "",
        f"ABOUT CASE: {about_case or 'none'}",
        f"ABOUT SELECTED ({len(about_links)}):",
    ]
    for u in about_links:
        lines.append(f"  - {u}")

    lines.append("-" * 60)
    lines.append("")

    with _lock, open(txt, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))
