from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from .models import (
    CombinedEmail,
    CombinedCompany,
    CombinedDescription,
    CombinedPerson,
)

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
        return _jsonl, _txt


def append_combine_result(
    *,
    domain: str,
    deterministic_emails: List[str],
    llm_emails: List[str],
    combined: List[CombinedEmail],
    company: Optional[CombinedCompany] = None,
    descriptions: Optional[List[CombinedDescription]] = None,
    people: Optional[List[CombinedPerson]] = None,
) -> None:
    jsonl, txt = _get_files()

    descriptions = descriptions or []
    people = people or []

    # -------------------------
    # JSONL
    # -------------------------
    with _lock, open(jsonl, "a", encoding="utf-8") as f:
        json.dump(
            {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "domain": domain,
                "deterministic_emails": deterministic_emails,
                "llm_emails": llm_emails,
                "combined_emails": [
                    {
                        "email": str(c.email),
                        "frequency": c.frequency,
                        "source_agreement": c.source_agreement,
                        "confidence": c.confidence,
                        "sources": getattr(c, "sources", []),
                    }
                    for c in combined
                ],
                "company": (
                    {
                        "name": company.name,
                        "frequency": company.frequency,
                        "confidence": company.confidence,
                        "sources": company.sources,
                    }
                    if company
                    else None
                ),
                "descriptions": [
                    {
                        "kind": d.kind,
                        "text": d.text,
                        "source_url": d.source_url,
                    }
                    for d in descriptions
                ],
                "people": [
                    {
                        "name": p.name,
                        "role": p.role,
                        "sources": p.sources,
                    }
                    for p in people
                ],
            },
            f,
            ensure_ascii=False,
        )
        f.write("\n")

    # -------------------------
    # TXT
    # -------------------------
    lines: List[str] = [
        f"DOMAIN: {domain}",
        "",
        f"DETERMINISTIC EMAILS ({len(deterministic_emails)}):",
    ]

    if deterministic_emails:
        for e in deterministic_emails:
            lines.append(f"  - {e}")
    else:
        lines.append("  - none")

    lines += ["", f"LLM EMAILS ({len(llm_emails)}):"]
    if llm_emails:
        for e in llm_emails:
            lines.append(f"  - {e}")
    else:
        lines.append("  - none")

    lines += ["", f"FINAL EMAILS WITH CONFIDENCE ({len(combined)}):"]
    if combined:
        for c in combined:
            lines.append(
                f"  - {c.email} | confidence={c.confidence:.3f} | "
                f"freq={c.frequency} | source={c.source_agreement}"
            )
            sources = getattr(c, "sources", None) or []
            if sources:
                lines.append("      sources:")
                for s in sources:
                    url = (s.get("url") or "-").strip()
                    src = (s.get("source") or "-").strip()
                    lines.append(f"        - {url} ({src})")
    else:
        lines.append("  - none")

    # -------------------------
    # COMPANY NAME
    # -------------------------
    lines += ["", "COMPANY NAME:"]
    if company:
        lines.append(
            f"  - {company.name} | confidence={company.confidence:.3f} | freq={company.frequency}"
        )
        lines.append("      sources:")
        for u in company.sources:
            lines.append(f"        - {u}")
    else:
        lines.append("  - none")

    # -------------------------
    # DESCRIPTIONS
    # -------------------------
    lines += ["", "DESCRIPTIONS:"]
    if descriptions:
        for d in descriptions:
            lines.append(f"  - [{d.kind}] {d.text}")
            lines.append("      source:")
            lines.append(f"        - {d.source_url}")
    else:
        lines.append("  - none")

    # -------------------------
    # PEOPLE
    # -------------------------
    lines += ["", "PEOPLE:"]
    if people:
        for p in people:
            role = f" | role={p.role}" if p.role else ""
            lines.append(f"  - {p.name}{role}")
            lines.append("      sources:")
            for u in (p.sources or []):
                lines.append(f"        - {u}")
    else:
        lines.append("  - none")

    lines += ["", "-" * 60, ""]

    with _lock, open(txt, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))
