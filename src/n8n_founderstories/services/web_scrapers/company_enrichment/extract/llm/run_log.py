# src/n8n_founderstories/services/web_scrapers/company_enrichment/llm/run_log.py
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_lock = threading.Lock()
_jsonl: Path | None = None
_txt: Path | None = None


def _get_files() -> Tuple[Path, Path]:
    global _jsonl, _txt
    if _jsonl and _txt:
        return _jsonl, _txt

    with _lock:
        if _jsonl and _txt:
            return _jsonl, _txt

        logs_dir = Path(__file__).parent / "logs"
        logs_dir.mkdir(exist_ok=True)

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        _jsonl = logs_dir / f"{ts}.jsonl"
        _txt = logs_dir / f"{ts}.txt"
        return _jsonl, _txt


def append_llm_input(
    *,
    domain: str,
    selected_links: List[str],
    crawl_meta: Optional[Dict[str, Any]] = None,
    note: str = "crawl_input",
) -> None:
    jsonl, txt = _get_files()
    crawl_meta = crawl_meta or {}

    event = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "event": note,
        "domain": domain,
        "selected_links": selected_links,
        "crawl_meta": crawl_meta,
    }
    with _lock, open(jsonl, "a", encoding="utf-8") as f:
        json.dump(event, f, ensure_ascii=False)
        f.write("\n")

    lines = [
        f"DOMAIN: {domain}",
        f"EVENT: {note}",
    ]

    contact_case = crawl_meta.get("contact_case")
    about_case = crawl_meta.get("about_case")
    if contact_case is not None or about_case is not None:
        lines.append("")
        lines.append(f"CONTACT_CASE: {contact_case}")
        lines.append(f"ABOUT_CASE: {about_case}")

    contact_selected = crawl_meta.get("contact_selected_links")
    about_selected = crawl_meta.get("about_selected_links")
    if isinstance(contact_selected, list) or isinstance(about_selected, list):
        lines.append("")
        if isinstance(contact_selected, list):
            lines.append(f"CONTACT_SELECTED ({len(contact_selected)}):")
            for u in contact_selected:
                lines.append(f"  - {u}")
        if isinstance(about_selected, list):
            lines.append(f"ABOUT_SELECTED ({len(about_selected)}):")
            for u in about_selected:
                lines.append(f"  - {u}")

    lines.append("")
    lines.append("")

    with _lock, open(txt, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))


def append_llm_event(
    *,
    event: str,
    domain: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    jsonl, _ = _get_files()
    payload = payload or {}

    rec = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "domain": domain,
        **payload,
    }
    with _lock, open(jsonl, "a", encoding="utf-8") as f:
        json.dump(rec, f, ensure_ascii=False)
        f.write("\n")


def append_llm_extraction(
    *,
    domain: str,
    contact_case: str,
    about_case: Optional[str],
    result: Dict[str, Any],
) -> None:
    jsonl, txt = _get_files()

    rec = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "event": "llm.extraction.result",
        "domain": domain,
        "contact_case": contact_case,
        "about_case": about_case,
        "result": result,
    }

    with _lock, open(jsonl, "a", encoding="utf-8") as f:
        json.dump(rec, f, ensure_ascii=False)
        f.write("\n")

    def _get_extracted(section: str) -> Tuple[Optional[str], Dict[str, Any]]:
        sec = result.get(section) or {}
        source_url = sec.get("source_url")
        extracted = sec.get("extracted") or {}
        return source_url, extracted

    def _val(extracted: Dict[str, Any], key: str) -> Optional[str]:
        obj = extracted.get(key)
        if isinstance(obj, dict):
            v = obj.get("value")
            return v if isinstance(v, str) and v.strip() else None
        return None

    def _emails(extracted: Dict[str, Any]) -> List[str]:
        out: List[str] = []
        for e in (extracted.get("emails") or []):
            if isinstance(e, dict):
                em = (e.get("email") or "").strip()
                if em:
                    out.append(em)
        return out

    def _contacts(extracted: Dict[str, Any]) -> List[Tuple[str, Optional[str]]]:
        out: List[Tuple[str, Optional[str]]] = []
        for c in (extracted.get("contacts") or []):
            if isinstance(c, dict):
                name = (c.get("name") or "").strip()
                role = c.get("role")
                role = role.strip() if isinstance(role, str) and role.strip() else None
                if name:
                    out.append((name, role))
        return out

    def _write_multiline_value(lines: List[str], label: str, value: str) -> None:
        lines.append(f"  {label}:")
        for chunk in value.splitlines() or [""]:
            chunk = chunk.strip()
            if chunk:
                lines.append(f"    {chunk}")
            else:
                lines.append("    ")

    def _write_section(section_label: str, section_key: str) -> List[str]:
        source_url, extracted = _get_extracted(section_key)
        if not source_url:
            return []

        lines: List[str] = []
        lines.append(f"{section_label}: {source_url}")

        is_about = section_key == "about"

        cn = _val(extracted, "company_name")
        if cn is not None or is_about:
            lines.append(f"  company_name: {cn if cn else 'not found'}")

        if not is_about:
            emails = _emails(extracted)
            lines.append(f"  emails ({len(emails)}):")
            if emails:
                for em in emails:
                    lines.append(f"    - {em}")
            else:
                lines.append("    - not found")

            contacts = _contacts(extracted)
            lines.append(f"  contacts ({len(contacts)}):")
            if contacts:
                for name, role in contacts:
                    lines.append(f"    - {name} ({role})" if role else f"    - {name}")
            else:
                lines.append("    - not found")

            sd = _val(extracted, "short_description")
            if sd is not None:
                _write_multiline_value(lines, "short_description", sd)

        ld = _val(extracted, "long_description")
        if ld is not None:
            _write_multiline_value(lines, "long_description", ld)

        lines.append("")
        return lines

    txt_lines: List[str] = []
    txt_lines += _write_section("IMPRESSUM", "impressum")
    txt_lines += _write_section("HOMEPAGE", "homepage")
    txt_lines += _write_section("CONTACT", "contact")
    txt_lines += _write_section("PRIVACY", "privacy")
    txt_lines += _write_section("ABOUT", "about")

    if txt_lines:
        txt_lines.append("-" * 60)
        txt_lines.append("")
        with _lock, open(txt, "a", encoding="utf-8") as f:
            f.write("\n".join(txt_lines))
