# n8n_founderstories/services/web_search/log_websearch.py
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

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


        print("[WEB SEARCH PIPELINE RUN LOG]")
        print(" JSONL:", _jsonl)
        print(" TXT:  ", _txt)

        return _jsonl, _txt


def _country_name(cc: str) -> str:
    # minimal map (extend anytime)
    m = {"DE": "Germany", "AT": "Austria", "CH": "Switzerland"}
    return m.get((cc or "").upper(), (cc or "").upper() or "-")


def _group_hits_by_country(payload: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Uses payload['classified'] + payload['hits'] to build per-country lines without repeating fields.
    """
    hits = payload.get("hits") or []
    classified = payload.get("classified") or []

    # Build lookup by url for hit meta (title/snippet + country/domain/gl/etc)
    hit_by_url: Dict[str, Dict[str, Any]] = {}
    for h in hits:
        url = (h.get("url") or "").strip()
        if url:
            hit_by_url[url] = h

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for item in classified:
        url = (item.get("url") or "").strip()
        h = hit_by_url.get(url, {})
        cc = (h.get("source_country") or "-").upper()
        grouped.setdefault(cc, []).append({"hit": h, "cls": item.get("classification") or {}})

    return grouped


def append_pipeline_result(
    *,
    request_id: str | None,
    query: str | None,
    prompt_language: str | None,
    payload: Dict[str, Any],
    status: str = "ok",
) -> None:
    jsonl, txt = _get_files()

    # --- JSONL (full payload, complete) ---
    record = {
        "request_id": request_id,
        "query": query,
        "prompt_language": prompt_language,
        "status": status,
        "payload": payload,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
    }

    with _lock, open(jsonl, "a", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False)
        f.write("\n")

    # --- TXT (complete, not summary) ---
    hits: List[Dict[str, Any]] = payload.get("hits") or []
    classified: List[Dict[str, Any]] = payload.get("classified") or []
    blog_extractions: List[Dict[str, Any]] = payload.get("blog_extractions") or []

    # counts
    blog_n = 0
    company_n = 0
    other_n = 0
    for x in classified:
        t = ((x.get("classification") or {}).get("type") or "other").lower()
        if t == "blog":
            blog_n += 1
        elif t == "company":
            company_n += 1
        else:
            other_n += 1

    # countries line from search_plan countries + settings inferred from hits
    # build cc -> (domain, gl)
    cc_meta: Dict[str, Tuple[str | None, str | None]] = {}
    for h in hits:
        cc = (h.get("source_country") or "").upper()
        if not cc:
            continue
        cc_meta.setdefault(cc, (h.get("source_domain"), None))
        # gl isn't stored on hit; infer from country code lower if needed
        dom = h.get("source_domain")
        cc_meta[cc] = (dom, cc.lower())

    countries_str = " | ".join(
        f"{cc} (domain={cc_meta.get(cc, (None, None))[0] or '-'} gl={cc_meta.get(cc, (None, None))[1] or '-'})"
        for cc in sorted(cc_meta.keys())
    ) or "-"

    lines: List[str] = [
        f"REQUEST_ID: {request_id or '-'}",
        f"QUERY: {query or '-'}",
        f"HL: {prompt_language or '-'}",
        f"STATUS: {status}",
        f"HITS: {len(hits)}   BLOGS: {blog_n}   COMPANIES: {company_n}   OTHER: {other_n}",
        "-" * 60,
        "",
        "COUNTRIES:",
        countries_str,
        "",
        "-" * 60,
        "",
    ]

    grouped = _group_hits_by_country(payload)

    # stable order: DE, AT, CH, then others alphabetically
    pref = ["DE", "AT", "CH"]
    ordered_countries = [c for c in pref if c in grouped] + sorted([c for c in grouped.keys() if c not in pref])

    for cc in ordered_countries:
        country_title = f"=== {cc} ({_country_name(cc)}) ==="
        lines += [country_title]

        items = grouped.get(cc) or []
        # keep order roughly as classified order; enumerate per country
        for i, it in enumerate(items, start=1):
            h = it["hit"] or {}
            c = it["cls"] or {}

            t = (c.get("type") or "other")
            conf = c.get("confidence")
            title = h.get("title") or "-"
            url = h.get("url") or "-"
            snippet = h.get("snippet") or ""

            conf_s = f"{conf:.2f}" if isinstance(conf, (int, float)) else "-"
            lines += [
                f"[{i}] {t} {conf_s} | {title}",
                f"    {url}",
            ]
            if snippet:
                lines.append(f'    "{snippet}"')
            lines.append("")

        lines += ["-" * 60, ""]

    # Blog extractions (full)
    lines += ["=== BLOG EXTRACTIONS (only for blog hits) ===", ""]

    for be in blog_extractions:
        src = be.get("source_url") or "-"
        companies = be.get("companies") or []

        lines += [
            "URL:",
            src,
            "",
            f"EXTRACTED COMPANIES: {len(companies)}",
        ]

        for c in companies:
            name = c.get("name") or "-"
            website = c.get("website") or "-"
            evidence = c.get("evidence") or "-"
            lines += [
                f"- {name} | {website}",
                f'  Evidence: "{evidence}"',
            ]
        lines += ["", "-" * 60, ""]

    lines += [f"TIMESTAMP (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}", ""]

    with _lock, open(txt, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))
