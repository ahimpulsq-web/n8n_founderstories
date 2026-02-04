# src/n8n_founderstories/services/web_scrapers/company_enrichment/extract/deterministic/extract.py
from __future__ import annotations

import time
from typing import Dict, List, Tuple

from .run_log import append_domain_result
from ...models import DeterministicEmail, DeterministicExtraction, PageArtifact
from .emails import extract_emails_from_text

# =====================================================
# GLOBALS
# =====================================================

MAX_EMAILS = 5

_DENY_LOCALPART_PREFIXES = {
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
    "mailer-daemon",
    "postmaster",
    "bounce",
    "bounces",
}
_DENY_DOMAIN_SUFFIXES = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".webp",
    ".ico",
    ".css",
    ".js",
    ".zip",
    ".pdf",
    ".mp4",
    ".mov",
}

_LOCALPART_PRIORITY = {
    "info": 0,
    "hallo": 1,
    "kontakt": 2,
    "contact": 2,
}


def _normalize_domain(d: str) -> str:
    s = (d or "").strip().lower()
    s = s.replace("https://", "").replace("http://", "")
    s = s.split("/", 1)[0]
    return s.lstrip("www.").rstrip(".")


def _canon_page_url(p: PageArtifact) -> str:
    """Prefer final_url when present (redirect-safe), else url."""
    u = str(getattr(p, "final_url", None) or getattr(p, "url", "") or "")
    return u.strip()


def _is_likely_asset_url(u: str) -> bool:
    low = (u or "").lower().strip()
    if not low:
        return False
    return any(low.endswith(sfx) for sfx in _DENY_DOMAIN_SUFFIXES)


def _pt(p: PageArtifact) -> str:
    try:
        return (p.meta.get("page_type") or "").strip().lower()
    except Exception:
        return ""


def _prio_email_page(p: PageArtifact) -> int:
    pt = _pt(p)
    if pt == "impressum":
        return 100
    if pt == "contact":
        return 90
    if pt == "privacy":
        return 60
    if pt == "home":
        return 40
    if pt in ("about", "team"):
        return 20
    return 10


def _clean_email(raw: str) -> str:
    if not raw:
        return ""
    e = raw.strip().lower()
    return e.strip(" \t\r\n<>\"'()[]{}.,;:")


def _is_valid_candidate_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    if email.count("@") != 1:
        return False

    local, domain = email.split("@", 1)
    if not local or not domain:
        return False

    if local in _DENY_LOCALPART_PREFIXES:
        return False

    dom_low = domain.lower().strip().rstrip(".")
    if any(dom_low.endswith(sfx) for sfx in _DENY_DOMAIN_SUFFIXES):
        return False

    return True


def _prefer_best_source_by_prio(
    current: Tuple[int, str],
    candidate: Tuple[int, str],
) -> Tuple[int, str]:
    cur_prio, cur_url = current
    cand_prio, cand_url = candidate

    if not cand_url:
        return current
    if not cur_url:
        return candidate

    return candidate if cand_prio > cur_prio else current


def _email_domain(email: str) -> str:
    try:
        return (email.split("@", 1)[1] or "").strip().lower().rstrip(".")
    except Exception:
        return ""


def _email_localpart(email: str) -> str:
    try:
        return (email.split("@", 1)[0] or "").strip().lower()
    except Exception:
        return ""


def _domain_match(company_domain_norm: str, email: str) -> int:
    ed = _normalize_domain(_email_domain(email))
    cd = company_domain_norm

    if not cd or not ed:
        return 0
    return 1 if (ed == cd or ed.endswith("." + cd)) else 0


def extract(domain: str, pages: List[PageArtifact]) -> DeterministicExtraction:
    """
    Deterministic extraction (EMAILS ONLY).

    Logging:
      - Writes deterministic run logs (JSONL + TXT)
      - Includes pages_used and pages_scanned (ordered)
    """
    t0 = time.perf_counter()
    domain_norm = _normalize_domain(domain)

    # Capture page URLs in the exact scan order for logging/debugging.
    pages_scanned: List[str] = []
    for p in pages or []:
        u = _canon_page_url(p)
        if u:
            pages_scanned.append(u)

    scan: List[Tuple[int, str, str]] = []
    for p in pages or []:
        url = _canon_page_url(p)
        if _is_likely_asset_url(url):
            url = ""

        text = (getattr(p, "cleaned_html", "") or "").strip()
        if text:
            scan.append((_prio_email_page(p), url, text))

    emails_by_value: Dict[str, Tuple[int, DeterministicEmail]] = {}

    for prio, url, html in scan:
        try:
            candidates = extract_emails_from_text(html)
        except Exception:
            continue

        for raw in candidates:
            e = _clean_email(raw)
            if not _is_valid_candidate_email(e):
                continue

            try:
                cur = emails_by_value.get(e)
                if cur is None:
                    emails_by_value[e] = (
                        prio,
                        DeterministicEmail(email=e, source_url=url or ""),
                    )
                else:
                    cur_prio, cur_obj = cur
                    best_prio, best_url = _prefer_best_source_by_prio(
                        (cur_prio, str(cur_obj.source_url or "")),
                        (prio, url or ""),
                    )
                    if best_prio != cur_prio or best_url != str(cur_obj.source_url or ""):
                        emails_by_value[e] = (
                            best_prio,
                            DeterministicEmail(email=e, source_url=best_url),
                        )
            except Exception:
                continue

    def _sort_key(item: Tuple[int, DeterministicEmail]):
        prio, obj = item
        email = str(obj.email or "").lower()

        match = _domain_match(domain_norm, email)  # 0/1
        lp_rank = _LOCALPART_PRIORITY.get(_email_localpart(email), 99)

        return (-prio, -match, lp_rank)

    emails_out = [obj for _, obj in sorted(emails_by_value.values(), key=_sort_key)]

    if MAX_EMAILS and MAX_EMAILS > 0:
        emails_out = emails_out[:MAX_EMAILS]

    # Deterministic stage logging (JSONL + TXT)
    append_domain_result(
        domain=domain_norm,
        emails=emails_out,
        pages_used=len(pages or []),
        pages_scanned=pages_scanned,
        reason="ok",
    )

    _ = int((time.perf_counter() - t0) * 1000)  # elapsed_ms available if needed later

    return DeterministicExtraction(
        emails=emails_out,
        pages_used=len(pages or []),
        reason="ok",
    )
