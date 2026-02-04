from __future__ import annotations

from typing import List
from urllib.parse import urlparse

from ..models import CrawlArtifacts, DeterministicExtraction, LLMExtraction
from .models import EmailOccurrence

_CATEGORY_PRIO = {
    "impressum": 100,
    "home": 90,
    "contact": 80,
    "legal": 50,
    "unknown": 0,
}


def _normalize_domain(domain: str) -> str:
    d = (domain or "").lower().strip()
    d = d.replace("http://", "").replace("https://", "")
    return d.lstrip("www.").rstrip(".")


def _email_domain(email: str) -> str:
    try:
        return email.split("@", 1)[1].lower().rstrip(".")
    except Exception:
        return ""


def _host(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower().lstrip("www.")
    except Exception:
        return ""


def _domain_score(*, input_domain: str, email: str, source_url: str) -> int:
    score = 0
    ed = _email_domain(email)

    if ed and ed == input_domain:
        score += 1

    src = _host(source_url)
    if ed and src and ed == src:
        score += 1

    return score


def _page_category_from_url(crawl: CrawlArtifacts, url: str) -> str:
    if not url:
        return "unknown"

    pages = (crawl.pages or []) + ([crawl.homepage] if crawl.homepage else [])
    for p in pages:
        if not p:
            continue
        if str(p.url) == url or (p.final_url and str(p.final_url) == url):
            pt = (p.meta.get("page_type") or "").strip().lower()
            if pt == "homepage":
                return "home"
            if pt == "privacy":
                return "legal"
            if pt in ("impressum", "home", "contact", "legal"):
                return pt
            return pt or "unknown"

    return "unknown"


def normalize_email_occurrences(
    *,
    domain: str,
    crawl: CrawlArtifacts,
    deterministic: DeterministicExtraction,
    llm: LLMExtraction,
) -> List[EmailOccurrence]:

    company_domain = _normalize_domain(domain)
    occurrences: List[EmailOccurrence] = []
    order = 0

    for e in deterministic.emails:
        email = str(e.email).strip().lower()
        if not email:
            continue

        url = str(e.source_url) if e.source_url else ""
        category = _page_category_from_url(crawl, url)

        occurrences.append(
            EmailOccurrence(
                email=email,
                source="deterministic",
                source_url=url,
                page_category=category,
                domain_score=_domain_score(
                    input_domain=company_domain,
                    email=email,
                    source_url=url,
                ),
                order_index=order,
            )
        )
        order += 1

    for e in (llm.emails or []):
        email = str(e.email).strip().lower()
        if not email:
            continue

        url = str(e.evidence.url) if getattr(e, "evidence", None) else ""
        category = _page_category_from_url(crawl, url)

        occurrences.append(
            EmailOccurrence(
                email=email,
                source="llm",
                source_url=url,
                page_category=category,
                domain_score=_domain_score(
                    input_domain=company_domain,
                    email=email,
                    source_url=url,
                ),
                order_index=order,
            )
        )
        order += 1

    occurrences.sort(
        key=lambda o: (
            -o.domain_score,
            -_CATEGORY_PRIO.get(o.page_category, 0),
            o.order_index,
        )
    )

    return occurrences
