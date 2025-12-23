from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from n8n_founderstories.core.utils.email import extract_emails, pick_best_email
from n8n_founderstories.core.utils.net import FetchConfig, fetch_text

_DEFAULT_PATHS = [
    "/", "/contact", "/contact-us", "/about", "/about-us",
    "/impressum", "/legal", "/privacy", "/terms",
]


@dataclass(frozen=True)
class ExtractionConfig:
    fetch: FetchConfig = FetchConfig()


def _candidate_urls(domain: str) -> list[str]:
    out: list[str] = []
    for scheme in ("https", "http"):
        for p in _DEFAULT_PATHS:
            out.append(f"{scheme}://{domain}{p}")
    return out


def extract_emails_for_domain(domain: str, *, cfg: Optional[ExtractionConfig] = None) -> tuple[list[str], Optional[str]]:
    """
    Fetches a bounded set of pages and extracts emails.
    Returns (unique_emails, first_source_url_where_found).
    """
    cfg = cfg or ExtractionConfig()

    collected: list[str] = []
    source_url: Optional[str] = None

    for url in _candidate_urls(domain):
        text = fetch_text(url, cfg.fetch)
        if not text:
            continue

        emails = extract_emails(text)
        if emails and source_url is None:
            source_url = url
        collected.extend(emails)

    # de-dupe preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for e in collected:
        if e not in seen:
            seen.add(e)
            unique.append(e)

    return unique, source_url


def choose_best_email(domain: str, emails: list[str]) -> Optional[str]:
    return pick_best_email(emails, prefer_domain=domain).best
