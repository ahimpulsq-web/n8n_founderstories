from __future__ import annotations

"""
Link Discovery - URL-based link selection for Case 5 (depth-based discovery).

This module is used ONLY for Case 5 classification via infer_page_type().
Primary link discovery is handled by text_link_finder.py using anchor text matching.

Responsibilities:
- Provide infer_page_type() for Case 5 classification
- Score and rank URLs based on keywords (impressum, contact, privacy)
- Filter links by domain and file extensions

NOT used for:
- Primary link discovery (text_link_finder.py handles this)
- About/team page selection (excluded from Case 5 per spec)
"""
"""
═══════════════════════════════════════════════════════════════════════════════
LINK DISCOVERY - URL Pattern Matching for Contact Pages
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [CORE] - URL-based contact page discovery

PURPOSE:
    Provides URL pattern matching to identify contact/impressum pages when
    anchor text analysis fails. Used as fallback in Cases 5.1, 5.2, and 5.3.

KEY FUNCTIONS:
    - infer_page_type(url): Classify URL as home/contact/impressum/privacy/about
    - is_impressum_url(url): Check if URL matches impressum patterns
    - is_contact_url(url): Check if URL matches contact patterns
    - is_privacy_url(url): Check if URL matches privacy patterns
    - is_about_url(url): Check if URL matches about patterns

URL PATTERNS:
    Impressum: /impressum, /imprint, /legal-notice
    Contact: /contact, /kontakt, /get-in-touch
    Privacy: /privacy, /datenschutz, /data-protection
    About: /about, /ueber-uns, /about-us

USAGE:
    Used by service.py when anchor text discovery fails (Cases 5.x):
    
    page_type = infer_page_type("https://example.com/impressum")
    # Returns: "impressum"
    
    if is_impressum_url(url):
        # Process as impressum page

DEPENDENCIES:
    - None (pure utility functions)

═══════════════════════════════════════════════════════════════════════════════
"""

from dataclasses import dataclass
from typing import Iterable, List, Optional
from urllib.parse import urlparse


@dataclass(frozen=True)
class LinkDiscoveryConfig:
    """
    Configuration for Case 5 URL-based link selection.

    Case 5 keywords: impressum, imprint, contact, privacy, datenschutz
    Case 5 explicitly EXCLUDES: about, team, jobs, karriere, etc.
    """
    top_k: int = 6
    allow_paths: Optional[List[str]] = None
    deny_ext: tuple[str, ...] = (
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".svg",
        ".pdf",
        ".zip",
        ".css",
        ".js",
        ".webp",
        ".ico",
        ".mp4",
        ".mov",
    )


@dataclass(frozen=True)
class SelectedLink:
    url: str
    page_type: str
    score: int


# Case 5 keywords: impressum, contact, privacy ONLY
# Explicitly excludes: about, team, jobs, karriere, people, story, etc.
KEYWORDS: list[tuple[str, int]] = [
    ("impressum", 150),
    ("imprint", 145),
    ("datenschutz", 90),
    ("privacy", 75),
    ("privacy-policy", 85),
    ("kontakt", 70),
    ("contact", 60),
    ("contact-us", 65),
]

KEYWORD_BOUNDARIES = ("/", "-", "_", ".")


def infer_page_type(url: str) -> str:
    u = (url or "").lower()

    if any(k in u for k in ("impressum", "imprint")):
        return "impressum"
    if any(k in u for k in ("kontakt", "contact", "contact-us")):
        return "contact"
    if any(k in u for k in ("datenschutz", "privacy", "privacy-policy")):
        return "privacy"

    return "other"


def _is_same_site(url: str, base_domain: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False

    if host.startswith("www."):
        host = host[4:]

    bd = base_domain.lower()
    if bd.startswith("www."):
        bd = bd[4:]

    return bool(host) and (host == bd or host.endswith("." + bd))


def _has_any_keyword(url_lower: str) -> bool:
    for kw, _ in KEYWORDS:
        if kw in url_lower:
            for b in KEYWORD_BOUNDARIES:
                if f"{b}{kw}" in url_lower or f"{kw}{b}" in url_lower:
                    return True
    return False


def _score(url: str) -> int:
    u = (url or "").lower()
    s = 0
    has_strong_keyword = False

    for kw, pts in KEYWORDS:
        if kw in u:
            s += pts
            if pts >= 80:
                has_strong_keyword = True

    try:
        path = urlparse(u).path or ""
        s += max(0, 20 - len(path))

        if not has_strong_keyword:
            depth = path.count("/")
            s -= depth * 5
    except Exception:
        pass

    return s


def select_top_links(*, base_domain: str, links: Iterable[str], cfg: LinkDiscoveryConfig) -> list[SelectedLink]:
    allow_paths = cfg.allow_paths or []
    seen: set[str] = set()
    candidates: list[str] = []

    for u in links:
        if not u:
            continue
        u = str(u).strip()
        if not u or u in seen:
            continue
        seen.add(u)

        low = u.lower()

        if any(low.endswith(ext) for ext in cfg.deny_ext):
            continue

        if not _is_same_site(u, base_domain):
            continue

        if allow_paths:
            if not any(p.lower() in low for p in allow_paths):
                continue
        else:
            if not _has_any_keyword(low):
                continue

        candidates.append(u)

    scored = [(u, _score(u)) for u in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)

    out: list[SelectedLink] = []
    top_n = max(0, int(cfg.top_k))
    for u, score in scored[:top_n]:
        out.append(SelectedLink(url=u, page_type=infer_page_type(u), score=score))

    return out
