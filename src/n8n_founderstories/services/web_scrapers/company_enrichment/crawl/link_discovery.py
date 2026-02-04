# src/n8n_founderstories/services/web_scrapers/company_enrichment/crawl/link_discovery.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional
from urllib.parse import urlparse


@dataclass(frozen=True)
class LinkDiscoveryConfig:
    """
    Used only for Case 5 (depth-based discovery) PRIMARY selection.

    Per your spec, Case 5 must NOT select about/team links here.
    Case 5 is strictly: Impressum/Imprint, homepage, contact, privacy.
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


# IMPORTANT: Primary Case 5 must NOT include about/team keywords.
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
