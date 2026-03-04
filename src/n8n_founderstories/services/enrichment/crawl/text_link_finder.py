from __future__ import annotations

"""
Text Link Finder - Primary link discovery using anchor text matching.

This module is the PRIMARY mechanism for discovering contact/legal/about links.
It uses anchor text matching (e.g., "Impressum", "Contact", "Privacy") to find links.

Responsibilities:
- Extract links from Crawl4AI raw link objects
- Parse HTML <a> tags and match anchor text
- Handle <button onclick> patterns
- Handle <a onclick="function()"> with fallback mapping
- Provide secondary about-page selection by URL pattern

Used by:
- DomainCrawlerService for primary link discovery in all cases

NOT used for:
- Case 5 classification (link_discovery.infer_page_type handles this)
"""
"""
═══════════════════════════════════════════════════════════════════════════════
TEXT LINK FINDER - Anchor Text Analysis for Contact Pages
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [CORE] - Anchor text-based contact page discovery

PURPOSE:
    Provides intelligent anchor text analysis to discover contact/impressum pages
    from homepage links. Used in Cases 1, 2, and 3 (primary discovery method).

KEY FUNCTIONS:
    - discover_text_links(): Find contact/impressum links by anchor text
    - choose_best_url(): Select best URL from multiple candidates
    - extract_impressum_to_end(): Truncate impressum content intelligently
    - select_about_by_href(): Select about page from candidates

ANCHOR TEXT MATCHING:
    Impressum Keywords: "impressum", "imprint", "legal notice"
    Contact Keywords: "kontakt", "contact", "get in touch"
    Privacy Keywords: "datenschutz", "privacy", "data protection"
    About Keywords: "über uns", "about us", "about"

INTELLIGENT TRUNCATION:
    Impressum pages often contain full contact info at the top, followed by
    legal text. The truncation algorithm:
    1. Finds "Datenschutz" or similar markers
    2. Extracts content before the marker
    3. Returns truncated content if successful
    4. Falls back to full content if truncation fails

BEST URL SELECTION:
    When multiple URLs match, selects based on:
    1. Exact keyword match in anchor text
    2. URL path simplicity (shorter is better)
    3. Domain proximity (same domain preferred)

USAGE:
    Used by service.py for primary contact page discovery:
    
    links_raw = page.meta["crawl4ai_links_raw"]
    impressum_url = discover_text_links(links_raw, "impressum")
    contact_url = discover_text_links(links_raw, "contact")

DEPENDENCIES:
    - None (pure utility functions)

═══════════════════════════════════════════════════════════════════════════════
"""

import re
import html as html_lib
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

# -----------------------
# Regexes and constants
# -----------------------

A_TAG_RE = re.compile(
    r'<a\s+[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)

A_ONCLICK_RE = re.compile(
    r"""<a\b[^>]*\bonclick\s*=\s*(["'])(.*?)\1[^>]*>(.*?)</a>""",
    re.IGNORECASE | re.DOTALL,
)

BUTTON_ONCLICK_BLOCK_RE = re.compile(
    r"""<button\b[^>]*\bonclick\s*=\s*(["'])(.*?)\1[^>]*>(.*?)</button>""",
    re.IGNORECASE | re.DOTALL,
)

ONCLICK_FUNC_CALL_RE = re.compile(r"""^\s*([A-Za-z_$][A-Za-z0-9_$]*)\s*\(""", re.IGNORECASE)

JS_URL_IN_BODY_RE = re.compile(
    r"""
    (?:
        location(?:\.href)?\s*=\s*["'](?P<u1>[^"']+)["'] |
        window\.location\s*=\s*["'](?P<u2>[^"']+)["'] |
        document\.location\s*=\s*["'](?P<u3>[^"']+)["'] |
        window\.open\s*\(\s*["'](?P<u4>[^"']+)["'] |
        fetch\s*\(\s*["'](?P<u5>[^"']+)["'] |
        \.load\s*\(\s*["'](?P<u6>[^"']+)["'] |
        openModal\s*\(\s*["'](?P<u7>[^"']+)["']
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

QUOTED_STR_RE = re.compile(r"""['"]([^'"]{1,500})['"]""", re.IGNORECASE)

IMPRINT_START_RE = re.compile(r"^\s*(?:#{1,6}\s*)?(impressum|imprint)\s*$", re.IGNORECASE)

IMPRINT_TEXT_RE = re.compile(r"\b(impressum|imprint)\b", re.IGNORECASE)
CONTACT_TEXT_RE = re.compile(r"\b(kontakt|contact(\s*us)?|contact-us)\b", re.IGNORECASE)
PRIVACY_TEXT_RE = re.compile(
    r"\b(legal(\s*notice)?|datenschutz|datenschutzerkl[aä]rung|privacy(\s*policy)?)\b",
    re.IGNORECASE,
)
ABOUT_TEXT_RE = re.compile(r"\b(ueber\s*uns|über\s*uns|ueber|about(\s*us)?)\b", re.IGNORECASE)

DENY_EXT = (
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".pdf", ".zip", ".css", ".js",
    ".webp", ".ico", ".mp4", ".mov",
)

_DENY_ABOUT_FRAGS = (
    "team", "jobs", "karriere", "career", "people", "story",
    "geschichte", "mission", "werte", "philosophie",
)

_HIDDEN_CHARS_RE = re.compile(r"[\u00ad\u200b\u200c\u200d\u2060]")


@dataclass(frozen=True)
class TextDiscoveryConfig:
    """
    Controls which text-link categories are extracted from the homepage.
    """
    include_about: bool = True


# -----------------------
# Text / URL utilities
# -----------------------

def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").strip()


def normalize_visible_text(s: str) -> str:
    """
    Convert inner HTML to a normalized, human-visible string:
    - strip tags
    - remove hidden/invisible chars
    - collapse whitespace
    """
    s = strip_html(s or "")
    s = _HIDDEN_CHARS_RE.sub("", s)
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def host_no_www(host: str) -> str:
    h = (host or "").lower()
    return h[4:] if h.startswith("www.") else h


def is_same_site(url: str, base_host: str) -> bool:
    try:
        host = host_no_www(urlparse(url).hostname or "")
    except Exception:
        return False
    bh = host_no_www(base_host)
    return bool(host) and (host == bh or host.endswith("." + bh))


def _is_http_like_or_relative(u: str) -> bool:
    """
    Allow http(s) and relative URLs. Deny mailto/tel/javascript/data and pure fragments.
    """
    if not u:
        return False
    low = u.strip().lower()
    if low.startswith(("mailto:", "tel:", "javascript:", "data:")):
        return False
    if low.startswith("#"):
        return False
    return True


def _admissible_http_url(u: str) -> bool:
    """
    Only allow absolute http(s) URLs and exclude obvious non-page assets.
    """
    if not u:
        return False
    low = u.lower().strip()
    if not low.startswith(("http://", "https://")):
        return False
    if any(low.endswith(ext) for ext in DENY_EXT):
        return False
    return True


def _score_preference(u: str, base_host: str) -> int:
    """
    Ranking heuristic to prefer:
    - same-site URLs
    - real pages (path/query) over homepage-only
    - non-fragment URLs
    - https (minor)
    """
    p = urlparse(u)
    s = 0
    if is_same_site(u, base_host):
        s += 100
    if p.fragment:
        s -= 10
    if (p.path and p.path != "/") or p.query:
        s += 10
    if p.scheme == "https":
        s += 1
    return s


def choose_best_url(candidates: List[str], base_host: str) -> Optional[str]:
    """
    Choose a single best URL from candidates using admissibility + preference scoring.
    """
    admissible = [u for u in candidates if _admissible_http_url(u)]
    if not admissible:
        return None
    same_site = [u for u in admissible if is_same_site(u, base_host)]
    pool = same_site if same_site else admissible
    pool.sort(key=lambda u: _score_preference(u, base_host), reverse=True)
    return pool[0] if pool else None


def extract_impressum_to_end(md: str) -> str:
    """
    [CORE] LLM-optimized Impressum extraction.

    - If an Impressum/Imprint header exists:
        return content starting from that header.
    - Otherwise:
        return empty string.
    """
    if not md:
        return ""

    match = re.search(r"(?im)^\s*(#{1,6}\s*)?(impressum|imprint)\s*$", md)
    if not match:
        return ""

    return md[match.start():].strip()



def _looks_like_page_urlish(s: str) -> bool:
    """
    Avoid pulling image/CDN assets when searching for contact/legal links.
    Accepts:
    - absolute http(s)
    - relative paths
    - common page extensions
    """
    if not s:
        return False
    t = s.strip()
    low = t.lower()

    if " " in t:
        return False
    if low.startswith(("mailto:", "tel:", "javascript:", "data:", "#")):
        return False
    if any(low.endswith(ext) for ext in DENY_EXT):
        return False

    if low.startswith(("http://", "https://", "/")):
        return True
    if low.endswith((".php", ".html", ".htm", ".aspx")):
        return True
    if "/" in low or "?" in low:
        return True

    return False


def _url_kind_match(name: str, url: str) -> bool:
    u = (url or "").lower()
    if name == "impressum":
        return ("impressum" in u) or ("imprint" in u)
    if name == "privacy":
        return ("datenschutz" in u) or ("privacy" in u)
    if name == "contact":
        return ("kontakt" in u) or ("contact" in u)
    if name == "about":
        return ("about" in u) or ("ueber" in u) or ("über" in u) or ("uber" in u)
    return False


# -----------------------
# Candidate finders
# -----------------------

def find_candidates_from_crawl4ai_links_text_only(
    links_raw: object,
    base_url: str,
    text_re: re.Pattern,
) -> List[str]:
    """
    Extract candidates from Crawl4AI raw link objects using anchor TEXT match.
    """
    out: List[str] = []
    seen: set[str] = set()
    if not isinstance(links_raw, dict):
        return out

    for bucket in ("internal", "external"):
        items = links_raw.get(bucket) or []
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            href = (item.get("href") or "").strip()
            text = (item.get("text") or "").strip()
            if not href or not text:
                continue
            if not text_re.search(text):
                continue
            if not _is_http_like_or_relative(href):
                continue

            full = urljoin(base_url, href)
            if full in seen:
                continue
            seen.add(full)
            out.append(full)

    return out


def find_candidates_from_html_anchor_text_only(html: str, base_url: str, text_re: re.Pattern) -> List[str]:
    """
    Extract <a href="...">TEXT</a> where visible TEXT matches the given regex.
    """
    out: List[str] = []
    seen: set[str] = set()

    for href, anchor_html in A_TAG_RE.findall(html or ""):
        anchor_text = normalize_visible_text(anchor_html)
        if not anchor_text:
            continue
        if not text_re.search(anchor_text):
            continue
        if not _is_http_like_or_relative(href):
            continue

        full = urljoin(base_url, href)
        if full in seen:
            continue
        seen.add(full)
        out.append(full)

    return out


def find_candidates_from_html_buttons_text_and_onclick(html: str, base_url: str, text_re: re.Pattern) -> List[str]:
    """
    Extract candidates from <button onclick="location.href='...'>TEXT</button>.
    """
    out: List[str] = []
    seen: set[str] = set()

    for _, onclick_js, inner_html in BUTTON_ONCLICK_BLOCK_RE.findall(html or ""):
        visible = normalize_visible_text(inner_html)
        if not visible:
            continue
        if not text_re.search(visible):
            continue

        onclick = (onclick_js or "").strip()
        for m in re.finditer(
            r"""(?:location(?:\.href)?|window\.location|document\.location)\s*=\s*['"]([^'"]+)['"]""",
            onclick,
            re.I,
        ):
            raw_u = (m.group(1) or "").strip()
            if not raw_u or not _is_http_like_or_relative(raw_u):
                continue
            full = urljoin(base_url, raw_u)
            if full in seen:
                continue
            seen.add(full)
            out.append(full)

    return out


def _onclick_function_fallback_urls(fname: str, base_url: str, name: str) -> List[str]:
    """
    Deterministic fallback mapping when we can see onclick function names
    but the JS bodies are not present in the HTML (external JS).
    """
    fname_low = (fname or "").lower()
    out: List[str] = []

    def add(path: str) -> None:
        if path:
            out.append(urljoin(base_url, path))

    if name == "impressum" and "impressum" in fname_low:
        add("/impressum")
        add("/impressum.php")
        return out

    if name == "privacy" and ("datenschutz" in fname_low or "privacy" in fname_low):
        add("/datenschutz")
        add("/datenschutz.php")
        add("/privacy")
        add("/privacy-policy")
        return out

    if name == "impressum" and fname_low in ("openimpressum",):
        add("/impressum")
        add("/impressum.php")
        return out

    if name == "privacy" and fname_low in ("opendatenschutz", "openprivacy", "openprivacypolicy"):
        add("/datenschutz")
        add("/datenschutz.php")
        add("/privacy")
        add("/privacy-policy")
        return out

    return out


def find_candidates_from_anchor_onclick_function(
    html: str,
    base_url: str,
    text_re: re.Pattern,
    *,
    name: str,
) -> List[str]:
    """
    Handles: <a onclick="openImpressum()">Impressum</a>

    Order:
      1) Try to extract URLs from inline JS body (if present in page HTML)
      2) If not available, use deterministic fallback mapping for legal pages
    """
    out: List[str] = []
    seen: set[str] = set()

    h = html_lib.unescape(html or "")

    for _, onclick_js, inner_html in A_ONCLICK_RE.findall(h):
        visible = normalize_visible_text(inner_html)
        if not visible:
            continue
        if not text_re.search(visible):
            continue

        m = ONCLICK_FUNC_CALL_RE.match((onclick_js or "").strip())
        if not m:
            continue

        fname = m.group(1)

        # Try extracting inline function bodies (rare)
        body: Optional[str] = None
        for tpl in (
            rf"""function\s+{re.escape(fname)}\s*\([^)]*\)\s*\{{(?P<body>.*?)\}}""",
            rf"""{re.escape(fname)}\s*=\s*function\s*\([^)]*\)\s*\{{(?P<body>.*?)\}}""",
            rf"""(?:var|let|const)\s+{re.escape(fname)}\s*=\s*function\s*\([^)]*\)\s*\{{(?P<body>.*?)\}}""",
            rf"""(?:var|let|const)\s+{re.escape(fname)}\s*=\s*\([^)]*\)\s*=>\s*\{{(?P<body>.*?)\}}""",
            rf"""window\.{re.escape(fname)}\s*=\s*function\s*\([^)]*\)\s*\{{(?P<body>.*?)\}}""",
        ):
            rx = re.compile(tpl, re.IGNORECASE | re.DOTALL)
            mm = rx.search(h)
            if mm:
                body = mm.group("body") or ""
                break

        if body:
            um = JS_URL_IN_BODY_RE.search(body)
            if um:
                raw_u = (
                    (um.group("u1") or "")
                    or (um.group("u2") or "")
                    or (um.group("u3") or "")
                    or (um.group("u4") or "")
                    or (um.group("u5") or "")
                    or (um.group("u6") or "")
                    or (um.group("u7") or "")
                ).strip()
                if raw_u and _looks_like_page_urlish(raw_u):
                    full = urljoin(base_url, raw_u)
                    if full not in seen:
                        seen.add(full)
                        out.append(full)

            if out:
                continue

            # Fallback inside body: any quoted string that looks like correct kind
            for qm in QUOTED_STR_RE.finditer(body):
                cand = (qm.group(1) or "").strip()
                if not _looks_like_page_urlish(cand):
                    continue
                full = urljoin(base_url, cand)
                if not _url_kind_match(name, full):
                    continue
                if full not in seen:
                    seen.add(full)
                    out.append(full)

            if out:
                continue

        # If we get here, function body likely external => deterministic mapping fallback
        for u in _onclick_function_fallback_urls(fname, base_url, name):
            if u and u not in seen:
                seen.add(u)
                out.append(u)

    return out


# -----------------------
# Public API
# -----------------------

def discover_text_links(
    *,
    base_url: str,
    base_host: str,
    cleaned_html: str,
    raw_html: str,
    crawl4ai_links_raw: object,
    cfg: TextDiscoveryConfig,
) -> Dict[str, List[str]]:
    """
    Discover legal/contact/about links using anchor TEXT matching.

    Strategy per category:
      1) Crawl4AI raw links (href + text) if available
      2) HTML <a> anchor text scan
      3) HTML <button onclick=...> scan
      4) <a onclick="function()"> with deterministic fallback mapping
    """
    home_html = raw_html or cleaned_html or ""

    def gather(name: str, text_re: re.Pattern) -> List[str]:
        c1 = find_candidates_from_crawl4ai_links_text_only(crawl4ai_links_raw, base_url, text_re)
        if c1:
            return c1

        c2 = find_candidates_from_html_anchor_text_only(home_html, base_url, text_re)
        if c2:
            return c2

        c3 = find_candidates_from_html_buttons_text_and_onclick(home_html, base_url, text_re)
        if c3:
            return c3

        return find_candidates_from_anchor_onclick_function(home_html, base_url, text_re, name=name)

    out: Dict[str, List[str]] = {
        "impressum": gather("impressum", IMPRINT_TEXT_RE),
        "contact": gather("contact", CONTACT_TEXT_RE),
        "privacy": gather("privacy", PRIVACY_TEXT_RE),
        "about": gather("about", ABOUT_TEXT_RE) if cfg.include_about else [],
    }

    # Filter to admissible + preference-rank (this is the "returned order")
    for k, urls in list(out.items()):
        filtered = [u for u in urls if _admissible_http_url(u)]
        filtered.sort(key=lambda u: _score_preference(u, base_host), reverse=True)
        out[k] = filtered

    return out


def select_about_by_href(links: List[str], *, base_host: str) -> Optional[str]:
    """
    Secondary about-page selector based on URL patterns (used when no about anchor text is found).
    """
    if not links:
        return None

    about_frags = ("ueber-uns", "uber-uns", "über-uns", "about-us", "about", "/ueber")
    for u in links:
        if not u:
            continue
        if not _admissible_http_url(u):
            continue
        if not is_same_site(u, base_host):
            continue
        low = u.lower()
        if any(df in low for df in _DENY_ABOUT_FRAGS):
            continue
        if any(af in low for af in about_frags):
            return u

    return None
