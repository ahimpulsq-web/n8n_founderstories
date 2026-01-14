from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urljoin, urlparse

from n8n_founderstories.core.utils.async_net import AsyncFetchConfig, AsyncFetcher, FetchResult
from n8n_founderstories.core.utils.email import extract_emails, pick_best_email

from .company_desc_extractor import extract_company_description
from .imprint_extractor import extract_contact_name


# Priority pages to probe first (keep bounded; max_pages_per_domain protects budget)
_PRIORITY_PATH_GROUPS: list[list[str]] = [
    ["/"],
    [
        # Standard impressum/imprint paths
        "/impressum", "/impressum/", "/impressum.html", "/impressum.php",
        "/imprint", "/imprint/", "/imprint.html", "/imprint.php",
        "/legal-notice", "/legal", "/legal/",
        # Common nested paths for impressum/imprint
        "/informationen/service/impressum/", "/informationen/service/imprint/",
        "/informationen/impressum/", "/informationen/imprint/",
        "/service/impressum/", "/service/imprint/",
        "/pages/impressum/", "/pages/imprint/",
        "/legal/impressum/", "/legal/imprint/",
        "/about/impressum/", "/about/imprint/",
        # German business-specific paths
        "/rechtliches/impressum/", "/rechtliches/imprint/", "/rechtliches/",
        "/rechtliche-hinweise", "/rechtliche-hinweise/",
        "/impressum-datenschutz", "/impressum-datenschutz/",
        "/rechtliches/impressum", "/rechtliches/imprint",
    ],
    [
        "/kontakt", "/kontakt/", "/kontakt.html", "/kontakt.php",
        "/contact", "/contact/", "/contact-us", "/contact-us/",
        "/pages/contact", "/pages/kontakt",
    ],
    [
        "/privacy", "/privacy-policy", "/privacy-policy/",
        "/datenschutz", "/datenschutzerklaerung", "/datenschutzerklärung",
        "/datenschutzerklaerung/", "/datenschutz/",
        "/terms", "/agb",
    ],
    [
        "/about", "/about-us", "/team",
        "/unternehmen", "/ueber-uns", "/über-uns",
        "/support", "/help", "/service",
        "/management", "/leadership", "/board",
        "/verband", "/association", "/people", "/vorstand",
    ],
]

# Robust href extraction: supports quoted and unquoted href
_HREF_RE = re.compile(
    r"""href\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s"'<>]+))""",
    re.IGNORECASE,
)

# sitemap/robots parsing
_SITEMAP_LOC_RE = re.compile(r"(?im)^\s*sitemap:\s*(\S+)\s*$")
_SITEMAP_URL_RE = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>", re.IGNORECASE)

# Common junk / assets
_ASSET_EXT = (
    ".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg", ".ico",
    ".css", ".js", ".map", ".woff", ".woff2", ".ttf", ".eot",
    ".pdf", ".zip",
)

# Link scoring: broadened keywords for better discovery
# Higher scores for impressum/imprint since names are typically found there
_LINK_KEYWORDS: list[tuple[str, int]] = [
    ("impressum", 150),  # Increased priority for name extraction
    ("imprint", 145),   # Increased priority for name extraction
    ("legal-notice", 95),
    ("legal", 80),
    ("datenschutz", 90),
    ("privacy-policy", 85),
    ("privacy", 75),
    ("kontakt", 70),
    ("contact-us", 65),
    ("contact", 60),
    ("pages/contact", 60),
    ("pages/kontakt", 60),
    ("pages/impressum", 150),  # Nested impressum paths
    ("pages/imprint", 145),   # Nested imprint paths
    ("informationen/service/impressum", 150),  # Common nested path
    ("informationen/service/imprint", 145),
    ("service/impressum", 150),
    ("service/imprint", 145),
    ("rechtliches/impressum", 150),  # German "legal" impressum paths
    ("rechtliches/imprint", 145),
    ("rechtliches", 90),  # German legal pages
    ("rechtliche-hinweise", 90),
    ("support", 45),
    ("help", 35),
    ("service", 30),
    ("about", 20),
    ("team", 15),
    ("company", 10),
    ("verband", 25),
    ("association", 25),
    ("management", 25),
    ("leadership", 25),
    ("vorstand", 25),
    ("board", 25),
]


@dataclass(frozen=True)
class AsyncExtractionConfig:
    fetch: AsyncFetchConfig = AsyncFetchConfig()

    # Total pages fetched per domain
    max_pages_per_domain: int = 14

    # backward-compat (runner still passes this)
    stop_on_best_match: bool = False

    # discovery
    discover_links: bool = True
    max_discovered_links: int = 10
    discover_from_groups: int = 3

    # sitemap discovery
    discover_sitemaps: bool = True
    max_sitemap_urls: int = 80
    max_sitemap_fetches: int = 2

    # loop / waste protection
    stop_on_soft404_loop: bool = True
    soft404_repeat_cap: int = 3

    # status classification
    blocked_statuses: tuple[int, ...] = (403, 406, 423)
    unavailable_statuses: tuple[int, ...] = (503,)



def _scheme_urls(domain: str, paths: list[str], scheme: str) -> list[str]:
    return [f"{scheme}://{domain}{p}" for p in paths]


def _dedupe_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _host_matches(host: str, base_host: str) -> bool:
    host = (host or "").lower()
    base = (base_host or "").lower()
    host = host[4:] if host.startswith("www.") else host
    base = base[4:] if base.startswith("www.") else base
    return host == base or host.endswith("." + base)


def _is_same_site(url: str, base_host: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    if not host:
        return False
    return _host_matches(host, base_host)


def _clean_url(u: str) -> str:
    # remove fragments; drop query to avoid “catalogsearch/result” spam
    try:
        p = urlparse(u)
        if not p.scheme or not p.netloc:
            return u
        return p._replace(fragment="", query="").geturl()
    except Exception:
        return u


def _score_link(u: str) -> int:
    lu = (u or "").lower()
    s = 0
    for k, pts in _LINK_KEYWORDS:
        if k in lu:
            s += pts
    try:
        path = urlparse(lu).path or ""
        # prefer shorter paths a bit
        s += max(0, 20 - len(path))
    except Exception:
        pass
    return s


def _page_priority_score(url: str) -> int:
    u = (url or "").lower()
    if "impressum" in u or "imprint" in u:
        return 100
    if "datenschutz" in u or "privacy" in u or "legal" in u:
        return 80
    if "kontakt" in u or "contact" in u:
        return 60
    if "support" in u or "help" in u:
        return 40
    if "about" in u or "team" in u:
        return 20
    if "verband" in u or "association" in u or "management" in u or "vorstand" in u:
        return 20
    if urlparse(u).path in {"", "/"}:
        return 5
    return 0


def _extract_internal_candidates(html: str, base_url: str, base_host: str) -> list[str]:
    if not html:
        return []

    found: list[str] = []
    for m in _HREF_RE.finditer(html):
        href = (m.group(1) or m.group(2) or m.group(3) or "").strip()
        if not href:
            continue

        hlow = href.lower()
        if hlow.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue

        abs_u = urljoin(base_url, href)
        abs_u = _clean_url(abs_u)

        try:
            p = urlparse(abs_u)
            scheme = (p.scheme or "").lower()
            path = (p.path or "").lower()
        except Exception:
            continue

        if scheme not in {"http", "https"}:
            continue
        if not _is_same_site(abs_u, base_host):
            continue

        # skip assets and obvious junk
        if path.endswith(_ASSET_EXT):
            continue
        if any(seg in path for seg in ("/wp-content/", "/assets/", "/static/", "/cdn-cgi/")):
            continue

        found.append(abs_u)

    # No “score threshold” here; keep a pool and rank later
    return _dedupe_preserve(found)


def _choose_best_from_pairs(domain: str, pairs: list[tuple[str, str]]) -> Optional[str]:
    if not pairs:
        return None
    emails = _dedupe_preserve([e for e, _ in pairs])
    base_pick = pick_best_email(emails, prefer_domain=domain).best

    page_best: dict[str, int] = {}
    for e, src in pairs:
        page_best[e] = max(page_best.get(e, 0), _page_priority_score(src))

    def key(e: str) -> tuple[int, int]:
        return (page_best.get(e, 0), 1 if e == base_pick else 0)

    ranked = sorted(emails, key=key, reverse=True)
    return ranked[0] if ranked else base_pick

def format_emails_best_first(*, domain: str, pairs: list[tuple[str, str]]) -> str:
    """
    Format emails with their source URLs for storage and display.
    
    Output format: (email: url),(email2: url2)
    - Each email:url pair is wrapped in parentheses
    - Space after colon
    - Comma separator without space
    
    Args:
        domain: Domain to prefer when ranking emails
        pairs: List of (email, source_url) tuples
        
    Returns:
        Formatted string like "(email1: url1),(email2: url2)"
    """
    if not pairs:
        return ""

    # dedupe email list deterministically
    emails = _dedupe_preserve([ (e or "").strip() for e, _ in pairs if (e or "").strip() ])
    base_pick = pick_best_email(emails, prefer_domain=domain).best if emails else None

    # best page score per email + choose best url per email
    best_score: dict[str, int] = {}
    best_url: dict[str, str] = {}
    for e, u in pairs:
        e = (e or "").strip()
        u = (u or "").strip()
        if not e:
            continue
        s = _page_priority_score(u)
        prev = best_score.get(e, -1)
        # higher score wins; tie-breaker url asc for determinism
        if s > prev or (s == prev and u and u < (best_url.get(e) or "\uffff")):
            best_score[e] = s
            best_url[e] = u

    def sort_key(e: str) -> tuple[int, int, str]:
        return (
            best_score.get(e, 0),              # page score desc
            1 if (base_pick and e == base_pick) else 0,  # base pick desc
            e.lower(),                         # tie-breaker
        )

    ranked = sorted(emails, key=sort_key, reverse=True)
    # Format as (email: url),(email2: url2) - parentheses, space after colon, no space after comma
    return ",".join([f"({e}: {best_url.get(e, '')})" for e in ranked])



def _norm_visit_key(u: str) -> str:
    """Deduplicate by host+path (ignore query/fragment)."""
    try:
        p = urlparse(u)
        host = (p.hostname or "").lower()
        path = p.path or "/"
        return f"{host}{path}"
    except Exception:
        return u


def _looks_like_impressum_html(html: str) -> bool:
    """Check if HTML content looks like an Impressum page."""
    t = (html or "").lower()
    markers = [
        "impressum",
        "anbieterkennung",
        "gesetzliche anbieterkennung",
        "vertreten durch",
        "vertretungsberechtigt",
        "ust-id", "ustid", "ust-",
        "amtsgericht",
        "handelsregister",
        "geschäftsführer", "geschäftsleitung",
        "inhaber",
        "mstv", "§ 18", "inhaltlich verantwortlich",
    ]
    # Require >=2 signals to reduce false positives
    return sum(1 for m in markers if m in t) >= 2


async def _discover_from_sitemaps(
    *,
    domain: str,
    scheme: str,
    fetcher: AsyncFetcher,
    cfg: AsyncExtractionConfig,
) -> list[str]:
    """
    Safe sitemap discovery: robots.txt + sitemap.xml variants.
    Returns internal URLs (same host family) ranked later by caller.
    """
    if not cfg.discover_sitemaps:
        return []

    base = f"{scheme}://{domain}"
    candidates: list[str] = []

    # 1) robots.txt
    robots_url = f"{base}/robots.txt"
    r = await fetcher.fetch(robots_url)
    if r.text and r.status_code and r.status_code < 400:
        for m in _SITEMAP_LOC_RE.finditer(r.text):
            candidates.append(m.group(1).strip())

    # 2) common sitemap locations (bounded)
    if len(candidates) < cfg.max_sitemap_fetches:
        for sp in ("/sitemap.xml", "/sitemap_index.xml"):
            candidates.append(f"{base}{sp}")

    # de-dupe and cap
    candidates = _dedupe_preserve(candidates)[: cfg.max_sitemap_fetches]

    urls: list[str] = []
    fetched = 0
    for sm_url in candidates:
        if fetched >= cfg.max_sitemap_fetches:
            break
        sm = await fetcher.fetch(sm_url)
        fetched += 1
        if not sm.text or not sm.status_code or sm.status_code >= 400:
            continue
        for m in _SITEMAP_URL_RE.finditer(sm.text):
            u = (m.group(1) or "").strip()
            if not u:
                continue
            u = _clean_url(u)
            urls.append(u)
            if len(urls) >= cfg.max_sitemap_urls:
                break
        if len(urls) >= cfg.max_sitemap_urls:
            break

    # keep only same-site URLs
    urls = [u for u in _dedupe_preserve(urls) if _is_same_site(u, domain)]
    return urls


async def extract_emails_for_domain_async(
    domain: str,
    *,
    fetcher: AsyncFetcher,
    cfg: Optional[AsyncExtractionConfig] = None,
) -> tuple[list[str], Optional[str], Optional[str], list[tuple[str, str]], Optional[str], Optional[str], str, str]:
    """
    Returns:
      (emails_unique, best_email, best_source_url, emails_with_sources, contact_name, company_description, reason, debug)
    """
    cfg = cfg or AsyncExtractionConfig()

    first_source: dict[str, str] = {}
    pairs: list[tuple[str, str]] = []
    visited: set[str] = set()
    visited_norm: set[str] = set()
    visited_req_norm: set[str] = set()
    visited_final_norm: set[str] = set()
    final_path_counts: dict[str, int] = {}

    tried: list[str] = []
    discovered_count = 0
    followed_count = 0

    blocked_hits = 0
    unavailable_hits = 0

    hit_imprint = False
    hit_contact = False
    hit_privacy = False

    max_pages = max(1, int(cfg.max_pages_per_domain))
    pages_used = 0

    homepage_ok = False
    homepage_status: Optional[int] = None
    
    # Collect HTML from imprint and about pages for extraction
    imprint_html: Optional[str] = None
    imprint_url: Optional[str] = None
    about_html: Optional[str] = None
    about_url: Optional[str] = None
    homepage_html: Optional[str] = None

    # ----- Probe selection (pin to final URL host to avoid DNS failures on subpaths) -----
    probe_urls = [
        f"https://{domain}/",
        f"https://www.{domain}/",
        f"http://{domain}/",
        f"http://www.{domain}/",
    ]

    selected_final: Optional[str] = None
    use_scheme = "https"
    scheme_reason = "ok"

    for pu in probe_urls:
        pr = await fetcher.fetch(pu)
        if pr.status_code is None:
            continue
        selected_final = pr.final_url or pu
        use_scheme = (urlparse(selected_final).scheme or "https").lower()
        host = (urlparse(selected_final).hostname or domain).strip()
        domain = host  # pin domain to working final host
        scheme_reason = "ok" if pu == probe_urls[0] else f"probe_selected:{selected_final}"
        # record homepage trial in debug
        homepage_status = pr.status_code
        homepage_ok = bool(pr.status_code and pr.status_code < 400 and pr.text)
        break

    if not selected_final:
        debug = f"scheme=https; scheme_note=probe_all_failed; pages=0/{max_pages}; tried=none"
        return [], None, None, [], None, None, "request_error", debug

    base_host = domain.lower().strip()

    async def fetch_and_extract(url: str) -> Optional[FetchResult]:
        nonlocal pages_used, homepage_ok, homepage_status
        nonlocal blocked_hits, unavailable_hits
        nonlocal hit_imprint, hit_contact, hit_privacy
        nonlocal imprint_html, imprint_url, about_html, about_url, homepage_html
        nonlocal visited_req_norm, visited_final_norm

        if pages_used >= max_pages:
            return None

        # Deduplicate by requested host+path (ignore query/fragment)
        req_key = _norm_visit_key(url)
        if req_key in visited_req_norm:
            return None
        visited_req_norm.add(req_key)

        # Fetch
        res = await fetcher.fetch(url)

        final_u = res.final_url or url
        final_key = _norm_visit_key(final_u)

        # Dedupe by final URL host+path BEFORE charging budget / processing
        if final_key in visited_final_norm:
            return res
        visited_final_norm.add(final_key)

        # Now charge budget (only for unique final pages)
        pages_used += 1

        p_final = urlparse(final_u)
        final_host = (p_final.hostname or "").lower()
        final_path = p_final.path or "/"

        # Update per-path loop counts (soft 404 loops often return 200 with an error page path)
        final_path_counts[final_path] = final_path_counts.get(final_path, 0) + 1

        sc = res.status_code
        tried.append(f"{final_host}{final_path}({sc if sc is not None else res.error or 'err'})")

        if sc in cfg.blocked_statuses:
            blocked_hits += 1
        if sc in cfg.unavailable_statuses:
            unavailable_hits += 1

        # homepage tracking (final path)
        if final_path in {"", "/"}:
            homepage_status = sc
            if sc is not None and sc < 400 and res.text:
                homepage_ok = True

        # hit flags on ok-ish and collect HTML for extraction
        if sc is not None and sc < 400 and res.text:
            u_low = final_u.lower()

            # hit flags
            if "impressum" in u_low or "imprint" in u_low or "rechtliches" in u_low:
                hit_imprint = True
            if "kontakt" in u_low or "contact" in u_low:
                hit_contact = True
            if "privacy" in u_low or "legal" in u_low or "datenschutz" in u_low or "rechtliche" in u_low:
                hit_privacy = True

            # Store imprint HTML if URL indicates it OR content looks like it
            # Check content first to catch impressum pages at non-standard URLs
            # This helps when impressum content is embedded in other pages
            if _looks_like_impressum_html(res.text):
                hit_imprint = True
                if not imprint_html:
                    imprint_html = res.text
                    imprint_url = final_u
            elif "impressum" in u_low or "imprint" in u_low or "rechtliches" in u_low:
                hit_imprint = True
                if not imprint_html:
                    imprint_html = res.text
                    imprint_url = final_u
            # Also check if this page might contain contact info even if not a full impressum page
            # Look for common impressum keywords in the content
            elif not imprint_html and any(keyword in res.text.lower() for keyword in [
                "gesetzliche anbieterkennung", "vertreten durch", "geschäftsführer",
                "inhaber", "ust-id", "amtsgericht", "handelsregister"
            ]):
                # This might be a partial impressum or contact page with name info
                # Store it as a potential source for name extraction
                if not imprint_html:
                    imprint_html = res.text
                    imprint_url = final_u

            # About page capture
            if (("about" in u_low or "ueber-uns" in u_low or "über-uns" in u_low or 
                 "unternehmen" in u_low or "verband" in u_low or "association" in u_low or
                 "management" in u_low or "leadership" in u_low or "vorstand" in u_low or
                 "team" in u_low) and not about_html):
                about_html = res.text
                about_url = final_u

            # homepage capture
            if final_path in {"", "/"}:
                homepage_html = res.text

        # extract emails
        if res.text:
            for e in extract_emails(res.text):
                if e not in first_source:
                    first_source[e] = final_u
                    pairs.append((e, final_u))

        return res

    # ----- 1) Static probing -----
    texts_for_discovery: list[tuple[str, str]] = []

    for gi, group in enumerate(_PRIORITY_PATH_GROUPS):
        if pages_used >= max_pages:
            break

        urls = _scheme_urls(domain, group, use_scheme)
        for u in urls:
            if pages_used >= max_pages:
                break

            res = await fetch_and_extract(u)

            # Early stop: clearly blocked everywhere
            if blocked_hits >= 3:
                break
            # Early stop: server unavailable everywhere
            if unavailable_hits >= 3:
                break

            # Soft-404 loop protection
            if cfg.stop_on_soft404_loop and any(v >= cfg.soft404_repeat_cap for v in final_path_counts.values()):
                # do not break instantly if emails already found; but avoid burning budget
                if not pairs:
                    break

            if not res or not res.text:
                continue

            is_home = urlparse(res.final_url or u).path in {"", "/"}
            # Always collect homepage for link discovery, and collect from first few groups
            # Also collect from any page that looks like it might have useful links (about, team, etc.)
            should_collect = (is_home or 
                            gi < int(cfg.discover_from_groups) or
                            any(keyword in (res.final_url or u).lower() for keyword in ["about", "team", "verband", "association", "management", "vorstand", "people"]))
            if cfg.discover_links and should_collect:
                texts_for_discovery.append((res.final_url or u, res.text))

        if blocked_hits >= 3 or unavailable_hits >= 3:
            break

    # ----- 2) Sitemap discovery (only if discovery is enabled and we have budget) -----
    sitemap_urls: list[str] = []
    if cfg.discover_links and cfg.discover_sitemaps and pages_used < max_pages and blocked_hits < 3:
        sitemap_urls = await _discover_from_sitemaps(domain=domain, scheme=use_scheme, fetcher=fetcher, cfg=cfg)

    # ----- 3) Link discovery from HTML + sitemaps -----
    discovery_candidates: list[str] = []
    if cfg.discover_links and pages_used < max_pages and blocked_hits < 3:
        candidates: list[str] = []

        # a) from HTML pages
        for src_url, html in texts_for_discovery:
            candidates.extend(_extract_internal_candidates(html, base_url=src_url, base_host=base_host))

        # b) from sitemap
        candidates.extend(sitemap_urls)

        # normalize, dedupe, remove already-visited
        candidates = _dedupe_preserve([_clean_url(c) for c in candidates if c])
        candidates = [c for c in candidates if _is_same_site(c, base_host)]
        candidates = [c for c in candidates if _norm_visit_key(c) not in visited_norm]

        # rank and cap follow set
        candidates = sorted(candidates, key=_score_link, reverse=True)
        discovery_candidates = candidates
        discovered_count = len(discovery_candidates)

        follow_cap = max(0, int(cfg.max_discovered_links))
        for u in discovery_candidates[:follow_cap]:
            if pages_used >= max_pages or blocked_hits >= 3 or unavailable_hits >= 3:
                break
            await fetch_and_extract(u)
            followed_count += 1

    emails_unique = _dedupe_preserve([e for e, _ in pairs])

    if not emails_unique:
        if blocked_hits >= 1 or any(f"({s})" in ";".join(tried) for s in map(str, cfg.blocked_statuses)):
            reason = "blocked_or_forbidden"
        elif unavailable_hits >= 1:
            reason = "server_unavailable"
        elif any("(timeout)" in t for t in tried):
            reason = "timeout"
        elif any("(request_error" in t for t in tried):
            reason = "request_error"
        else:
            reason = "no_emails_found" if pages_used > 0 else "fetch_failed"

        # Instrumentation for debugging
        imprint_len = len(imprint_html or "")
        imprint_path = urlparse(imprint_url or "").path if imprint_url else ""
        final_unique = len(visited_final_norm)

        debug = (
            f"scheme={use_scheme}; scheme_note={scheme_reason}; pages={pages_used}/{max_pages}; "
            f"home_status={homepage_status}; home_ok={int(homepage_ok)}; "
            f"tried={';'.join(tried[:25])}; discovered={discovered_count}; followed={followed_count}; "
            f"sitemaps={int(bool(sitemap_urls))}; "
            f"hit_imprint={int(hit_imprint)}; hit_privacy={int(hit_privacy)}; hit_contact={int(hit_contact)}; "
            f"imprint_len={imprint_len}; imprint_path={imprint_path}; final_unique={final_unique}"
        )
        return [], None, None, [], None, None, reason, debug

    best_email = _choose_best_from_pairs(domain, pairs)
    best_src = first_source.get(best_email or "", "") if best_email else ""
    
    # Extract contact name from imprint page
    contact_name: Optional[str] = None
    if imprint_html:
        contact_name = extract_contact_name(imprint_html, imprint_url or "")
    
    # Fallback: Try extracting contact name from About/Team page if not found in Imprint
    if not contact_name and about_html:
        contact_name = extract_contact_name(about_html, about_url or "")
    
    # Fallback: Check homepage if it looks like it contains impressum content
    if not contact_name and homepage_html and _looks_like_impressum_html(homepage_html):
        contact_name = extract_contact_name(homepage_html, "")

    # Extract company description (prefer about page, fallback to homepage)
    company_description: Optional[str] = None
    if about_html:
        company_description = extract_company_description(about_html, about_url or "")
    elif homepage_html:
        company_description = extract_company_description(homepage_html, "")

    # Instrumentation for debugging
    imprint_len = len(imprint_html or "")
    imprint_path = urlparse(imprint_url or "").path if imprint_url else ""
    final_unique = len(visited_final_norm)

    debug = (
        f"scheme={use_scheme}; scheme_note={scheme_reason}; pages={pages_used}/{max_pages}; "
        f"home_status={homepage_status}; home_ok={int(homepage_ok)}; "
        f"tried={';'.join(tried[:25])}; discovered={discovered_count}; followed={followed_count}; "
        f"sitemaps={int(bool(sitemap_urls))}; "
        f"hit_imprint={int(hit_imprint)}; hit_privacy={int(hit_privacy)}; hit_contact={int(hit_contact)}; "
        f"imprint_len={imprint_len}; imprint_path={imprint_path}; final_unique={final_unique}"
    )

    return emails_unique, best_email, (best_src or None), pairs, contact_name, company_description, "ok", debug
