from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List
from urllib.parse import urljoin, urlparse

from ..models import CrawlArtifacts
from .crawl4ai_client import Crawl4AIClient
from .link_discovery import infer_page_type
from .run_log import append_domain_result
from .text_link_finder import (
    TextDiscoveryConfig,
    choose_best_url,
    discover_text_links,
    extract_impressum_to_end,
    select_about_by_href,
)


@dataclass(frozen=True)
class DomainCrawlConfig:
    """
    Domain-level crawl limits.
    """
    top_k: int = 6  # kept for compatibility; not used for 5.3 anymore
    depth1_max_pages: int = 10
    depth1_max_new_links: int = 500


class DomainCrawlerService:
    """
    Domain crawler implementing strict, ordered case logic.

    CASE ORDER (first match wins):
      Case 4   - Hard failure (site broken / empty)
      Case 1   - Impressum via anchor text, truncation OK
      Case 2   - Impressum via anchor text, truncation failed
      Case 3   - Contact / Privacy via anchor text
      Case 5.1 - URL-based impressum, truncation OK
      Case 5.2 - URL-based impressum, truncation failed
      Case 5.3 - URL-based contact/privacy only
    """

    def __init__(self, client: Crawl4AIClient):
        self._client = client

    # ---------------------------
    # URL / domain helpers
    # ---------------------------

    def _normalize_home(self, domain: str) -> str:
        d = domain.replace("https://", "").replace("http://", "").strip("/")
        return f"https://{d}"

    @staticmethod
    def _base_host(url: str) -> str:
        return urlparse(url).hostname or ""

    @staticmethod
    def _dedupe_preserve_order(urls: List[str]) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        for u in urls:
            u = (u or "").strip()
            if not u or u in seen:
                continue
            seen.add(u)
            out.append(u)
        return out

    @staticmethod
    def _typed_list(home_url: str, items: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Typed list used by downstream pipeline:
        - First entry is always homepage typed as "home"
        - Remaining items are de-duped, preserve order
        """
        out: List[Dict[str, str]] = []
        seen: set[str] = set()

        def add(url: str, kind: str) -> None:
            u = str(url).strip()
            if not u or u in seen:
                return
            seen.add(u)
            out.append({"url": u, "kind": kind})

        add(home_url, "home")
        for it in items:
            u = (it.get("url") or "").strip()
            k = (it.get("kind") or "").strip()
            if u and k:
                add(u, k)

        return out

    # ---------------------------
    # Core crawl
    # ---------------------------

    async def crawl_domain(self, domain: str, cfg: DomainCrawlConfig) -> CrawlArtifacts:
        base_url = self._normalize_home(domain)

        # Fetch homepage
        homepage = await self._client.fetch_page(base_url)
        base_url = homepage.final_url or base_url
        base_host = self._base_host(base_url)

        # Ensure meta is safe to mutate (Crawl4AIClient guarantees dict, but keep defensive)
        homepage.meta = homepage.meta or {}
        homepage.meta["page_type"] = "home"

        # ----------------
        # Case 4 — hard failure
        # ----------------
        if homepage.error or not (homepage.cleaned_html or homepage.markdown):
            append_domain_result(
                domain=base_host,
                contact_case="4",
                contact_links=[],
                about_case=None,
                about_links=[],
                contact_typed_links=[],
            )
            return CrawlArtifacts(
                domain=base_host,
                homepage=homepage,
                pages=[],
                discovered_links=[],
                selected_links=[],
                meta={
                    "contact_selected_links": [],
                    "contact_selected_typed_links": [],
                    "about_selected_links": [],
                    "contact_case": "4",
                    "about_case": None,
                },
            )

        # ----------------
        # Anchor-text discovery (ranked by preference inside discover_text_links)
        # ----------------
        raw_html = (homepage.meta or {}).get("raw_html") or ""
        text_discovery = discover_text_links(
            base_url=base_url,
            base_host=base_host,
            cleaned_html=homepage.cleaned_html,
            raw_html=raw_html,
            crawl4ai_links_raw=(homepage.meta or {}).get("crawl4ai_links_raw"),
            cfg=TextDiscoveryConfig(include_about=True),
        )

        # ----------------
        # Case 1 / 2 — Impressum via anchor
        # ----------------
        imprint_url = choose_best_url(text_discovery["impressum"], base_host)
        if imprint_url:
            page = await self._client.fetch_page(imprint_url)
            page.meta = page.meta or {}
            page.meta["page_type"] = "impressum"

            truncated = extract_impressum_to_end(page.markdown or "")
            contact_case = "1" if truncated else "2"

            about_url, about_case = self._select_about(text_discovery, homepage.links, base_host)

            contact_links = [imprint_url, base_url]
            contact_typed = self._typed_list(
                base_url,
                [{"url": imprint_url, "kind": "impressum"}],
            )

            append_domain_result(
                domain=base_host,
                contact_case=contact_case,
                contact_links=contact_links,
                about_case=about_case,
                about_links=[about_url] if about_url else [],
                contact_typed_links=contact_typed,
            )

            return CrawlArtifacts(
                domain=base_host,
                homepage=homepage,
                pages=[page],
                discovered_links=[],
                selected_links=contact_links + ([about_url] if about_url else []),
                meta={
                    "contact_selected_links": contact_links,
                    "contact_selected_typed_links": contact_typed,
                    "about_selected_links": [about_url] if about_url else [],
                    "contact_case": contact_case,
                    "about_case": about_case,
                },
            )

        # ----------------
        # Case 3 — Contact / Privacy via anchor
        #
        # Note:
        # - We preserve the ORDER RETURNED by discover_text_links() (already preference-ranked).
        # - We de-dupe final link list to prevent repeats.
        # ----------------
        contact_urls = [u for u in (text_discovery.get("contact") or []) if u]
        privacy_urls = [u for u in (text_discovery.get("privacy") or []) if u]

        if contact_urls or privacy_urls:
            contact_links: List[str] = [base_url] + contact_urls + privacy_urls
            contact_links = self._dedupe_preserve_order(contact_links)

            typed_items: List[Dict[str, str]] = (
                [{"url": u, "kind": "contact"} for u in contact_urls]
                + [{"url": u, "kind": "legal"} for u in privacy_urls]
            )
            contact_typed = self._typed_list(base_url, typed_items)

            about_url, about_case = self._select_about(text_discovery, homepage.links, base_host)

            append_domain_result(
                domain=base_host,
                contact_case="3",
                contact_links=contact_links,
                about_case=about_case,
                about_links=[about_url] if about_url else [],
                contact_typed_links=contact_typed,
            )

            return CrawlArtifacts(
                domain=base_host,
                homepage=homepage,
                pages=[],
                discovered_links=[],
                selected_links=contact_links + ([about_url] if about_url else []),
                meta={
                    "contact_selected_links": contact_links,
                    "contact_selected_typed_links": contact_typed,
                    "about_selected_links": [about_url] if about_url else [],
                    "contact_case": "3",
                    "about_case": about_case,
                },
            )

        # ----------------
        # Case 5 — URL-based fallback (no ranking for 5.3 selection beyond infer_page_type)
        # ----------------
        discovered = self._canonicalize(homepage.links, base_url)[: cfg.depth1_max_new_links]

        # Classify discovered links by URL patterns
        impressum_links: List[str] = []
        contact_links_u: List[str] = []
        legal_links_u: List[str] = []

        for u in discovered:
            pt = infer_page_type(u)
            if pt == "impressum":
                impressum_links.append(u)
            elif pt == "contact":
                contact_links_u.append(u)
            elif pt == "privacy":
                legal_links_u.append(u)

        about_url, about_case = self._select_about(text_discovery, discovered, base_host)

        # --- Case 5.1 / 5.2 (impressum found by URL patterns; first match wins) ---
        if impressum_links:
            imprint_url = impressum_links[0]
            page = await self._client.fetch_page(imprint_url)
            page.meta = page.meta or {}
            page.meta["page_type"] = "impressum"

            truncated = extract_impressum_to_end(page.markdown or "")
            contact_case = "5.1" if truncated else "5.2"

            contact_links = [imprint_url, base_url]
            contact_typed = self._typed_list(
                base_url,
                [{"url": imprint_url, "kind": "impressum"}],
            )

            append_domain_result(
                domain=base_host,
                contact_case=contact_case,
                contact_links=contact_links,
                about_case=about_case,
                about_links=[about_url] if about_url else [],
                contact_typed_links=contact_typed,
            )

            return CrawlArtifacts(
                domain=base_host,
                homepage=homepage,
                pages=[page],
                discovered_links=discovered,
                selected_links=contact_links + ([about_url] if about_url else []),
                meta={
                    "contact_selected_links": contact_links,
                    "contact_selected_typed_links": contact_typed,
                    "about_selected_links": [about_url] if about_url else [],
                    "contact_case": contact_case,
                    "about_case": about_case,
                },
            )

        # --- Case 5.3 ---
        # Requirement: homepage + all contact links + all legal/privacy links (typed)
        if not contact_links_u and not legal_links_u:
            append_domain_result(
                domain=base_host,
                contact_case="4",
                contact_links=[],
                about_case=None,
                about_links=[],
                contact_typed_links=[],
            )
            return CrawlArtifacts(
                domain=base_host,
                homepage=homepage,
                pages=[],
                discovered_links=[],
                selected_links=[],
                meta={
                    "contact_selected_links": [],
                    "contact_selected_typed_links": [],
                    "about_selected_links": [],
                    "contact_case": "4",
                    "about_case": None,
                },
            )

        contact_links = [base_url] + contact_links_u + legal_links_u
        contact_links = self._dedupe_preserve_order(contact_links)

        typed_items = (
            [{"url": u, "kind": "contact"} for u in contact_links_u]
            + [{"url": u, "kind": "legal"} for u in legal_links_u]
        )
        contact_typed = self._typed_list(base_url, typed_items)

        append_domain_result(
            domain=base_host,
            contact_case="5.3",
            contact_links=contact_links,
            about_case=about_case,
            about_links=[about_url] if about_url else [],
            contact_typed_links=contact_typed,
        )

        return CrawlArtifacts(
            domain=base_host,
            homepage=homepage,
            pages=[],
            discovered_links=discovered,
            selected_links=contact_links + ([about_url] if about_url else []),
            meta={
                "contact_selected_links": contact_links,
                "contact_selected_typed_links": contact_typed,
                "about_selected_links": [about_url] if about_url else [],
                "contact_case": "5.3",
                "about_case": about_case,
            },
        )

    # ---------------------------
    # Selection helpers
    # ---------------------------

    def _select_about(self, text_discovery, href_pool, base_host):
        """
        About selection is separate from primary contact/legal logic:
        - Prefer anchor-discovered "about"
        - Else fallback to href pattern matching
        """
        about_url = choose_best_url(text_discovery["about"], base_host)
        if about_url:
            return about_url, "about_anchor"

        about_url = select_about_by_href(href_pool, base_host=base_host)
        if about_url:
            return about_url, "about_link"

        return None, "about_not_found"

    def _canonicalize(self, links: List[str], base_url: str) -> List[str]:
        """
        Resolve relative URLs against base_url and de-dupe while preserving order.
        """
        out: List[str] = []
        seen: set[str] = set()
        for u in links or []:
            cu = urljoin(base_url, u)
            if cu not in seen:
                seen.add(cu)
                out.append(cu)
        return out
