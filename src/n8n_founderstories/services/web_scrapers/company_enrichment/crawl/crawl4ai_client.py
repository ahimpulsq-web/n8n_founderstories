from __future__ import annotations

import os
import sys

# CRITICAL: Fix Windows console encoding BEFORE importing crawl4ai
# crawl4ai uses rich console which fails with Unicode characters on Windows
os.environ['PYTHONIOENCODING'] = 'utf-8'
if os.name == 'nt':  # Windows
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')

# Disable crawl4ai's verbose logging to avoid encoding issues
os.environ['CRAWL4AI_LOG_LEVEL'] = 'ERROR'

import asyncio
import time
from dataclasses import dataclass
from typing import Any, List, Optional
from urllib.parse import urlparse

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

from ..log_events import log_event
from ..models import PageArtifact


@dataclass(frozen=True)
class Crawl4AIClientConfig:
    """
    Runtime configuration for the shared Crawl4AI browser instance.
    """
    headless: bool = True
    timeout_s: float = 45.0
    max_concurrency: int = 4
    user_agent: str = "Mozilla/5.0 (company_enrichment)"
    wait_after_load_s: float = 0.0


class Crawl4AIClient:
    """
    Single shared Crawl4AI browser instance.

    Design constraints:
    - PageArtifact.links MUST remain a flat List[str]
    - Raw Crawl4AI links (href + text) are preserved in:
        PageArtifact.meta["crawl4ai_links_raw"]
      for strict anchor-text discovery.
    """

    def __init__(self, cfg: Crawl4AIClientConfig):
        self._closed = False
        self._cfg = cfg
        self._crawler: Optional[AsyncWebCrawler] = None
        self._lock = asyncio.Lock()
        self._sem = asyncio.Semaphore(max(1, int(cfg.max_concurrency)))

        self._browser_cfg = BrowserConfig(
            headless=cfg.headless,
            user_agent=cfg.user_agent,
        )
        self._run_cfg = CrawlerRunConfig(
            page_timeout=int(cfg.timeout_s * 1000)
        )

    async def start(self) -> None:
        async with self._lock:
            if self._closed:
                raise RuntimeError("Crawl4AIClient is closed")
            if self._crawler is not None:
                return

            try:
                self._crawler = AsyncWebCrawler(config=self._browser_cfg)
            except Exception as e:
                raise RuntimeError(f"Failed to create AsyncWebCrawler: {e}")
            
            try:
                # Add timeout to browser initialization
                await asyncio.wait_for(self._crawler.__aenter__(), timeout=60.0)
            except asyncio.TimeoutError:
                self._crawler = None
                raise RuntimeError("Crawl4AI browser initialization timed out after 60 seconds")
            except Exception as e:
                self._crawler = None
                raise

            log_event(
                "company_enrichment.crawl4ai.started",
                max_concurrency=self._cfg.max_concurrency,
            )

    async def close(self) -> None:
        async with self._lock:
            if self._crawler is None:
                self._closed = True
                return

            crawler = self._crawler
            self._crawler = None
            self._closed = True

            await crawler.__aexit__(None, None, None)
            log_event("company_enrichment.crawl4ai.closed")

    async def __aenter__(self) -> "Crawl4AIClient":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # -----------------------------
    # URL filtering & normalization
    # -----------------------------

    @staticmethod
    def _is_http_url(u: str) -> bool:
        """
        Keep only http(s) URLs in PageArtifact.links.
        Prevents mailto:, tel:, javascript:, #fragment, etc.
        """
        if not u:
            return False
        low = u.strip().lower()
        return low.startswith("http://") or low.startswith("https://")

    @classmethod
    def _normalize_links_flat(cls, links_raw: Any) -> List[str]:
        """
        Normalize Crawl4AI result.links into a flat List[str].

        Supported inputs:
          - list[str]
          - dict[str, list[str]]
          - dict[str, list[dict]]  (href/text objects)

        Important:
          - Filters out non-http(s) schemes like mailto:, tel:, javascript:
        """
        out: List[str] = []

        def add(x: Any) -> None:
            if not x:
                return
            s = str(x).strip()
            if not s:
                return
            if cls._is_http_url(s):
                out.append(s)

        if isinstance(links_raw, list):
            for x in links_raw:
                add(x)
            return out

        if isinstance(links_raw, dict):
            for v in links_raw.values():
                if isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict):
                            href = item.get("href") or item.get("url") or item.get("link")
                            add(href)
                        else:
                            add(item)

        return out

    # -----------------------------
    # Public API
    # -----------------------------

    async def fetch_page(self, url: str) -> PageArtifact:
        """
        Fetch a page and return a PageArtifact.

        Contract:
        - Always returns PageArtifact.meta as a dict (never None).
        - Never attempts to fetch non-http(s) URLs.
        """
        if self._closed:
            return PageArtifact(
                url=url,
                cleaned_html="",
                markdown="",
                error="client_closed",
                links=[],
                meta={},
            )

        # Defensive: never attempt to fetch non-http(s)
        if not self._is_http_url(str(url)):
            return PageArtifact(
                url=url,
                cleaned_html="",
                markdown="",
                error="invalid_url_scheme",
                links=[],
                meta={},
            )

        await self.start()
        crawler = self._crawler
        if crawler is None:
            return PageArtifact(
                url=url,
                cleaned_html="",
                markdown="",
                error="crawler_not_started",
                links=[],
                meta={},
            )

        t0 = time.perf_counter()
        try:
            async with self._sem:
                res = await crawler.arun(url=url, config=self._run_cfg)

            html = getattr(res, "html", "") or ""
            md = getattr(res, "markdown", "") or ""
            cleaned = getattr(res, "cleaned_html", "") or ""
            links_raw = getattr(res, "links", None)

            cleaned_html = cleaned if cleaned.strip() else html

            final_url = getattr(res, "final_url", None)
            if final_url:
                final_url = _strip_fragment_query(str(final_url))

            links = self._normalize_links_flat(links_raw)

            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            log_event(
                "company_enrichment.crawl4ai.page_fetched",
                url=url,
                elapsed_ms=elapsed_ms,
                links_count=len(links),
            )

            meta: dict[str, Any] = {}
            if html:
                meta["raw_html"] = html
            if links_raw is not None:
                meta["crawl4ai_links_raw"] = links_raw

            return PageArtifact(
                url=url,
                final_url=final_url,
                cleaned_html=str(cleaned_html),
                markdown=str(md),
                title=getattr(res, "title", None),
                links=links,
                meta=meta,
            )

        except asyncio.TimeoutError:
            return PageArtifact(
                url=url,
                cleaned_html="",
                markdown="",
                error="timeout",
                links=[],
                meta={},
            )

        except Exception as e:
            return PageArtifact(
                url=url,
                cleaned_html="",
                markdown="",
                error=f"{type(e).__name__}: {str(e)[:200]}",
                links=[],
                meta={},
            )


def _strip_fragment_query(u: str) -> str:
    """Drop #fragment and ?query from URL (used for final_url normalization)."""
    try:
        p = urlparse(u)
        return p._replace(fragment="", query="").geturl()
    except Exception:
        return u
