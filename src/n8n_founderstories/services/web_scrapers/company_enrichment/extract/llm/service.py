# src/n8n_founderstories/services/web_scrapers/company_enrichment/llm/service.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List
from urllib.parse import urlparse

from ...crawl.crawl4ai_client import Crawl4AIClient
from ...models import CrawlArtifacts, PageArtifact
from .run_log import append_llm_event, append_llm_input

from . import run_log as _llm_run_log
print("[LLM RUN_LOG FILE]", _llm_run_log.__file__)



@dataclass(frozen=True)
class LLMInputArtifacts:
    """
    Output of the LLM input preparation stage (no extraction yet).
    """
    domain: str
    selected_links: List[str]
    pages: List[PageArtifact]


class LLMService:
    """
    Step 1-2:
      - accept CrawlArtifacts
      - log inputs
      - reuse crawl-fetched PageArtifacts when possible
      - fetch PageArtifacts for missing selected_links
      - return LLMInputArtifacts
    """

    def __init__(self, client: Crawl4AIClient):
        self._client = client

    @staticmethod
    def _norm(u: str) -> str:
        """
        Normalize for dedup:
        - strip fragment/query
        - strip trailing slash (except root-ish)
        """
        if not u:
            return ""
        try:
            p = urlparse(u)
            base = p._replace(fragment="", query="").geturl()
        except Exception:
            base = u

        # best-effort trailing slash normalization
        if base.endswith("/") and len(base) > len("https://a.b/"):
            base = base.rstrip("/")

        return base

    async def build_inputs(self, crawl: CrawlArtifacts) -> LLMInputArtifacts:
        domain = crawl.domain
        selected_links = list(crawl.selected_links or [])

        # ---- log raw inputs from crawl layer ----
        append_llm_input(
            domain=domain,
            selected_links=selected_links,
            crawl_meta=crawl.meta or {},
            note="llm.input_received",
        )

        if not selected_links:
            append_llm_event(
                event="llm.no_selected_links",
                domain=domain,
                payload={"reason": "empty_selected_links"},
            )
            return LLMInputArtifacts(domain=domain, selected_links=[], pages=[])

        # ---- index already-fetched pages from crawl layer ----
        existing: dict[str, PageArtifact] = {}

        if crawl.homepage is not None:
            existing[self._norm(str(crawl.homepage.url))] = crawl.homepage
            if crawl.homepage.final_url:
                existing[self._norm(str(crawl.homepage.final_url))] = crawl.homepage

        for p in (crawl.pages or []):
            existing[self._norm(str(p.url))] = p
            if p.final_url:
                existing[self._norm(str(p.final_url))] = p

        # ---- build pages list in selected_links order, reuse when possible ----
        pages: List[PageArtifact] = []
        fetched_now = 0

        for url in selected_links:
            key = self._norm(str(url))
            if key in existing:
                pages.append(existing[key])
                continue

            page = await self._client.fetch_page(str(url))
            pages.append(page)
            fetched_now += 1

            # store for possible duplicates later in selected_links
            existing[self._norm(str(page.url))] = page
            if page.final_url:
                existing[self._norm(str(page.final_url))] = page

        append_llm_event(
            event="llm.pages_fetched",
            domain=domain,
            payload={
                "pages_total": len(pages),
                "pages_fetched_now": fetched_now,
                "pages_reused_from_crawl": len(pages) - fetched_now,
                "pages_with_error": sum(1 for p in pages if p.error),
            },
        )

        return LLMInputArtifacts(domain=domain, selected_links=selected_links, pages=pages)
