from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from crawl4ai import AsyncWebCrawler


@dataclass(frozen=True)
class CrawlResult:
    url: str
    html: str
    markdown: Optional[str] = None


async def fetch_page(url: str) -> CrawlResult:
    async with AsyncWebCrawler(verbose=False) as crawler:
        result = await crawler.arun(url=url)

    # crawl4ai result usually exposes html/markdown; keep defensive
    html = getattr(result, "html", None) or ""
    markdown = getattr(result, "markdown", None)

    return CrawlResult(url=url, html=html, markdown=markdown)
