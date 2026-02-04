from __future__ import annotations

import asyncio
from typing import Dict, Any, List

from .crawler import fetch_page
from .cleaner import clean_html
from .prompts import EXTRACT_COMPANIES_FROM_BLOG_PROMPT
from ..openrouter_client import OpenRouterClient
from .run_log import append_blog_extraction_result


class BlogLeadExtractorService:
    def __init__(self, llm: OpenRouterClient):
        self._llm = llm

    async def extract_from_url(self, url: str) -> Dict[str, Any]:
        # 1. crawl
        crawl = await fetch_page(url)

        # 2. clean
        text = clean_html(crawl.html)
        if not text.strip():

            append_blog_extraction_result(
                source_url=url,
                search_intent="Saas AI startups",
                companies=[],
                model=self._llm.model,
                status="no_content",
            )

            return {"source_url": url, "companies": []}

        # 3. llm
        prompt = f"""
{EXTRACT_COMPANIES_FROM_BLOG_PROMPT}

SEARCH INTENT:
Saas AI startups

SOURCE URL:
{url}

ARTICLE TEXT:
{text[:12000]}
"""

        result = self._llm.complete_json(prompt)

        companies = result.get("companies", [])

        append_blog_extraction_result(
            source_url=url,
            search_intent="Saas AI startups",
            companies=companies,
            model=self._llm.model,
            status="ok",
        )

        return {
            "source_url": url,
            "companies": result.get("companies", []),
        }
