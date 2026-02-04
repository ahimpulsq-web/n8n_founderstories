# src/n8n_founderstories/services/web_scrapers/company_enrichment/llm/test_llm.py
from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import List

from n8n_founderstories.services.web_scrapers.company_enrichment.crawl.crawl4ai_client import (
    Crawl4AIClient,
    Crawl4AIClientConfig,
)
from n8n_founderstories.services.web_scrapers.company_enrichment.crawl.service import (
    DomainCrawlerService,
    DomainCrawlConfig,
)
from n8n_founderstories.services.web_scrapers.company_enrichment.extract.llm.service import (
    LLMService,
)
from n8n_founderstories.services.web_scrapers.company_enrichment.extract.llm.router import (
    LLMRouterConfig,
    OpenRouterLLMRouter,
)
from n8n_founderstories.services.web_scrapers.company_enrichment.extract.llm.extract import (
    extract,
)


# -------------------------
# HARD-CODED SETTINGS
# -------------------------

# domains file path (defaults to same folder as this script)
DOMAINS_TXT_NAME = "domains.txt"

# Crawl4AI
HEADLESS = True
CRAWL_TIMEOUT_S = 45.0
CRAWL_MAX_CONCURRENCY = 4
CRAWL_USER_AGENT = "Mozilla/5.0 (company_enrichment)"
WAIT_AFTER_LOAD_S = 0.0

# Crawl selection
TOP_K = 6
DEPTH1_MAX_PAGES = 10
DEPTH1_MAX_NEW_LINKS = 500

# OpenRouter
# Provide keys via env var OR hard-code directly in API_KEYS
OPENROUTER_API_KEYS_ENV = "OPENROUTER_API_KEYS"
API_KEYS = [
    "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",
]


# Model priority: first is primary, rest are fallbacks
MODELS: List[str] = [
    "openai/gpt-4o-mini",
]

LLM_TIMEOUT_S = 40.0
LLM_MAX_CONCURRENCY = 6
LLM_MAX_RETRIES_PER_MODEL = 1


def _read_domains(domains_path: Path) -> List[str]:
    if not domains_path.exists():
        raise FileNotFoundError(f"domains.txt not found at: {domains_path}")

    domains: List[str] = []
    for line in domains_path.read_text(encoding="utf-8").splitlines():
        d = line.strip()
        if not d or d.startswith("#"):
            continue
        domains.append(d)
    return domains


def _load_api_keys() -> List[str]:
    # 1) env var: OPENROUTER_API_KEYS="key1,key2,key3"
    raw = os.getenv(OPENROUTER_API_KEYS_ENV, "").strip()
    if raw:
        keys = [k.strip() for k in raw.split(",") if k.strip()]
        if keys:
            return keys

    # 2) fallback: hard-coded list above
    return [k.strip() for k in API_KEYS if k.strip()]


async def run(domains_path: Path) -> None:
    domains = _read_domains(domains_path)
    if not domains:
        print("No domains found in domains.txt")
        return

    api_keys = _load_api_keys()
    if not api_keys:
        raise RuntimeError(
            f"No OpenRouter API keys provided. Set env var {OPENROUTER_API_KEYS_ENV} "
            "or hard-code API_KEYS in this file."
        )

    client_cfg = Crawl4AIClientConfig(
        headless=HEADLESS,
        timeout_s=CRAWL_TIMEOUT_S,
        max_concurrency=CRAWL_MAX_CONCURRENCY,
        user_agent=CRAWL_USER_AGENT,
        wait_after_load_s=WAIT_AFTER_LOAD_S,
    )

    crawl_cfg = DomainCrawlConfig(
        top_k=TOP_K,
        depth1_max_pages=DEPTH1_MAX_PAGES,
        depth1_max_new_links=DEPTH1_MAX_NEW_LINKS,
    )

    router_cfg = LLMRouterConfig(
        api_keys=api_keys,
        models=MODELS,
        timeout_s=LLM_TIMEOUT_S,
        max_concurrency=LLM_MAX_CONCURRENCY,
        max_retries_per_model=LLM_MAX_RETRIES_PER_MODEL,
    )

    router = OpenRouterLLMRouter(router_cfg)

    async with Crawl4AIClient(client_cfg) as client:
        crawler = DomainCrawlerService(client)
        llm_service = LLMService(client)

        try:
            for i, domain in enumerate(domains, start=1):
                print(f"[{i}/{len(domains)}] {domain}")

                crawl_artifacts = await crawler.crawl_domain(domain, crawl_cfg)

                # Build LLM inputs: reuse already-fetched pages when possible
                llm_inputs = await llm_service.build_inputs(crawl_artifacts)

                # Run extraction (currently Case 1 only; others will log "skipped")
                result = await extract(
                    domain=crawl_artifacts.domain,
                    crawl_meta=crawl_artifacts.meta or {},
                    pages=llm_inputs.pages,
                    router=router,
                )

                # quick console summary
                pages_with_error = sum(1 for p in llm_inputs.pages if p.error)
                have_primary = bool(result.contact_primary)
                have_short = bool(result.about_short)
                have_long = bool(result.about_long)

                print(
                    f"  case={crawl_artifacts.meta.get('contact_case')} "
                    f"about={crawl_artifacts.meta.get('about_case')} "
                    f"selected={len(crawl_artifacts.selected_links)} "
                    f"pages={len(llm_inputs.pages)} "
                    f"errors={pages_with_error} "
                    f"llm_primary={have_primary} short={have_short} long={have_long}"
                )
        finally:
            await router.close()


if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    domains_txt = here / DOMAINS_TXT_NAME
    asyncio.run(run(domains_txt))
