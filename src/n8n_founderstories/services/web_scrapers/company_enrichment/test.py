import asyncio
import time
from pathlib import Path

from n8n_founderstories.services.web_scrapers.company_enrichment.crawl.crawl4ai_client import (
    Crawl4AIClient,
    Crawl4AIClientConfig,
)
from n8n_founderstories.services.web_scrapers.company_enrichment.crawl.service import (
    DomainCrawlerService,
    DomainCrawlConfig,
)
from n8n_founderstories.services.web_scrapers.company_enrichment.extract.deterministic.extract import (
    extract as deterministic_extract,
)
from n8n_founderstories.services.web_scrapers.company_enrichment.extract.llm.router import (
    OpenRouterLLMRouter,
    LLMRouterConfig,
)
from n8n_founderstories.services.web_scrapers.company_enrichment.extract.llm.service import (
    LLMService,
)
from n8n_founderstories.services.web_scrapers.company_enrichment.extract.llm.extract import (
    extract as llm_extract,
)
from n8n_founderstories.services.web_scrapers.company_enrichment.combine.service import (
    combine_enrichment,
)


DOMAINS_FILE = Path("domains.txt")

OPENROUTER_KEYS = [
    "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",
]

LLM_MODELS = [
    "openai/gpt-4o-mini",
]


async def run_domain(domain: str, crawl_client: Crawl4AIClient, llm_router: OpenRouterLLMRouter):
    t0 = time.perf_counter()

    # -------------------------
    # Crawl
    # -------------------------
    crawler = DomainCrawlerService(crawl_client)
    crawl = await crawler.crawl_domain(domain, DomainCrawlConfig())

    # -------------------------
    # Fan-out: DET + LLM
    # -------------------------
    det_task = asyncio.to_thread(
        deterministic_extract,
        domain=crawl.domain,
        pages=[crawl.homepage] + crawl.pages,
    )

    async def run_llm():
        llm_service = LLMService(crawl_client)
        llm_inputs = await llm_service.build_inputs(crawl)
        return await llm_extract(
            domain=crawl.domain,
            crawl_meta=crawl.meta,
            pages=llm_inputs.pages,
            router=llm_router,
        )

    det, llm_res = await asyncio.gather(det_task, run_llm())

    # -------------------------
    # Combine
    # -------------------------
    email_results, company_result, descriptions, people = combine_enrichment(
        domain=crawl.domain,
        crawl=crawl,
        deterministic=det,
        llm=llm_res,
    )

    elapsed = int((time.perf_counter() - t0) * 1000)
    print(f"[OK] {domain} done in {elapsed} ms")


async def main():
    if not DOMAINS_FILE.exists():
        raise FileNotFoundError("domains.txt not found")

    domains = [
        d.strip()
        for d in DOMAINS_FILE.read_text().splitlines()
        if d.strip() and not d.strip().startswith("#")
    ]

    if not domains:
        print("No domains to process")
        return

    crawl_client = Crawl4AIClient(
        Crawl4AIClientConfig(
            max_concurrency=4,
        )
    )

    llm_router = OpenRouterLLMRouter(
        LLMRouterConfig(
            api_keys=OPENROUTER_KEYS,
            models=LLM_MODELS,
            max_concurrency=4,
            max_retries_per_model=1,
        )
    )

    async with crawl_client:
        for domain in domains:
            try:
                await run_domain(domain, crawl_client, llm_router)
            except Exception as e:
                print(f"[ERROR] {domain}: {e}")

    await llm_router.close()


if __name__ == "__main__":
    asyncio.run(main())
