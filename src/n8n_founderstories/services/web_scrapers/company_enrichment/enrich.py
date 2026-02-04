import asyncio
import time

from n8n_founderstories.services.web_scrapers.company_enrichment.crawl.crawl4ai_client import (
    Crawl4AIClient,
)
from n8n_founderstories.services.web_scrapers.company_enrichment.crawl.service import (
    DomainCrawlerService,
)
from n8n_founderstories.services.web_scrapers.company_enrichment.extract.deterministic.extract import (
    extract as deterministic_extract,
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


async def run(domain: str):
    t0 = time.perf_counter()

    async with Crawl4AIClient() as crawl_client:
        # -------------------------
        # Crawl (blocking dependency)
        # -------------------------
        crawler = DomainCrawlerService(crawl_client)
        crawl = await crawler.crawl_domain(domain)

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
            )

        det, llm_res = await asyncio.gather(
            det_task,
            run_llm(),
        )

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


if __name__ == "__main__":
    asyncio.run(run("example.com"))
