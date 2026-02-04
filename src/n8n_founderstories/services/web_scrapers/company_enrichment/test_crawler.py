# src/n8n_founderstories/services/web_scrapers/company_enrichment/crawl/test_crawler.py

import asyncio
from pathlib import Path

from n8n_founderstories.services.web_scrapers.company_enrichment.crawl.crawl4ai_client import (
    Crawl4AIClient,
    Crawl4AIClientConfig,
)
from n8n_founderstories.services.web_scrapers.company_enrichment.crawl.service import (
    DomainCrawlerService,
    DomainCrawlConfig,
)


async def main() -> None:
    """
    Batch test runner for domain crawling.

    - Reads domains from domains.txt
    - Runs crawler sequentially (safe for debugging)
    - Logs results via run_log.py
    """
    base_dir = Path(__file__).resolve().parent
    domains_file = base_dir / "domains.txt"

    if not domains_file.exists():
        raise FileNotFoundError(f"domains.txt not found at {domains_file}")

    # Load domains (ignore comments and empty lines)
    domains = [
        line.strip()
        for line in domains_file.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    if not domains:
        print("No domains found in domains.txt")
        return

    # Crawl4AI client configuration
    client_cfg = Crawl4AIClientConfig(
        headless=True,
        timeout_s=45.0,
        max_concurrency=4,
    )

    # Domain crawl limits
    crawl_cfg = DomainCrawlConfig(
        top_k=4,
        depth1_max_pages=10,
        depth1_max_new_links=500,
    )

    async with Crawl4AIClient(client_cfg) as client:
        crawler = DomainCrawlerService(client)

        for domain in domains:
            print(f"\n=== Crawling: {domain} ===")
            try:
                await crawler.crawl_domain(domain, crawl_cfg)
            except Exception as e:
                # Safety net: one domain must not break the batch
                print(f"[ERROR] {domain}: {type(e).__name__}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
