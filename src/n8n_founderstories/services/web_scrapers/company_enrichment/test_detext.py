# src/n8n_founderstories/services/web_scrapers/company_enrichment/extract/deterministic/test_extractor.py

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
from n8n_founderstories.services.web_scrapers.company_enrichment.extract.deterministic.extract import (
    extract as extract_emails,
)


async def main() -> None:
    """
    End-to-end deterministic email extraction test.

    Flow:
      domain -> crawl -> select pages -> deterministic email extraction
      logs written to:
        crawl/logs/
        extract/deterministic/logs/
    """

    base_dir = Path(__file__).resolve().parent
    domains_file = base_dir / "domains.txt"

    if not domains_file.exists():
        raise FileNotFoundError(f"domains.txt not found at {domains_file}")

    domains = [
        line.strip()
        for line in domains_file.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    if not domains:
        print("No domains found in domains.txt")
        return

    # ---- Crawl config ----
    client_cfg = Crawl4AIClientConfig(
        headless=True,
        timeout_s=45.0,
        max_concurrency=4,
    )

    crawl_cfg = DomainCrawlConfig(
        top_k=4,
        depth1_max_pages=10,
        depth1_max_new_links=500,
    )

    async with Crawl4AIClient(client_cfg) as client:
        crawler = DomainCrawlerService(client)

        for domain in domains:
            print(f"\n=== DOMAIN: {domain} ===")
            try:
                crawl = await crawler.crawl_domain(domain, crawl_cfg)

                # ----------------------------
                # Page selection for extractor
                # ----------------------------
                # Priority:
                #   1. pages (impressum/contact/etc.)
                #   2. homepage (always last)
                pages = []
                pages.extend(crawl.pages or [])
                if crawl.homepage:
                    pages.append(crawl.homepage)

                extraction = extract_emails(domain, pages)

                print(
                    f"[OK] {domain} | emails={len(extraction.emails)} | pages_used={extraction.pages_used}"
                )

            except Exception as e:
                print(f"[ERROR] {domain}: {type(e).__name__}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
