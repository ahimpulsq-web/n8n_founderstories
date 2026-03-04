"""
═══════════════════════════════════════════════════════════════════════════════
ENRICHMENT SERVICE - Contact Information Discovery Pipeline
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
    Automated discovery and extraction of contact information from company websites
    through a three-stage pipeline: Crawl → Extract → Aggregate

ARCHITECTURE:
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │  STAGE 1: CRAWL                                                     │
    │  ├─ Discovers contact/impressum pages using intelligent algorithms  │
    │  ├─ Extracts page content (HTML + Markdown)                         │
    │  └─ Stores results in crawl_results table                           │
    └─────────────────────────────────────────────────────────────────────┘
                                    ↓
    ┌─────────────────────────────────────────────────────────────────────┐
    │  STAGE 2: EXTRACT                                                   │
    │  ├─ Deterministic: Regex-based email extraction                     │
    │  └─ LLM: AI-powered extraction of emails, contacts, descriptions    │
    └─────────────────────────────────────────────────────────────────────┘
                                    ↓
    ┌─────────────────────────────────────────────────────────────────────┐
    │  STAGE 3: AGGREGATE                                                 │
    │  ├─ Combines deterministic + LLM results                            │
    │  ├─ Selects best company name and emails                            │
    │  └─ Stores final results in agg_results table                       │
    └─────────────────────────────────────────────────────────────────────┘

MODULES:

    crawl/          - Web crawling and contact page discovery
    extract/        - Information extraction (deterministic + LLM)
    aggregate/      - Result aggregation and selection
    models.py       - Shared data models

WORKERS:

    Three independent background workers process domains asynchronously:
    
    1. Crawler Worker (crawl.worker)
       - Monitors master_results for domains with crawl_status IS NULL
       - Crawls domains and stores results
       - Updates crawl_status to 'succeeded'/'failed'
    
    2. LLM Extraction Worker (extract.llm.worker)
       - Monitors master_results for domains with extraction_status IS NULL
       - Runs LLM extraction on crawled pages
       - Updates extraction_status to 'succeeded'/'failed'
    
    3. Aggregate Worker (aggregate.worker)
       - Monitors master_results for domains with agg_status IS NULL
       - Combines and selects best results
       - Updates agg_status to 'succeeded'/'failed'

WORKFLOW:

    1. Master service adds domains to master_results (all statuses NULL)
    2. Crawler worker picks up domains → crawls → sets crawl_status
    3. LLM worker picks up crawled domains → extracts → sets extraction_status
    4. Aggregate worker picks up extracted domains → aggregates → sets agg_status
    5. Sheets updater syncs final results to Google Sheets

KEY FEATURES:

    ✓ Global Reuse: Avoids re-crawling previously successful domains
    ✓ Parallel Processing: Three independent workers run concurrently
    ✓ Fault Tolerance: Each stage can fail independently without blocking others
    ✓ Incremental Progress: Results saved after each domain
    ✓ Clean Logging: Structured, minimal output for production

USAGE:

    # Workers start automatically with the application
    python -m n8n_founderstories
    
    # Workers are imported in main.py:
    from .services.enrichment.crawl.worker import run_worker as run_crawler
    from .services.enrichment.extract.llm.worker import run_worker as run_llm_worker
    from .services.enrichment.aggregate.worker import run_worker as run_aggregate_worker

═══════════════════════════════════════════════════════════════════════════════
"""

# Export shared models for convenience
from .models import (
    # Crawl models
    PageArtifact,
    CrawlArtifacts,
    
    # Deterministic extraction models
    DeterministicEmail,
    DeterministicExtraction,
    
    # LLM extraction models
    Evidence,
    LLMEmail,
    LLMContact,
    LLMCompany,
    LLMAbout,
    LLMExtraction,
)

__all__ = [
    # Crawl models
    "PageArtifact",
    "CrawlArtifacts",
    
    # Deterministic extraction models
    "DeterministicEmail",
    "DeterministicExtraction",
    
    # LLM extraction models
    "Evidence",
    "LLMEmail",
    "LLMContact",
    "LLMCompany",
    "LLMAbout",
    "LLMExtraction",
]