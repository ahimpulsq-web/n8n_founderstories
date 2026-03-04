"""
═══════════════════════════════════════════════════════════════════════════════
CRAWL MODULE - Intelligent Contact Page Discovery System
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
    Automated discovery and extraction of contact information from company websites
    using intelligent case-based crawling and anchor text analysis.

ARCHITECTURE:
    ┌─────────────────────────────────────────────────────────────────────┐
    │  WORKER LAYER                                                       │
    │  ├─ worker.py: Background process that monitors for pending domains │
    │  └─ runner.py: Processes domains for a specific request_id          │
    └─────────────────────────────────────────────────────────────────────┘
                                    ↓
    ┌─────────────────────────────────────────────────────────────────────┐
    │  CORE LAYER                                                         │
    │  ├─ service.py: Case-based contact page discovery logic             │
    │  ├─ link_discovery.py: URL pattern matching                         │
    │  └─ text_link_finder.py: Anchor text analysis                       │
    └─────────────────────────────────────────────────────────────────────┘
                                    ↓
    ┌─────────────────────────────────────────────────────────────────────┐
    │  INFRASTRUCTURE LAYER                                               │
    │  ├─ crawl4ai_client.py: Adapter for browser automation              │
    │  ├─ crawl4ai_engine.py: Low-level browser control                   │
    │  ├─ global_limiter.py: Concurrency management                       │
    │  └─ repo.py: Database persistence                                   │
    └─────────────────────────────────────────────────────────────────────┘

MODULE STRUCTURE:

[WORKER] - Background Services
    worker.py          - Standalone crawler worker (polls database)
    runner.py          - Request-scoped domain processing

[CORE] - Business Logic
    service.py         - Case-based contact page discovery
    link_discovery.py  - URL pattern matching (Cases 5.x)
    text_link_finder.py - Anchor text analysis (Cases 1-3)

[INFRASTRUCTURE] - Technical Foundation
    crawl4ai_client.py - Adapter layer (CrawlResult → PageArtifact)
    crawl4ai_engine.py - Browser automation (Crawl4AI wrapper)
    global_limiter.py  - Global concurrency control
    repo.py            - Database persistence (crawl_results table)

CASE-BASED DISCOVERY:
    Case 4   - Hard failure (site broken/empty)
    Case 1   - Impressum via anchor text + truncation OK
    Case 2   - Impressum via anchor text + truncation failed
    Case 3   - Contact/Privacy via anchor text
    Case 5.1 - URL-based impressum + truncation OK
    Case 5.2 - URL-based impressum + truncation failed
    Case 5.3 - URL-based contact/privacy only

WORKFLOW:
    1. Master service adds domains to master_results (crawl_status = NULL)
    2. Worker polls database every 5 seconds
    3. Runner processes each request_id sequentially
    4. Service crawls domains using case-based algorithm
    5. Repo persists results to crawl_results table
    6. Master service updates crawl_status to 'succeeded'/'failed'

KEY FEATURES:
    ✓ Global Reuse: Avoids re-crawling previously successful domains
    ✓ Intelligent Discovery: Case-based algorithm for contact pages
    ✓ Anchor Text Analysis: Finds pages by link text (impressum, kontakt)
    ✓ URL Pattern Matching: Fallback when anchor text fails
    ✓ Content Truncation: Extracts relevant portions of impressum pages
    ✓ Concurrency Control: Global and per-domain limits
    ✓ Clean Logging: Minimal, focused output (no Rich console spam)

CONFIGURATION:
    Environment Variables:
    - CRAWL4AI_TIMEOUT_S: Page load timeout (default: 30.0)
    - CRAWL4AI_HEADLESS: Run browser headless (default: true)
    - CRAWL4AI_WAIT_AFTER_LOAD_S: Wait after page load (default: 0.1)
    - CRAWL_MAX_CONCURRENCY: Max concurrent browser tabs (default: 3)
    - CRAWL_GLOBAL_MAX_CONCURRENCY: Global concurrency limit (default: disabled)

USAGE:
    # Automatic (recommended):
    python -m n8n_founderstories  # Worker starts automatically
    
    # Manual (for testing):
    python run_crawler_worker.py --poll-interval 5

DEPENDENCIES:
    - crawl4ai: Browser automation library
    - playwright: Headless browser
    - psycopg: PostgreSQL database driver

═══════════════════════════════════════════════════════════════════════════════
"""