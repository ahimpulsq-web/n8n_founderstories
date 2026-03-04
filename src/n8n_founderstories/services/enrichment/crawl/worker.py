"""
═══════════════════════════════════════════════════════════════════════════════
CRAWLER WORKER - Standalone Background Process
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [WORKER] - Background service that monitors and processes domains

PURPOSE:
    Continuously monitors the master_results table for domains that need crawling
    (crawl_status IS NULL) and processes them independently of the Master service.

ARCHITECTURE:
    ┌─────────────────────────────────────────────────────────────────────┐
    │  Master Service (Google Maps, Hunter.io)                            │
    │  └─> Updates master_results table                                   │
    │      └─> Sets crawl_status = NULL for new domains                   │
    └─────────────────────────────────────────────────────────────────────┘
                                    ↓
    ┌─────────────────────────────────────────────────────────────────────┐
    │  Crawler Worker (This File)                                         │
    │  └─> Polls database every 5 seconds                                 │
    │      └─> Finds domains with crawl_status IS NULL                    │
    │          └─> Processes them via runner.run_for_request()            │
    └─────────────────────────────────────────────────────────────────────┘

KEY FEATURES:
    - Decoupled: Runs independently from Master service
    - Single Instance: Only one worker should run at a time
    - Continuous: Polls database in infinite loop
    - Sequential: Processes requests one at a time
    - Automatic: Starts with application, no manual triggering needed

LIFECYCLE:
    1. Application starts → Worker starts automatically (via main.py)
    2. Worker polls database every 5 seconds
    3. Finds pending request_ids (domains with crawl_status IS NULL)
    4. Processes each request_id sequentially
    5. Application stops → Worker stops automatically (daemon thread)

USAGE:
    # Automatic (recommended):
    python -m n8n_founderstories  # Worker starts automatically
    
    # Manual (for testing):
    python run_crawler_worker.py --poll-interval 5

CONFIGURATION:
    - poll_interval_s: Seconds between polls (default: 5.0)
    - max_iterations: Max iterations before stopping (default: infinite)

DEPENDENCIES:
    - runner.py: Handles actual crawling for a request_id
    - Database: Reads from master_results table

THREAD SAFETY:
    - Designed to run as single instance
    - Multiple instances will cause duplicate work
    - Use daemon thread to ensure clean shutdown

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations
import asyncio
import logging
import time
from typing import Any
from uuid import uuid4

import psycopg

from n8n_founderstories.core.db import get_conn
from . import runner as crawl_runner
from . import cleanup as crawl_cleanup

logger = logging.getLogger(__name__)


def get_pending_requests(conn: psycopg.Connection[Any]) -> list[tuple[str, str]]:
    """
    Get all request_ids that have pending domains to crawl.
    
    Returns list of (request_id, job_id) tuples where at least one domain
    has crawl_status IS NULL.
    
    Args:
        conn: Active database connection
        
    Returns:
        List of (request_id, job_id) tuples with pending work
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT request_id, job_id
            FROM mstr_results
            WHERE crawl_status IS NULL
            ORDER BY request_id
        """)
        
        return [(row[0], row[1]) for row in cur.fetchall()]


def run_worker(
    poll_interval_s: float = 5.0,
    max_iterations: int | None = None
) -> None:
    """
    Run the crawler worker in a continuous loop.
    
    The worker:
    1. Polls master_results for pending domains (crawl_status IS NULL)
    2. Groups by request_id
    3. Processes each request_id sequentially
    4. Sleeps between iterations
    
    Note: No job tracking - crawler is a global worker that processes all requests.
    
    Args:
        poll_interval_s: Seconds to wait between poll cycles (default: 5.0)
        max_iterations: Maximum iterations before stopping (None = infinite)
    """
    iteration = 0
    
    try:
        while True:
            iteration += 1
            
            if max_iterations and iteration > max_iterations:
                break
            
            try:
                # Get pending requests
                conn = get_conn()
                try:
                    # Clean up stale processing statuses every iteration
                    try:
                        reset_count = crawl_cleanup.cleanup_stale_processing_status(
                            conn, timeout_minutes=5
                        )
                        
                        if reset_count > 0:
                            logger.warning(
                                "CRAWLER_WORKER | CLEANUP | reset_stale_domains=%d",
                                reset_count
                            )
                    except Exception as cleanup_error:
                        logger.error("CRAWLER_WORKER | CLEANUP_ERROR | error=%s", str(cleanup_error))
                    
                    # Get processing stats for monitoring
                    try:
                        stats = crawl_cleanup.get_processing_stats(conn)
                        logger.info(
                            "CRAWLER_WORKER | STATS | null=%d | processing=%d | succeeded=%d | failed=%d | reused=%d",
                            stats["null"],
                            stats["processing"],
                            stats["succeeded"],
                            stats["failed"],
                            stats["reused"]
                        )
                    except Exception as stats_error:
                        logger.error("CRAWLER_WORKER | STATS_ERROR | error=%s", str(stats_error))
                    
                    pending_requests = get_pending_requests(conn)
                finally:
                    conn.close()
                
                if not pending_requests:
                    time.sleep(poll_interval_s)
                    continue
                
                logger.info("CRAWLER_WORKER | PROCESSING | pending_requests=%d", len(pending_requests))
                
                # Process each request sequentially
                for idx, (request_id, job_id) in enumerate(pending_requests, 1):
                    try:
                        result = crawl_runner.run_for_request(
                            request_id=request_id,
                            job_id=job_id
                        )
                    except Exception as e:
                        logger.error("CRAWLER_WORKER | REQUEST_ERROR | request_id=%s | error=%s",
                                   request_id, str(e), exc_info=True)
                        # Continue with next request
                        continue
                
                # Brief pause before next poll
                time.sleep(poll_interval_s)
            
            except KeyboardInterrupt:
                break
            
            except Exception as e:
                logger.error("CRAWLER_WORKER | ERROR | error=%s", str(e))
                # Sleep longer on error to avoid tight error loop
                time.sleep(poll_interval_s * 2)
    
    except Exception as e:
        logger.error("CRAWLER_WORKER | FATAL_ERROR | error=%s", str(e))
        raise


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)-8s %(name)s | %(message)s"
    )
    
    # Run worker
    run_worker()