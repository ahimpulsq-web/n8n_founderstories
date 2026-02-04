# C:\Projects\N8N-FounderStories\src\n8n_founderstories\services\web_scrapers\company_enrichment\runner.py

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from ....core.utils.text import norm
from ...jobs.store import mark_failed, mark_running, mark_succeeded
from ...master_data.repos import MasterResultsRepository
from ....core.config import DeterministicTestConfig  # uses your existing env-based config
from .enrich import enrich_domains

logger = logging.getLogger(__name__)


def _get_domains_from_master(*, request_id: str) -> list[str]:
    repo = MasterResultsRepository()
    success, error, results = repo.get_results_by_request(request_id=request_id)
    if not success:
        raise RuntimeError(f"Failed to load master_results for request_id={request_id}: {error}")

    domains: list[str] = []
    seen: set[str] = set()

    for r in results:
        d = (r.get("domain") or "").strip().lower()
        if not d:
            continue
        if d not in seen:
            seen.add(d)
            domains.append(d)

    return domains


def run_web_scraper_enrichment_job(
    *,
    job_id: str,
    request_id: str,
    spreadsheet_id: str,
) -> None:
    """
    Background-job entrypoint for web scraper enrichment.

    - Reads domains from master_results for this request_id
    - Runs web scraping enrichment (crawl + det + llm)
    - Marks job succeeded/failed with basic metrics
    """
    rid = norm(request_id)

    try:
        mark_running(job_id)

        # STEP 1: select domains from master_results
        domains = _get_domains_from_master(request_id=rid)

        # Optional: if no domains, succeed quickly
        if not domains:
            mark_succeeded(
                job_id,
                message="Web scraper enrichment skipped (no domains in master_results).",
                metrics={"domains": 0, "results": 0, "skipped": True},
            )
            return

        # STEP 2: load cfg from env so behavior matches your other runners
        cfg = DeterministicTestConfig.from_env()

        # STEP 3: run enrichment
        results = asyncio.run(
            enrich_domains(
                domains,
                cfg=cfg,
                allow_paths=None,
                language_hint="de",
            )
        )

        mark_succeeded(
            job_id,
            message="Web scraper enrichment completed",
            metrics={
                "domains": len(domains),
                "results": len(results),
            },
        )

    except Exception as e:
        logger.exception(
            "WEB_SCRAPER_ENRICHMENT_FAILED | request_id=%s | job_id=%s",
            rid,
            job_id,
        )
        mark_failed(
            job_id,
            error=str(e),
            message="Web scraper enrichment failed",
        )
