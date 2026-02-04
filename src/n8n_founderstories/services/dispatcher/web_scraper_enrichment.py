# src/n8n_founderstories/services/dispatchers/web_scraper_enrichment.py

from __future__ import annotations

from uuid import uuid4

from  ..background_jobs import submit_job
from ..jobs.store import create_job
from ..web_scraper_enrichment.runner import run_web_scraper_enrichment_for_request

def dispatch_web_scraper_enrichment_job(*, request_id: str, spreadsheet_id: str) -> str:
    """
    Create a job record and submit the Step-1 web scraper enrichment runner.
    Mirrors the dispatch pattern used by other enrichment modules.
    """
    job_id = f"webscraper_{uuid4().hex}"

    create_job(
        job_id=job_id,
        tool="web_scraper_enrichment",
        request_id=request_id,
        meta={"spreadsheet_id": spreadsheet_id, "step": 1},
    )

    submit_job(
        run_web_scraper_enrichment_for_request,
        request_id=request_id,
        job_id=job_id,
        spreadsheet_id=spreadsheet_id,
    )

    return job_id
