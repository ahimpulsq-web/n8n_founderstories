"""
Web Search Job Runner.

This module provides the job runner for web search operations,
integrating the existing pipeline with the job system and DB persistence.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from ...services.jobs.store import mark_running, mark_succeeded, mark_failed
from ...services.search_plan.models import SearchPlan
from .factory import build_web_search_deps
from .pipeline import run_pipeline
from .db.mapper import build_web_search_rows
from .db.safe_insert import safe_insert_web_search_results

logger = logging.getLogger(__name__)


def run_web_search_job(
    *,
    job_id: str,
    plan: SearchPlan,
    spreadsheet_id: str,  # accepted but unused for now
    max_pages: int = 1,
    engine: str = "google_light",
    resolve_locations: bool = True,
    classify_workers: int = 5,
    extract_blog_limit: int = 10,
) -> None:
    """
    Execute web search job with pipeline + DB persistence.
    
    This runner:
    1. Marks job as running
    2. Calls the existing pipeline (run_pipeline)
    3. Pipeline handles DB persistence internally
    4. Marks job as succeeded with metrics
    5. On error, marks job as failed
    
    Args:
        job_id: Job identifier
        plan: SearchPlan with request_id, target_search, etc.
        spreadsheet_id: Spreadsheet ID (accepted for n8n contract, not used for DB)
        max_pages: Maximum pages to search per query
        engine: Search engine to use (google_light, google, etc.)
        resolve_locations: Whether to resolve geo locations
        classify_workers: Number of parallel classification workers
        extract_blog_limit: Maximum number of blog pages to extract
    """
    try:
        # Mark job as running
        mark_running(job_id)
        logger.info(
            "Starting web search job | job_id=%s | request_id=%s | query=%s",
            job_id,
            plan.request_id,
            plan.target_search,
        )
        
        # Build dependencies
        deps = build_web_search_deps()
        
        # Convert SearchPlan to dict for pipeline
        search_plan_dict = {
            "request_id": plan.request_id,
            "target_search": plan.target_search,
            "prompt_language": getattr(plan, "prompt_language", None),
            "geo_location_keywords": getattr(plan, "geo_location_keywords", None) or {},
        }

        logger.info(
            "WEB_SEARCH_INPUT | query=%r | prompt_language=%r | geo_keys=%s",
            search_plan_dict.get("target_search"),
            search_plan_dict.get("prompt_language"),
            list((search_plan_dict.get("geo_location_keywords") or {}).keys()),
        )

        
        # Run pipeline (async function, so we need to run it in event loop)
        result = asyncio.run(
            run_pipeline(
                deps=deps,
                search_plan=search_plan_dict,
                max_pages=max_pages,
                engine=engine,
                resolve_locations=resolve_locations,
                classify_workers=classify_workers,
                extract_blog_limit=extract_blog_limit,
                job_id=job_id,
            )
        )
        
        # Persist to database
        rows = build_web_search_rows(
            job_id=job_id,
            search_plan=search_plan_dict,
            pipeline_out=result,
        )
        safe_insert_web_search_results(
            job_id=job_id,
            request_id=plan.request_id,
            rows=rows,
        )
        
        # Extract metrics from pipeline result
        hits_count = result.get("hits_count", 0)
        classified = result.get("classified", [])
        classified_count = len(classified)
        
        # Count company hits
        company_hits = sum(
            1 for item in classified
            if (item.get("classification") or {}).get("type") == "company"
        )
        
        # Count blog extractions
        blog_extractions = result.get("blog_extractions", [])
        blog_extractions_count = sum(
            len(extraction.get("companies", []))
            for extraction in blog_extractions
        )
        
        # Total inserted rows = company hits + blog extracted companies
        inserted_rows_count = company_hits + blog_extractions_count
        
        # Mark job as succeeded with metrics
        mark_succeeded(
            job_id,
            message=f"Web search completed: {inserted_rows_count} results",
            metrics={
                "hits_count": hits_count,
                "classified_count": classified_count,
                "company_hits_count": company_hits,
                "blog_extractions_count": blog_extractions_count,
                "inserted_rows_count": inserted_rows_count,
            },
        )
        
        logger.info(
            "Web search job succeeded | job_id=%s | hits=%d | classified=%d | companies=%d | blog_companies=%d | total_rows=%d",
            job_id,
            hits_count,
            classified_count,
            company_hits,
            blog_extractions_count,
            inserted_rows_count,
        )
        
    except Exception as exc:
        error_msg = str(exc)
        logger.exception(
            "Web search job failed | job_id=%s | error=%s",
            job_id,
            error_msg,
        )
        
        # Mark job as failed (do not crash the process)
        mark_failed(
            job_id,
            error=error_msg,
            message="Web search job failed.",
        )