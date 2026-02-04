"""
Google Search API endpoints.

This module provides FastAPI endpoints for web search operations
with job management and database persistence.
"""

from __future__ import annotations

from uuid import uuid4
from typing import List

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from ...core.utils.text import norm
from ...services.jobs.store import create_job
from ...services.search_plan import SearchPlan
from ...services.web_search.runner import run_web_search_job
from ...services.web_search.db.repos import (
    WebSearchResultsRepository,
    convert_db_results_to_sheets_format,
)
from ...services.database.config import db_config

router = APIRouter()


class GoogleSearchJobRequest(BaseModel):
    """Request model for starting a Google Search job."""
    search_plan: SearchPlan = Field(..., description="SearchPlan passed from n8n.")
    spreadsheet_id: str = Field(..., description="Target Google Spreadsheet ID (for n8n contract, not used for DB write).")
    max_pages: int = Field(default=1, ge=1, le=10, description="Maximum pages to search per query.")
    extract_blog_limit: int = Field(default=10, ge=0, le=50, description="Maximum number of blog pages to extract companies from.")


class GoogleSearchJobResponse(BaseModel):
    """Response model for job creation."""
    status: str
    job_id: str
    request_id: str


class GoogleSearchDatabaseResponse(BaseModel):
    """Response model for database queries with sheets parity."""
    headers: List[str] = Field(description="Column headers matching Google Sheets format")
    rows: List[List[str]] = Field(description="Data rows in sheets format")
    total_records: int = Field(description="Total number of records")


@router.post("/google_search/jobs", response_model=GoogleSearchJobResponse, tags=["google_search"])
async def start_google_search_job(
    payload: GoogleSearchJobRequest,
    background_tasks: BackgroundTasks,
) -> GoogleSearchJobResponse:
    """
    Start a new Google Search job.
    
    This endpoint:
    1. Validates the search plan
    2. Creates a job record
    3. Starts background processing with the web search pipeline
    4. Returns job_id for polling
    
    The job will:
    - Execute SERP search
    - Classify results (company/blog/other)
    - Extract companies from blog pages
    - Persist results to PostgreSQL database
    """
    plan = payload.search_plan
    rid = norm(getattr(plan, "request_id", None))
    sid = norm(payload.spreadsheet_id)
    
    # Validate required fields
    if not rid:
        raise HTTPException(
            status_code=400,
            detail="search_plan.request_id must not be empty."
        )
    
    if not sid:
        raise HTTPException(
            status_code=400,
            detail="spreadsheet_id must not be empty."
        )
    
    target_search = norm(getattr(plan, "target_search", None))
    if not target_search:
        raise HTTPException(
            status_code=400,
            detail="search_plan.target_search must not be empty."
        )
    
    # Create job
    job_id = f"google_search_{uuid4().hex}"
    
    create_job(
        job_id=job_id,
        tool="google_search",
        request_id=rid,
        meta={
            "spreadsheet_id": sid,
            "target_search": target_search,
            "max_pages": payload.max_pages,
            "extract_blog_limit": payload.extract_blog_limit,
        },
    )
    
    # Start background task
    background_tasks.add_task(
        run_web_search_job,
        job_id=job_id,
        plan=plan,
        spreadsheet_id=sid,
        max_pages=payload.max_pages,
        extract_blog_limit=payload.extract_blog_limit,
    )
    
    return GoogleSearchJobResponse(
        status="accepted",
        job_id=job_id,
        request_id=rid,
    )


@router.get("/google_search/db/job/{job_id}", response_model=GoogleSearchDatabaseResponse, tags=["google_search"])
async def get_google_search_results_by_job(job_id: str) -> GoogleSearchDatabaseResponse:
    """
    Get Google Search results from PostgreSQL for a specific job ID.
    
    Returns data in Google Sheets format with headers:
    ["Organisation", "Website", "Source Type", "Query", "Location", "Country", "Evidence/Reason", "Source URL"]
    
    This provides "Sheets parity" - the response format matches what you would see in Google Sheets.
    """
    if not db_config.is_enabled:
        raise HTTPException(
            status_code=503,
            detail="PostgreSQL integration is disabled. Enable with appropriate environment variables."
        )
    
    repo = WebSearchResultsRepository()
    success, error, results = repo.get_results_by_job(job_id)
    
    if not success:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve results: {error}"
        )
    
    # Convert to sheets format
    headers = [
        "Organisation",
        "Website",
        "Source Type",
        "Query",
        "Location",
        "Country",
        "Evidence/Reason",
        "Source URL",
    ]
    
    rows = convert_db_results_to_sheets_format(results)
    
    return GoogleSearchDatabaseResponse(
        headers=headers,
        rows=rows,
        total_records=len(rows),
    )


@router.get("/google_search/db/request/{request_id}", response_model=GoogleSearchDatabaseResponse, tags=["google_search"])
async def get_google_search_results_by_request(request_id: str) -> GoogleSearchDatabaseResponse:
    """
    Get Google Search results from PostgreSQL for a specific request ID.
    
    Returns data in Google Sheets format with headers:
    ["Organisation", "Website", "Source Type", "Query", "Location", "Country", "Evidence/Reason", "Source URL"]
    
    This provides "Sheets parity" - the response format matches what you would see in Google Sheets.
    """
    if not db_config.is_enabled:
        raise HTTPException(
            status_code=503,
            detail="PostgreSQL integration is disabled. Enable with appropriate environment variables."
        )
    
    repo = WebSearchResultsRepository()
    success, error, results = repo.get_results_by_request(request_id)
    
    if not success:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve results: {error}"
        )
    
    # Convert to sheets format
    headers = [
        "Organisation",
        "Website",
        "Source Type",
        "Query",
        "Location",
        "Country",
        "Evidence/Reason",
        "Source URL",
    ]
    
    rows = convert_db_results_to_sheets_format(results)
    
    return GoogleSearchDatabaseResponse(
        headers=headers,
        rows=rows,
        total_records=len(rows),
    )
