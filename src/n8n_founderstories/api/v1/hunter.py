from __future__ import annotations

# =============================================================================
# hunter.py
#
# Classification:
# - Role: Start Hunter background jobs.
# - Input: SearchPlan from n8n.
# - Output: job_id for polling (GET /api/v1/jobs/{job_id})
# =============================================================================

from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

from ...core.utils.text import norm
from ...services.jobs.store import create_job
from ...services.search_plan import SearchPlan
from ...services.outreach.hunterio.runner import run_hunter_job
from ...services.exports.sheets_schema import HEADERS_HUNTER_MAIN, HEADERS_HUNTER_AUDIT
from ...services.outreach.hunterio.repos import HunterIOResultsRepository, HunterAuditRepository
from ...services.database.config import db_config

router = APIRouter()

# Backend-owned defaults - NOT user-configurable
DEFAULT_MAX_WEB_QUERIES = 15
DEFAULT_MAX_KEYWORDS = 15
DEFAULT_MAX_CITIES_PER_COUNTRY = 4


class HunterJobRequest(BaseModel):
    search_plan: SearchPlan = Field(..., description="SearchPlan passed from n8n.")
    spreadsheet_id: str = Field(..., description="Target Google Spreadsheet ID.")
    target_unique_domains: int = Field(default=350, ge=1, le=5000, description="Target number of unique domains to find.")


class HunterJobResponse(BaseModel):
    status: str
    job_id: str
    request_id: str


@router.post("/hunter/jobs", response_model=HunterJobResponse, tags=["hunter"])
async def start_hunter_job(payload: HunterJobRequest, background_tasks: BackgroundTasks) -> HunterJobResponse:
    plan = payload.search_plan
    rid = norm(getattr(plan, "request_id", None))
    sid = norm(payload.spreadsheet_id)

    if not rid:
        raise HTTPException(status_code=400, detail="search_plan.request_id must not be empty.")
    if not sid:
        raise HTTPException(status_code=400, detail="spreadsheet_id must not be empty.")

    job_id = f"hunter_{uuid4().hex}"

    create_job(
        job_id=job_id,
        tool="hunter",
        request_id=rid,
        meta={
            "spreadsheet_id": sid,
            "max_web_queries": DEFAULT_MAX_WEB_QUERIES,
            "max_keywords": DEFAULT_MAX_KEYWORDS,
            "target_unique_domains": payload.target_unique_domains,
            "max_cities_per_country": DEFAULT_MAX_CITIES_PER_COUNTRY,
            "keyword_execution": "ALL",
        },
    )

    background_tasks.add_task(
        run_hunter_job,
        job_id=job_id,
        plan=plan,
        spreadsheet_id=sid,
        max_web_queries=DEFAULT_MAX_WEB_QUERIES,
        max_keywords=DEFAULT_MAX_KEYWORDS,
        target_unique_domains=payload.target_unique_domains,
        max_cities_per_country=DEFAULT_MAX_CITIES_PER_COUNTRY,
    )

    return HunterJobResponse(status="accepted", job_id=job_id, request_id=rid)


class HunterDatabaseResponse(BaseModel):
    """Response model for Hunter database queries with sheets parity."""
    main: List[List[str]] = Field(description="Main Hunter companies data (same format as Google Sheets)")
    audit: List[List[str]] = Field(description="Audit Hunter data (same format as Google Sheets)")
    main_headers: List[str] = Field(description="Column headers for main data")
    audit_headers: List[str] = Field(description="Column headers for audit data")
    total_main_records: int = Field(description="Total number of main records")
    total_audit_records: int = Field(description="Total number of audit records")


class HunterCompaniesResponse(BaseModel):
    """Response model for Hunter companies only."""
    companies: List[List[str]] = Field(description="Hunter companies data")
    headers: List[str] = Field(description="Column headers")
    total_records: int = Field(description="Total number of records")


class HunterAuditResponse(BaseModel):
    """Response model for Hunter audit only."""
    audit: List[List[str]] = Field(description="Hunter audit data")
    headers: List[str] = Field(description="Column headers")
    total_records: int = Field(description="Total number of records")


@router.get("/hunter/db/{job_id}", response_model=HunterDatabaseResponse, tags=["hunter"])
async def get_hunter_data_by_job(job_id: str) -> HunterDatabaseResponse:
    """
    Get Hunter data from PostgreSQL for a specific job ID.
    
    Returns the same data structure as Google Sheets tabs:
    - main: Hunter companies data (HunterIO tab)
    - audit: Hunter audit data (HunterIO_Audit tab)
    
    This endpoint provides "Sheets parity" - the response format matches
    exactly what you would see in the Google Sheets tabs.
    """
    if not db_config.is_enabled:
        raise HTTPException(
            status_code=503,
            detail="PostgreSQL integration is disabled. Enable with HUNTER_COMPANIES_DB_ENABLED=true or HUNTER_AUDIT_DB_ENABLED=true"
        )
    
    # Get companies data
    companies_repo = HunterIOResultsRepository()
    companies_success, companies_error, companies_data = companies_repo.get_companies_by_job(job_id)
    
    if not companies_success:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve companies: {companies_error}")
    
    # Get audit data
    audit_repo = HunterAuditRepository()
    audit_success, audit_error, audit_data = audit_repo.get_audit_by_job(job_id)
    
    if not audit_success:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve audit data: {audit_error}")
    
    # Convert to sheets format (same column order as headers)
    main_rows = []
    for company in companies_data:
        row = [
            company.get('domain', ''),
            company.get('organization', ''),
            company.get('applied_location', ''),
            company.get('applied_headcount_bucket', ''),
            company.get('intended_location', ''),
            company.get('intended_headcount_bucket', ''),
            company.get('source_query', ''),
            company.get('query_type', ''),
        ]
        main_rows.append(row)
    
    audit_rows = []
    for audit in audit_data:
        row = [
            audit.get('job_id', ''),
            audit.get('request_id', ''),
            audit.get('query_type', ''),
            audit.get('intended_location', ''),
            audit.get('intended_headcount', ''),
            audit.get('applied_location', ''),
            audit.get('applied_headcount', ''),
            audit.get('query_text', ''),
            audit.get('keywords', ''),
            audit.get('keyword_match', ''),
            str(audit.get('total_results', '')),
            str(audit.get('returned_count', '')),
            str(audit.get('appended_rows', '')),
            str(audit.get('applied_filters', '')) if audit.get('applied_filters') else '',
        ]
        audit_rows.append(row)
    
    return HunterDatabaseResponse(
        main=main_rows,
        audit=audit_rows,
        main_headers=HUNTER_HEADERS_MAIN,
        audit_headers=HUNTER_HEADERS_AUDIT,
        total_main_records=len(main_rows),
        total_audit_records=len(audit_rows)
    )


@router.get("/hunter/db/{job_id}/companies", response_model=HunterCompaniesResponse, tags=["hunter"])
async def get_hunter_companies_by_job(
    job_id: str,
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Limit number of results"),
    offset: Optional[int] = Query(0, ge=0, description="Offset for pagination")
) -> HunterCompaniesResponse:
    """
    Get Hunter companies from PostgreSQL for a specific job ID.
    
    Returns only the main companies data (HunterIO tab equivalent).
    Supports pagination with limit and offset parameters.
    """
    if not db_config.is_hunter_companies_enabled:
        raise HTTPException(
            status_code=503,
            detail="Hunter companies PostgreSQL integration is disabled. Enable with HUNTER_COMPANIES_DB_ENABLED=true"
        )
    
    companies_repo = HunterIOResultsRepository()
    success, error, companies_data = companies_repo.get_companies_by_job(job_id)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve companies: {error}")
    
    # Apply pagination
    total_records = len(companies_data)
    if offset:
        companies_data = companies_data[offset:]
    if limit:
        companies_data = companies_data[:limit]
    
    # Convert to sheets format
    companies_rows = []
    for company in companies_data:
        row = [
            company.get('domain', ''),
            company.get('organization', ''),
            company.get('applied_location', ''),
            company.get('applied_headcount_bucket', ''),
            company.get('intended_location', ''),
            company.get('intended_headcount_bucket', ''),
            company.get('source_query', ''),
            company.get('query_type', ''),
        ]
        companies_rows.append(row)
    
    return HunterCompaniesResponse(
        companies=companies_rows,
        headers=HUNTER_HEADERS_MAIN,
        total_records=total_records
    )


@router.get("/hunter/db/{job_id}/audit", response_model=HunterAuditResponse, tags=["hunter"])
async def get_hunter_audit_by_job(
    job_id: str,
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Limit number of results"),
    offset: Optional[int] = Query(0, ge=0, description="Offset for pagination")
) -> HunterAuditResponse:
    """
    Get Hunter audit data from PostgreSQL for a specific job ID.
    
    Returns only the audit data (HunterIO_Audit tab equivalent).
    Supports pagination with limit and offset parameters.
    """
    if not db_config.is_hunter_audit_enabled:
        raise HTTPException(
            status_code=503,
            detail="Hunter audit PostgreSQL integration is disabled. Enable with HUNTER_AUDIT_DB_ENABLED=true"
        )
    
    audit_repo = HunterAuditRepository()
    success, error, audit_data = audit_repo.get_audit_by_job(job_id)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve audit data: {error}")
    
    # Apply pagination
    total_records = len(audit_data)
    if offset:
        audit_data = audit_data[offset:]
    if limit:
        audit_data = audit_data[:limit]
    
    # Convert to sheets format
    audit_rows = []
    for audit in audit_data:
        row = [
            audit.get('job_id', ''),
            audit.get('request_id', ''),
            audit.get('query_type', ''),
            audit.get('intended_location', ''),
            audit.get('intended_headcount', ''),
            audit.get('applied_location', ''),
            audit.get('applied_headcount', ''),
            audit.get('query_text', ''),
            audit.get('keywords', ''),
            audit.get('keyword_match', ''),
            str(audit.get('total_results', '')),
            str(audit.get('returned_count', '')),
            str(audit.get('appended_rows', '')),
            str(audit.get('applied_filters', '')) if audit.get('applied_filters') else '',
        ]
        audit_rows.append(row)
    
    return HunterAuditResponse(
        audit=audit_rows,
        headers=HUNTER_HEADERS_AUDIT,
        total_records=total_records
    )
