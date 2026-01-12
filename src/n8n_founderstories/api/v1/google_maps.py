from __future__ import annotations

# =============================================================================
# google_maps.py
#
# Classification:
# - Role: Start Google Maps background jobs.
# - Input: SearchPlan from n8n + spreadsheet_id.
# - Output: job_id for polling (GET /api/v1/jobs/{job_id})
#
# Policy:
# - SearchPlan is mandatory and authoritative.
# - mode=discover -> discovery job (queues enrich items)
# - mode=enrich   -> ALWAYS start discovery job + enrichment job
#   (enrichment uses linger to wait for queued items)
# - User-friendly API: only mode and max_results exposed to users
# - Internal defaults are system-controlled and not user-configurable
# =============================================================================

from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from fastapi import Query, HTTPException

from ...core.utils.text import norm
from ...services.jobs.store import create_job
from ...services.search_plan import SearchPlan
from ...services.location.google_maps.dispatch import run_google_maps_dispatch
from ...services.location.google_maps.repos import GoogleMapsResultsRepository, GoogleMapsEnrichedRepository, convert_db_results_to_sheets_format, convert_db_enriched_to_sheets_format
from ...services.database.config import db_config
from .deps import require_google_maps_key, require_search_plan

router = APIRouter()

# Backend-owned defaults - NOT user-configurable
DEFAULT_MAX_QUERIES = 5
DEFAULT_MAX_LOCATIONS = 3
DEFAULT_DEDUPE_PLACES = True
DEFAULT_MAX_ITEMS = 500
DEFAULT_ENRICH_BATCH_SIZE = 200
DEFAULT_LINGER_SECONDS = 3.0
DEFAULT_MAX_EMPTY_BATCHES = 10


class GoogleMapsJobRequest(BaseModel):
    """
    Simplified Google Maps job request.
    
    Users only need to specify:
    - search_plan: SearchPlan from n8n (required, authoritative)
    - spreadsheet_id: Target Google Spreadsheet ID (required)
    - mode: "discover" to find places, "enrich" to find and enrich places (default: "discover")
    - max_results: Target number of places to collect (default: 250)
    
    All other parameters are system-controlled for production stability.
    """
    search_plan: SearchPlan = Field(..., description="SearchPlan passed from n8n.")
    spreadsheet_id: str = Field(..., description="Target Google Spreadsheet ID.")
    mode: str = Field(default="discover", description="discover | enrich")
    max_results: int = Field(default=350, ge=1, le=5000, description="Target number of places to collect.")


class GoogleMapsJobResponse(BaseModel):
    status: str
    job_id: str
    request_id: str
    discover_job_id: str | None = None
    enrich_job_id: str | None = None


class GoogleMapsResultsResponse(BaseModel):
    """Response model for Google Maps results from database."""
    results: list[list[str]]
    headers: list[str]
    total_records: int


class GoogleMapsEnrichedResponse(BaseModel):
    """Response model for Google Maps enriched data from database."""
    enriched: list[list[str]]
    headers: list[str]
    total_records: int


class GoogleMapsDbResponse(BaseModel):
    """Combined response model for Google Maps database data."""
    results: list[list[str]]
    enriched: list[list[str]]
    results_headers: list[str]
    enriched_headers: list[str]
    total_results: int
    total_enriched: int


@router.post("/google_maps/jobs", response_model=GoogleMapsJobResponse, tags=["location"])
async def start_google_maps_job(
    payload: GoogleMapsJobRequest,
    background_tasks: BackgroundTasks,
) -> GoogleMapsJobResponse:
    """
    Start a Google Maps job with simplified user interface.
    
    Users specify:
    - mode: "discover" to find places, "enrich" to find and enrich places
    - max_results: target number of places to collect
    
    All other parameters use production-optimized defaults.
    """
    plan = payload.search_plan

    require_search_plan(plan)
    require_google_maps_key()

    rid = norm(getattr(plan, "request_id", None))
    if not rid:
        raise HTTPException(status_code=400, detail="search_plan.request_id must not be empty.")

    sid = norm(payload.spreadsheet_id)
    if not sid:
        raise HTTPException(status_code=400, detail="spreadsheet_id must not be empty.")

    mode = norm(payload.mode).lower()
    if mode not in {"discover", "enrich"}:
        raise HTTPException(status_code=400, detail="mode must be one of: discover | enrich")

    # Discovery inputs are required for BOTH modes because enrich implies discover
    maps_queries = getattr(plan, "maps_queries", None)
    if not isinstance(maps_queries, list) or not any(norm(q) for q in maps_queries):
        raise HTTPException(status_code=400, detail="search_plan.maps_queries must not be empty.")

    geo_buckets = getattr(plan, "geo_location_keywords", None)
    if not isinstance(geo_buckets, dict) or not geo_buckets:
        raise HTTPException(status_code=400, detail="search_plan.geo_location_keywords must not be empty.")

    # mode=discover: single job
    if mode == "discover":
        job_id = f"google_maps_discover_{uuid4().hex}"

        create_job(
            job_id=job_id,
            tool="google_maps",
            request_id=rid,
            meta={
                "spreadsheet_id": sid,
                "mode": "discover",
                "max_queries": DEFAULT_MAX_QUERIES,
                "max_locations": DEFAULT_MAX_LOCATIONS,
                "max_results": payload.max_results,
                "dedupe_places": DEFAULT_DEDUPE_PLACES,
            },
        )

        background_tasks.add_task(
            run_google_maps_dispatch,
            job_id=job_id,
            plan=plan,
            spreadsheet_id=sid,
            mode="discover",
            max_queries=DEFAULT_MAX_QUERIES,
            max_locations=DEFAULT_MAX_LOCATIONS,
            max_results=payload.max_results,
            dedupe_places=DEFAULT_DEDUPE_PLACES,
        )

        return GoogleMapsJobResponse(status="accepted", job_id=job_id, request_id=rid)

    # mode=enrich: ALWAYS start discovery + enrichment
    parent_job_id = f"google_maps_enrich_with_discover_{uuid4().hex}"
    discover_job_id = f"google_maps_discover_{uuid4().hex}"
    enrich_job_id = f"google_maps_enrich_{uuid4().hex}"

    create_job(
        job_id=parent_job_id,
        tool="google_maps",
        request_id=rid,
        meta={
            "spreadsheet_id": sid,
            "mode": "enrich",
            "max_results": payload.max_results,
            "discover_job_id": discover_job_id,
            "enrich_job_id": enrich_job_id,
        },
    )

    create_job(
        job_id=discover_job_id,
        tool="google_maps",
        request_id=rid,
        meta={
            "spreadsheet_id": sid,
            "mode": "discover",
            "max_queries": DEFAULT_MAX_QUERIES,
            "max_locations": DEFAULT_MAX_LOCATIONS,
            "max_results": payload.max_results,
            "dedupe_places": DEFAULT_DEDUPE_PLACES,
            "started_by": parent_job_id,
        },
    )

    create_job(
        job_id=enrich_job_id,
        tool="google_maps",
        request_id=rid,
        meta={
            "spreadsheet_id": sid,
            "mode": "enrich",
            "max_items": DEFAULT_MAX_ITEMS,
            "batch_size": DEFAULT_ENRICH_BATCH_SIZE,
            "linger_seconds": DEFAULT_LINGER_SECONDS,
            "max_empty_batches": DEFAULT_MAX_EMPTY_BATCHES,
            "started_by": parent_job_id,
        },
    )

    # Start discovery first (queues items)
    background_tasks.add_task(
        run_google_maps_dispatch,
        job_id=discover_job_id,
        plan=plan,
        spreadsheet_id=sid,
        mode="discover",
        max_queries=DEFAULT_MAX_QUERIES,
        max_locations=DEFAULT_MAX_LOCATIONS,
        max_results=payload.max_results,
        dedupe_places=DEFAULT_DEDUPE_PLACES,
    )

    # Start enrichment (will linger until queue items exist)
    background_tasks.add_task(
        run_google_maps_dispatch,
        job_id=enrich_job_id,
        plan=plan,
        spreadsheet_id=sid,
        mode="enrich",
        max_items=DEFAULT_MAX_ITEMS,
        batch_size=DEFAULT_ENRICH_BATCH_SIZE,
        linger_seconds=DEFAULT_LINGER_SECONDS,
        max_empty_batches=DEFAULT_MAX_EMPTY_BATCHES,
    )

    return GoogleMapsJobResponse(
        status="accepted",
        job_id=parent_job_id,
        request_id=rid,
        discover_job_id=discover_job_id,
        enrich_job_id=enrich_job_id,
    )


# Headers for Google Maps results (matching discover_runner.py HEADERS_MAIN)
GOOGLE_MAPS_HEADERS_MAIN = [
    "Place Name",
    "Location Label",
    "Address",
    "Place ID",
    "Type",
    "Website",
    "Domain",
    "Phone",
    "Search Query",
    "Business Status",
    "Google Maps URL",
]

# Headers for Google Maps enriched data
GOOGLE_MAPS_HEADERS_ENRICHED = [
    "Place ID",
    "Rating",
    "Reviews Count",
    "Photos Count",
    "Opening Hours",
]


@router.get("/google_maps/db/{job_id}", response_model=GoogleMapsDbResponse, tags=["location"])
async def get_google_maps_db_by_job(
    job_id: str,
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Limit number of results"),
    offset: Optional[int] = Query(0, ge=0, description="Offset for pagination")
) -> GoogleMapsDbResponse:
    """
    Get Google Maps data from PostgreSQL for a specific job ID.
    
    Returns both results and enriched data with headers and total counts.
    Supports pagination with limit and offset parameters.
    """
    if not db_config.is_google_maps_results_enabled and not db_config.is_google_maps_enriched_enabled:
        raise HTTPException(
            status_code=503,
            detail="Google Maps PostgreSQL integration is disabled. Enable with GOOGLE_MAPS_RESULTS_DB_ENABLED=true and/or GOOGLE_MAPS_ENRICHED_DB_ENABLED=true"
        )
    
    results_data = []
    enriched_data = []
    
    # Get results data if enabled
    if db_config.is_google_maps_results_enabled:
        results_repo = GoogleMapsResultsRepository()
        success, error, results_raw = results_repo.get_results_by_job(job_id)
        
        if not success:
            raise HTTPException(status_code=500, detail=f"Failed to retrieve results: {error}")
        
        results_data = results_raw
    
    # Get enriched data if enabled
    if db_config.is_google_maps_enriched_enabled:
        enriched_repo = GoogleMapsEnrichedRepository()
        success, error, enriched_raw = enriched_repo.get_enriched_by_job(job_id)
        
        if not success:
            raise HTTPException(status_code=500, detail=f"Failed to retrieve enriched data: {error}")
        
        enriched_data = enriched_raw
    
    # Apply pagination to results
    total_results = len(results_data)
    if offset:
        results_data = results_data[offset:]
    if limit:
        results_data = results_data[:limit]
    
    # Apply pagination to enriched (same pagination)
    total_enriched = len(enriched_data)
    if offset:
        enriched_data = enriched_data[offset:]
    if limit:
        enriched_data = enriched_data[:limit]
    
    # Convert to sheets format
    results_rows = convert_db_results_to_sheets_format(results_data)
    enriched_rows = convert_db_enriched_to_sheets_format(enriched_data)
    
    return GoogleMapsDbResponse(
        results=results_rows,
        enriched=enriched_rows,
        results_headers=GOOGLE_MAPS_HEADERS_MAIN,
        enriched_headers=GOOGLE_MAPS_HEADERS_ENRICHED,
        total_results=total_results,
        total_enriched=total_enriched
    )


@router.get("/google_maps/db/{job_id}/results", response_model=GoogleMapsResultsResponse, tags=["location"])
async def get_google_maps_results_by_job(
    job_id: str,
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Limit number of results"),
    offset: Optional[int] = Query(0, ge=0, description="Offset for pagination")
) -> GoogleMapsResultsResponse:
    """
    Get Google Maps results from PostgreSQL for a specific job ID.
    
    Returns only the results data (GoogleMaps tab equivalent).
    Supports pagination with limit and offset parameters.
    """
    if not db_config.is_google_maps_results_enabled:
        raise HTTPException(
            status_code=503,
            detail="Google Maps results PostgreSQL integration is disabled. Enable with GOOGLE_MAPS_RESULTS_DB_ENABLED=true"
        )
    
    results_repo = GoogleMapsResultsRepository()
    success, error, results_data = results_repo.get_results_by_job(job_id)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve results: {error}")
    
    # Apply pagination
    total_records = len(results_data)
    if offset:
        results_data = results_data[offset:]
    if limit:
        results_data = results_data[:limit]
    
    # Convert to sheets format
    results_rows = convert_db_results_to_sheets_format(results_data)
    
    return GoogleMapsResultsResponse(
        results=results_rows,
        headers=GOOGLE_MAPS_HEADERS_MAIN,
        total_records=total_records
    )


@router.get("/google_maps/db/{job_id}/enriched", response_model=GoogleMapsEnrichedResponse, tags=["location"])
async def get_google_maps_enriched_by_job(
    job_id: str,
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Limit number of results"),
    offset: Optional[int] = Query(0, ge=0, description="Offset for pagination")
) -> GoogleMapsEnrichedResponse:
    """
    Get Google Maps enriched data from PostgreSQL for a specific job ID.
    
    Returns only the enriched data.
    Supports pagination with limit and offset parameters.
    """
    if not db_config.is_google_maps_enriched_enabled:
        raise HTTPException(
            status_code=503,
            detail="Google Maps enriched PostgreSQL integration is disabled. Enable with GOOGLE_MAPS_ENRICHED_DB_ENABLED=true"
        )
    
    enriched_repo = GoogleMapsEnrichedRepository()
    success, error, enriched_data = enriched_repo.get_enriched_by_job(job_id)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve enriched data: {error}")
    
    # Apply pagination
    total_records = len(enriched_data)
    if offset:
        enriched_data = enriched_data[offset:]
    if limit:
        enriched_data = enriched_data[:limit]
    
    # Convert to sheets format
    enriched_rows = convert_db_enriched_to_sheets_format(enriched_data)
    
    return GoogleMapsEnrichedResponse(
        enriched=enriched_rows,
        headers=GOOGLE_MAPS_HEADERS_ENRICHED,
        total_records=total_records
    )
