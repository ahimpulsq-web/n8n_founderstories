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
# =============================================================================

from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from ...core.utils.text import norm
from ...services.jobs.store import create_job
from ...services.search_plan import SearchPlan
from ...services.location.google_maps.dispatch import run_google_maps_dispatch
from .deps import require_google_maps_key, require_search_plan

router = APIRouter()


class GoogleMapsJobRequest(BaseModel):
    # Required
    search_plan: SearchPlan = Field(..., description="SearchPlan passed from n8n.")
    spreadsheet_id: str = Field(..., description="Target Google Spreadsheet ID.")

    # Mode
    mode: str = Field(default="discover", description="discover | enrich")

    # Discover controls
    max_queries: int = Field(default=5, ge=1, le=50)
    max_locations: int = Field(default=3, ge=0, le=25)
    max_results: int = Field(default=250, ge=1, le=5000)
    dedupe_places: bool = Field(default=True)

    # Enrich controls
    max_items: int = Field(default=500, ge=1, le=5000)
    batch_size: int = Field(default=200, ge=1, le=1000)

    # Enrich linger controls (robust enrichment runs)
    linger_seconds: float = Field(default=3.0, ge=0.0, le=60.0)
    max_empty_batches: int = Field(default=10, ge=0, le=200)


class GoogleMapsJobResponse(BaseModel):
    status: str
    job_id: str
    request_id: str
    discover_job_id: str | None = None
    enrich_job_id: str | None = None


@router.post("/google_maps/jobs", response_model=GoogleMapsJobResponse, tags=["location"])
async def start_google_maps_job(
    payload: GoogleMapsJobRequest,
    background_tasks: BackgroundTasks,
) -> GoogleMapsJobResponse:
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
                "max_queries": payload.max_queries,
                "max_locations": payload.max_locations,
                "max_results": payload.max_results,
                "dedupe_places": payload.dedupe_places,
            },
        )

        background_tasks.add_task(
            run_google_maps_dispatch,
            job_id=job_id,
            plan=plan,
            spreadsheet_id=sid,
            mode="discover",
            max_queries=payload.max_queries,
            max_locations=payload.max_locations,
            max_results=payload.max_results,
            dedupe_places=payload.dedupe_places,
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
            "max_queries": payload.max_queries,
            "max_locations": payload.max_locations,
            "max_results": payload.max_results,
            "dedupe_places": payload.dedupe_places,
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
            "max_items": payload.max_items,
            "batch_size": payload.batch_size,
            "linger_seconds": float(payload.linger_seconds),
            "max_empty_batches": int(payload.max_empty_batches),
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
        max_queries=payload.max_queries,
        max_locations=payload.max_locations,
        max_results=payload.max_results,
        dedupe_places=payload.dedupe_places,
    )

    # Start enrichment (will linger until queue items exist)
    background_tasks.add_task(
        run_google_maps_dispatch,
        job_id=enrich_job_id,
        plan=plan,
        spreadsheet_id=sid,
        mode="enrich",
        max_items=payload.max_items,
        batch_size=payload.batch_size,
        linger_seconds=float(payload.linger_seconds),
        max_empty_batches=int(payload.max_empty_batches),
    )

    return GoogleMapsJobResponse(
        status="accepted",
        job_id=parent_job_id,
        request_id=rid,
        discover_job_id=discover_job_id,
        enrich_job_id=enrich_job_id,
    )
