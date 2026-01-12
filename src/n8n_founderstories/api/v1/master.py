# =============================================================================
# C:\Projects\N8N-FounderStories\src\n8n_founderstories\api\v1\master.py
# =============================================================================

from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from ...core.utils.text import norm
from ...core.errors import require_field
from ...services.jobs.store import create_job
from ...services.search_plan import SearchPlan
from ...services.master_data.runner import run_master_job_db_first

router = APIRouter()


class MasterJobRequest(BaseModel):
    # Required
    search_plan: SearchPlan = Field(..., description="SearchPlan passed from n8n.")
    spreadsheet_id: str = Field(..., description="Target Google Spreadsheet ID.")

    # Optional controls (DB-first)
    source_tools: list[str] | None = Field(
        default=None,
        description="Tool names to process (e.g., ['HunterIO', 'GoogleMaps']). If None, auto-detects.",
    )
    
    # Ingestion parameters
    window_size: int = Field(
        default=500,
        ge=10,
        le=2000,
        description="Number of rows to fetch per adapter per pass.",
    )
    max_empty_passes: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Stop after this many passes with no new data.",
    )
    linger_seconds: float = Field(
        default=3.0,
        ge=0.0,
        le=60.0,
        description="Sleep between passes (0 in test mode).",
    )
    
    # Export control
    export_to_sheets: bool = Field(
        default=True,
        description="Whether to export results to Sheets at job end.",
    )


class MasterJobResponse(BaseModel):
    status: str
    job_id: str
    request_id: str


@router.post("/master/jobs", response_model=MasterJobResponse, tags=["master"])
async def start_master_job(payload: MasterJobRequest, background_tasks: BackgroundTasks) -> MasterJobResponse:
    """
    Start a DB-first Master ingestion job.
    
    This endpoint:
    - Reads from tool DB tables (Hunter, Google Maps, etc.)
    - Aggregates results into master_results with idempotent upserts
    - Tracks watermarks for incremental ingestion
    - Optionally exports to Sheets at job end
    """
    plan = payload.search_plan

    # Validate required fields using centralized error handling
    rid = norm(getattr(plan, "request_id", None))
    require_field("request_id", rid, "search_plan.request_id")

    sid = norm(payload.spreadsheet_id)
    require_field("spreadsheet_id", sid)

    job_id = f"master_{uuid4().hex}"

    # Normalize source_tools if provided
    source_tools = None
    if payload.source_tools:
        source_tools = [norm(t) for t in payload.source_tools if norm(t)]

    create_job(
        job_id=job_id,
        tool="master",
        request_id=rid,
        meta={
            "spreadsheet_id": sid,
            "source_tools": source_tools,
            "window_size": payload.window_size,
            "max_empty_passes": payload.max_empty_passes,
            "linger_seconds": payload.linger_seconds,
            "export_to_sheets": payload.export_to_sheets,
            "db_first": True,  # Flag to indicate DB-first architecture
        },
    )

    background_tasks.add_task(
        run_master_job_db_first,
        job_id=job_id,
        request_id=rid,
        spreadsheet_id=sid,
        source_tools=source_tools,
        window_size=payload.window_size,
        max_empty_passes=payload.max_empty_passes,
        linger_seconds=payload.linger_seconds,
        export_to_sheets=payload.export_to_sheets,
    )

    return MasterJobResponse(status="accepted", job_id=job_id, request_id=rid)
