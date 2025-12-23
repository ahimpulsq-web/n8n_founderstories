from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from ...core.utils.text import norm
from ...services.jobs.store import create_job
from ...services.search_plan import SearchPlan
from ...services.web_scrapers.email_extractor.runner import run_email_extractor_job
from .deps import require_search_plan

router = APIRouter()

class EmailExtractorJobRequest(BaseModel):
    search_plan: SearchPlan = Field(..., description="SearchPlan passed from n8n.")
    spreadsheet_id: str = Field(..., description="Target Google Spreadsheet ID.")

    sheet_title: str = Field(default="Master", description="Sheet tab title.")

    # Linger mode (prevents race when Master is still filling)
    linger_seconds: float = Field(default=3.0, ge=0.0, le=60.0)
    max_empty_passes: int = Field(default=10, ge=0, le=200)

    # Bounded reads (no last-row scan)
    window_rows: int = Field(default=500, ge=10, le=5000)

    # Safety cap for one run
    max_total_updates: int = Field(default=5000, ge=1, le=50000)

class EmailExtractorJobResponse(BaseModel):
    status: str
    job_id: str
    request_id: str


@router.post("/email_extractor/jobs", response_model=EmailExtractorJobResponse, tags=["web_scrapper"])
async def start_email_extractor_job(
    payload: EmailExtractorJobRequest,
    background_tasks: BackgroundTasks,
) -> EmailExtractorJobResponse:
    plan = payload.search_plan
    require_search_plan(plan)

    rid = norm(getattr(plan, "request_id", None))
    sid = norm(payload.spreadsheet_id)
    tab = norm(payload.sheet_title) or "Master"

    if not rid:
        raise HTTPException(status_code=400, detail="search_plan.request_id must not be empty.")
    if not sid:
        raise HTTPException(status_code=400, detail="spreadsheet_id must not be empty.")

    job_id = f"email_extractor_{uuid4().hex}"

    create_job(
        job_id=job_id,
        tool="email_extractor",
        request_id=rid,
        meta={
            "spreadsheet_id": sid,
            "sheet_title": tab,
        },
    )

    background_tasks.add_task(
        run_email_extractor_job,
        job_id=job_id,
        request_id=rid,
        spreadsheet_id=sid,
        sheet_title=tab,
        linger_seconds=float(payload.linger_seconds),
        max_empty_passes=int(payload.max_empty_passes),
        window_rows=int(payload.window_rows),
        max_total_updates=int(payload.max_total_updates),
    )

    return EmailExtractorJobResponse(status="accepted", job_id=job_id, request_id=rid)
