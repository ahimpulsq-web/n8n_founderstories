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

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from ...core.utils.text import norm
from ...services.jobs.store import create_job
from ...services.search_plan import SearchPlan
from ...services.outreach.hunterio.runner import run_hunter_job

router = APIRouter()


class HunterJobRequest(BaseModel):
    search_plan: SearchPlan = Field(..., description="SearchPlan passed from n8n.")
    spreadsheet_id: str = Field(..., description="Target Google Spreadsheet ID.")

    # n8n execution caps
    max_web_queries: int | None = Field(default=None, ge=0)
    max_keywords: int | None = Field(default=None, ge=0)

    # runtime controls
    target_unique_domains: int = Field(default=250, ge=1)
    max_cities_per_country: int = Field(default=4, ge=0)


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
            "max_web_queries": payload.max_web_queries,
            "max_keywords": payload.max_keywords,
            "target_unique_domains": payload.target_unique_domains,
            "max_cities_per_country": payload.max_cities_per_country,
            "keyword_execution": "ALL",
        },
    )

    background_tasks.add_task(
        run_hunter_job,
        job_id=job_id,
        plan=plan,
        spreadsheet_id=sid,
        max_web_queries=payload.max_web_queries,
        max_keywords=payload.max_keywords,
        target_unique_domains=payload.target_unique_domains,
        max_cities_per_country=payload.max_cities_per_country,
    )

    return HunterJobResponse(status="accepted", job_id=job_id, request_id=rid)
