from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from ...core.utils.text import norm
from ...services.jobs.store import create_job
from ...services.search_plan import SearchPlan
from ...services.web_search.SerpAPI_GoogleSearch.runner import run_google_search_job
from .deps import require_serpapi_key, require_search_plan

router = APIRouter()


class GoogleSearchJobRequest(BaseModel):
    search_plan: SearchPlan = Field(..., description="SearchPlan passed from n8n.")
    spreadsheet_id: str = Field(..., description="Target Google Spreadsheet ID.")

    # Optional: protects against n8n retries creating duplicate jobs
    idempotency_key: str | None = Field(default=None, description="Optional idempotency key for safe retries.")

    # Optional caps (n8n controls)
    max_queries: int = Field(default=10, ge=1, le=100)
    max_results_per_query: int = Field(default=10, ge=1, le=100)
    max_total_results: int = Field(default=250, ge=1, le=5000)

    # Behavior controls
    dedupe_in_run: bool = Field(default=True)
    use_cache: bool = Field(default=True)

    # Optional runner policy (only if you implemented it in runner)
    # search_model: str = Field(default="geo_in_query", description="geo_in_query | location_routing | hybrid")


class GoogleSearchJobResponse(BaseModel):
    status: str
    job_id: str
    request_id: str


@router.post("/google_search/jobs", response_model=GoogleSearchJobResponse, tags=["google_search"])
async def start_google_search_job(payload: GoogleSearchJobRequest, background_tasks: BackgroundTasks) -> GoogleSearchJobResponse:
    plan = payload.search_plan

    # API boundary validation
    require_search_plan(plan)
    require_serpapi_key()

    request_id = norm(getattr(plan, "request_id", None))
    spreadsheet_id = norm(payload.spreadsheet_id)

    if not request_id:
        raise HTTPException(status_code=400, detail="search_plan.request_id must not be empty.")
    if not spreadsheet_id:
        raise HTTPException(status_code=400, detail="spreadsheet_id must not be empty.")

    web_queries = getattr(plan, "web_queries", None)
    if not isinstance(web_queries, list) or not any(norm(q) for q in web_queries):
        raise HTTPException(status_code=400, detail="search_plan.web_queries must not be empty.")

    # Deterministic default: if caller does not provide, fall back to request_id
    idem_key = norm(payload.idempotency_key) or request_id

    job_id = f"google_search_{uuid4().hex}"

    create_job(
        job_id=job_id,
        tool="google_search",
        request_id=request_id,
        meta={
            "spreadsheet_id": spreadsheet_id,
            "idempotency_key": idem_key,
            "max_queries": payload.max_queries,
            "max_results_per_query": payload.max_results_per_query,
            "max_total_results": payload.max_total_results,
            "dedupe_in_run": payload.dedupe_in_run,
            "use_cache": payload.use_cache,
            # "search_model": payload.search_model,
        },
    )

    background_tasks.add_task(
        run_google_search_job,
        job_id=job_id,
        plan=plan,
        spreadsheet_id=spreadsheet_id,
        max_queries=payload.max_queries,
        max_results_per_query=payload.max_results_per_query,
        max_total_results=payload.max_total_results,
        dedupe_in_run=payload.dedupe_in_run,
        use_cache=payload.use_cache,
        # search_model=payload.search_model,
    )

    return GoogleSearchJobResponse(status="accepted", job_id=job_id, request_id=request_id)
