# =============================================================================
# C:\Projects\N8N-FounderStories\src\n8n_founderstories\api\v1\master.py
# =============================================================================

from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from ...core.utils.text import norm
from ...services.jobs.store import create_job
from ...services.search_plan import SearchPlan
from ...services.master_data.runner import run_master_job

router = APIRouter()


class MasterJobRequest(BaseModel):
    # Required
    search_plan: SearchPlan = Field(..., description="SearchPlan passed from n8n.")
    spreadsheet_id: str = Field(..., description="Target Google Spreadsheet ID.")

    # Optional controls
    source_tabs: list[str] = Field(
        default_factory=lambda: ["HunterIO", "GoogleMaps", "GoogleSearch"],
        description="Tabs to ingest into Master.",
    )

    # Optional mapping overrides (defaults applied in runner if omitted)
    domain_col_map: dict[str, int] | None = Field(
        default=None,
        description="Per-tab 0-based domain column index override. Example: {'HunterIO': 0}.",
    )
    column_map: dict[str, dict[str, int]] | None = Field(
        default=None,
        description=(
            "Per-tab 0-based column mapping override. Example: "
            "{'HunterIO': {'domain': 0, 'company': 1}, 'GoogleMaps': {'domain': 6, 'company': 0}}"
        ),
    )

    # Behavior flags
    apply_formatting: bool = Field(default=True)
    hide_state_tab: bool = Field(default=True, description="Hide Master_State tab.")
    hide_audit_tabs: bool = Field(default=True, description="Hide *_Audit tabs if present.")
    reorder_tabs: bool = Field(default=True)


class MasterJobResponse(BaseModel):
    status: str
    job_id: str
    request_id: str


@router.post("/master/jobs", response_model=MasterJobResponse, tags=["master"])
async def start_master_job(payload: MasterJobRequest, background_tasks: BackgroundTasks) -> MasterJobResponse:
    plan = payload.search_plan

    rid = norm(getattr(plan, "request_id", None))
    if not rid:
        raise HTTPException(status_code=400, detail="search_plan.request_id must not be empty.")

    sid = norm(payload.spreadsheet_id)
    if not sid:
        raise HTTPException(status_code=400, detail="spreadsheet_id must not be empty.")

    source_tabs = [norm(t) for t in (payload.source_tabs or []) if norm(t)]
    if not source_tabs:
        raise HTTPException(status_code=400, detail="source_tabs must not be empty.")

    job_id = f"master_{uuid4().hex}"

    create_job(
        job_id=job_id,
        tool="master",
        request_id=rid,
        meta={
            "spreadsheet_id": sid,
            "source_tabs": source_tabs,
            "apply_formatting": payload.apply_formatting,
            "hide_state_tab": payload.hide_state_tab,
            "hide_audit_tabs": payload.hide_audit_tabs,
            "reorder_tabs": payload.reorder_tabs,
            "domain_col_map": payload.domain_col_map,
            "column_map": payload.column_map,
        },
    )

    background_tasks.add_task(
        run_master_job,
        job_id=job_id,
        plan=plan,
        spreadsheet_id=sid,
        source_tabs=source_tabs,
        domain_col_map=payload.domain_col_map,
        column_map=payload.column_map,
        apply_formatting=payload.apply_formatting,
        hide_state_tab=payload.hide_state_tab,
        hide_audit_tabs=payload.hide_audit_tabs,
        reorder_tabs=payload.reorder_tabs,
    )

    return MasterJobResponse(status="accepted", job_id=job_id, request_id=rid)
