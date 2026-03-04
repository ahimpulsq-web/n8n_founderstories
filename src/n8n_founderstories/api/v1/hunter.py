from __future__ import annotations

import logging
from uuid import uuid4
from typing import Any, Dict

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from ...core.utils.text import norm
from ...services.jobs import create_job
from ...services.sheets.exports.jobs_tool_status import write_single_job_status
from ...services.sources.hunterio.runner import run_hunter_from_search_plan

logger = logging.getLogger(__name__)
router = APIRouter()


class HunterRunRequest(BaseModel):
    search_plan: Dict[str, Any] = Field(..., description="Search plan object (as produced by /search_plan).")
    sheet_id: str | None = Field(default=None, description="Target Google Sheet ID for export")
    use_industries_filter: bool = Field(default=True, description="Whether to apply the industries filter from matched_industries. Set to false to disable industry filtering.")


class HunterRunResponse(BaseModel):
    status: str
    job_id: str
    request_id: str


class StatusWriterImpl:
    """Implementation of StatusWriter protocol for writing job status to Google Sheets."""
    
    def __init__(self, sheet_id: str):
        self.sheet_id = sheet_id
    
    def write(self, *, job_id: str, tool: str, request_id: str, state: str) -> None:
        """Write a status update for a job to Google Sheets."""
        try:
            write_single_job_status(
                sheet_id=self.sheet_id,
                job_id=job_id,
                tool=tool,
                request_id=request_id,
                state=state,
            )
        except Exception as e:
            logger.warning("Failed to write job status: %s", e)


def _run_hunter_job(*, job_id: str, search_plan: Dict[str, Any], use_industries_filter: bool, sheet_id: str | None) -> None:
    """
    Background task that runs Hunter.
    State/progress updates are now handled inside the service layer.
    """
    request_id = norm(search_plan.get("request_id"))
    
    # Create status writer instance if sheet_id provided
    status_writer = None
    if sheet_id:
        status_writer = StatusWriterImpl(sheet_id)
    
    try:
        run_hunter_from_search_plan(
            search_plan=search_plan,
            job_id=job_id,
            use_industries_filter=use_industries_filter,
            status_writer=status_writer,
        )
        # Service layer handles all completion logging (COMPLETED, SHEETS, DATABASE)
    except Exception as e:
        logger.error(
            "HUNTERIOV2 | STATE=FAILED | request_id=%s | error=%s",
            request_id, str(e)
        )
        raise


@router.post("/hunter/run", response_model=HunterRunResponse, tags=["hunter"])
async def start_hunter_run(payload: HunterRunRequest, background_tasks: BackgroundTasks) -> HunterRunResponse:
    sp = payload.search_plan
    rid = norm(sp.get("request_id"))
    if not rid:
        raise HTTPException(status_code=400, detail="search_plan.request_id must not be empty.")
    
    sheet_id = payload.sheet_id
    if sheet_id:
        sp["sheet_id"] = sheet_id

    job_id = f"htrio__{uuid4().hex}"

    # OPTIMIZATION: Move ALL I/O operations to background task
    # Only create job record in memory (fast, no I/O)
    create_job(
        job_id=job_id,
        tool="hunteriov2",
        request_id=rid,
        meta={"sheet_id": sheet_id} if sheet_id else {},
    )

    # OPTIMIZATION: Move sheet initialization to background task
    # This removes 3-4 seconds of Google Sheets API calls from response path
    background_tasks.add_task(
        _run_hunter_job,
        job_id=job_id,
        search_plan=sp,
        use_industries_filter=payload.use_industries_filter,
        sheet_id=sheet_id,
    )

    # Return immediately - background task handles all I/O
    return HunterRunResponse(status="accepted", job_id=job_id, request_id=rid)
