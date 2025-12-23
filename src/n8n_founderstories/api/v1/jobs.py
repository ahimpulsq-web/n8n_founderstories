from __future__ import annotations

# =============================================================================
# jobs.py
#
# Classification:
# - Role: universal job status endpoints for n8n polling.
# - Policy: read-only; job creation is done by tool endpoints (hunter/maps/etc.).
# =============================================================================

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ...services.jobs.store import load_job

router = APIRouter()


class JobStatusResponse(BaseModel):
    job_id: str
    tool: str
    request_id: str
    state: str
    progress: dict
    created_at: str
    started_at: str | None
    updated_at: str
    finished_at: str | None
    meta: dict
    error: str | None


@router.get("/jobs/{job_id}", response_model=JobStatusResponse, tags=["jobs"])
async def get_job_status(job_id: str) -> JobStatusResponse:
    job = load_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")

    return JobStatusResponse(
        job_id=job.job_id,
        tool=job.tool,
        request_id=job.request_id,
        state=job.state.value,
        progress=job.progress.model_dump(mode="python") if job.progress else {},
        created_at=job.created_at.isoformat(),
        started_at=job.started_at.isoformat() if job.started_at else None,
        updated_at=job.updated_at.isoformat(),
        finished_at=job.finished_at.isoformat() if job.finished_at else None,
        meta=dict(job.meta or {}),
        error=job.error,
    )
