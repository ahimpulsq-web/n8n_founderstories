from __future__ import annotations

import logging
from uuid import uuid4
from typing import Any, Dict

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from ...core.utils.text import norm
from ...core.db import get_conn
from ...services.jobs import create_job, JobsSheetWriter
from ...services.location.google_mapsv2.runner import run_google_places_from_search_plan

logger = logging.getLogger(__name__)
router = APIRouter()


class GoogleMapsRunRequest(BaseModel):
    search_plan: Dict[str, Any] = Field(..., description="Search plan object (as produced by /search_plan).")
    sheet_id: str | None = Field(default=None, description="Target Google Sheet ID for export")


class GoogleMapsRunResponse(BaseModel):
    status: str
    job_id: str
    request_id: str


def _run_google_maps_job(*, job_id: str, search_plan: Dict[str, Any], sheet_id: str | None) -> None:
    """
    Background task that runs Google Maps Places search.
    State/progress updates are now handled inside the service layer.
    """
    request_id = norm(search_plan.get("request_id"))
    
    # Create status writer if sheet_id provided
    status_writer = None
    if sheet_id:
        try:
            status_writer = JobsSheetWriter(sheet_id=sheet_id)
        except Exception as e:
            logger.warning("Failed to create JobsSheetWriter: %s", e)
    
    try:
        run_google_places_from_search_plan(
            search_plan=search_plan,
            job_id=job_id,
            status_writer=status_writer,
        )
        
        # Query database for actual persisted row count
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM googlemaps_places_results WHERE job_id=%s",
                    (job_id,),
                )
                db_count = int(cur.fetchone()[0])
        finally:
            conn.close()
        
        logger.info(
            "GOOGLEMAPSV2 | STATE=COMPLETED | job_id=%s | results=%d",
            job_id, db_count
        )
    except Exception as e:
        logger.error(
            "GOOGLEMAPSV2 | STATE=FAILED | request_id=%s | error=%s",
            request_id, str(e)
        )
        raise


@router.post("/google_maps/run", response_model=GoogleMapsRunResponse, tags=["google_maps"])
async def start_google_maps_run(payload: GoogleMapsRunRequest, background_tasks: BackgroundTasks) -> GoogleMapsRunResponse:
    sp = payload.search_plan
    rid = norm(sp.get("request_id"))
    if not rid:
        raise HTTPException(status_code=400, detail="search_plan.request_id must not be empty.")
    
    sheet_id = payload.sheet_id
    if sheet_id:
        sp["sheet_id"] = sheet_id

    job_id = f"gmap__{uuid4().hex}"

    create_job(
        job_id=job_id,
        tool="googlemapsv2",
        request_id=rid,
        meta={"sheet_id": sheet_id} if sheet_id else {},
    )

    # CRITICAL: Write initial RUNNING row to Tool_Status IMMEDIATELY
    # This ensures the sheet exists and shows the job before background work starts
    if sheet_id:
        try:
            status_writer = JobsSheetWriter(sheet_id=sheet_id)
            status_writer.write(
                job_id=job_id,
                tool="google_maps",
                request_id=rid,
                state="RUNNING",
                current=0,
                total=0,
            )
        except Exception as e:
            logger.warning("Failed to write initial Tool_Status row: %s", e)

    background_tasks.add_task(
        _run_google_maps_job,
        job_id=job_id,
        search_plan=sp,
        sheet_id=sheet_id,
    )

    return GoogleMapsRunResponse(status="accepted", job_id=job_id, request_id=rid)