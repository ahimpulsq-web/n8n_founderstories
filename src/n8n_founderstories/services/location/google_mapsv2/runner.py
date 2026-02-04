from __future__ import annotations

from typing import Any, Optional

from .models import GooglePlacesInput
from .search_plan_parser import parse_search_plan
from .service import run_google_places
from ...jobs import JobsSheetWriter


def run_google_places_from_search_plan(
    *,
    search_plan: dict[str, Any],
    job_id: str | None = None,
    status_writer: Optional[JobsSheetWriter] = None,
) -> list[dict[str, str]]:
    """
    Single entrypoint:
    search_plan (dict) -> GooglePlacesInput -> run_google_places()
    
    Args:
        search_plan: Search plan dictionary
        job_id: Optional job ID for tracking
        status_writer: Optional JobsSheetWriter for live status updates
    """
    inp: GooglePlacesInput = parse_search_plan(search_plan)
    return run_google_places(inp=inp, job_id=job_id, status_writer=status_writer)