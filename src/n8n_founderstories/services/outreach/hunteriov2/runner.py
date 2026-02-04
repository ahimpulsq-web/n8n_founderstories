from __future__ import annotations

from typing import Any, Optional

from .models import HunterInput
from .search_plan_parser import parse_search_plan
from .service import run_hunter
from ...jobs import JobsSheetWriter


def run_hunter_from_search_plan(
    *,
    search_plan: dict[str, Any],
    job_id: str | None = None,
    use_industries_filter: bool = True,
    status_writer: Optional[JobsSheetWriter] = None,
) -> list[dict[str, str]]:
    """
    Single entrypoint:
    search_plan (dict) -> HunterInput -> run_hunter()
    
    Args:
        search_plan: Search plan dictionary
        job_id: Optional job ID for tracking
        use_industries_filter: Whether to apply industries filter
        status_writer: Optional JobsSheetWriter for live status updates
    """
    inp: HunterInput = parse_search_plan(search_plan)
    return run_hunter(
        inp=inp,
        job_id=job_id,
        use_industries_filter=use_industries_filter,
        status_writer=status_writer,
    )
