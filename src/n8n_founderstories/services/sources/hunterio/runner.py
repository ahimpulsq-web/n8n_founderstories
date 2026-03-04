"""
Hunter.io runner module.

Provides a high-level entrypoint for running Hunter.io searches
from search plan dictionaries. This is the primary interface used
by the API layer.

Architecture:
    API Layer (api/v1/hunter.py)
         ↓
    Runner (THIS MODULE)
         ↓
    Parser (parser.py) → HunterInput
         ↓
    Orchestrator (orchestrator.py) → run_hunter()
"""

from __future__ import annotations

from typing import Any

from .models import HunterInput
from .parser import parse_search_plan
from .orchestrator import run_hunter
from n8n_founderstories.services.jobs.status_writer import StatusWriterLike


def run_hunter_from_search_plan(
    *,
    search_plan: dict[str, Any],
    job_id: str | None = None,
    use_industries_filter: bool = True,
    status_writer: StatusWriterLike = None,
) -> list[dict[str, str]]:
    """
    Run Hunter.io search from a search plan dictionary.
    
    This is the primary entrypoint used by the API layer.
    It handles the conversion from search plan dict to HunterInput
    and delegates to the orchestrator.
    
    Workflow:
    1. Parse search plan dict → HunterInput (via parser.py)
    2. Run Hunter.io workflow (via orchestrator.py)
    3. Return cleaned results
    
    Args:
        search_plan: Search plan dictionary with keys:
            - request_id: Request identifier
            - prompt_target/prompt_target_en: Target query
            - prompt_keywords: List of keywords
            - matched_industries: Industry filters
            - resolved_locations: Location filters
            - sheet_id/google_sheet_id: Optional sheet ID for export
        job_id: Optional job ID for tracking (generated if not provided)
        use_industries_filter: Whether to apply industries filter
        status_writer: Optional JobsSheetWriter for live status updates
        
    Returns:
        List of cleaned row dictionaries with keys:
        - organization: Company name
        - domain: Company domain
        - location: Formatted location string
        - headcount: Headcount bucket
        - query: Search query that found this lead
        
    Raises:
        ValueError: If search plan validation fails
        Exception: If Hunter.io API calls or database operations fail
    """
    inp: HunterInput = parse_search_plan(search_plan)
    return run_hunter(
        inp=inp,
        job_id=job_id,
        use_industries_filter=use_industries_filter,
        status_writer=status_writer,
    )
