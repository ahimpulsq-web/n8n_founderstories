"""
Google Maps Places runner module.

Provides a high-level entrypoint for running Google Maps Places searches
from search plan dictionaries. This is the primary interface used
by the API layer.

Architecture:
    API Layer (api/v1/google_maps.py)
         ↓
    Runner (THIS MODULE)
         ↓
    Parser (parser.py) → GooglePlacesInput
         ↓
    Orchestrator (orchestrator.py) → run_google_places()
"""

from __future__ import annotations

from typing import Any

from .models import GooglePlacesInput
from .parser import parse_search_plan
from .orchestrator import run_google_places
from n8n_founderstories.services.jobs.status_writer import StatusWriterLike


def run_google_places_from_search_plan(
    *,
    search_plan: dict[str, Any],
    job_id: str | None = None,
    status_writer: StatusWriterLike = None,
) -> list[dict[str, str]]:
    """
    Run Google Maps Places search from a search plan dictionary.
    
    This is the primary entrypoint used by the API layer.
    It handles the conversion from search plan dict to GooglePlacesInput
    and delegates to the orchestrator.
    
    Workflow:
    1. Parse search plan dict → GooglePlacesInput (via parser.py)
    2. Run Google Maps Places workflow (via orchestrator.py)
    3. Return cleaned results
    
    Args:
        search_plan: Search plan dictionary with keys:
            - request_id: Request identifier
            - places_text_queries: List of search queries
            - resolved_locations: Location filters
            - sheet_id/google_sheet_id: Optional sheet ID for export
        job_id: Optional job ID for tracking (generated if not provided)
        status_writer: Optional JobsSheetWriter for live status updates
        
    Returns:
        List of cleaned row dictionaries with keys:
        - organization: Business name
        - domain: Website domain
        - location: Formatted location string
        - query: Search query that found this place
        
    Raises:
        ValueError: If search plan validation fails
        Exception: If Google Maps API calls or database operations fail
    """
    inp: GooglePlacesInput = parse_search_plan(search_plan)
    return run_google_places(inp=inp, job_id=job_id, status_writer=status_writer)