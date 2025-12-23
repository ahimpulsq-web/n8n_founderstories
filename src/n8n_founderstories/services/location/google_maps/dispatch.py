from __future__ import annotations

# =============================================================================
# dispatch.py
# Dispatch wrapper for Google Maps jobs.
# =============================================================================

from ....core.utils.text import norm
from ....services.search_plan import SearchPlan
from ....services.location.google_maps.discover_runner import run_google_maps_discover_job
from ....services.location.google_maps.enrich_runner import run_google_maps_enrich_job


def run_google_maps_dispatch(
    *,
    job_id: str,
    plan: SearchPlan,
    spreadsheet_id: str,
    mode: str,
    # discover args
    max_queries: int = 5,
    max_locations: int = 3,
    max_results: int = 250,
    dedupe_places: bool = True,
    # enrich args
    max_items: int = 500,
    batch_size: int = 200,
    linger_seconds: float = 3.0,
    max_empty_batches: int = 10,
) -> None:
    m = norm(mode).lower()

    if m == "discover":
        run_google_maps_discover_job(
            job_id=job_id,
            plan=plan,
            spreadsheet_id=spreadsheet_id,
            max_queries=max_queries,
            max_locations=max_locations,
            max_results=max_results,
            dedupe_places=dedupe_places,
            enqueue_for_enrich=True,
        )
        return

    if m == "enrich":
        run_google_maps_enrich_job(
            job_id=job_id,
            plan=plan,
            spreadsheet_id=spreadsheet_id,
            max_items=max_items,
            batch_size=batch_size,
            linger_seconds=linger_seconds,
            max_empty_batches=max_empty_batches,
        )
        return

    raise ValueError("mode must be one of: discover | enrich")
