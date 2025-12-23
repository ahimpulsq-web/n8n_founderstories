from __future__ import annotations

# =============================================================================
# models.py
#
# Classification:
# - Role: Google Maps (Places Text Search + optional Details) data contract.
# - Consumers:
#   - runner.py (orchestration + sheets + persistence)
#   - api/v1/google_maps.py (request/response)
# - Design:
#   - Minimal fields required for Sheets + JSON artifact
#   - Keep raw payload for traceability
# =============================================================================

from typing import Any
from pydantic import BaseModel, Field


class GoogleMapsPlace(BaseModel):
    """
    Normalized place record (Text Search baseline).
    """
    name: str | None = None
    place_id: str | None = None
    formatted_address: str | None = None
    types: list[str] = Field(default_factory=list)

    business_status: str | None = None
    rating: float | None = None
    user_ratings_total: int | None = None

    geometry: dict[str, Any] | None = None

    # Enrichment fields (Place Details)
    google_maps_url: str | None = None
    website: str | None = None
    domain: str | None = None
    phone: str | None = None

    raw: dict[str, Any] = Field(default_factory=dict)


class GoogleMapsRunResult(BaseModel):
    """
    Audit record for one Places Text Search request.
    """
    phase: str = "TEXT_SEARCH"

    iso2: str
    hl_plan: str | None = None
    language_used: str

    location_label: str
    base_query: str
    final_query: str

    region_param: str

    take_n: int
    returned_count: int = 0
    eligible_after_dedupe: int = 0
    appended_rows: int = 0
    unique_places_job: int = 0

    stop_reason: str | None = None
    error: str | None = None

    meta: dict[str, Any] = Field(default_factory=dict)


class GoogleMapsJobResult(BaseModel):
    """
    Canonical JSON artifact for one Google Maps job execution.
    Mirrors HunterJobResult shape.
    """
    request_id: str
    raw_prompt: str
    provider_name: str | None = None
    geo: str | None = None

    max_queries: int
    max_locations: int
    max_results: int
    dedupe_places: bool
    enrich: bool

    queries_used: list[str] = Field(default_factory=list)
    locations_used: list[str] = Field(default_factory=list)  # labels used
    iso2_used: list[str] = Field(default_factory=list)

    total_runs_est: int = 0
    runs_done: int = 0

    total_unique_places: int = 0
    total_enriched_places: int = 0

    runs: list[GoogleMapsRunResult] = Field(default_factory=list)
    places: list[GoogleMapsPlace] = Field(default_factory=list)
