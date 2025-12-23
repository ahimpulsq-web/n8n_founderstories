from __future__ import annotations

# =============================================================================
# models.py
#
# Classification:
# - Role: SerpAPI Google Search data contract (tool-specific).
# - Consumers:
#   - runner.py (orchestration + persistence)
#   - api/v1/google_search.py (request/response)
# - Design:
#   - Minimal fields required for Sheets + JSON artifact
#   - Keep raw payload for traceability
# =============================================================================

from typing import Any
from pydantic import BaseModel, Field


class GoogleSearchRow(BaseModel):
    """
    Normalized row record for business-facing Sheets.
    """
    key: str
    domain: str
    possible_company_name: str = ""
    google_search_location: str = ""
    query_executed: str = ""
    source_type: str = ""
    timestamp: str = ""


class GoogleSearchRunResult(BaseModel):
    """
    Audit record for one SerpAPI request.
    """
    phase: str = "SEARCH"

    iso2: str
    hl_plan: str | None = None
    hl_used: str
    gl: str
    google_domain: str
    location_for_serp: str | None = None

    query: str
    query_executed: str
    search_model: str  # geo_in_query | location_routing

    num: int
    start: int

    returned_count: int = 0
    domains: list[str] = Field(default_factory=list)

    appended_rows: int = 0
    unique_domains_job: int = 0

    error: str | None = None
    meta: dict[str, Any] = Field(default_factory=dict)


class GoogleSearchJobResult(BaseModel):
    """
    Canonical JSON artifact for one Google Search job execution.
    Mirrors HunterJobResult / GoogleMapsJobResult shape.
    """
    request_id: str
    raw_prompt: str
    provider_name: str | None = None
    geo: str | None = None

    max_queries: int
    max_results_per_query: int
    max_total_results: int
    dedupe_domains: bool
    use_cache: bool
    search_model: str

    queries_used: list[str] = Field(default_factory=list)
    iso2_used: list[str] = Field(default_factory=list)
    locations_used: list[str] = Field(default_factory=list)

    total_runs_est: int = 0
    runs_done: int = 0

    total_unique_domains: int = 0

    runs: list[GoogleSearchRunResult] = Field(default_factory=list)
