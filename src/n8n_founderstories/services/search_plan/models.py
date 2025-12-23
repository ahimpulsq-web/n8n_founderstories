from __future__ import annotations

from typing import Literal, Optional, TypedDict

from pydantic import BaseModel, Field


class GeoBucket(TypedDict):
    """
    SerpAPI-ready per-country geo bucket.

    Dict key: ISO2 country code (gl)
    Values:
      - hl: language code (hl)
      - locations: ordered list (country/state/cities)
    """

    hl: str
    locations: list[str]


class SearchPlanPayload(BaseModel):
    """
    Final payload used by downstream tools.

    Contains:
      - LLM-owned fields (industry, category, alternates, keywords, sources_to_use,
        web_queries, maps_queries)
      - System-owned geo fields (geo, geo_location_keywords)
    """

    # LLM-owned core
    industry: str
    category: Optional[str] = None
    alternates: list[str] = Field(default_factory=list)

    # Hunter.io seed keywords (no geo; single-word preferred)
    keywords: list[str] = Field(default_factory=list)

    # System-owned geo
    geo: str = Field(default="DACH")
    geo_location_keywords: dict[str, GeoBucket] = Field(default_factory=dict)

    # Which discovery channels to use
    sources_to_use: list[Literal["llm", "search_engine", "google_maps"]] = Field(
        default_factory=lambda: ["llm"]
    )

    # GEO-NEUTRAL web search queries (geo routing is separate)
    web_queries: list[str] = Field(default_factory=list)

    # Google Maps query strings (execution-ready; geo routed separately)
    maps_queries: list[str] = Field(default_factory=list)


class SearchPlanPayloadLLM(BaseModel):
    """
    LLM-owned fields ONLY.

    No geo fields here by design: geo is computed deterministically by the system.
    """

    industry: str
    category: Optional[str] = None
    alternates: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)

    web_queries: list[str] = Field(default_factory=list)
    maps_queries: list[str] = Field(default_factory=list)


class SearchPlanMeta(BaseModel):
    """
    Metadata fields placed first in the final output.

    Pydantic v2 preserves inheritance order, so we keep meta first for readability.
    """

    raw_prompt: str
    request_id: Optional[str] = None
    provider_name: str


class SearchPlan(SearchPlanPayload, SearchPlanMeta):
    """
    Final SearchPlan model.

    Field order follows inheritance order (Pydantic v2):
      1) SearchPlanMeta
      2) SearchPlanPayload
    """

    pass
