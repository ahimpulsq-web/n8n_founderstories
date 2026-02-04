from __future__ import annotations

from typing import Literal, Optional
from typing_extensions import TypedDict

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


class GeoBucketLLM(BaseModel):
    """
    LLM-owned geo bucket for structured output.
    
    Used in ResolvedGeoLLM for LLM-based geo resolution.
    """
    hl: str = Field(..., description="Language/locale hint for downstream search, e.g. de, en.")
    locations: list[str] = Field(default_factory=list, description="Location phrases for query routing/bias.")


class ResolvedGeoLLM(BaseModel):
    """
    LLM-based geo resolution result.
    
    This model is used when geo resolution is performed by the LLM instead of
    deterministic rules. It provides structured geo intent extraction from user prompts.
    """
    resolved_geo: str = Field(..., description="High-level geo scope label. Example: DACH, Germany, Europe, Global.")
    geo_mode: Literal["global", "region", "country", "city"] = Field(..., description="One of: global, region, country, city.")
    geo_location_keywords: dict[str, GeoBucketLLM] = Field(
        default_factory=dict,
        description="ISO2 -> {hl, locations}. Example: DE/AT/CH buckets.",
    )


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
    
    # System-owned fields from prompt cleaning step
    target_search: str
    target_search_en: str
    prompt_language: str
    location: Optional[str] = None


class SearchPlan(SearchPlanPayload, SearchPlanMeta):
    """
    Final SearchPlan model.

    Field order follows inheritance order (Pydantic v2):
      1) SearchPlanMeta
      2) SearchPlanPayload
    """

    pass

class CleanPromptPayloadLLM(BaseModel):
    """
    LLM output for prompt cleaning step.
    
    Extracts clean intent, language, and optional location from raw user prompt.
    """
    target_search: str = Field(..., description="What the user intends to search (cleaned, corrected, concise). No location. Same language as prompt_language.")
    target_search_en: str = Field(..., description="English translation of target_search. If already English, repeat unchanged.")
    prompt_language: str = Field(..., description="Detected language code (e.g. de, en, tr, fr).")
    location: str | None = Field(None, description="Location mentioned by user, if any. Otherwise null.")
