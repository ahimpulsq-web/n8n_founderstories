from __future__ import annotations

from typing import Literal
from pydantic import BaseModel, Field

Continent = Literal[
    "Europe", "Asia", "North America", "South America", "Africa", "Oceania", "Antarctica"
]
BusinessRegion = Literal["AMER", "EMEA", "APAC", "LATAM"]


class ResolvedLocation(BaseModel):
    city: str | None = None
    state: str | None = None
    country: str | None = Field(default=None, description="ISO 3166-1 alpha-2 (e.g., DE, IN, US)")
    country_name: str | None = Field(default=None, description="Human-readable country name in English (e.g., Germany)")
    continent: Continent | None = None
    region: BusinessRegion | None = None


class PromptInterpretation(BaseModel):
    request_id: str
    raw_prompt: str
    normalized_prompt: str
    language: str
    normalized_prompt_en: str
    prompt_target: str

    prompt_keywords: list[str] | None = Field(default=None, json_schema_extra={"vote": "union"})
    places_text_queries: list[str] | None = Field(default=None, json_schema_extra={"vote": "union"})
    prompt_location: list[str] | None = None
    resolved_locations: list[ResolvedLocation] | None = None
    matched_industries: list[str] | None = None
