from __future__ import annotations

# =============================================================================
# geo_locator.py
# Deterministic geo resolver used by SearchPlan generation.
#
# Classification:
# - Role: system-owned location resolution (no LLM dependence).
# - Output: SerpAPI-ready geo buckets keyed by ISO2 ("gl") with per-bucket language ("hl").
# - Failure policy: never crash upstream pipelines; fall back to region defaults (currently DACH).
# =============================================================================

import logging
from typing import Dict, List, Literal, TypedDict

from pydantic import BaseModel, Field

from ...core.utils.text import norm
from .geo_resolution import GeoResolutionError, resolve

logger = logging.getLogger(__name__)


# =============================================================================
# Data models (public contract)
# =============================================================================

class GeoBucket(TypedDict):
    """SerpAPI-ready bucket for a single ISO2 region code."""
    hl: str
    locations: List[str]


GeoMode = Literal["ANY", "CITY", "STATE", "COUNTRY", "MULTI_COUNTRY", "CONTINENT"]


class ResolvedGeo(BaseModel):
    """
    Deterministic geo resolution result.

    Fields:
    - resolved_geo: human-readable location string (e.g., "Germany", "California", "DACH")
    - geo_mode: resolution granularity classification
    - geo_location_keywords: ISO2 -> {hl, locations[]}
    """
    resolved_geo: str = Field(..., description="Resolved geo name used for display/context.")
    geo_mode: GeoMode = Field(..., description="Resolution mode / granularity.")
    geo_location_keywords: Dict[str, GeoBucket] = Field(default_factory=dict)


# =============================================================================
# Defaults (single source of truth)
# =============================================================================

_DACH_DEFAULT = ResolvedGeo(
    resolved_geo="DACH",
    geo_mode="MULTI_COUNTRY",
    geo_location_keywords={
        "DE": {"hl": "de", "locations": ["Germany"]},
        "AT": {"hl": "de", "locations": ["Austria"]},
        "CH": {"hl": "de", "locations": ["Switzerland"]},
    },
)


# =============================================================================
# Public API
# =============================================================================

def resolve_geo(
    *,
    prompt: str,
    region: str = "DACH",
    llm_client=None,  # intentionally unused; kept for backward compatibility
) -> ResolvedGeo:
    """
    Resolve prompt into deterministic geo buckets.

    Classification:
    - Input: raw prompt text
    - Output: ResolvedGeo model (stable contract for downstream routing)
    - Policy: deterministic; never calls LLM; falls back to region defaults if resolution fails.

    Notes:
    - `region` is currently used only as a fallback selector; kept for future expansion.
    - `llm_client` is accepted to keep the signature stable across the codebase.
    """
    _ = llm_client  # explicitly unused

    p = norm(prompt)
    if not p:
        logger.info("GEO_RESOLVE_FALLBACK | reason=empty_prompt | region=%s", region)
        return _fallback(region)

    try:
        result = resolve(p)
    except GeoResolutionError as exc:
        logger.info("GEO_RESOLVE_FALLBACK | reason=%s | region=%s", exc, region)
        return _fallback(region)
    except Exception as exc:
        # Defensive: never let geo resolution crash upstream pipelines.
        logger.exception("GEO_RESOLVE_FALLBACK | reason=unexpected_error | region=%s | error=%s", region, exc)
        return _fallback(region)

    t = (result.get("type") or "").strip().lower()

    # -------------------------------------------------------------------------
    # City resolution
    # -------------------------------------------------------------------------
    if t == "city":
        iso2 = norm(result.get("iso2")).upper()
        city = norm(result.get("name"))
        hl = norm(result.get("hl")) or "en"
        if not iso2 or not city:
            logger.warning("GEO_RESOLVE_FALLBACK | reason=invalid_city_payload | region=%s | result=%r", region, result)
            return _fallback(region)
        return ResolvedGeo(
            resolved_geo=city,
            geo_mode="CITY",
            geo_location_keywords={iso2: {"hl": hl, "locations": [city]}},
        )

    # -------------------------------------------------------------------------
    # State resolution
    # -------------------------------------------------------------------------
    if t == "state":
        iso2 = norm(result.get("iso2")).upper()
        state = norm(result.get("name"))
        hl = norm(result.get("hl")) or "en"
        cities = [norm(x) for x in (result.get("cities") or []) if norm(x)]
        if not iso2 or not state:
            logger.warning("GEO_RESOLVE_FALLBACK | reason=invalid_state_payload | region=%s | result=%r", region, result)
            return _fallback(region)
        return ResolvedGeo(
            resolved_geo=state,
            geo_mode="STATE",
            geo_location_keywords={iso2: {"hl": hl, "locations": [state] + cities}},
        )

    # -------------------------------------------------------------------------
    # Country resolution
    # -------------------------------------------------------------------------
    if t == "country":
        iso2 = norm(result.get("iso2")).upper()
        country = norm(result.get("name"))
        hl = norm(result.get("hl")) or "en"
        cities = [norm(x) for x in (result.get("cities") or []) if norm(x)]
        if not iso2 or not country:
            logger.warning("GEO_RESOLVE_FALLBACK | reason=invalid_country_payload | region=%s | result=%r", region, result)
            return _fallback(region)
        return ResolvedGeo(
            resolved_geo=country,
            geo_mode="COUNTRY",
            geo_location_keywords={iso2: {"hl": hl, "locations": [country] + cities}},
        )

    # -------------------------------------------------------------------------
    # Continent / multi-country resolution
    # -------------------------------------------------------------------------
    if t == "continent":
        continent = norm(result.get("name"))
        countries = result.get("countries") or []
        out: Dict[str, GeoBucket] = {}

        for c in countries:
            try:
                iso2 = norm(c.get("iso2")).upper()
                name = norm(c.get("name"))
                hl = norm(c.get("hl")) or "en"
                if iso2 and name:
                    out[iso2] = {"hl": hl, "locations": [name]}
            except Exception:
                continue

        if not continent or not out:
            logger.warning("GEO_RESOLVE_FALLBACK | reason=invalid_continent_payload | region=%s | result=%r", region, result)
            return _fallback(region)

        return ResolvedGeo(
            resolved_geo=continent,
            geo_mode="MULTI_COUNTRY",
            geo_location_keywords=out,
        )

    # -------------------------------------------------------------------------
    # Safety fallback (unknown type)
    # -------------------------------------------------------------------------
    logger.warning("GEO_RESOLVE_FALLBACK | reason=unknown_type:%r | region=%s | result=%r", t, region, result)
    return _fallback(region)


# =============================================================================
# Internal helpers
# =============================================================================

def _fallback(region: str) -> ResolvedGeo:
    """
    Return fallback geo for a region.

    Current policy:
    - Only DACH preset is supported.
    - Unsupported regions fall back to DACH with a warning.
    """
    r = norm(region).upper()
    if r and r != "DACH":
        logger.warning("GEO_RESOLVE_REGION_UNSUPPORTED | region=%s | fallback=DACH", r)
    return _DACH_DEFAULT
