from __future__ import annotations

# ============================================================================
# deps.py
# API v1 shared helpers and lightweight validation.
#
# Design intent:
# - Keep this module "API-facing": raise HTTPException for request issues.
# - Avoid service-layer imports here; keep it transport-focused.
# ============================================================================

from typing import Any
from urllib.parse import urlparse

from fastapi import HTTPException

from ...core.config import settings
from ...core.utils.text import norm


# ============================================================================
# Required configuration gates
# ============================================================================

def require_serpapi_key() -> None:
    """Ensure SerpAPI is configured for endpoints that depend on it."""
    if not settings.serpapi_api_key:
        raise HTTPException(status_code=500, detail="SERPAPI_API_KEY is not configured.")


def require_google_maps_key() -> None:
    """Ensure Google Maps is configured for endpoints that depend on it."""
    if not settings.google_maps_api_key:
        raise HTTPException(status_code=500, detail="GOOGLE_MAPS_API_KEY is not configured.")


# ============================================================================
# Shared input validation
# ============================================================================

def require_search_plan(plan: Any) -> None:
    """
    Validate that a SearchPlan-like object has the minimum contract expected by endpoints.

    We intentionally do not enforce exact class typing to keep the API resilient to internal
    refactors, as long as the contract remains stable.
    """
    request_id = getattr(plan, "request_id", None)
    raw_prompt = getattr(plan, "raw_prompt", None)
    provider_name = getattr(plan, "provider_name", None)

    if not request_id or not str(request_id).strip():
        raise HTTPException(status_code=400, detail="search_plan.request_id is required.")
    if not raw_prompt or not str(raw_prompt).strip():
        raise HTTPException(status_code=400, detail="search_plan.raw_prompt must not be empty.")
    if not provider_name or not str(provider_name).strip():
        raise HTTPException(status_code=400, detail="search_plan.provider_name is required.")


def require_spreadsheet_fields(spreadsheet_id: str | None, sheet_title: str | None) -> tuple[str, str]:
    """Validate spreadsheet inputs shared by spreadsheet-integrated endpoints."""
    sid = norm(spreadsheet_id)
    st = norm(sheet_title)

    if not sid:
        raise HTTPException(status_code=400, detail="spreadsheet_id must not be empty.")
    if not st:
        raise HTTPException(status_code=400, detail="sheet_title must not be empty.")

    return sid, st


# ============================================================================
# Small parsing utilities
# ============================================================================

def extract_domain(url: str) -> str | None:
    """Best-effort domain extraction used in various enrichment steps."""
    try:
        host = urlparse(str(url)).netloc.lower()
        host = host[4:] if host.startswith("www.") else host
        return host or None
    except Exception:
        return None


def infer_regions_from_geo_buckets(geo_buckets: Any) -> list[str]:
    """
    Infer ISO2-like region codes from geo_location_keywords bucket keys.

    Expects geo_buckets to be a dict where keys are ISO2 codes (e.g., "de", "at").
    """
    if not isinstance(geo_buckets, dict):
        return []

    out: list[str] = []
    seen: set[str] = set()

    for iso2 in geo_buckets.keys():
        k = norm(str(iso2)).lower()
        if len(k) == 2 and k.isalpha() and k not in seen:
            seen.add(k)
            out.append(k)

    return out
