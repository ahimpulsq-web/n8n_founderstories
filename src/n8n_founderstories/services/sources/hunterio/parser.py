"""
Hunter.io search plan parser module.

Converts search plan dictionaries into validated HunterInput objects.
Handles location resolution, deduplication, and field normalization.

Architecture:
    API Layer
         ↓
    Runner
         ↓
    Parser (THIS MODULE) - search_plan dict → HunterInput
         ↓
    Orchestrator

This module is responsible for:
- Extracting fields from search plan dictionaries
- Resolving locations to Hunter.io-compatible format
- Deduplicating locations
- Creating validated HunterInput objects
"""

from __future__ import annotations

from typing import Any

from .models import HunterInput

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def _clean_str(x: Any) -> str | None:
    """
    Clean and validate string input.
    
    Args:
        x: Input value of any type
        
    Returns:
        Cleaned string or None if invalid/empty
    """
    if not isinstance(x, str):
        return None
    s = x.strip()
    return s or None


def _effective_location(loc: dict[str, Any]) -> dict[str, str] | None:
    """
    Resolve location to Hunter.io-compatible format.
    
    Hunter.io API supports these location filters:
    - city (requires country)
    - country
    - continent
    - business_region
    
    This function selects the most specific usable filter based on
    available data, following Hunter.io's requirements.
    
    Priority (most to least specific):
    1. city + country (most specific)
    2. country only
    3. continent
    4. business_region (least specific)
    
    Note: State is ignored completely as it's US-only and not well-supported.
    
    Args:
        loc: Location dictionary with optional keys:
            - city: City name
            - country: Country code
            - continent: Continent name
            - region/business_region: Business region
            - state: US state (ignored)
            
    Returns:
        Resolved location dict with Hunter.io-compatible keys,
        or None if no usable location data
    """
    country = _clean_str(loc.get("country"))
    city = _clean_str(loc.get("city"))
    continent = _clean_str(loc.get("continent"))
    business_region = _clean_str(loc.get("region")) or _clean_str(loc.get("business_region"))

    # Ignore "state" entirely by design.
    # If caller provides only state+US, we fall back to country=US (below).

    if city and country:
        return {"country": country, "city": city}

    if country:
        return {"country": country}

    if continent:
        return {"continent": continent}

    if business_region:
        return {"business_region": business_region}

    return None


def _dedupe_locations(locs: list[dict[str, str]]) -> list[dict[str, str]]:
    """
    Remove duplicate locations from list.
    
    Deduplication is based on the complete set of key-value pairs
    in each location dictionary.
    
    Args:
        locs: List of location dictionaries
        
    Returns:
        Deduplicated list preserving original order
    """
    seen: set[tuple[tuple[str, str], ...]] = set()
    out: list[dict[str, str]] = []
    for loc in locs:
        key = tuple(sorted(loc.items()))
        if key in seen:
            continue
        seen.add(key)
        out.append(loc)
    return out

# ============================================================================
# MAIN PARSER
# ============================================================================


def parse_search_plan(plan: dict[str, Any]) -> HunterInput:
    """
    Parse search plan dictionary into validated HunterInput object.
    
    Extracts and normalizes all fields from the search plan dictionary,
    resolves locations to Hunter.io-compatible format, and creates
    a validated HunterInput object.
    
    Field extraction:
    - request_id: From "request_id"
    - target_prompt: From "prompt_target", "prompt_target_en", or "normalized_prompt_en"
    - keywords: From "prompt_keywords"
    - industries: From "matched_industries"
    - sheet_id: From "sheet_id" or "google_sheet_id"
    - locations: From "resolved_locations" (resolved and deduplicated)
    
    Args:
        plan: Search plan dictionary from API or LLM service
        
    Returns:
        Validated HunterInput object ready for orchestrator
        
    Raises:
        ValueError: If HunterInput validation fails (via HunterInput.validate())
    """
    # Extract and clean basic fields
    request_id = _clean_str(plan.get("request_id")) or ""
    target_prompt = (
        _clean_str(plan.get("prompt_target")) or
        _clean_str(plan.get("prompt_target_en")) or
        _clean_str(plan.get("normalized_prompt_en"))
    )
    keywords = plan.get("prompt_keywords") or []
    industries = plan.get("matched_industries") or None
    sheet_id = _clean_str(plan.get("sheet_id")) or _clean_str(plan.get("google_sheet_id"))

    # Resolve and deduplicate locations
    resolved = plan.get("resolved_locations") or []
    effective: list[dict[str, str]] = []
    if isinstance(resolved, list):
        for item in resolved:
            if isinstance(item, dict):
                eff = _effective_location(item)
                if eff:
                    effective.append(eff)
    
    effective = _dedupe_locations(effective)

    # Create and return validated HunterInput
    return HunterInput(
        request_id=request_id,
        target_prompt=target_prompt,
        keywords=[k.strip() for k in keywords if isinstance(k, str) and k.strip()],
        industries=industries,
        locations=effective,
        sheet_id=sheet_id,
    )
