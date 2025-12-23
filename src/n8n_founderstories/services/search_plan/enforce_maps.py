# src/n8n_founderstories/services/search_plan/enforce_maps.py

from __future__ import annotations

import re

from ...core.utils.collections import cap, dedupe_strings_keep_order_case_insensitive
from ...core.utils.text import norm
from .geo_utils import build_geo_token_blocklist, strip_geo_tokens_from_query
from .models import SearchPlanPayload

"""
Maps query enforcement.

Invariant:
- maps_queries are ALWAYS GEO-NEUTRAL.
- Geo routing/bias is applied later (Google Maps API params), using payload.geo_location_keywords.

Goal:
- Produce execution-ready, geo-neutral Google Maps queries (payload.maps_queries).
- Bias toward company/producer intent terms (not retail venues).
- If LLM provides maps_queries, keep them but normalize/dedupe/cap and enforce geo-neutrality.

Input:
- payload (industry/category/alternates + system-owned geo)
- raw_prompt (user input)

Output:
- payload.maps_queries (cleaned, deduped, capped, geo-neutral)
"""

# =============================================================================
# Constants: heuristics and token lists
# =============================================================================

RETAIL_VENUE_TOKENS: set[str] = {
    "supermarket", "grocery", "store", "shop", "retail", "market", "mall",
    "pharmacy", "apotheke", "drogerie",
    "gym", "fitness", "studio",
    "restaurant", "cafe", "bar",
}

# Producer/company intent for discovery.
PRODUCER_BIAS_TERMS: list[str] = ["manufacturer", "brand", "company"]
SERVICE_BIAS_TERMS: list[str] = ["company"]

# Product vs service heuristic
SERVICE_MARKERS: set[str] = {"agency", "agentur", "consulting", "beratung", "services", "dienstleistung"}
PRODUCT_MARKERS: set[str] = {
    "drink", "drinks", "beverage", "energy drink",
    "supplement", "protein", "snack", "food",
    "cosmetic", "skincare", "vitamin",
    "getränk", "nahrungsergänzung", "lebensmittel",
}

# Extra geo safety tokens (beyond geo_location_keywords)
MAPS_COMMON_GEO_TOKENS: set[str] = {
    "dach", "eu", "europe", "european", "union",
    "de", "at", "ch",
    "germany", "deutschland",
    "austria", "österreich", "oesterreich",
    "switzerland", "schweiz",
}

# =============================================================================
# Public API
# =============================================================================


def enforce_maps_queries(payload: SearchPlanPayload, *, raw_prompt: str, max_total: int) -> None:
    """
    Ensure payload.maps_queries exists and is geo-neutral.

    Rules:
    - If LLM provided maps_queries: normalize/dedupe/cap.
    - Else: build deterministically from prompt/alternates/category/industry.
    - Always enforce geo-neutrality by stripping known geo tokens/phrases.
    """
    existing = dedupe_strings_keep_order_case_insensitive(payload.maps_queries or [])

    if existing:
        built = existing
    else:
        built = build_maps_queries(payload, raw_prompt=raw_prompt, max_total=max_total)

    cleaned = _clean_maps_queries_geo_neutral(payload, built)
    payload.maps_queries = cap(dedupe_strings_keep_order_case_insensitive(cleaned), max_total)


def build_maps_queries(payload: SearchPlanPayload, *, raw_prompt: str, max_total: int) -> list[str]:
    """
    Deterministically build geo-neutral Maps queries.

    Steps:
    1) Select base terms from (prompt + alternates + category/industry fallback)
    2) Decide product vs service (intent terms)
    3) Compose query variants (base + intent terms; base-only)
    4) Filter retail venue steering for product prompts
    """
    base_terms = _select_base_terms(payload, raw_prompt=raw_prompt, max_terms=6)
    if not base_terms:
        return []

    is_product = _looks_like_product_prompt(payload, raw_prompt=raw_prompt)
    intent_terms = PRODUCER_BIAS_TERMS if is_product else SERVICE_BIAS_TERMS

    queries = _compose_queries(base_terms=base_terms, intent_terms=intent_terms)

    if is_product:
        queries = _filter_retail_venues(queries)

    return cap(dedupe_strings_keep_order_case_insensitive(queries), max_total)


# =============================================================================
# Base term selection
# =============================================================================


def _select_base_terms(payload: SearchPlanPayload, *, raw_prompt: str, max_terms: int) -> list[str]:
    """
    Select terms that form the core of Maps queries.

    Priority:
    1) raw_prompt (as-is, normalized)
    2) alternates (first 12)
    3) category / industry fallback
    """
    terms: list[str] = []

    rp = norm(raw_prompt or "")
    if rp:
        terms.append(rp)

    for alt in (payload.alternates or [])[:12]:
        a = norm(alt)
        if a:
            terms.append(a)

    if not terms:
        for t in (payload.category or "", payload.industry or ""):
            tn = norm(t)
            if tn:
                terms.append(tn)

    return cap(dedupe_strings_keep_order_case_insensitive(terms), max_terms)


def _looks_like_product_prompt(payload: SearchPlanPayload, *, raw_prompt: str) -> bool:
    """
    Heuristic: product prompts tend to target manufacturers/brands;
    service prompts tend to target agencies/consulting.
    """
    text = " ".join(
        [payload.industry or "", payload.category or "", " ".join(payload.alternates or []), raw_prompt or ""]
    ).lower()

    if any(x in text for x in SERVICE_MARKERS):
        return False

    return any(x in text for x in PRODUCT_MARKERS)


# =============================================================================
# Query composition and filtering
# =============================================================================


def _compose_queries(*, base_terms: list[str], intent_terms: list[str]) -> list[str]:
    """
    Compose geo-neutral query strings from base terms + intent terms.

    Examples:
      "Vegan Protein company"
      "Vegan Protein manufacturer"
      "Vegan Protein" (base-only fallback)
    """
    queries: list[str] = []
    for base in base_terms:
        for it in intent_terms:
            queries.append(norm(f"{base} {it}"))
        queries.append(norm(base))
    return queries


def _filter_retail_venues(queries: list[str]) -> list[str]:
    """
    For product prompts, avoid queries that steer toward retail venues.
    """
    return [q for q in queries if not any(t in q.lower() for t in RETAIL_VENUE_TOKENS)]


# =============================================================================
# Geo-neutral enforcement (single source of truth)
# =============================================================================


def _clean_maps_queries_geo_neutral(payload: SearchPlanPayload, queries: list[str]) -> list[str]:
    """
    Remove geo terms from maps queries.

    Uses:
    - payload.geo
    - payload.geo_location_keywords (ISO2, hl, and locations)
    - common DACH/EU tokens
    """
    geo_block = build_geo_token_blocklist(payload, common_geo_tokens=MAPS_COMMON_GEO_TOKENS)

    out: list[str] = []
    for q in queries or []:
        q2 = strip_geo_tokens_from_query(q, geo_blocklist=geo_block)
        q2 = norm(q2)
        if q2:
            out.append(q2)

    return out
