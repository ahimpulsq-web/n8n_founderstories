from __future__ import annotations

# ============================================================================
# validators.py
#
# Role:
# - Deterministic cleanup / normalization of LLM-produced fields
# - No extra inference (keep LLM output as source of truth)
# ============================================================================

import re

from .models import PromptInterpretation, ResolvedLocation

_WORD_RE = re.compile(r"^[a-zA-Z]+$")

_BANNED_GENERIC = {
    "company", "companies", "brand", "brands", "product", "products", "business"
}


def _norm_kw(s: str) -> str | None:
    """
    Normalize a keyword token:
    - lowercase
    - letters-only
    - remove banned generic words
    """
    if not s:
        return None

    x = re.sub(r"[^a-zA-Z]", "", str(s).strip().lower())
    if not x or not _WORD_RE.match(x):
        return None
    if x in _BANNED_GENERIC:
        return None
    return x


def _norm_text_query(s: str) -> str | None:
    """
    Normalize a places text query:
    - strip whitespace
    - remove empty strings
    - basic cleanup
    """
    if not s:
        return None
    
    x = str(s).strip()
    if not x:
        return None
    
    return x


def post_validate_prompt_interpretation(pi: PromptInterpretation) -> PromptInterpretation:
    """
    Post-process LLM output for internal consistency.

    Notes:
    - We do NOT attempt to reconstruct prompt_location from resolved_locations.
      (ResolvedLocation has no 'raw' field; prompt_location is already mandated
       by the system prompt and should be the source of truth.)
    """

    # ------------------------------------------------------------------------
    # 1) Location consistency
    # ------------------------------------------------------------------------
    if pi.prompt_location is None:
        pi.prompt_location = None
        pi.resolved_locations = [
            ResolvedLocation(country="DE", country_name="Germany", continent="Europe", region="EMEA"),
            ResolvedLocation(country="AT", country_name="Austria", continent="Europe", region="EMEA"),
            ResolvedLocation(country="CH", country_name="Switzerland", continent="Europe", region="EMEA"),
        ]
    else:
        if pi.resolved_locations is None:
            pi.resolved_locations = []


    # ------------------------------------------------------------------------
    # 2) Keywords (deterministic cleanup)
    # ------------------------------------------------------------------------
    cleaned_kw: list[str] = []
    seen_kw: set[str] = set()

    for k in (pi.prompt_keywords or []):
        kk = _norm_kw(k)
        if not kk or kk in seen_kw:
            continue
        seen_kw.add(kk)
        cleaned_kw.append(kk)

    pi.prompt_keywords = cleaned_kw[:10]

    # ------------------------------------------------------------------------
    # 3) Places text queries (deterministic cleanup - pool from multiple LLMs)
    # ------------------------------------------------------------------------
    cleaned_queries: list[str] = []
    seen_queries: set[str] = set()

    for q in (pi.places_text_queries or []):
        qq = _norm_text_query(q)
        if not qq:
            continue
        # Case-insensitive deduplication
        qq_lower = qq.lower()
        if qq_lower in seen_queries:
            continue
        seen_queries.add(qq_lower)
        cleaned_queries.append(qq)

    pi.places_text_queries = cleaned_queries
    
    return pi
