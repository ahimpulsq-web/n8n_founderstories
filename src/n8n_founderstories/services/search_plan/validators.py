from __future__ import annotations

# ============================================================================
# validators.py
#
# Role:
# - Deterministic cleanup / normalization of LLM-produced fields only
# - No extra inference (keep LLM output as source of truth)
# - No external calls, no business defaults
# ============================================================================

import re

from .models import PromptInterpretation
from .constants import WORD_PATTERN, BANNED_GENERIC_KEYWORDS


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
    if not x or not WORD_PATTERN.match(x):
        return None
    if x in BANNED_GENERIC_KEYWORDS:
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

    Location handling:
    - If global_search is True: clear prompt_location (no location filtering)
    - Location resolution will be handled deterministically after LLM processing
    - No DACH defaults applied here - downstream services handle location resolution

    Notes:
    - This validator is deterministic and does NOT call external services.
    - Location resolution happens in downstream services (google_maps, hunterio).
    """

    # ------------------------------------------------------------------------
    # 1) Location consistency
    # ------------------------------------------------------------------------
    if pi.global_search:
        # Global search: no location filtering
        pi.prompt_location = None


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

    # Classification: Contract enforcement - downstream expects 2–3 queries
    pi.places_text_queries = cleaned_queries[:3]
    
    return pi
