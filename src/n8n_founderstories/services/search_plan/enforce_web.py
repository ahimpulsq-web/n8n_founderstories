# src/n8n_founderstories/services/search_plan/enforce_web.py

from __future__ import annotations

from ...core.utils.collections import cap, dedupe_strings_keep_order_case_insensitive
from ...core.utils.text import norm
from .geo_utils import (
    build_geo_token_blocklist,
    strip_geo_tokens_from_query,
    strip_trailing_dangling_prepositions,
)
from .models import SearchPlanPayload

"""
Web query enforcement.

Goal:
- Keep web_queries GEO-NEUTRAL (geo routing happens elsewhere).
- Reject “content intent” queries (news, reports, blogs, market analysis).
- Clean up dangling prepositions after geo removal.

Input:
- payload.web_queries (LLM output)

Output:
- payload.web_queries (cleaned, deduped, capped)
"""

# =============================================================================
# Constants: content-intent blocklist + dangling prepositions
# =============================================================================

# Content/research intent tokens that tend to return articles/reports instead of company sites.
WEBQUERY_CONTENT_INTENT_TOKENS: set[str] = {
    "news", "trend", "trends", "market", "report", "analysis", "insights", "forecast",
    "statistics", "stats", "press", "magazine", "blog", "blogs",
    "industry leaders", "market leaders", "business news",
}

# Tokens that often remain at the end after geo stripping.
WEBQUERY_DANGLING_PREPOSITIONS: set[str] = {
    "in", "on", "at", "near", "nahe", "around", "within", "bei", "im", "in der", "in dem",
}

# Extra geo safety tokens (beyond geo_location_keywords)
WEBQUERY_COMMON_GEO_TOKENS: set[str] = {
    "dach", "eu", "europe", "european", "union",
    "de", "at", "ch",
    "germany", "deutschland",
    "austria", "österreich", "oesterreich",
    "switzerland", "schweiz",
}

# =============================================================================
# Public API
# =============================================================================


def enforce_web_queries(payload: SearchPlanPayload, *, max_web_queries: int) -> None:
    """
    Enforce geo-neutral, company-finding web queries.

    Pipeline:
      1) Build geo token blocklist
      2) Strip geo tokens from each query
      3) Strip trailing dangling prepositions (created by geo stripping)
      4) Drop content-intent queries
      5) Normalize, dedupe, cap
    """
    geo_block = build_geo_token_blocklist(payload, common_geo_tokens=WEBQUERY_COMMON_GEO_TOKENS)

    cleaned: list[str] = []
    for q in payload.web_queries or []:
        q2 = strip_geo_tokens_from_query(q, geo_blocklist=geo_block)
        if not q2:
            continue

        q2 = strip_trailing_dangling_prepositions(q2, dangling_prepositions=WEBQUERY_DANGLING_PREPOSITIONS)
        if not q2:
            continue

        if _looks_like_content_intent_query(q2):
            continue

        q2 = norm(q2)
        if q2:
            cleaned.append(q2)

    payload.web_queries = cap(dedupe_strings_keep_order_case_insensitive(cleaned), max_web_queries)


# =============================================================================
# Content-intent rejection
# =============================================================================


def _looks_like_content_intent_query(q: str) -> bool:
    """
    Reject queries that likely return content rather than company domains.
    """
    low = norm(q).lower()
    if not low:
        return False
    return any(phrase in low for phrase in WEBQUERY_CONTENT_INTENT_TOKENS)
