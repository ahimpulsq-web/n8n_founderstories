# src/n8n_founderstories/services/search_plan/geo_utils.py

from __future__ import annotations

import re

from ...core.utils.text import norm
from .models import SearchPlanPayload


# =============================================================================
# Geo phrases iteration
# =============================================================================

def iter_geo_phrases(payload: SearchPlanPayload) -> list[str]:
    """
    Flatten geo_location_keywords into lowercase phrases for geo stripping.

    Includes:
    - ISO2 bucket keys (DE/AT/CH)
    - hl values
    - each location phrase (country/state/city strings)
    """
    out: list[str] = []
    buckets = payload.geo_location_keywords or {}

    for iso2, bucket in buckets.items():
        if iso2:
            out.append(norm(iso2).lower())

        if not bucket:
            continue

        hl = norm(bucket.get("hl", "")).lower()
        if hl:
            out.append(hl)

        for loc in bucket.get("locations") or []:
            loc_norm = norm(loc).lower()
            if loc_norm:
                out.append(loc_norm)

    return out


# =============================================================================
# Geo token blocklist
# =============================================================================

def build_geo_token_blocklist(
    payload: SearchPlanPayload,
    *,
    common_geo_tokens: set[str] | None = None,
    include_payload_geo: bool = True,
    include_geo_location_keywords: bool = True,
) -> set[str]:
    """
    Build a token/phrase blocklist used to keep queries geo-neutral.

    Includes (configurable):
    - common_geo_tokens (caller supplies: e.g. DACH/EU tokens)
    - payload.geo
    - geo_location_keywords phrases and their component tokens
    """
    block: set[str] = set(common_geo_tokens or set())

    if include_payload_geo:
        geo_norm = norm(payload.geo).lower()
        if geo_norm:
            block.add(geo_norm)

    if include_geo_location_keywords:
        for phrase in iter_geo_phrases(payload):
            phrase_norm = norm(phrase).lower()
            if not phrase_norm:
                continue

            # full phrase
            block.add(phrase_norm)

            # component tokens (e.g., Mumbai, Baden-Württemberg, etc.)
            for part in re.split(r"[^a-z0-9äöüß]+", phrase_norm):
                token = part.strip()
                if token:
                    block.add(token)

    return block


# =============================================================================
# Geo stripping helpers
# =============================================================================

def strip_geo_tokens_from_query(q: str, *, geo_blocklist: set[str]) -> str:
    """
    Best-effort geo stripping:
    - splits on whitespace
    - compares lowercase tokens (punct trimmed) against geo_blocklist
    - keeps original token casing for non-removed tokens
    """
    qn = norm(q)
    if not qn:
        return ""

    kept: list[str] = []
    for token in qn.split():
        t = token.strip().lower()
        t = re.sub(r"^[^\wäöüß]+|[^\wäöüß]+$", "", t)  # trim punctuation
        if not t:
            continue
        if t in geo_blocklist:
            continue
        kept.append(token)

    return norm(" ".join(kept))


def strip_trailing_dangling_prepositions(q: str, *, dangling_prepositions: set[str]) -> str:
    """
    Remove trailing prepositions that often remain after geo stripping.

    Example:
      "SEO Agencies in" -> "SEO Agencies"

    Note: Keep this geo-related (it exists to cleanup after geo token removal).
    """
    s = norm(q)
    if not s:
        return ""

    while True:
        low = s.lower()

        # multi-word endings first (DE patterns)
        if low.endswith(" in der"):
            s = norm(s[: -len(" in der")])
            continue
        if low.endswith(" in dem"):
            s = norm(s[: -len(" in dem")])
            continue

        parts = low.split()
        last = parts[-1] if parts else ""
        if last in dangling_prepositions:
            s = norm(" ".join(s.split()[:-1]))
            continue

        break

    return norm(s)
