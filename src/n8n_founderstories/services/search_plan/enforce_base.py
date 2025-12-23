# src/n8n_founderstories/services/search_plan/enforce_base.py

from __future__ import annotations

import re

from ...core.utils.collections import cap, dedupe_strings_keep_order_case_insensitive
from ...core.utils.text import norm, tokenize_words
from .geo_utils import iter_geo_phrases, build_geo_token_blocklist
from .models import SearchPlanPayload

# =============================================================================
# Keyword enforcement: constants
# =============================================================================
"""
These constants support keyword generation for Hunter.io.

- STOPWORDS: remove entirely (noise or legal suffixes)
- GENERIC_KEYWORD_PENALTY: lower ranking but still allowed
- DANGLING_COMPONENT_WORDS: drop if they show up alone (meaningless fragments)
- RECOGNIZED_ACRONYMS: preserve casing and prioritize (SEO, SaaS, etc.)
"""

KEYWORD_STOPWORDS: set[str] = {
    # Articles / connectors
    "and", "or", "the", "a", "an", "of", "for", "to", "in", "on", "with",
    # Business noise
    "company", "companies", "startup", "startups", "scaleup", "scaleups",
    "service", "services", "provider", "providers",
    # Legal suffixes
    "gmbh", "ag", "kg", "sa", "inc", "ltd", "llc",
}

DANGLING_COMPONENT_WORDS: set[str] = {
    "based", "driven", "powered", "focused", "ready", "enabled", "centric",
}

GENERIC_KEYWORD_PENALTY: set[str] = {
    "digital", "search", "engine",
    "solution", "solutions",
    "service", "services",
    "group", "company", "business",
    "platform",
    "technology", "tech",
    "online",
    "agency",
    "marketing",
}

RECOGNIZED_ACRONYMS: set[str] = {
    "SEO", "SEM", "SEA", "PPC",
    "CRO", "CRM", "ERP",
    "B2B", "B2C",
    "SaaS",
    "AI", "ML", "BI",
    "API", "IT", "HR", "PR",
}

# Common geo tokens (extra safety)
KEYWORDS_COMMON_GEO_TOKENS: set[str] = {
    "dach", "de", "at", "ch",
    "eu", "europe",
    "germany", "deutschland",
    "austria", "österreich", "oesterreich",
    "switzerland", "schweiz",
}

# =============================================================================
# Public API
# =============================================================================


def enforce_alternates(payload: SearchPlanPayload, *, max_alternates: int) -> None:
    """Normalize + de-dupe alternates and cap to max_alternates."""
    payload.alternates = cap(
        dedupe_strings_keep_order_case_insensitive(payload.alternates or []),
        max_alternates,
    )


def enforce_keywords(payload: SearchPlanPayload, *, raw_prompt: str | None, max_keywords: int) -> None:
    """
    Build Hunter.io keyword seeds from:
    - LLM-provided keywords
    - deterministic enrichment from industry/category/alternates

    Rules:
    - no geo terms (location_blocklist)
    - no stopwords
    - prefer single words; allow 2-word phrases only if the first word is too generic
    - drop dangling component fragments
    - rank candidates and cap to max_keywords
    """
    location_blocklist = _build_location_blocklist(payload)

    # 1) Start with any LLM-provided keywords
    base = dedupe_strings_keep_order_case_insensitive(payload.keywords or [])

    # 2) Deterministically enrich from plan text fields
    enrich_text = " ".join(
        [
            payload.industry or "",
            payload.category or "",
            " ".join(payload.alternates or []),
        ]
    )
    enrich_tokens = dedupe_strings_keep_order_case_insensitive(_keywordize_from_text(enrich_text))

    # 3) Merge and clean
    merged = dedupe_strings_keep_order_case_insensitive(base + enrich_tokens)
    cleaned = _drop_geo_and_stopwords(merged, location_blocklist=location_blocklist)

    # 4) Prefer single word, fallback to 2-word phrases only when needed
    simplified = _prefer_single_word_keywords(cleaned)

    # 5) Drop meaningless fragments ("based", "driven", etc.)
    simplified = _drop_dangling_component_words(simplified)

    # 6) Remove component words that appear only as part of multi-word alternates
    tightened = _drop_component_words_from_alternates(payload, simplified, raw_prompt=raw_prompt)

    # 7) Rank and cap
    ranked = _rank_keywords(payload, tightened)

    if not ranked:
        ranked = dedupe_strings_keep_order_case_insensitive(_keywordize_from_text(payload.industry or ""))

    payload.keywords = cap(ranked, max_keywords)


# =============================================================================
# Keyword building helpers
# =============================================================================


def _keywordize_from_text(text: str) -> list[str]:
    """
    Deterministic token extraction:
    - preserves hyphenated words
    - drops stopwords
    - preserves acronyms casing
    - title-cases normal tokens (Plant-based -> Plant-Based)
    """
    t = (text or "").strip()
    if not t:
        return []

    tokens = re.findall(r"[A-Za-z0-9ÄÖÜäöüß]+(?:-[A-Za-z0-9ÄÖÜäöüß]+)*", t)
    out: list[str] = []

    for tok in tokens:
        tok = tok.strip()
        if not tok or len(tok) <= 1:
            continue

        low = tok.lower()
        if low in KEYWORD_STOPWORDS:
            continue

        if tok.isupper() and 2 <= len(tok) <= 10:
            out.append(tok)
        else:
            out.append("-".join([p[:1].upper() + p[1:].lower() for p in tok.split("-")]))

    return out


def _drop_geo_and_stopwords(items: list[str], *, location_blocklist: set[str]) -> list[str]:
    """Remove geo tokens and known stopwords."""
    cleaned: list[str] = []

    for k in items or []:
        kk = norm(k)
        if not kk:
            continue

        parts = kk.lower().split()
        if any(p in location_blocklist for p in parts):
            continue

        if kk.lower() in KEYWORD_STOPWORDS:
            continue

        cleaned.append(kk)

    return dedupe_strings_keep_order_case_insensitive(cleaned)


def _prefer_single_word_keywords(items: list[str]) -> list[str]:
    """
    Prefer single word keywords.

    If a candidate is multi-word, keep the first word unless it is too generic,
    in which case keep a 2-word phrase.
    """
    enforced: list[str] = []

    for k in items:
        parts = k.split()
        if len(parts) == 1:
            enforced.append(parts[0])
            continue

        first = parts[0]
        two_word = " ".join(parts[:2])

        enforced.append(two_word if _is_meaningless_single_word(first) else first)

    return dedupe_strings_keep_order_case_insensitive(enforced)


def _drop_dangling_component_words(items: list[str]) -> list[str]:
    """Drop words like 'based' or 'driven' if they appear alone."""
    cleaned = [
        k for k in items
        if not (len(k.split()) == 1 and k.lower() in DANGLING_COMPONENT_WORDS)
    ]
    return dedupe_strings_keep_order_case_insensitive(cleaned)


# =============================================================================
# Ranking logic
# =============================================================================


def _rank_keywords(payload: SearchPlanPayload, candidates: list[str]) -> list[str]:
    scored = [(c, _score_keyword(c, payload=payload)) for c in candidates]
    scored.sort(key=lambda x: (-x[1], x[0].lower()))
    return [c for c, _ in scored]


def _score_keyword(k: str, *, payload: SearchPlanPayload) -> float:
    """
    Scoring heuristic:
    - boosts acronyms
    - boosts matches to industry/category/alternates
    - penalizes generic keywords
    """
    kw = norm(k)
    low = kw.lower()

    industry_text = " ".join(tokenize_words(payload.industry or "")).lower()
    category_text = " ".join(tokenize_words(payload.category or "")).lower()
    alternates_text = " ".join(tokenize_words(" ".join(payload.alternates or []))).lower()

    score = 0.0

    # Acronyms: strong boost
    if kw in RECOGNIZED_ACRONYMS or (kw.isupper() and 2 <= len(kw) <= 10):
        score += 5.0

    # Field matches
    if low and low in industry_text:
        score += 5.0
    if low and low in category_text:
        score += 3.0
    if low and low in alternates_text:
        score += 2.0

    # Multi-word phrase small preference
    if len(kw.split()) == 2:
        score += 1.0

    # Penalize generic terms
    if low in GENERIC_KEYWORD_PENALTY:
        score -= 2.0

    # Penalize meaningless single words
    if len(kw.split()) == 1 and _is_meaningless_single_word(kw):
        score -= 3.0

    return score


def _is_meaningless_single_word(word: str) -> bool:
    """
    Words that are usually too generic alone.

    Note: keep 'seo' as meaningful.
    """
    w = (word or "").strip().lower()
    if w == "seo":
        return False

    too_generic = {
        "energy", "food", "drink", "drinks", "bar", "bars",
        "beverage", "beverages",
        "software", "tech", "digital",
    }
    return w in too_generic


# =============================================================================
# Geo blocking + alternate component dropping
# =============================================================================


def _build_anchor_keywords(payload: SearchPlanPayload, *, raw_prompt: str | None) -> set[str]:
    """
    Anchor tokens must never be dropped because they represent core intent:
    - raw prompt tokens
    - industry tokens
    - category tokens
    """
    anchors: set[str] = set()

    if raw_prompt:
        anchors.update(t.lower() for t in tokenize_words(raw_prompt))

    anchors.update(t.lower() for t in tokenize_words(payload.industry or ""))
    anchors.update(t.lower() for t in tokenize_words(payload.category or ""))

    return anchors


def _build_location_blocklist(payload: SearchPlanPayload) -> set[str]:
    """
    Build a token-level blocklist of location/geo terms.

    This ensures keywords stay geo-neutral for Hunter.io.

    Uses shared geo utils to remain consistent with web/maps geo stripping.
    """
    return build_geo_token_blocklist(payload, common_geo_tokens=KEYWORDS_COMMON_GEO_TOKENS)


def _drop_component_words_from_alternates(
    payload: SearchPlanPayload,
    candidates: list[str],
    *,
    raw_prompt: str | None,
) -> list[str]:
    """
    Drop single-word candidates that exist only as a component of longer alternates,
    unless they are:
    - a recognized acronym
    - a keyword anchor (from raw_prompt / industry / category)
    - an explicitly provided single-word alternate
    """
    anchors = _build_anchor_keywords(payload, raw_prompt=raw_prompt)

    single_word_alts: set[str] = set()
    component_words: set[str] = set()

    for alt in payload.alternates or []:
        alt_n = norm(alt)
        if not alt_n:
            continue

        parts = alt_n.split()
        if len(parts) == 1:
            single_word_alts.add(parts[0].lower())
        else:
            component_words.update(p.lower() for p in parts)

    out: list[str] = []

    for k in candidates:
        kn = norm(k)
        if not kn:
            continue

        # Keep phrases unchanged
        if len(kn.split()) != 1:
            out.append(kn)
            continue

        # Keep acronyms
        if kn in RECOGNIZED_ACRONYMS or (kn.isupper() and 2 <= len(kn) <= 10):
            out.append(kn)
            continue

        low = kn.lower()

        # Keep explicit single-word alternates
        if low in single_word_alts:
            out.append(kn)
            continue

        # Keep anchor terms
        if low in anchors:
            out.append(kn)
            continue

        # Drop words that only appear as components
        if low in component_words:
            continue

        out.append(kn)

    return dedupe_strings_keep_order_case_insensitive(out)
