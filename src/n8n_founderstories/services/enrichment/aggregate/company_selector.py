"""
═══════════════════════════════════════════════════════════════════════════════
COMPANY NAME SELECTOR - Intelligent Company Name Selection
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [CORE] - Company name selection logic

PURPOSE:
    Selects the best company name from multiple candidates using a scoring system
    that prioritizes impressum legal entities while considering organization matches.

ALGORITHM:
    1. Normalize all names for comparison
    2. Score each candidate based on:
       - Page type (impressum gets highest score)
       - Legal form presence
       - Organization name agreement
    3. Apply guardrail to prevent non-impressum from beating near-top impressum
    4. Convert raw score to confidence (0-1)
    5. Return selected company name with metadata

SCORING SYSTEM:
    - Base score: impressum=100, other=60
    - Legal form bonus: +25
    - Organization match: exact=+30, contains=+20, brand=+10
    - Guardrail: Protects impressum candidates within 15 points of top

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

# Legal form tokens (for removal during normalization)
LEGAL_TOKENS = {
    "gmbh", "ag", "se", "kg", "gbr", "ug", "ek", "e.k", "e k", "e.u",
    "ltd", "inc", "llc", "corp", "sarl", "bv", "nv", "co", "co kg",
    "und", "u", "co."
}

# Generic stopwords (for brand token extraction)
STOPWORDS = {
    "online", "shop", "store", "official", "germany", "deutschland",
    "gmbh", "ag", "se", "kg", "ug", "ek", "gbr", "ltd", "inc", "llc",
    "corp", "sarl", "bv", "nv", "co", "und", "u"
}

# Scoring constants
RAW_MIN = 60
RAW_MAX = 155  # 100 base + 25 legal + 30 exact org match
GUARDRAIL_THRESHOLD = 15


# =============================================================================
# NORMALIZATION FUNCTIONS
# =============================================================================

def normalize_name(s: str) -> str:
    """
    Normalize a company name for comparison.
    
    Steps:
    1. Lowercase
    2. Strip whitespace
    3. Replace & with "and"
    4. Remove punctuation (convert to spaces)
    5. Collapse whitespace
    6. Remove legal suffix tokens
    
    Args:
        s: Company name string
    
    Returns:
        Normalized string for comparison
    """
    if not s:
        return ""
    
    # Lowercase and strip
    result = s.lower().strip()
    
    # Replace & with "and"
    result = result.replace("&", " and ")
    
    # Remove punctuation (convert to spaces)
    result = re.sub(r'[.,;:/\(\)\[\]\{\}\|\-\\_"\'`]', ' ', result)
    
    # Collapse whitespace
    result = re.sub(r'\s+', ' ', result).strip()
    
    # Split into tokens
    tokens = result.split()
    
    # Remove legal tokens
    filtered_tokens = [t for t in tokens if t not in LEGAL_TOKENS]
    
    return " ".join(filtered_tokens)


def brand_token(s: str) -> str:
    """
    Extract brand token(s) from company name.
    
    Takes first 1-2 meaningful tokens after normalization,
    skipping generic stopwords.
    
    Args:
        s: Original company name string
    
    Returns:
        Brand token(s) as string (e.g., "ehrmann" or "schwarzwaldmilch")
    """
    if not s:
        return ""
    
    # Normalize first
    normalized = normalize_name(s)
    tokens = normalized.split()
    
    # Filter out stopwords
    meaningful_tokens = [t for t in tokens if t not in STOPWORDS]
    
    if not meaningful_tokens:
        return ""
    
    # Take first 1-2 tokens
    if len(meaningful_tokens) == 1:
        return meaningful_tokens[0]
    else:
        return " ".join(meaningful_tokens[:2])


# =============================================================================
# SCORING FUNCTIONS
# =============================================================================

def has_legal_form(value: str) -> bool:
    """
    Check if company name contains a legal form token.
    
    Args:
        value: Company name string
    
    Returns:
        True if legal form found
    """
    value_lower = value.lower()
    
    # Check for legal tokens as substrings or tokens
    for token in LEGAL_TOKENS:
        # Check as substring
        if token in value_lower:
            return True
        # Check as word boundary
        if re.search(rf'\b{re.escape(token)}\b', value_lower):
            return True
    
    return False


def compute_org_agreement_bonus(organization: Optional[str], candidate_value: str) -> int:
    """
    Compute organization agreement bonus.
    
    Returns the MAX bonus that matches:
    - Exact match: +30
    - Contains match: +20
    - Brand token match: +10
    - No match: +0
    
    Args:
        organization: Organization name from source
        candidate_value: Candidate company name
    
    Returns:
        Bonus score (0, 10, 20, or 30)
    """
    if not organization or not candidate_value:
        return 0
    
    org_norm = normalize_name(organization)
    cand_norm = normalize_name(candidate_value)
    
    if not org_norm or not cand_norm:
        return 0
    
    # C1: Exact match
    if org_norm == cand_norm:
        return 30
    
    # C2: Contains match (either direction)
    if org_norm in cand_norm or cand_norm in org_norm:
        return 20
    
    # C3: Brand token match
    org_brand = brand_token(organization)
    cand_brand = brand_token(candidate_value)
    
    if org_brand and cand_brand and org_brand == cand_brand:
        return 10
    
    return 0


def score_candidate(
    candidate: dict[str, Any],
    organization: Optional[str]
) -> tuple[int, dict[str, Any]]:
    """
    Score a single candidate.
    
    Args:
        candidate: Candidate dict with "value" and "evidence"
        organization: Organization name from source
    
    Returns:
        Tuple of (raw_score, scored_candidate_dict)
    """
    raw_value = candidate.get("value", "")
    evidence = candidate.get("evidence", {})
    
    if isinstance(evidence, list) and len(evidence) > 0:
        evidence = evidence[0]  # Take first evidence if array
    
    page_type = evidence.get("page_type", "unknown") if isinstance(evidence, dict) else "unknown"
    
    # B1: Base score by page type
    if page_type == "impressum":
        raw_score = 100
    else:
        raw_score = 60
    
    # B2: Legal form bonus
    if has_legal_form(raw_value):
        raw_score += 25
    
    # C: Organization agreement bonus
    org_bonus = compute_org_agreement_bonus(organization, raw_value)
    raw_score += org_bonus
    
    scored_candidate = {
        "value": raw_value,
        "page_type": page_type,
        "raw_score": raw_score,
        "org_bonus": org_bonus,
        "has_legal_form": has_legal_form(raw_value),
        "evidence": evidence,
    }
    
    return raw_score, scored_candidate


# =============================================================================
# SELECTION LOGIC
# =============================================================================

def select_company_name(
    organization: Optional[str],
    company_candidates: list[dict[str, Any]]
) -> dict[str, Any]:
    """
    Select the best company name from candidates.
    
    Args:
        organization: Organization name from source (e.g., Google Maps)
        company_candidates: List of candidate dicts from company_json
    
    Returns:
        Dictionary with:
        - company: Selected company name (str | None)
        - confidence: Confidence score 0-1 (float)
        - raw_score: Raw score for debugging (int)
        - selected_candidate: Full candidate dict (dict | None)
        - all_scored: List of all scored candidates for debugging
    """
    # Handle empty input
    if not company_candidates:
        return {
            "company": None,
            "confidence": 0.0,
            "raw_score": 0,
            "selected_candidate": None,
            "all_scored": [],
        }
    
    # Score all candidates
    scored_candidates = []
    for candidate in company_candidates:
        if not candidate.get("value"):
            continue
        
        raw_score, scored_cand = score_candidate(candidate, organization)
        scored_candidates.append(scored_cand)
    
    if not scored_candidates:
        return {
            "company": None,
            "confidence": 0.0,
            "raw_score": 0,
            "selected_candidate": None,
            "all_scored": [],
        }
    
    # Sort by raw_score desc, then prefer impressum, then prefer longer value
    scored_candidates.sort(
        key=lambda x: (
            -x["raw_score"],  # Higher score first
            0 if x["page_type"] == "impressum" else 1,  # Impressum first
            -len(x["value"])  # Longer value first
        )
    )
    
    # D: Guardrail - protect impressum candidates
    top_candidate = scored_candidates[0]
    top_score = top_candidate["raw_score"]
    top_is_impressum = top_candidate["page_type"] == "impressum"
    
    if not top_is_impressum:
        # Find best impressum candidate within threshold
        impressum_candidates = [
            c for c in scored_candidates
            if c["page_type"] == "impressum" and c["raw_score"] >= (top_score - GUARDRAIL_THRESHOLD)
        ]
        
        if impressum_candidates:
            # Pick best impressum candidate
            top_candidate = impressum_candidates[0]
            logger.info(
                "COMPANY_SELECTOR | GUARDRAIL_TRIGGERED | "
                f"Switched from {scored_candidates[0]['value']} (score={scored_candidates[0]['raw_score']}) "
                f"to {top_candidate['value']} (score={top_candidate['raw_score']})"
            )
    
    # E: Convert raw score to confidence
    raw_score = top_candidate["raw_score"]
    confidence = max(0.0, min(1.0, (raw_score - RAW_MIN) / (RAW_MAX - RAW_MIN)))
    
    # Log selection
    logger.info(
        "COMPANY_SELECTOR | SELECTED | "
        f"company={top_candidate['value']} | "
        f"page_type={top_candidate['page_type']} | "
        f"raw_score={raw_score} | "
        f"confidence={confidence:.3f} | "
        f"organization={organization}"
    )
    
    return {
        "company": top_candidate["value"],
        "confidence": confidence,
        "raw_score": raw_score,
        "selected_candidate": top_candidate,
        "all_scored": scored_candidates,
    }