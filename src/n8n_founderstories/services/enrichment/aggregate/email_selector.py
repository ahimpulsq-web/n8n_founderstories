"""
═══════════════════════════════════════════════════════════════════════════════
EMAIL SELECTOR - Intelligent Email Selection
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [CORE] - Email selection logic

PURPOSE:
    Selects the best email from LLM and deterministic extraction results using
    a scoring system that prioritizes impressum emails while considering domain
    alignment, source agreement, and evidence quality.

ALGORITHM:
    1. Canonicalize and deduplicate emails across LLM + deterministic sources
    2. Score each email based on:
       - Page type (impressum gets highest score)
       - Support count (evidence frequency)
       - Source agreement (found in both LLM and deterministic)
       - Domain alignment
       - Local-part quality
    3. Apply guardrail to protect impressum emails
    4. Convert raw score to confidence (0-1)
    5. Return selected email with metadata

SCORING SYSTEM:
    - Base score: impressum=100, other=60
    - Support count: 1=+0, 2=+10, 3+=+20
    - Source agreement (both LLM+DET): +15
    - Domain alignment: exact=+15, subdomain=+10
    - Local-part quality: generic=+5, personal=+3
    - Vendor penalty: -10
    - Guardrail: Protects impressum within 15 points of top

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

# Generic email local parts
GENERIC_LOCAL_PARTS = {"info", "kontakt", "hello", "office", "support", "mail", "contact"}

# Vendor/agency patterns
VENDOR_PATTERNS = {"valantic", "agency", "marketing", "digital", "web", "hosting", "media", "consulting"}

# Page type priority for tie-breaking
PAGE_TYPE_PRIORITY = {
    "impressum": 0,
    "contact": 1,
    "privacy": 2,
    "home": 3,
    "other": 4,
}

# Scoring constants
RAW_MIN = 0
RAW_MAX = 155  # 100 base + 20 support + 15 source + 15 domain + 5 local
GUARDRAIL_THRESHOLD = 15


# =============================================================================
# NORMALIZATION FUNCTIONS
# =============================================================================

def normalize_email(email: str) -> str:
    """
    Normalize an email address for comparison.
    
    Steps:
    1. Lowercase
    2. Strip whitespace
    3. Remove surrounding punctuation (<, >, ., ,, ;, :)
    
    Args:
        email: Email address string
    
    Returns:
        Normalized email address
    """
    if not email:
        return ""
    
    # Lowercase and strip
    result = email.lower().strip()
    
    # Remove surrounding punctuation
    result = result.strip('<>.,;:')
    
    return result


def extract_domain_from_email(email: str) -> str:
    """
    Extract domain from email address.
    
    Args:
        email: Email address
    
    Returns:
        Domain part of email (e.g., "example.com")
    """
    if not email or '@' not in email:
        return ""
    
    return email.split('@')[1].lower().strip()


def extract_local_part(email: str) -> str:
    """
    Extract local part from email address.
    
    Args:
        email: Email address
    
    Returns:
        Local part of email (before @)
    """
    if not email or '@' not in email:
        return ""
    
    return email.split('@')[0].lower().strip()


def get_registrable_domain(domain: str) -> str:
    """
    Extract registrable domain (last 2 labels for most TLDs).
    
    Simple implementation - handles most cases.
    
    Args:
        domain: Full domain (e.g., "shop.ehrmann.de")
    
    Returns:
        Registrable domain (e.g., "ehrmann.de")
    """
    if not domain:
        return ""
    
    parts = domain.lower().strip().split('.')
    
    # Handle special cases like .co.uk
    if len(parts) >= 3 and parts[-2] in ('co', 'com', 'org', 'net', 'gov', 'edu'):
        return '.'.join(parts[-3:])
    
    # Standard case: last 2 labels
    if len(parts) >= 2:
        return '.'.join(parts[-2:])
    
    return domain


def is_personal_email(local_part: str) -> bool:
    """
    Check if local part looks like a personal email (firstname.lastname pattern).
    
    Args:
        local_part: Local part of email
    
    Returns:
        True if looks like personal email
    """
    # Check for firstname.lastname or firstname_lastname pattern
    if '.' in local_part or '_' in local_part:
        parts = re.split(r'[._]', local_part)
        # If 2 parts and both are alphabetic and reasonable length
        if len(parts) == 2 and all(p.isalpha() and 2 <= len(p) <= 20 for p in parts):
            return True
    
    return False


# =============================================================================
# EVIDENCE PROCESSING
# =============================================================================

def canonicalize_evidence(evidence: Any) -> list[dict[str, str]]:
    """
    Canonicalize evidence to list of dicts with url, page_type, quote.
    
    Args:
        evidence: Evidence object or list
    
    Returns:
        List of evidence dicts
    """
    if not evidence:
        return []
    
    # If single dict, wrap in list
    if isinstance(evidence, dict):
        evidence = [evidence]
    
    if not isinstance(evidence, list):
        return []
    
    result = []
    for item in evidence:
        if not isinstance(item, dict):
            continue
        
        result.append({
            "url": item.get("url", ""),
            "page_type": item.get("page_type", "other"),
            "quote": item.get("quote", ""),
        })
    
    return result


def deduplicate_evidence(evidence_list: list[dict[str, str]]) -> list[dict[str, str]]:
    """
    Deduplicate evidence by (url, page_type).
    
    Args:
        evidence_list: List of evidence dicts
    
    Returns:
        Deduplicated list
    """
    seen = set()
    result = []
    
    for ev in evidence_list:
        key = (ev["url"], ev["page_type"])
        if key not in seen:
            seen.add(key)
            result.append(ev)
    
    return result


# =============================================================================
# SCORING FUNCTIONS
# =============================================================================

def compute_domain_alignment_bonus(email_domain: str, target_domain: Optional[str]) -> int:
    """
    Compute domain alignment bonus.
    
    Args:
        email_domain: Domain from email address
        target_domain: Target domain (from crawl)
    
    Returns:
        Bonus score (0, 10, or 15)
    """
    if not email_domain or not target_domain:
        return 0
    
    email_reg = get_registrable_domain(email_domain)
    target_reg = get_registrable_domain(target_domain)
    
    # Exact match on registrable domain
    if email_reg == target_reg:
        return 15
    
    # Subdomain match
    if email_domain.endswith('.' + target_reg) or target_domain.endswith('.' + email_reg):
        return 10
    
    return 0


def is_vendor_email(email_domain: str, target_domain: Optional[str]) -> bool:
    """
    Check if email appears to be from a vendor/agency.
    
    Args:
        email_domain: Domain from email
        target_domain: Target domain
    
    Returns:
        True if likely vendor email
    """
    if not email_domain:
        return False
    
    # If matches target domain, not a vendor
    if target_domain:
        email_reg = get_registrable_domain(email_domain)
        target_reg = get_registrable_domain(target_domain)
        if email_reg == target_reg:
            return False
    
    # Check for vendor patterns
    email_lower = email_domain.lower()
    return any(pattern in email_lower for pattern in VENDOR_PATTERNS)


def compute_local_part_bonus(local_part: str) -> int:
    """
    Compute local part quality bonus.
    
    Args:
        local_part: Local part of email
    
    Returns:
        Bonus score (0, 3, or 5)
    """
    if not local_part:
        return 0
    
    # Generic local parts
    if local_part in GENERIC_LOCAL_PARTS:
        return 5
    
    # Personal email pattern
    if is_personal_email(local_part):
        return 3
    
    return 0


def score_email_candidate(
    email_data: dict[str, Any],
    target_domain: Optional[str],
    organization: Optional[str]
) -> tuple[int, dict[str, Any]]:
    """
    Score a single email candidate.
    
    Args:
        email_data: Email data with email, evidence, source_flags
        target_domain: Target domain from crawl
        organization: Organization name from source
    
    Returns:
        Tuple of (raw_score, scored_candidate_dict)
    """
    email = email_data["email"]
    evidence = email_data["evidence"]
    source_flags = email_data["source_flags"]
    
    # Extract email components
    email_domain = extract_domain_from_email(email)
    local_part = extract_local_part(email)
    
    # 1) Page-type base
    has_impressum = any(ev["page_type"] == "impressum" for ev in evidence)
    raw_score = 100 if has_impressum else 60
    
    # 2) Support count bonus
    support_count = len(evidence)
    if support_count == 2:
        raw_score += 10
    elif support_count >= 3:
        raw_score += 20
    
    # 3) Source agreement bonus
    if source_flags.get("llm") and source_flags.get("det"):
        raw_score += 15
    
    # 4) Domain alignment bonus
    domain_bonus = compute_domain_alignment_bonus(email_domain, target_domain)
    raw_score += domain_bonus
    
    # 5) Vendor penalty
    if is_vendor_email(email_domain, target_domain):
        raw_score -= 10
    
    # 6) Local-part quality
    local_bonus = compute_local_part_bonus(local_part)
    raw_score += local_bonus
    
    # Determine best page_type for this email
    if has_impressum:
        best_page_type = "impressum"
    else:
        # Find best page_type by priority
        page_types = [ev["page_type"] for ev in evidence]
        best_page_type = min(page_types, key=lambda pt: PAGE_TYPE_PRIORITY.get(pt, 99))
    
    scored_candidate = {
        "email": email,
        "raw_score": raw_score,
        "support_count": support_count,
        "has_impressum": has_impressum,
        "page_type": best_page_type,
        "source_flags": source_flags,
        "domain_bonus": domain_bonus,
        "local_bonus": local_bonus,
        "evidence": evidence,
    }
    
    return raw_score, scored_candidate


# =============================================================================
# SELECTION LOGIC
# =============================================================================

def select_best_email(
    emails_llm: list[dict[str, Any]],
    emails_det: list[dict[str, Any]],
    domain: Optional[str],
    organization: Optional[str]
) -> Optional[dict[str, Any]]:
    """
    Select the best email from LLM and deterministic sources.
    
    Args:
        emails_llm: List of email objects from LLM extraction
        emails_det: List of email objects from deterministic extraction
        domain: Target domain
        organization: Organization name from source
    
    Returns:
        Dictionary with selected email or None:
        {
            "email": str,
            "score": float (0-1),
            "page_type": str,
            "all_scored": list (for emails column)
        }
    """
    # Step 0: Canonicalize and dedupe
    email_map = {}
    
    # Process LLM emails
    for item in (emails_llm or []):
        if not isinstance(item, dict):
            continue
        
        email = normalize_email(item.get("email", ""))
        if not email or '@' not in email:
            continue
        
        evidence = canonicalize_evidence(item.get("evidence"))
        
        if email not in email_map:
            email_map[email] = {
                "email": email,
                "evidence": [],
                "source_flags": {"llm": False, "det": False}
            }
        
        email_map[email]["evidence"].extend(evidence)
        email_map[email]["source_flags"]["llm"] = True
    
    # Process deterministic emails
    for item in (emails_det or []):
        if not isinstance(item, dict):
            continue
        
        email = normalize_email(item.get("email", ""))
        if not email or '@' not in email:
            continue
        
        evidence = canonicalize_evidence(item.get("evidence"))
        
        if email not in email_map:
            email_map[email] = {
                "email": email,
                "evidence": [],
                "source_flags": {"llm": False, "det": False}
            }
        
        email_map[email]["evidence"].extend(evidence)
        email_map[email]["source_flags"]["det"] = True
    
    if not email_map:
        return None
    
    # Deduplicate evidence for each email
    for email_data in email_map.values():
        email_data["evidence"] = deduplicate_evidence(email_data["evidence"])
    
    # Step 1: Score all candidates
    scored_candidates = []
    for email_data in email_map.values():
        raw_score, scored_cand = score_email_candidate(email_data, domain, organization)
        scored_candidates.append(scored_cand)
    
    if not scored_candidates:
        return None
    
    # Sort by raw_score desc, then tie-breakers
    scored_candidates.sort(
        key=lambda x: (
            -x["raw_score"],  # Higher score first
            0 if x["has_impressum"] else 1,  # Impressum first
            -x["support_count"],  # More evidence first
            1 if not (x["source_flags"]["llm"] and x["source_flags"]["det"]) else 0,  # Both sources first
            x["email"]  # Lexical for stability
        )
    )
    
    # Step 2: Guardrail - protect impressum
    top_candidate = scored_candidates[0]
    top_score = top_candidate["raw_score"]
    top_is_impressum = top_candidate["has_impressum"]
    
    if not top_is_impressum:
        # Find best impressum candidate within threshold
        impressum_candidates = [
            c for c in scored_candidates
            if c["has_impressum"] and c["raw_score"] >= (top_score - GUARDRAIL_THRESHOLD)
        ]
        
        if impressum_candidates:
            # Pick best impressum candidate
            top_candidate = impressum_candidates[0]
            logger.info(
                "EMAIL_SELECTOR | GUARDRAIL_TRIGGERED | "
                f"Switched from {scored_candidates[0]['email']} (score={scored_candidates[0]['raw_score']}) "
                f"to {top_candidate['email']} (score={top_candidate['raw_score']})"
            )
    
    # Step 3: Convert raw score to 0..1
    raw_score = top_candidate["raw_score"]
    confidence = max(0.0, min(1.0, raw_score / RAW_MAX))
    
    # Prepare all_scored for emails column (all emails with their scores)
    all_scored = []
    for cand in scored_candidates:
        cand_score = max(0.0, min(1.0, cand["raw_score"] / RAW_MAX))
        all_scored.append({
            "email": cand["email"],
            "score": round(cand_score, 4),
            "page_type": cand["page_type"]
        })
    
    logger.info(
        "EMAIL_SELECTOR | SELECTED | "
        f"email={top_candidate['email']} | "
        f"page_type={top_candidate['page_type']} | "
        f"raw_score={raw_score} | "
        f"confidence={confidence:.4f} | "
        f"support_count={top_candidate['support_count']}"
    )
    
    return {
        "email": top_candidate["email"],
        "score": round(confidence, 4),
        "page_type": top_candidate["page_type"],
        "all_scored": all_scored,
    }