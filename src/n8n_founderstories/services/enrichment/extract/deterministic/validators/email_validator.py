"""
Email Validation Module

This module provides comprehensive email validation functionality for the
deterministic extraction system. It includes:
- Structural validation (RFC-compliant checks)
- Basic plausibility checks (HTML context safety)
- Quality scoring
- Detailed validation results

IMPORTANT: This module handles ONLY structural validation.
Business policy filtering (assets, system emails, etc.) is handled by filters.py.

Author: N8N FounderStories Team
Last Modified: 2026-02-19
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from ..config import config
from ..utils.domain_utils import extract_domain_from_email, extract_localpart_from_email


# ============================================================================
# COMPILED REGEX PATTERNS
# ============================================================================

# Basic email pattern (RFC 5322 simplified)
_EMAIL_PATTERN = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)

# Pattern to detect suspicious characters in local part
_SUSPICIOUS_LOCAL_CHARS = re.compile(r"[<>\"'\\]")


# ============================================================================
# VALIDATION RESULT MODEL
# ============================================================================

@dataclass
class EmailValidationResult:
    """
    Detailed result of email validation.
    
    Attributes:
        is_valid: Whether the email passes all validation checks
        email: The validated email address
        reason: Explanation if validation failed
        warnings: List of non-fatal issues detected
        quality_score: Quality score (0-100, higher is better)
    """
    is_valid: bool
    email: str
    reason: str = ""
    warnings: list[str] = field(default_factory=list)
    quality_score: int = 100


# ============================================================================
# STRUCTURAL VALIDATION
# ============================================================================

def validate_email_structure(email: Optional[str]) -> EmailValidationResult:
    """
    Validate the structural correctness of an email address.
    
    This performs comprehensive validation including:
    - Basic format check (local@domain.tld)
    - Length validation
    - Character validation
    - Domain structure validation
    - Quality scoring
    
    Args:
        email: Email address to validate
        
    Returns:
        EmailValidationResult with detailed validation information
        
    Examples:
        >>> result = validate_email_structure("user@example.com")
        >>> result.is_valid
        True
        
        >>> result = validate_email_structure("invalid-email")
        >>> result.is_valid
        False
        >>> result.reason
        'Missing @ symbol'
    """
    if not email:
        return EmailValidationResult(
            is_valid=False,
            email="",
            reason="Email is empty or None"
        )
    
    email = email.strip().lower()
    warnings = []
    quality_score = 100
    
    # Check 1: Length validation
    if len(email) < config.validation.min_email_length:
        return EmailValidationResult(
            is_valid=False,
            email=email,
            reason=f"Email too short (minimum {config.validation.min_email_length} characters)"
        )
    
    if len(email) > config.validation.max_email_length:
        return EmailValidationResult(
            is_valid=False,
            email=email,
            reason=f"Email too long (maximum {config.validation.max_email_length} characters)"
        )
    
    # Check 2: Must contain exactly one @ symbol
    if "@" not in email:
        return EmailValidationResult(
            is_valid=False,
            email=email,
            reason="Missing @ symbol"
        )
    
    if email.count("@") != 1:
        return EmailValidationResult(
            is_valid=False,
            email=email,
            reason=f"Invalid number of @ symbols (found {email.count('@')})"
        )
    
    # Check 3: Split into local and domain parts
    try:
        local_part, domain_part = email.split("@")
    except ValueError:
        return EmailValidationResult(
            is_valid=False,
            email=email,
            reason="Failed to split email into local and domain parts"
        )
    
    # Check 4: Both parts must be non-empty
    if not local_part:
        return EmailValidationResult(
            is_valid=False,
            email=email,
            reason="Local part (before @) is empty"
        )
    
    if not domain_part:
        return EmailValidationResult(
            is_valid=False,
            email=email,
            reason="Domain part (after @) is empty"
        )
    
    # Check 5: Domain must contain at least one dot
    if "." not in domain_part:
        return EmailValidationResult(
            is_valid=False,
            email=email,
            reason="Domain must contain at least one dot"
        )
    
    # Check 6: Domain must have minimum number of parts
    domain_parts = domain_part.split(".")
    if len(domain_parts) < config.validation.min_domain_parts:
        return EmailValidationResult(
            is_valid=False,
            email=email,
            reason=f"Domain must have at least {config.validation.min_domain_parts} parts"
        )
    
    # Check 7: All domain parts must be non-empty
    if any(not part for part in domain_parts):
        return EmailValidationResult(
            is_valid=False,
            email=email,
            reason="Domain contains empty parts (consecutive dots)"
        )
    
    # Check 8: Basic pattern matching
    if not _EMAIL_PATTERN.match(email):
        return EmailValidationResult(
            is_valid=False,
            email=email,
            reason="Email does not match valid pattern"
        )
    
    # Check 9: Suspicious characters in local part
    if _SUSPICIOUS_LOCAL_CHARS.search(local_part):
        warnings.append("Local part contains suspicious characters")
        quality_score -= 20
    
    # Check 10: Very short local part (might be suspicious)
    if len(local_part) < 2:
        warnings.append("Local part is very short")
        quality_score -= 10
    
    # Check 11: Very short domain (might be suspicious)
    if len(domain_part) < 4:  # e.g., "a.co"
        warnings.append("Domain is very short")
        quality_score -= 10
    
    # Check 12: TLD validation
    tld = domain_parts[-1]
    if len(tld) < 2:
        return EmailValidationResult(
            is_valid=False,
            email=email,
            reason="TLD must be at least 2 characters"
        )
    
    # All checks passed
    return EmailValidationResult(
        is_valid=True,
        email=email,
        warnings=warnings,
        quality_score=max(0, quality_score)
    )


def is_valid_email(email: Optional[str]) -> bool:
    """
    Quick validation check for email address.
    
    This is a simplified version of validate_email_structure() that
    returns only a boolean result.
    
    Args:
        email: Email address to validate
        
    Returns:
        True if email is structurally valid, False otherwise
        
    Examples:
        >>> is_valid_email("user@example.com")
        True
        
        >>> is_valid_email("invalid-email")
        False
    """
    result = validate_email_structure(email)
    return result.is_valid


# ============================================================================
# PLAUSIBILITY CHECKS
# ============================================================================

def is_plausible_email(email: Optional[str]) -> bool:
    """
    Check if an email is plausible in HTML context (not a false positive).
    
    This performs ONLY basic structural checks beyond validation to filter
    out obvious false positives that can't be emails in HTML context:
    - File paths (containing / or \\)
    - Suspicious characters that break HTML parsing
    
    IMPORTANT: This does NOT check for asset patterns (logo@2x.png, etc.).
    Asset filtering is a business policy handled by filters.is_asset_email().
    
    Args:
        email: Email address to check
        
    Returns:
        True if email appears structurally plausible, False if obviously invalid
        
    Examples:
        >>> is_plausible_email("user@example.com")
        True
        
        >>> is_plausible_email("path/to/file@location.com")
        False
        
        >>> is_plausible_email("logo@2x.png")
        True  # Structurally valid, but filters.is_asset_email() will catch it
    """
    if not email:
        return False
    
    email = email.strip().lower()
    
    # Check 1: Must be structurally valid first
    if not is_valid_email(email):
        return False
    
    # Check 2: Reject if contains path separators (can't be email in HTML)
    if "/" in email or "\\" in email:
        return False
    
    # Check 3: Extract parts for basic validation
    local_part = extract_localpart_from_email(email)
    domain_part = extract_domain_from_email(email)
    
    if not local_part or not domain_part:
        return False
    
    return True


def calculate_email_quality_score(email: Optional[str]) -> int:
    """
    Calculate a quality score for an email address (0-100).
    
    Higher scores indicate higher confidence that the email is:
    - Structurally correct
    - Not a false positive in HTML context
    - Likely to be a real business contact
    
    IMPORTANT: This does NOT check business policy (assets, system emails).
    Use filters.should_filter_email() for business policy filtering.
    
    Args:
        email: Email address to score
        
    Returns:
        Quality score (0-100)
        
    Examples:
        >>> calculate_email_quality_score("info@example.com")
        100
        
        >>> calculate_email_quality_score("x@y.co")
        70
    """
    if not email:
        return 0
    
    # Start with structural validation
    result = validate_email_structure(email)
    if not result.is_valid:
        return 0
    
    score = result.quality_score
    
    # Additional quality checks
    local_part = extract_localpart_from_email(email)
    domain_part = extract_domain_from_email(email)
    
    # Bonus for common business email patterns
    business_patterns = ["info", "contact", "hello", "mail", "office"]
    if any(pattern in local_part for pattern in business_patterns):
        score += 10
    
    # Penalty for very generic local parts
    if local_part in ["a", "b", "c", "x", "y", "z"]:
        score -= 30
    
    # Bonus for reasonable length
    if 10 <= len(email) <= 50:
        score += 5
    
    # Penalty for very long emails
    if len(email) > 100:
        score -= 20
    
    # Check basic plausibility (HTML context safety only)
    if not is_plausible_email(email):
        score -= 50
    
    return max(0, min(100, score))


# ============================================================================
# BATCH VALIDATION
# ============================================================================

def validate_email_batch(emails: list[str]) -> list[EmailValidationResult]:
    """
    Validate multiple email addresses in batch.
    
    Args:
        emails: List of email addresses to validate
        
    Returns:
        List of validation results
    """
    return [validate_email_structure(email) for email in emails]


def filter_valid_emails(emails: list[str]) -> list[str]:
    """
    Filter a list of emails to only include valid ones.
    
    Args:
        emails: List of email addresses
        
    Returns:
        List of valid email addresses
        
    Examples:
        >>> filter_valid_emails(["valid@example.com", "invalid", "also@valid.com"])
        ['valid@example.com', 'also@valid.com']
    """
    return [email for email in emails if is_valid_email(email)]


def filter_plausible_emails(emails: list[str]) -> list[str]:
    """
    Filter a list of emails to only include plausible ones.
    
    Args:
        emails: List of email addresses
        
    Returns:
        List of plausible email addresses
    """
    return [email for email in emails if is_plausible_email(email)]