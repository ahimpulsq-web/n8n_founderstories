"""
Email Prioritization Module

This module handles the ranking and sorting of extracted emails based on:
- Page type priority (Impressum > Contact > Privacy > Home > Other)
- Domain matching with company domain
- Local-part patterns (info@, contact@, etc.)
- Email quality scores

Author: N8N FounderStories Team
Last Modified: 2026-02-18
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from ..config import config
from ..utils.domain_utils import (
    normalize_domain,
    extract_domain_from_email,
    extract_localpart_from_email,
    email_domain_matches,
)
from ..validators.email_validator import calculate_email_quality_score


# ============================================================================
# EMAIL PRIORITY MODEL
# ============================================================================

@dataclass
class EmailPriority:
    """
    Priority information for an email address.
    
    This encapsulates all factors that contribute to email ranking:
    - Page priority: Where the email was found
    - Domain match: Whether email domain matches company domain
    - Local-part rank: Quality of email prefix (info, contact, etc.)
    - Quality score: Overall email quality
    - Source URL: Where the email was extracted from
    
    Attributes:
        email: The email address
        page_priority: Priority score from page type (0-100)
        domain_match: Whether email domain matches company (0 or 1)
        localpart_rank: Rank of local-part pattern (lower is better)
        quality_score: Overall quality score (0-100)
        source_url: URL where email was found
        final_score: Computed final priority score
    """
    email: str
    page_priority: int = 0
    domain_match: int = 0
    localpart_rank: int = 99
    quality_score: int = 0
    source_url: str = ""
    final_score: float = field(init=False)
    
    def __post_init__(self):
        """Calculate final score based on all factors."""
        self.final_score = self._calculate_final_score()
    
    def _calculate_final_score(self) -> float:
        """
        Calculate final priority score using weighted factors.
        
        Scoring formula:
        - Page priority: 40% weight
        - Domain match: 30% weight (1000 bonus if matched)
        - Local-part rank: 20% weight (inverted, lower is better)
        - Quality score: 10% weight
        
        Returns:
            Final score (higher is better)
        """
        # Domain match bonus (very important)
        domain_bonus = self.domain_match * config.prioritization.domain_match_bonus
        
        # Page priority (normalized to 0-100)
        page_score = self.page_priority
        
        # Local-part rank (inverted and normalized)
        # Lower rank = better, so we invert it
        localpart_score = max(0, 100 - (self.localpart_rank * 10))
        
        # Quality score (already 0-100)
        quality = self.quality_score
        
        # Weighted combination
        final = (
            domain_bonus +
            (page_score * 0.4) +
            (localpart_score * 0.2) +
            (quality * 0.1)
        )
        
        return final
    
    def __lt__(self, other: EmailPriority) -> bool:
        """Compare priorities (for sorting)."""
        return self.final_score < other.final_score
    
    def __repr__(self) -> str:
        return (
            f"EmailPriority(email={self.email}, "
            f"final_score={self.final_score:.2f}, "
            f"page_priority={self.page_priority}, "
            f"domain_match={self.domain_match})"
        )


# ============================================================================
# PAGE TYPE PRIORITY
# ============================================================================

def get_page_type_priority(page_type: Optional[str]) -> int:
    """
    Get priority score for a page type.
    
    Args:
        page_type: Type of page (e.g., "impressum", "contact", "home")
        
    Returns:
        Priority score (0-100, higher is better)
        
    Examples:
        >>> get_page_type_priority("impressum")
        100
        
        >>> get_page_type_priority("contact")
        90
        
        >>> get_page_type_priority("unknown")
        10
    """
    if not page_type:
        return config.prioritization.page_type_priority.get("default", 10)
    
    page_type_lower = page_type.strip().lower()
    return config.prioritization.page_type_priority.get(
        page_type_lower,
        config.prioritization.page_type_priority.get("default", 10)
    )


def get_localpart_rank(email: Optional[str]) -> int:
    """
    Get rank for email's local-part pattern.
    
    Lower rank = higher priority (0 is best).
    
    Args:
        email: Email address
        
    Returns:
        Rank (0-99, lower is better)
        
    Examples:
        >>> get_localpart_rank("info@example.com")
        0
        
        >>> get_localpart_rank("contact@example.com")
        2
        
        >>> get_localpart_rank("random@example.com")
        99
    """
    if not email:
        return 99
    
    localpart = extract_localpart_from_email(email)
    if not localpart:
        return 99
    
    return config.prioritization.localpart_priority.get(localpart, 99)


# ============================================================================
# EMAIL PRIORITIZATION
# ============================================================================

def calculate_email_priority(
    email: str,
    company_domain: Optional[str] = None,
    page_type: Optional[str] = None,
    source_url: Optional[str] = None,
) -> EmailPriority:
    """
    Calculate comprehensive priority for an email address.
    
    Args:
        email: Email address to prioritize
        company_domain: Company's domain for matching
        page_type: Type of page where email was found
        source_url: URL where email was found
        
    Returns:
        EmailPriority object with all priority factors
        
    Examples:
        >>> priority = calculate_email_priority(
        ...     "info@example.com",
        ...     company_domain="example.com",
        ...     page_type="impressum"
        ... )
        >>> priority.domain_match
        1
        >>> priority.page_priority
        100
    """
    # Calculate page priority
    page_priority = get_page_type_priority(page_type)
    
    # Calculate domain match
    domain_match = 0
    if company_domain:
        domain_match = 1 if email_domain_matches(email, company_domain) else 0
    
    # Calculate local-part rank
    localpart_rank = get_localpart_rank(email)
    
    # Calculate quality score
    quality_score = calculate_email_quality_score(email)
    
    return EmailPriority(
        email=email,
        page_priority=page_priority,
        domain_match=domain_match,
        localpart_rank=localpart_rank,
        quality_score=quality_score,
        source_url=source_url or "",
    )


def prioritize_emails(
    emails: list[str],
    company_domain: Optional[str] = None,
    page_type: Optional[str] = None,
    source_url: Optional[str] = None,
) -> list[EmailPriority]:
    """
    Prioritize a list of emails and return sorted by priority.
    
    Args:
        emails: List of email addresses
        company_domain: Company's domain for matching
        page_type: Type of page where emails were found
        source_url: URL where emails were found
        
    Returns:
        List of EmailPriority objects, sorted by priority (best first)
        
    Examples:
        >>> emails = ["random@example.com", "info@example.com", "contact@other.com"]
        >>> priorities = prioritize_emails(emails, company_domain="example.com")
        >>> priorities[0].email
        'info@example.com'
    """
    priorities = [
        calculate_email_priority(
            email,
            company_domain=company_domain,
            page_type=page_type,
            source_url=source_url,
        )
        for email in emails
    ]
    
    # Sort by final score (descending)
    return sorted(priorities, key=lambda p: p.final_score, reverse=True)


# ============================================================================
# MULTI-SOURCE PRIORITIZATION
# ============================================================================

@dataclass
class EmailSource:
    """
    Information about where an email was found.
    
    Attributes:
        email: The email address
        page_type: Type of page
        source_url: URL where found
        page_priority: Priority of the page
    """
    email: str
    page_type: str = ""
    source_url: str = ""
    page_priority: int = 0


def merge_email_sources(
    sources: list[EmailSource],
    company_domain: Optional[str] = None,
) -> list[EmailPriority]:
    """
    Merge emails from multiple sources and prioritize.
    
    When the same email appears on multiple pages, we:
    1. Keep the highest priority source
    2. Aggregate information from all sources
    
    Args:
        sources: List of EmailSource objects
        company_domain: Company's domain for matching
        
    Returns:
        List of EmailPriority objects, deduplicated and sorted
        
    Examples:
        >>> sources = [
        ...     EmailSource("info@example.com", "contact", "url1", 90),
        ...     EmailSource("info@example.com", "impressum", "url2", 100),
        ... ]
        >>> priorities = merge_email_sources(sources)
        >>> priorities[0].page_priority
        100
    """
    # Group by email
    email_map: dict[str, list[EmailSource]] = {}
    
    for source in sources:
        email_lower = source.email.lower().strip()
        if email_lower not in email_map:
            email_map[email_lower] = []
        email_map[email_lower].append(source)
    
    # For each email, select best source
    priorities: list[EmailPriority] = []
    
    for email, email_sources in email_map.items():
        # Find source with highest page priority
        best_source = max(email_sources, key=lambda s: s.page_priority)
        
        # Calculate priority using best source
        priority = calculate_email_priority(
            email,
            company_domain=company_domain,
            page_type=best_source.page_type,
            source_url=best_source.source_url,
        )
        
        priorities.append(priority)
    
    # Sort by final score
    return sorted(priorities, key=lambda p: p.final_score, reverse=True)


def select_top_emails(
    priorities: list[EmailPriority],
    max_count: Optional[int] = None,
    min_score: Optional[float] = None,
) -> list[EmailPriority]:
    """
    Select top emails based on count and/or score threshold.
    
    Args:
        priorities: List of EmailPriority objects (should be sorted)
        max_count: Maximum number of emails to return
        min_score: Minimum score threshold
        
    Returns:
        Filtered list of EmailPriority objects
        
    Examples:
        >>> priorities = [
        ...     EmailPriority("info@example.com", final_score=100),
        ...     EmailPriority("contact@example.com", final_score=80),
        ...     EmailPriority("random@example.com", final_score=20),
        ... ]
        >>> top = select_top_emails(priorities, max_count=2, min_score=50)
        >>> len(top)
        2
    """
    result = priorities
    
    # Filter by minimum score
    if min_score is not None:
        result = [p for p in result if p.final_score >= min_score]
    
    # Limit count
    if max_count is not None and max_count > 0:
        result = result[:max_count]
    
    return result


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_best_email(
    emails: list[str],
    company_domain: Optional[str] = None,
) -> Optional[str]:
    """
    Get the single best email from a list.
    
    Args:
        emails: List of email addresses
        company_domain: Company's domain for matching
        
    Returns:
        Best email address, or None if list is empty
        
    Examples:
        >>> get_best_email(["random@x.com", "info@example.com"], "example.com")
        'info@example.com'
    """
    if not emails:
        return None
    
    priorities = prioritize_emails(emails, company_domain=company_domain)
    return priorities[0].email if priorities else None


def group_emails_by_domain(priorities: list[EmailPriority]) -> dict[str, list[EmailPriority]]:
    """
    Group emails by their domain.
    
    Args:
        priorities: List of EmailPriority objects
        
    Returns:
        Dictionary mapping domain to list of priorities
        
    Examples:
        >>> priorities = [
        ...     EmailPriority("info@example.com"),
        ...     EmailPriority("sales@example.com"),
        ...     EmailPriority("contact@other.com"),
        ... ]
        >>> grouped = group_emails_by_domain(priorities)
        >>> len(grouped["example.com"])
        2
    """
    result: dict[str, list[EmailPriority]] = {}
    
    for priority in priorities:
        domain = extract_domain_from_email(priority.email)
        if not domain:
            continue
        
        if domain not in result:
            result[domain] = []
        result[domain].append(priority)
    
    return result