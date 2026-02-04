"""
Web Search database models.

This module provides data structures for web search results persistence.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from ....core.utils.text import norm


def _normalize_url(url: str | None) -> str | None:
    """Normalize URL for deduplication (lowercase domain + path)."""
    if not url:
        return None
    url = norm(url)
    if not url:
        return None
    try:
        parsed = urlparse(url)
        # Normalize: lowercase domain, preserve path
        domain = parsed.netloc.lower() if parsed.netloc else ""
        path = parsed.path.rstrip("/") if parsed.path else ""
        if domain:
            return f"{domain}{path}".lower()
        return url.lower()
    except Exception:
        return url.lower()


def _build_dedupe_key_company(website: str | None, url: str | None) -> str:
    """
    Build dedupe key for company_hit.
    
    Uses website if present, otherwise falls back to url.
    Normalized to lowercase for case-insensitive deduplication.
    """
    key = _normalize_url(website or url)
    return key or "unknown"


def _build_dedupe_key_blog(
    organisation: str | None,
    website: str | None,
    source_url: str | None
) -> str:
    """
    Build dedupe key for blog_extracted.
    
    If website present: use normalized website
    Otherwise: use organisation + '|' + source_url
    """
    if website:
        normalized = _normalize_url(website)
        if normalized:
            return normalized
    
    # Fallback: org|blog_url
    org = norm(organisation) or "unknown"
    blog = _normalize_url(source_url) or "unknown"
    return f"{org.lower()}|{blog}"


@dataclass
class WebSearchResultRow:
    """
    Data structure for web_search_results database rows.
    
    Represents one organisation lead from either:
    - A company hit (classifier type=company)
    - A blog extracted company (from blog extractor)
    """
    job_id: str | None
    request_id: str
    source_type: str  # 'company_hit' | 'blog_extracted'
    organisation: str
    website: str | None
    query: str | None
    country: str | None  # ISO2
    location: str | None
    language: str | None
    domain: str | None
    source_url: str
    confidence: float | None
    reason: str | None
    evidence: str | None
    snippet: str | None
    raw_json: dict | None
    dedupe_key: str
    
    @classmethod
    def from_company_hit(
        cls,
        *,
        job_id: str | None,
        request_id: str,
        url: str,
        title: str | None = None,
        snippet: str | None = None,
        classification: dict | None = None,
        # Geo fields from hit lookup
        country: str | None = None,
        location: str | None = None,
        language: str | None = None,
        domain: str | None = None,
        query: str | None = None,
    ) -> "WebSearchResultRow":
        """
        Create WebSearchResultRow from a company hit.
        
        Args:
            job_id: Job identifier (optional)
            request_id: Request identifier
            url: Hit URL (becomes source_url)
            title: Hit title
            snippet: Hit snippet
            classification: Classification dict with type, confidence, reason, company_name, company_website
            country: ISO2 country code from hit
            location: Location string from hit
            language: Language code from hit
            domain: Google domain from hit
            query: Search query used
            
        Returns:
            WebSearchResultRow instance
        """
        classification = classification or {}
        
        # Extract organisation name
        organisation = norm(classification.get("company_name"))
        if not organisation:
            # Fallback: derive from title or domain
            organisation = norm(title) or urlparse(url).netloc or "Unknown"
        
        # Extract website
        website = norm(classification.get("company_website")) or norm(url)
        
        # Build dedupe key
        dedupe_key = _build_dedupe_key_company(website, url)
        
        # Build raw_json for provenance
        raw_json = {
            "url": url,
            "title": title,
            "snippet": snippet,
            "classification": classification,
            "country": country,
            "location": location,
            "language": language,
            "domain": domain,
        }
        
        return cls(
            job_id=norm(job_id) or None,
            request_id=norm(request_id),
            source_type="company_hit",
            organisation=organisation,
            website=website,
            query=norm(query) or None,
            country=norm(country) or None,
            location=norm(location) or None,
            language=norm(language) or None,
            domain=norm(domain) or None,
            source_url=norm(url),
            confidence=classification.get("confidence"),
            reason=norm(classification.get("reason")) or None,
            evidence=None,
            snippet=norm(snippet) or None,
            raw_json=raw_json,
            dedupe_key=dedupe_key,
        )
    
    @classmethod
    def from_blog_company(
        cls,
        *,
        job_id: str | None,
        request_id: str,
        blog_url: str,
        company_name: str,
        company_website: str | None = None,
        company_evidence: str | None = None,
        # Geo fields from blog hit lookup
        country: str | None = None,
        location: str | None = None,
        language: str | None = None,
        domain: str | None = None,
        query: str | None = None,
    ) -> "WebSearchResultRow":
        """
        Create WebSearchResultRow from a blog extracted company.
        
        Args:
            job_id: Job identifier (optional)
            request_id: Request identifier
            blog_url: Blog URL where company was extracted (becomes source_url)
            company_name: Extracted company name
            company_website: Extracted company website (optional)
            company_evidence: Evidence sentence from blog
            country: ISO2 country code from blog hit
            location: Location string from blog hit
            language: Language code from blog hit
            domain: Google domain from blog hit
            query: Search query used
            
        Returns:
            WebSearchResultRow instance
        """
        organisation = norm(company_name)
        if not organisation:
            raise ValueError("company_name must not be empty for blog extraction")
        
        website = norm(company_website) or None
        
        # Build dedupe key
        dedupe_key = _build_dedupe_key_blog(organisation, website, blog_url)
        
        # Build raw_json for provenance
        raw_json = {
            "blog_url": blog_url,
            "company_name": company_name,
            "company_website": company_website,
            "evidence": company_evidence,
            "country": country,
            "location": location,
            "language": language,
            "domain": domain,
        }
        
        return cls(
            job_id=norm(job_id) or None,
            request_id=norm(request_id),
            source_type="blog_extracted",
            organisation=organisation,
            website=website,
            query=norm(query) or None,
            country=norm(country) or None,
            location=norm(location) or None,
            language=norm(language) or None,
            domain=norm(domain) or None,
            source_url=norm(blog_url),
            confidence=None,
            reason=None,
            evidence=norm(company_evidence) or None,
            snippet=None,
            raw_json=raw_json,
            dedupe_key=dedupe_key,
        )