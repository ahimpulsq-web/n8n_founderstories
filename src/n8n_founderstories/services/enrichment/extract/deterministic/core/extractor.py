"""
Main Email Extractor Module

This module provides the main extraction orchestration that ties together:
- Email parsing from pages
- Validation and filtering
- Prioritization and ranking
- Result aggregation

This is the primary interface for deterministic email extraction.

Author: N8N FounderStories Team
Last Modified: 2026-02-18
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from ....models import DeterministicEmail, DeterministicExtraction, PageArtifact
from ..config import config
from ..utils.domain_utils import normalize_domain
from .parser import parse_emails_from_text
from .prioritizer import EmailSource, merge_email_sources, select_top_emails, get_page_type_priority
from ..validators.filters import apply_standard_filters


# ============================================================================
# EXTRACTION RESULT MODEL
# ============================================================================

@dataclass
class ExtractionMetrics:
    """
    Metrics collected during extraction process.
    
    Attributes:
        pages_processed: Number of pages processed
        pages_with_emails: Number of pages containing emails
        total_candidates: Total email candidates found
        filtered_candidates: Candidates after filtering
        final_emails: Final number of emails returned
        extraction_time_ms: Time taken for extraction (milliseconds)
    """
    pages_processed: int = 0
    pages_with_emails: int = 0
    total_candidates: int = 0
    filtered_candidates: int = 0
    final_emails: int = 0
    extraction_time_ms: int = 0


# ============================================================================
# MAIN EXTRACTOR CLASS
# ============================================================================

class DeterministicExtractor:
    """
    Main class for deterministic email extraction.
    
    This class orchestrates the entire extraction pipeline:
    1. Parse emails from page content
    2. Validate and filter candidates
    3. Prioritize based on page type and domain
    4. Select top results
    5. Return structured output
    
    Usage:
        extractor = DeterministicExtractor(domain="example.com")
        result = extractor.extract(pages)
    """
    
    def __init__(
        self,
        domain: str,
        max_emails: Optional[int] = None,
        filter_generic: bool = False,
    ):
        """
        Initialize the extractor.
        
        Args:
            domain: Company domain for email matching
            max_emails: Maximum emails to return (uses config default if None)
            filter_generic: Whether to filter generic email providers
        """
        self.domain = normalize_domain(domain)
        self.max_emails = max_emails or config.limits.max_emails
        self.filter_generic = filter_generic
        self.metrics = ExtractionMetrics()
    
    def extract(self, pages: list[PageArtifact]) -> DeterministicExtraction:
        """
        Extract emails from a list of pages.
        
        This is the main entry point for extraction.
        
        Args:
            pages: List of PageArtifact objects to extract from
            
        Returns:
            DeterministicExtraction with results and metadata
        """
        start_time = time.perf_counter()
        
        try:
            # Step 1: Parse emails from all pages
            sources = self._parse_emails_from_pages(pages)
            
            # Step 2: Filter and validate
            filtered_sources = self._filter_sources(sources)
            
            # Step 3: Prioritize and merge
            priorities = merge_email_sources(filtered_sources, company_domain=self.domain)
            
            # Step 4: Select top emails
            top_priorities = select_top_emails(priorities, max_count=self.max_emails)
            
            # Step 5: Convert to output format
            emails = [
                DeterministicEmail(
                    email=p.email,
                    source_url=p.source_url or None,
                )
                for p in top_priorities
            ]
            
            # Update metrics
            self.metrics.final_emails = len(emails)
            self.metrics.extraction_time_ms = int((time.perf_counter() - start_time) * 1000)
            
            return DeterministicExtraction(
                emails=emails,
                pages_used=self.metrics.pages_processed,
                reason="ok",
            )
            
        except Exception as e:
            # Handle extraction errors gracefully
            self.metrics.extraction_time_ms = int((time.perf_counter() - start_time) * 1000)
            
            return DeterministicExtraction(
                emails=[],
                pages_used=self.metrics.pages_processed,
                reason=f"extraction_error: {str(e)}",
            )
    
    def _parse_emails_from_pages(self, pages: list[PageArtifact]) -> list[EmailSource]:
        """
        Parse emails from all pages and create EmailSource objects.
        
        Args:
            pages: List of PageArtifact objects
            
        Returns:
            List of EmailSource objects
        """
        sources: list[EmailSource] = []
        
        for page in pages or []:
            self.metrics.pages_processed += 1
            
            # Get page URL (prefer final_url if available)
            url = self._get_page_url(page)
            
            # Skip if URL looks like an asset
            if self._is_asset_url(url):
                continue
            
            # Get page type from metadata
            page_type = self._get_page_type(page)
            page_priority = get_page_type_priority(page_type)
            
            # Extract text content
            text = self._get_page_text(page)
            if not text:
                continue
            
            # Parse emails from text
            try:
                emails = parse_emails_from_text(text)
                
                if emails:
                    self.metrics.pages_with_emails += 1
                    self.metrics.total_candidates += len(emails)
                
                # Create EmailSource for each email
                for email in emails:
                    sources.append(
                        EmailSource(
                            email=email,
                            page_type=page_type,
                            source_url=url,
                            page_priority=page_priority,
                        )
                    )
            except Exception:
                # Skip pages that fail to parse
                continue
        
        return sources
    
    def _filter_sources(self, sources: list[EmailSource]) -> list[EmailSource]:
        """
        Filter email sources using standard filters.
        
        Args:
            sources: List of EmailSource objects
            
        Returns:
            Filtered list of EmailSource objects
        """
        # Extract unique emails
        emails = list(set(s.email for s in sources))
        
        # Apply standard filters
        filtered_emails = apply_standard_filters(emails, company_domain=self.domain)
        
        # Update metrics
        self.metrics.filtered_candidates = len(filtered_emails)
        
        # Keep only sources with filtered emails
        filtered_set = set(filtered_emails)
        return [s for s in sources if s.email in filtered_set]
    
    def _get_page_url(self, page: PageArtifact) -> str:
        """Get canonical URL for a page (prefer final_url)."""
        return str(getattr(page, "final_url", None) or getattr(page, "url", "") or "")
    
    def _get_page_type(self, page: PageArtifact) -> str:
        """Extract page type from page metadata."""
        try:
            return (page.meta.get("page_type") or "").strip().lower()
        except Exception:
            return ""
    
    def _get_page_text(self, page: PageArtifact) -> str:
        """Extract text content from page (prefer cleaned_html)."""
        return (getattr(page, "cleaned_html", "") or "").strip()
    
    def _is_asset_url(self, url: str) -> bool:
        """Check if URL appears to be an asset file."""
        if not url:
            return False
        
        url_lower = url.lower().strip()
        
        for suffix in config.validation.deny_domain_suffixes:
            if url_lower.endswith(suffix):
                return True
        
        return False
    
    def get_metrics(self) -> ExtractionMetrics:
        """Get extraction metrics."""
        return self.metrics


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def extract_emails_from_pages(
    domain: str,
    pages: list[PageArtifact],
    max_emails: Optional[int] = None,
) -> DeterministicExtraction:
    """
    Convenience function for extracting emails from pages.
    
    This is a simplified interface that creates an extractor and runs extraction.
    
    Args:
        domain: Company domain
        pages: List of PageArtifact objects
        max_emails: Maximum emails to return
        
    Returns:
        DeterministicExtraction with results
        
    Examples:
        >>> pages = [PageArtifact(url="https://example.com", cleaned_html="...")]
        >>> result = extract_emails_from_pages("example.com", pages)
        >>> result.emails
        [DeterministicEmail(email='info@example.com', ...)]
    """
    extractor = DeterministicExtractor(
        domain=domain,
        max_emails=max_emails,
    )
    return extractor.extract(pages)


def extract_emails_from_text(
    text: str,
    domain: Optional[str] = None,
    max_emails: Optional[int] = None,
) -> list[str]:
    """
    Extract and prioritize emails from a single text block.
    
    This is useful for quick extraction from a single source.
    
    Args:
        text: Text content
        domain: Optional company domain for prioritization
        max_emails: Maximum emails to return
        
    Returns:
        List of email addresses
        
    Examples:
        >>> text = "Contact us at info@example.com or sales@example.com"
        >>> extract_emails_from_text(text, domain="example.com", max_emails=1)
        ['info@example.com']
    """
    from .parser import parse_emails_from_text
    from .prioritizer import prioritize_emails
    
    # Parse emails
    emails = parse_emails_from_text(text)
    
    # Apply filters
    emails = apply_standard_filters(emails, company_domain=domain)
    
    # Prioritize if domain provided
    if domain:
        priorities = prioritize_emails(emails, company_domain=domain)
        emails = [p.email for p in priorities]
    
    # Limit count
    if max_emails and max_emails > 0:
        emails = emails[:max_emails]
    
    return emails


def quick_extract(
    domain: str,
    html_content: str,
    page_type: Optional[str] = None,
) -> list[str]:
    """
    Quick extraction from a single HTML content block.
    
    This is the simplest interface for one-off extractions.
    
    Args:
        domain: Company domain
        html_content: HTML content to extract from
        page_type: Optional page type for prioritization
        
    Returns:
        List of email addresses
        
    Examples:
        >>> html = "<html><body>Contact: info@example.com</body></html>"
        >>> quick_extract("example.com", html, page_type="contact")
        ['info@example.com']
    """
    # Create a minimal PageArtifact
    page = PageArtifact(
        url="http://temp.local",
        cleaned_html=html_content,
        meta={"page_type": page_type} if page_type else {},
    )
    
    # Extract
    result = extract_emails_from_pages(domain, [page])
    
    return [email.email for email in result.emails]


# ============================================================================
# BATCH EXTRACTION
# ============================================================================

def extract_from_multiple_domains(
    domain_pages: dict[str, list[PageArtifact]],
    max_emails_per_domain: Optional[int] = None,
) -> dict[str, DeterministicExtraction]:
    """
    Extract emails for multiple domains in batch.
    
    Args:
        domain_pages: Dictionary mapping domain to list of pages
        max_emails_per_domain: Maximum emails per domain
        
    Returns:
        Dictionary mapping domain to extraction results
        
    Examples:
        >>> domain_pages = {
        ...     "example.com": [page1, page2],
        ...     "other.com": [page3, page4],
        ... }
        >>> results = extract_from_multiple_domains(domain_pages)
        >>> results["example.com"].emails
        [...]
    """
    results = {}
    
    for domain, pages in domain_pages.items():
        extractor = DeterministicExtractor(
            domain=domain,
            max_emails=max_emails_per_domain,
        )
        results[domain] = extractor.extract(pages)
    
    return results