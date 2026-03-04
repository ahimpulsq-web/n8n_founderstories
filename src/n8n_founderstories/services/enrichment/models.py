"""
═══════════════════════════════════════════════════════════════════════════════
ENRICHMENT DATA MODELS - Shared Type Definitions
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [MODELS] - Shared data structures for enrichment pipeline

PURPOSE:
    Defines Pydantic models used across the enrichment pipeline for type safety,
    validation, and data consistency. These models represent the core data
    structures flowing through crawl → extract → aggregate stages.

MODEL CATEGORIES:

    [CRAWL MODELS]
    - PageArtifact: Single crawled page with HTML and markdown content
    - CrawlArtifacts: Complete crawl result for a domain (homepage + pages)

    [EXTRACTION MODELS - Deterministic]
    - DeterministicEmail: Email found via regex/parsing
    - DeterministicExtraction: Collection of deterministically extracted emails

    [EXTRACTION MODELS - LLM]
    - Evidence: Provenance container (URL + quote from source)
    - LLMEmail: Email extracted by LLM with evidence
    - LLMContact: Contact person extracted by LLM with evidence
    - LLMCompany: Company name extracted by LLM with evidence
    - LLMAbout: Company description extracted by LLM with evidence
    - LLMExtraction: Complete LLM extraction result

USAGE:
    These models are imported throughout the enrichment pipeline:
    - crawl/: Uses PageArtifact, CrawlArtifacts
    - extract/deterministic/: Uses PageArtifact, DeterministicEmail, DeterministicExtraction
    - extract/llm/: Uses PageArtifact, LLM* models, Evidence
    - aggregate/: Consumes all extraction models

VALIDATION:
    All models use Pydantic for automatic validation:
    - Type checking at runtime
    - Field constraints (min/max length, patterns)
    - Automatic serialization/deserialization

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, EmailStr, Field, HttpUrl

# Type alias for language codes
Lang = Literal["de", "en"]


# ═══════════════════════════════════════════════════════════════════════════
# CRAWL MODELS
# ═══════════════════════════════════════════════════════════════════════════

class PageArtifact(BaseModel):
    """
    Canonical representation of a single crawled web page.
    
    Used by ALL extractors (deterministic and LLM) as the input format.
    Contains both HTML (for deterministic parsing) and markdown (for LLM extraction).
    
    Attributes:
        url: Original requested URL
        final_url: Final URL after redirects (optional)
        status_code: HTTP status code (optional)
        cleaned_html: Sanitized HTML suitable for deterministic parsing
        markdown: Markdown conversion suitable for LLM extraction
        title: Page title (optional)
        fetched_at_utc: ISO timestamp when page was fetched (optional)
        error: Error message if fetch failed (optional)
        links: List of discovered links on the page
        meta: Flexible metadata bag for page classification and signals
              Example: {"page_type": "impressum", "rank": 1, "keywords": ["kontakt"]}
    """
    url: HttpUrl
    final_url: Optional[HttpUrl] = None
    status_code: Optional[int] = None

    cleaned_html: str = Field(
        default="",
        description="Sanitized/clean HTML suitable for deterministic parsing.",
    )
    markdown: str = Field(
        default="",
        description="Markdown suitable for LLM extraction.",
    )

    title: Optional[str] = None
    fetched_at_utc: Optional[str] = None
    error: Optional[str] = None
    links: List[str] = Field(default_factory=list)

    # Flexible metadata bag for page classification and other signals
    meta: Dict[str, Any] = Field(default_factory=dict)


class CrawlArtifacts(BaseModel):
    """
    Complete crawl result for a domain.
    
    Contains the homepage plus any discovered contact/impressum/about pages.
    The selected_links list represents the canonical set of URLs that downstream
    extractors should process.
    
    Attributes:
        domain: Normalized domain name (lowercase, no protocol)
        homepage: Homepage PageArtifact (always present if crawl succeeded)
        pages: Additional discovered pages (contact, impressum, etc.)
        discovered_links: All links found during crawl
        selected_links: Canonical list of URLs for downstream processing
        meta: Metadata including contact_case, about_case, and typed link lists
              Example: {
                  "contact_case": "1",  # Case-based discovery result
                  "contact_selected_links": [...],
                  "contact_selected_typed_links": [{"url": "...", "kind": "impressum"}],
                  "about_selected_links": [...],
                  "about_case": "about_anchor"
              }
    """
    domain: str
    homepage: Optional[PageArtifact] = None
    pages: List[PageArtifact] = Field(default_factory=list)

    discovered_links: List[str] = Field(default_factory=list)
    selected_links: List[str] = Field(default_factory=list)

    meta: Dict[str, Any] = Field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════════════════
# DETERMINISTIC EXTRACTION MODELS
# ═══════════════════════════════════════════════════════════════════════════

class DeterministicEmail(BaseModel):
    """
    Email address found via deterministic extraction (regex/parsing).
    
    Includes provenance information to track where the email was found.
    
    Attributes:
        email: Validated email address
        source_url: URL where email was found (optional)
        evidence: Detailed provenance with quote from source (optional)
    """
    email: EmailStr
    source_url: Optional[HttpUrl] = None
    evidence: Optional['Evidence'] = None


class DeterministicExtraction(BaseModel):
    """
    Collection of emails found via deterministic extraction.
    
    Includes metadata about the extraction process for debugging and telemetry.
    
    Attributes:
        emails: List of extracted emails with provenance
        pages_used: Number of pages processed
        reason: Status message ("ok" or error description)
        debug: Optional debug information
    """
    emails: List[DeterministicEmail] = Field(default_factory=list)

    # Debugging/telemetry fields (safe to log)
    pages_used: int = 0
    reason: str = "ok"
    debug: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════
# LLM EXTRACTION MODELS
# ═══════════════════════════════════════════════════════════════════════════

class Evidence(BaseModel):
    """
    Provenance container for any LLM-extracted fact.
    
    Provides traceability by linking extracted data back to its source.
    The quote should be a short excerpt copied directly from the page content.
    
    Attributes:
        url: Source URL where information was found
        quote: Short excerpt from source (5-300 chars, copied verbatim)
    """
    url: HttpUrl
    quote: str = Field(
        ...,
        min_length=5,
        max_length=300,
        description="Short excerpt copied from the source page content.",
    )


class LLMEmail(BaseModel):
    """
    Email address extracted by LLM with provenance.
    
    Attributes:
        email: Validated email address
        evidence: Source URL and quote showing where email was found
    """
    email: EmailStr
    evidence: Evidence


class LLMContact(BaseModel):
    """
    Contact person extracted by LLM with provenance.
    
    Attributes:
        name: Person's name (1-160 chars)
        role: Person's role/title (optional, max 160 chars)
        evidence: Source URL and quote showing where contact was found
    """
    name: str = Field(..., min_length=1, max_length=160)
    role: Optional[str] = Field(default=None, max_length=160)
    evidence: Evidence


class LLMCompany(BaseModel):
    """
    Company name extracted by LLM with provenance.
    
    Attributes:
        name: Company name (1-200 chars)
        evidence: Source URL and quote showing where company name was found
    """
    name: str = Field(..., min_length=1, max_length=200)
    evidence: Evidence


class LLMAbout(BaseModel):
    """
    Company description extracted by LLM with provenance.
    
    Supports two description types:
    - short_description: Marketing-style summary from homepage/contact pages (20-600 chars)
    - long_description: Detailed description from about page (20-1200 chars)
    
    Attributes:
        short_description: Brief company summary (optional)
        short_evidence: Source for short description (optional)
        long_description: Detailed company description (optional)
        long_evidence: Source for long description (optional)
    """
    # Homepage / contact-pages: shorter, marketing-style / hero text summary
    short_description: Optional[str] = Field(default=None, min_length=20, max_length=600)
    short_evidence: Optional[Evidence] = None

    # About page: longer description
    long_description: Optional[str] = Field(default=None, min_length=20, max_length=1200)
    long_evidence: Optional[Evidence] = None


class LLMExtraction(BaseModel):
    """
    Complete LLM extraction result for a domain.
    
    Aggregates all information extracted by the LLM from crawled pages.
    
    Attributes:
        language: Detected/specified language (de or en)
        company: Extracted company name with evidence (optional)
        emails: List of extracted emails with evidence
        contacts: List of extracted contact persons with evidence
        about: Extracted company descriptions with evidence (optional)
    """
    language: Lang = "de"

    company: Optional[LLMCompany] = None
    emails: List[LLMEmail] = Field(default_factory=list)
    contacts: List[LLMContact] = Field(default_factory=list)
    about: Optional[LLMAbout] = None