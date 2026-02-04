# src/n8n_founderstories/services/web_scrapers/company_enrichment/models.py
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, EmailStr, Field, HttpUrl

Lang = Literal["de", "en"]


class Evidence(BaseModel):
    """
    Provenance container for any extracted fact.
    quote should be a short excerpt copied from the page content (no guessing).
    """
    url: HttpUrl
    quote: str = Field(
        ...,
        min_length=5,
        max_length=300,
        description="Short excerpt copied from the source page content.",
    )


class PageArtifact(BaseModel):
    """
    Canonical representation of a crawled page used by ALL extractors.
    - cleaned_html: preferred input for deterministic parsing
    - markdown: preferred input for LLM parsing
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

    # Optional bag for page classification + other signals.
    # Example: {"page_type": "impressum", "rank": 1, "keywords": ["kontakt"]}
    meta: Dict[str, Any] = Field(default_factory=dict)


class CrawlArtifacts(BaseModel):
    """
    Crawl output contract.

    selected_links is the canonical list of URLs that downstream extractors should use.
    If you want to track "contact vs about" split, store it in meta, e.g.:
      meta["contact_selected_links"] = [...]
      meta["about_selected_links"] = [...]
    """
    domain: str
    homepage: Optional[PageArtifact] = None
    pages: List[PageArtifact] = Field(default_factory=list)

    discovered_links: List[str] = Field(default_factory=list)
    selected_links: List[str] = Field(default_factory=list)

    meta: Dict[str, Any] = Field(default_factory=dict)


# -----------------------------
# Deterministic extraction output
# -----------------------------

class DeterministicEmail(BaseModel):
    """
    Deterministic email with provenance.
    Keep source_url for now; evidence can be added later without breaking callers.
    """
    email: EmailStr
    source_url: Optional[HttpUrl] = None
    evidence: Optional[Evidence] = None


class DeterministicExtraction(BaseModel):
    emails: List[DeterministicEmail] = Field(default_factory=list)

    # Debugging/telemetry fields (safe to log)
    pages_used: int = 0
    reason: str = "ok"
    debug: Optional[str] = None


# -----------------------------
# LLM extraction output
# -----------------------------

class LLMEmail(BaseModel):
    email: EmailStr
    evidence: Evidence


class LLMContact(BaseModel):
    name: str = Field(..., min_length=1, max_length=160)
    role: Optional[str] = Field(default=None, max_length=160)
    evidence: Evidence


class LLMCompany(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    evidence: Evidence


class LLMAbout(BaseModel):
    # Homepage / contact-pages: shorter, marketing-style / hero text summary
    short_description: Optional[str] = Field(default=None, min_length=20, max_length=600)
    short_evidence: Optional[Evidence] = None

    # About page: longer description
    long_description: Optional[str] = Field(default=None, min_length=20, max_length=1200)
    long_evidence: Optional[Evidence] = None


class LLMExtraction(BaseModel):
    language: Lang = "de"

    company: Optional[LLMCompany] = None
    emails: List[LLMEmail] = Field(default_factory=list)
    contacts: List[LLMContact] = Field(default_factory=list)
    about: Optional[LLMAbout] = None


# -----------------------------
# Enrichment Combine
# -----------------------------

class DomainEnrichment(BaseModel):
    domain: str

    crawl: CrawlArtifacts
    deterministic: DeterministicExtraction
    llm: LLMExtraction

    status: Literal["ok", "partial", "error"] = "ok"
    error: Optional[str] = None

    timings_ms: Dict[str, int] = Field(default_factory=dict)
    meta: Dict[str, Any] = Field(default_factory=dict)
