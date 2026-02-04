from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID


@dataclass(frozen=True)
class WebScraperEnrichmentResultCreate:
    """
    Model for creating web scraper enrichment results.
    All pipeline fields are optional to support incremental updates.
    """
    # Keys
    request_id: str
    master_result_id: UUID

    # Snapshot from master_results
    organization: Optional[str] = None
    domain: Optional[str] = None
    source: Optional[str] = None

    # Crawl outputs - stored as JSON strings in DB
    contact_links: Optional[str] = None  # JSON string: ["url1", "url2"]
    contact_case: Optional[str] = None
    about_links: Optional[str] = None    # JSON string: ["url1"]
    about_case: Optional[str] = None

    # Crawl artifacts - stored as JSONB in DB
    crawl_homepage: Optional[str] = None  # JSON string: PageArtifact
    crawl_pages: Optional[str] = None     # JSON string: [PageArtifact]

    # Deterministic extraction
    det_status: Optional[str] = None   # running | ok | error
    det_emails: Optional[str] = None   # JSON string: [{"email": "...", "source_url": "..."}]
    det_error: Optional[str] = None

    # LLM extraction
    llm_status: Optional[str] = None   # running | ok | error
    llm_company: Optional[str] = None  # JSON string: {"name": "...", "evidence_url": "...", "evidence_quote": "..."}
    llm_emails: Optional[str] = None   # JSON string: [{"email": "...", "evidence_url": "...", "evidence_quote": "..."}]
    llm_contacts: Optional[str] = None # JSON string: [{"name": "...", "role": "...", "evidence_url": "...", "evidence_quote": "..."}]
    llm_about: Optional[str] = None    # JSON string: {"short_description": "...", "long_description": "..."}
    llm_error: Optional[str] = None

    # Overall pipeline status/debug (crawl phase only)
    extraction_status: Optional[str] = None  # pending | crawl_running | crawl_ok | crawl_error
    debug_message: Optional[str] = None


@dataclass(frozen=True)
class WebScraperEnrichmentResultRow(WebScraperEnrichmentResultCreate):
    """
    Model for web scraper enrichment result rows from database.
    Includes system fields (id, timestamps).
    """
    id: UUID = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
