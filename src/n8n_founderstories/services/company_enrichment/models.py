from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID


@dataclass(frozen=True)
class CompanyEnrichmentResultCreate:
    # Keys
    request_id: str
    master_result_id: UUID

    # Denormalized master fields (copied in for convenience)
    organization: Optional[str] = None
    domain: Optional[str] = None
    source: Optional[str] = None

    # Extractor outputs
    emails: Optional[str] = None
    contacts: Optional[str] = None
    extraction_status: Optional[str] = None
    debug_message: Optional[str] = None


@dataclass(frozen=True)
class CompanyEnrichmentResultRow(CompanyEnrichmentResultCreate):
    id: UUID = None  # set by DB
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
