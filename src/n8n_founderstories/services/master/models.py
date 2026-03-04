"""
Master consolidation models.

Keep these minimal and stable:
- Used by source adapters + repository merge layer.
- No DB or Sheets logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True, slots=True)
class LeadCandidate:
    """
    Normalized lead candidate from a source.

    domain is the dedupe key and must be normalized to lowercase, stripped.
    sheet_id is the Google Sheet ID where results should be written.
    """
    domain: str
    organization: Optional[str] = None
    request_id: Optional[str] = None
    job_id: Optional[str] = None
    sheet_id: Optional[str] = None

    def __post_init__(self) -> None:
        d = (self.domain or "").strip().lower()
        if not d:
            raise ValueError("domain cannot be empty")

        org = (self.organization or "").strip() or None
        rid = (self.request_id or "").strip() or None
        jid = (self.job_id or "").strip() or None
        sid = (self.sheet_id or "").strip() or None

        # frozen=True => use object.__setattr__
        object.__setattr__(self, "domain", d)
        object.__setattr__(self, "organization", org)
        object.__setattr__(self, "request_id", rid)
        object.__setattr__(self, "job_id", jid)
        object.__setattr__(self, "sheet_id", sid)
