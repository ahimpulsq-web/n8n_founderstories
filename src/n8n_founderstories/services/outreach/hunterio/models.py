from __future__ import annotations

# =============================================================================
# models.py
#
# Classification:
# - Role: Hunter outreach data contract (tool-specific).
# - Consumers:
#   - runner.py (orchestration + persistence)
#   - api/v1/hunter.py (request/response)
# - Design:
#   - Minimal: only fields required for Sheets + JSON artifact.
# =============================================================================

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class HunterQueryType(str, Enum):
    """
    Classification: how a result was produced.
    """
    WEB_QUERY = "WEB_QUERY"
    KW_ANY_SINGLE = "KW_ANY_SINGLE"
    KW_ALL_FULL = "KW_ALL_FULL"
    KW_ALL_PAIRS = "KW_ALL_PAIRS"


class HunterCompany(BaseModel):
    """
    Normalized company record from Hunter Discover.

    Notes:
    - Hunter returns domain + organization reliably.
    - Location fields are not guaranteed; we store the *filter location* we used.
    """
    domain: str
    organization: str | None = None

    # Unified location label (country/city/state etc., as used in filter)
    location: str | None = None
    headcount_bucket: str | None = None
    # Intended (what our loop asked for)
    intended_location: str | None = None
    intended_headcount_bucket: str | None = None

    source_query: str | None = None
    query_type: HunterQueryType | None = None

    raw: dict[str, Any] = Field(default_factory=dict)

    @staticmethod
    def from_hunter_item(item: dict[str, Any]) -> "HunterCompany":
        """
        Robust mapping for Hunter Discover domain items.

        Expected minimal payload:
          { "domain": "cloudflare.com", "organization": "Cloudflare", ... }
        """
        if not isinstance(item, dict):
            return HunterCompany(domain="", raw={"_non_dict_item": repr(item)})

        domain = (item.get("domain") or "").strip().lower()
        org = (item.get("organization") or "").strip() or None

        return HunterCompany(
            domain=domain,
            organization=org,
            raw=item,
        )


class HunterRunResult(BaseModel):
    """
    Audit record for one /discover request.
    """
    query_type: HunterQueryType

    # Intended inputs (what we attempted)
    location: str | None = None
    headcount_bucket: str | None = None

    query_text: str | None = None
    keywords: list[str] = Field(default_factory=list)
    keyword_match: str | None = None  # "any" | "all" | None

    # Applied filters (what Hunter says it applied)
    applied_filters: dict[str, Any] = Field(default_factory=dict)
    applied_location: str | None = None
    applied_headcount_bucket: str | None = None

    returned_count: int = 0
    total_results: int | None = None



class HunterJobResult(BaseModel):
    """
    Canonical JSON artifact for one Hunter job execution.
    """
    request_id: str
    raw_prompt: str
    provider_name: str | None = None
    geo: str | None = None

    # Execution controls (what actually ran)
    web_queries_used: list[str] = Field(default_factory=list)
    keywords_used: list[str] = Field(default_factory=list)
    locations_used: list[str] = Field(default_factory=list)  # labels used (country + cities)
    headcount_buckets_used: list[str] = Field(default_factory=list)

    target_unique_domains: int = 250
    max_cities_per_country: int = 4

    runs: list[HunterRunResult] = Field(default_factory=list)
    companies: list[HunterCompany] = Field(default_factory=list)

    total_unique_domains: int = 0
