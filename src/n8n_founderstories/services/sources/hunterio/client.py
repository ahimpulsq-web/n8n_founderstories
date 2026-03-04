"""
Hunter.io HTTP client module.

Provides a minimal HTTP client for Hunter.io API discover endpoint.
Handles request construction and response parsing.

Architecture:
    Policy (retry + rate limit)
         ↓
    Client (THIS MODULE) - pure HTTP calls
         ↓
    Hunter.io API

This module focuses ONLY on HTTP communication.
It does NOT handle:
- Retry logic (see policy.py)
- Rate limiting (see policy.py)
- Business logic (see orchestrator.py)
"""

from __future__ import annotations

import httpx
from typing import Any, Sequence

from n8n_founderstories.core.config import settings

# ============================================================================
# HTTP CLIENT
# ============================================================================

class HunterClient:
    """
    Hunter.io API HTTP client.
    
    Minimal client for the Hunter.io discover endpoint.
    Handles request construction and basic HTTP operations.
    
    Features:
    - Context manager support (auto-close)
    - Request parameter validation
    - Clean API response parsing
    
    Note: This client does NOT handle retry or rate limiting.
    Use HunterAPIPolicy to wrap calls with retry and rate limiting.
    
    Usage:
        with HunterClient() as client:
            data = client.discover(
                query="SaaS companies",
                location={"country": "DE"},
                headcount=["1-10"],
            )
    """

    def __init__(self) -> None:
        api_key = getattr(settings, "hunter_api_key", None)
        if not api_key:
            raise ValueError("Hunter API key is missing (settings.hunter_api_key)")

        self._api_key = api_key
        self._base_url = "https://api.hunter.io/v2"
        self._http = httpx.Client(timeout=30.0)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "HunterClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def discover(
        self,
        *,
        query: str | None,
        keywords: list[str] | None,
        location: dict[str, str] | None,
        headcount: Sequence[str] | None = None,
        industries: Sequence[str] | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """
        Call Hunter.io discover endpoint to find companies.
        
        Constructs and executes a POST request to the Hunter.io discover API.
        Validates parameters and formats them according to API requirements.
        
        Search modes:
        - Query mode: Use `query` parameter (natural language search)
        - Keyword mode: Use `keywords` parameter (exact keyword matching)
        - At least one of query or keywords is required
        
        Args:
            query: Natural language search query (e.g., "SaaS companies")
            keywords: List of exact keywords to match (e.g., ["SaaS", "B2B"])
            location: Location filter with keys:
                - continent: Continent name
                - business_region: Business region
                - country: Country code (required if city is provided)
                - city: City name
            headcount: List of headcount buckets (e.g., ["1-10", "11-50"])
            industries: List of industry filters
            limit: Maximum results per request (default: 100, max: 100)
            
        Returns:
            Hunter.io API response dictionary with structure:
            {
                "data": [{"domain": "...", "organization": "...", ...}],
                "meta": {"results": 42, ...}
            }
            
        Raises:
            ValueError: If validation fails (missing query/keywords, invalid location)
            httpx.HTTPStatusError: If API returns error status
            httpx.TimeoutException: If request times out
            httpx.TransportError: If network error occurs
            
        Example:
            >>> client = HunterClient()
            >>> data = client.discover(
            ...     query="SaaS companies",
            ...     location={"country": "DE", "city": "Berlin"},
            ...     headcount=["1-10", "11-50"],
            ...     limit=100
            ... )
            >>> len(data["data"])
            42
        """
        # Build request payload
        payload: dict[str, Any] = {"limit": limit}

        # Add query parameter (natural language search)
        if query and query.strip():
            payload["query"] = query.strip()

        # Add keywords parameter (exact keyword matching)
        if keywords:
            cleaned_keywords = [k.strip() for k in keywords if k and k.strip()]
            if cleaned_keywords:
                # Hunter API v2 uses "keywords" (plural) with include/match structure
                payload["keywords"] = {"include": cleaned_keywords, "match": "any"}

        # Add location filter
        if location:
            # Allowed keys: continent, business_region, country, city
            include_item: dict[str, str] = {}
            for key in ("continent", "business_region", "country", "city"):
                val = location.get(key)
                if val:
                    include_item[key] = val

            # Validate: city requires country
            if "city" in include_item and "country" not in include_item:
                raise ValueError(f"city requires country in location: {location!r}")

            if include_item:
                payload["headquarters_location"] = {"include": [include_item]}

        # Add headcount filter
        if headcount:
            cleaned_headcount = [h.strip() for h in headcount if isinstance(h, str) and h.strip()]
            if cleaned_headcount:
                payload["headcount"] = cleaned_headcount

        # Add industry filter
        if industries:
            cleaned = [x.strip() for x in industries if x and x.strip()]
            if cleaned:
                payload["industry"] = {"include": cleaned}

        # Validate: require at least query OR keywords
        if "query" not in payload and "keywords" not in payload:
            raise ValueError("Hunter discover requires query or keywords")

        # Execute HTTP request
        url = f"{self._base_url}/discover"
        resp = self._http.post(
            url,
            params={"api_key": self._api_key},
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()
