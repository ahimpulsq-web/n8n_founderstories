"""
Hunter.io data models module.

Defines data structures for Hunter.io service inputs.
Provides validation logic for search parameters.

Architecture:
    Parser → HunterInput (THIS MODULE) → Orchestrator

This module contains only data models and validation logic.
No business logic, no API calls, no database operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

# ============================================================================
# INPUT MODELS
# ============================================================================

@dataclass(frozen=True)
class HunterInput:
    """
    Validated input parameters for Hunter.io search.
    
    This immutable dataclass represents all parameters needed to execute
    a Hunter.io lead discovery search. It enforces validation rules and
    provides a clean interface between the parser and orchestrator.
    
    Search modes:
    - Query mode: target_prompt is set, keywords is empty
    - Keyword mode: keywords is set, target_prompt is empty
    - At least one mode must be active
    
    Attributes:
        request_id: Unique identifier for this search request (required)
        target_prompt: Natural language query (used as "query" in API)
        keywords: List of exact keywords (used as "keywords" in API)
        industries: Optional industry filters
        locations: List of location dicts with keys:
            - country: Country code
            - city: City name (requires country)
            - continent: Continent name
            - business_region: Business region
        sheet_id: Optional Google Sheet ID for export
        
    Example:
        >>> inp = HunterInput(
        ...     request_id="req_123",
        ...     keywords=["SaaS", "B2B"],
        ...     locations=[{"country": "DE", "city": "Berlin"}],
        ...     sheet_id="1A2B3C..."
        ... )
        >>> inp.validate()  # Raises ValueError if invalid
    """
    
    request_id: str
    """Unique identifier for this search request."""

    # Search parameters (at least one required)
    target_prompt: str | None = None
    """Natural language query (used as "query" in Hunter.io API)."""
    
    keywords: Sequence[str] = ()
    """List of exact keywords (used as "keywords" in Hunter.io API)."""

    # Filters
    industries: Sequence[str] | None = None
    """Optional industry filters."""

    locations: Sequence[dict[str, str]] = ()
    """
    List of location filters.
    
    Each location dict may contain:
    - country: Country code (required if city is present)
    - city: City name
    - continent: Continent name
    - business_region: Business region
    """

    # Export configuration
    sheet_id: str | None = None
    """Optional Google Sheet ID for exporting results."""

    def validate(self) -> None:
        """
        Validate input parameters.
        
        Validation rules:
        1. request_id must not be empty
        2. At least one of target_prompt or keywords must be provided
        3. Locations must be valid dicts
        4. City requires country in location
        5. Industries must not be empty if provided
        
        Raises:
            ValueError: If any validation rule fails
        """
        # Validate request_id
        if not self.request_id.strip():
            raise ValueError("request_id is required")

        # Validate search parameters (at least one required)
        has_target = bool(self.target_prompt and self.target_prompt.strip())
        has_keywords = bool(self.keywords and any(k.strip() for k in self.keywords))

        if not (has_target or has_keywords):
            raise ValueError("Either target_prompt or keywords must be provided")

        # Validate locations
        for loc in self.locations or []:
            if not isinstance(loc, dict):
                raise ValueError(f"Invalid location entry: {loc!r}")
            if "city" in loc and "country" not in loc:
                raise ValueError(f"Location has city but no country: {loc!r}")

        # Validate industries
        if self.industries is not None:
            cleaned = [x.strip() for x in self.industries if x and x.strip()]
            if not cleaned:
                raise ValueError("industries was provided but is empty after cleaning")
