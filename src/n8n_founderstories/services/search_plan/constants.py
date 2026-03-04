from __future__ import annotations

# ============================================================================
# constants.py
#
# Role:
# - Static configuration for search plan generation
# - Regex patterns, banned keywords, and default geo locations
# - No business logic - pure data
#
# Note: Geo models are defined in models.py (Pydantic)
#       This file contains only plain dict data
# ============================================================================

import re
from typing import Any


# ============================================================================
# Regex Patterns
# ============================================================================

# Match single words (letters only, a-z)
WORD_PATTERN = re.compile(r"^[a-zA-Z]+$")


# ============================================================================
# Banned Keywords
# ============================================================================

# Generic filler words that should be excluded from keywords
BANNED_GENERIC_KEYWORDS = {
    "company",
    "companies",
    "brand",
    "brands",
    "product",
    "products",
    "business",
}


# ============================================================================
# DACH Default Locations
# ============================================================================
# Geographic bounds for Germany, Austria, and Switzerland
# Used as fallback when no location is specified and global_search is False
# Source: Standard geographic boundaries for these countries
#
# Note: These are plain dicts that will be converted to ResolvedLocation
#       Pydantic models in geo.py. The structure matches the ResolvedLocation
#       model defined in models.py.
# ============================================================================

DACH_DEFAULT_RESOLVED_LOCATIONS: list[dict[str, Any]] = [
    {
        "city": None,
        "state": None,
        "country": "DE",
        "country_name": "Germany",
        "geo": {
            "lat": 51.165691,
            "lng": 10.451526,
            "viewport": {
                "northeast": {"lat": 55.0815, "lng": 15.0418962},
                "southwest": {"lat": 47.270114, "lng": 5.8663425},
            },
            "rectangle": {
                "low":  {"latitude": 47.270114, "longitude": 5.8663425},
                "high": {"latitude": 55.0815, "longitude": 15.0418962},
            },
        },
    },
    {
        "city": None,
        "state": None,
        "country": "AT",
        "country_name": "Austria",
        "geo": {
            "lat": 47.516231,
            "lng": 14.550072,
            "viewport": {
                "northeast": {"lat": 49.0205305, "lng": 17.1608018},
                "southwest": {"lat": 46.37233579999999, "lng": 9.530734799999999},
            },
            "rectangle": {
                "low":  {"latitude": 46.37233579999999, "longitude": 9.530734799999999},
                "high": {"latitude": 49.0205305, "longitude": 17.1608018},
            },
        },
    },
    {
        "city": None,
        "state": None,
        "country": "CH",
        "country_name": "Switzerland",
        "geo": {
            "lat": 46.818188,
            "lng": 8.227511999999999,
            "viewport": {
                "northeast": {"lat": 47.8084546, "lng": 10.4923401},
                "southwest": {"lat": 45.81792, "lng": 5.9558978},
            },
            "rectangle": {
                "low":  {"latitude": 45.81792, "longitude": 5.9558978},
                "high": {"latitude": 47.8084546, "longitude": 10.4923401},
            },
        },
    },
]


# ============================================================================
# Language Codes
# ============================================================================

DACH_LANGUAGE_CODES = {
    "DE": "de",  # Germany: German
    "AT": "de",  # Austria: German
    "CH": "de",  # Switzerland: German (also fr, it, rm - but de is primary)
}