"""
Source adapters for Master consolidation.

Each adapter fetches normalized candidates from a specific source table.
"""

from .hunterio import fetch_candidates as fetch_hunter_candidates
from .google_maps import fetch_candidates as fetch_google_maps_candidates

__all__ = [
    "fetch_hunter_candidates",
    "fetch_google_maps_candidates",
]