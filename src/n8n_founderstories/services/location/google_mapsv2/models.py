from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Sequence


@dataclass(frozen=True)
class GooglePlacesInput:
    request_id: str
    language: str = "en"

    # Search
    places_text_queries: Sequence[str] = ()
    include_pure_service_area: bool = True

    # Locations (raw resolved_locations from search plan)
    resolved_locations: Sequence[dict[str, Any]] = ()

    # Pagination
    page_size: int = 20          # max 20
    max_pages: int = 3           # default 3 pages
    
    # Sheets export (optional)
    sheet_id: str | None = None

    def validate(self) -> None:
        if not (self.request_id or "").strip():
            raise ValueError("request_id is required")

        # Validate places_text_queries
        queries = [q.strip() for q in self.places_text_queries if isinstance(q, str) and q.strip()]
        if not queries:
            raise ValueError("places_text_queries must contain at least 1 non-empty string")

        if not (1 <= int(self.page_size) <= 20):
            raise ValueError("page_size must be 1..20")

        if int(self.max_pages) < 1:
            raise ValueError("max_pages must be >= 1")

        if self.resolved_locations is None:
            raise ValueError("resolved_locations must not be None")
