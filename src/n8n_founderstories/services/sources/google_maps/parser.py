from __future__ import annotations
from typing import Any
from .models import GooglePlacesInput


def _clean_str(x: Any) -> str | None:
    if not isinstance(x, str):
        return None
    s = x.strip()
    return s or None


def parse_search_plan(plan: dict[str, Any]) -> GooglePlacesInput:
    request_id = _clean_str(plan.get("request_id")) or ""
    language = _clean_str(plan.get("language")) or "en"

    # Read places_text_queries from plan
    places_text_queries = plan.get("places_text_queries") or []
    resolved_locations = plan.get("resolved_locations") or []
    
    # Read sheet_id (optional, can be sheet_id or google_sheet_id)
    sheet_id = _clean_str(plan.get("sheet_id")) or _clean_str(plan.get("google_sheet_id"))
    
    # Read max_pages from plan, default to 3 (matches GooglePlacesInput default)
    max_pages = plan.get("max_pages", 3)
    if isinstance(max_pages, (int, float)):
        max_pages = int(max_pages)
    else:
        max_pages = 3

    return GooglePlacesInput(
        request_id=request_id,
        language=language,
        places_text_queries=[q.strip() for q in places_text_queries if isinstance(q, str) and q.strip()],
        resolved_locations=resolved_locations if isinstance(resolved_locations, list) else [],
        include_pure_service_area=True,
        page_size=20,
        max_pages=max_pages,
        sheet_id=sheet_id,
    )
