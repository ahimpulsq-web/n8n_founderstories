from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .serpapi_client import SerpApiClient
from .run_log import append_query_page_result

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GoogleSearchHit:
    title: str
    link: str
    snippet: Optional[str] = None
    displayed_link: Optional[str] = None

    source_query: Optional[str] = None
    source_location: Optional[str] = None
    source_language: Optional[str] = None
    source_country: Optional[str] = None
    source_domain: Optional[str] = None
    source_page: Optional[int] = None


class GoogleSearchService:
    def __init__(self, client: SerpApiClient):
        self._client = client

    def run_search_plan(
        self,
        *,
        web_queries: List[str],
        geo_location_keywords: Dict[str, Any],
        max_pages: int = 1,
        max_queries_per_location: int = 0,  # 0 = all
        engine: str = "google_light",
        resolve_locations: bool = True,
        prompt_language: Optional[str] = None,
    ) -> List[GoogleSearchHit]:
        hits: List[GoogleSearchHit] = []

        for country_code, cfg in (geo_location_keywords or {}).items():
            country_settings = self._client.resolve_country_settings(country_code, prompt_language)
            google_domain = country_settings.get("google_domain")
            hl = country_settings.get("hl")
            gl = country_settings.get("gl")

            raw_locations = (cfg or {}).get("locations") or []
            for raw_loc in raw_locations:
                location_to_use: Optional[str] = None

                if resolve_locations:
                    try:
                        location_to_use = self._client.resolve_location_id(raw_loc)
                        logger.info(
                            "LOCATION_RESOLVE | raw=%r | location_id=%r | country=%s",
                            raw_loc,
                            location_to_use,
                            country_code,
                        )
                    except Exception as e:
                        logger.warning("LOCATION_RESOLVE_FAILED | raw=%r | err=%s", raw_loc, str(e))

                # IMPORTANT: if no location_id, we fallback to None (no location restriction)
                # This prevents 0-hit runs due to bad location strings.
                queries = web_queries or []
                if max_queries_per_location and max_queries_per_location > 0:
                    queries = queries[:max_queries_per_location]

                for q in queries:
                    for page in range(max_pages):
                        data = self._client.search_google(
                            q,
                            engine=engine,
                            location=location_to_use,  # may be None
                            google_domain=google_domain,
                            hl=hl,
                            gl=gl,
                            start=page * 10,
                        )

                        page_hits = self._parse_organic_results(
                            data,
                            source_query=q,
                            source_country=country_code,
                            source_location=raw_loc,  # keep original label for your provenance
                            source_language=hl,
                            source_domain=google_domain,
                            source_page=page,
                        )

                        append_query_page_result(
                            query=q,
                            country=country_code,
                            location=raw_loc,
                            language=hl,
                            domain=google_domain,
                            page=page,
                            hits=page_hits,
                            reason="ok",
                        )

                        hits.extend(page_hits)

        return self._dedupe_hits(hits)

    def _parse_organic_results(
        self,
        data: Dict[str, Any],
        *,
        source_query: str,
        source_country: str,
        source_location: str,
        source_language: Optional[str],
        source_domain: Optional[str],
        source_page: int,
    ) -> List[GoogleSearchHit]:
        out: List[GoogleSearchHit] = []

        organic = data.get("organic_results") or []
        if not isinstance(organic, list):
            organic = []

        for item in organic:
            if not isinstance(item, dict):
                continue
            title = item.get("title")
            link = item.get("link")
            if not title or not link:
                continue

            out.append(
                GoogleSearchHit(
                    title=title,
                    link=link,
                    snippet=item.get("snippet"),
                    displayed_link=item.get("displayed_link"),
                    source_query=source_query,
                    source_country=source_country,
                    source_location=source_location,
                    source_language=source_language,
                    source_domain=source_domain,
                    source_page=source_page,
                )
            )
        return out

    def _dedupe_hits(self, hits: List[GoogleSearchHit]) -> List[GoogleSearchHit]:
        seen: set[str] = set()
        out: List[GoogleSearchHit] = []
        for h in hits:
            key = (h.link or "").strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(h)
        return out
