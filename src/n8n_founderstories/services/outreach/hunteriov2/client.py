from __future__ import annotations

import httpx
from typing import Any, Sequence

from ....core.config import settings


class HunterClient:
    """
    Minimal Hunter.io client (discover only).
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
        payload: dict[str, Any] = {"limit": limit}

        if query and query.strip():
            payload["query"] = query.strip()

        if keywords:
            cleaned_keywords = [k.strip() for k in keywords if k and k.strip()]
            if cleaned_keywords:
                # Hunter API v2 uses "keywords" (plural) with include/match structure
                payload["keywords"] = {"include": cleaned_keywords, "match": "any"}

        if location:
            # Allowed keys: continent, business_region, country, city
            include_item: dict[str, str] = {}
            for key in ("continent", "business_region", "country", "city"):
                val = location.get(key)
                if val:
                    include_item[key] = val

            # enforce: if city present, country must be present
            if "city" in include_item and "country" not in include_item:
                raise ValueError(f"city requires country in location: {location!r}")

            if include_item:
                payload["headquarters_location"] = {"include": [include_item]}

        if headcount:
            cleaned_headcount = [h.strip() for h in headcount if isinstance(h, str) and h.strip()]
            if cleaned_headcount:
                payload["headcount"] = cleaned_headcount

        if industries:
            cleaned = [x.strip() for x in industries if x and x.strip()]
            if cleaned:
                payload["industry"] = {"include": cleaned}

        # require at least query OR keywords
        if "query" not in payload and "keywords" not in payload:
            raise ValueError("Hunter discover requires query or keywords")

        url = f"{self._base_url}/discover"
        resp = self._http.post(
            url,
            params={"api_key": self._api_key},
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()
