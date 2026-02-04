from __future__ import annotations

from typing import Any, Dict, Optional
import httpx

from ....core.config import settings


class GoogleGeocodingClient:
    """
    Google Geocoding API (legacy HTTP) used only to obtain geometry.viewport.
    GET https://maps.googleapis.com/maps/api/geocode/json
    """
    def __init__(self) -> None:
        api_key = getattr(settings, "geocoding_api_key", None) or getattr(settings, "google_maps_api_key", None)
        if not api_key:
            raise ValueError("Missing Geocoding API key (settings.geocoding_api_key or settings.google_maps_api_key)")
        self._api_key = api_key
        self._http = httpx.Client(timeout=30.0)
        self._url = "https://maps.googleapis.com/maps/api/geocode/json"

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "GoogleGeocodingClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def geocode(self, *, address: str) -> Dict[str, Any]:
        resp = self._http.get(self._url, params={"address": address, "key": self._api_key})
        resp.raise_for_status()
        return resp.json()


class GooglePlacesClient:
    """
    Places API (New) Text Search.
    POST https://places.googleapis.com/v1/places:searchText
    """
    def __init__(self) -> None:
        api_key = getattr(settings, "google_maps_api_key", None)
        if not api_key:
            raise ValueError("Missing Places API key (settings.google_maps_api_key)")

        self._api_key = api_key
        self._http = httpx.Client(timeout=30.0)
        self._url = "https://places.googleapis.com/v1/places:searchText"

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "GooglePlacesClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def search_text(
        self,
        *,
        text_query: str,
        language_code: str,
        include_pure_service_area: bool,
        page_size: int,
        page_token: Optional[str],
        location_restriction_rectangle: dict[str, Any],
        field_mask: str,
    ) -> Dict[str, Any]:
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self._api_key,
            "X-Goog-FieldMask": field_mask,
        }

        payload: Dict[str, Any] = {
            "textQuery": text_query,
            "languageCode": language_code,
            "includePureServiceAreaBusinesses": bool(include_pure_service_area),
            "pageSize": int(page_size),
            "locationRestriction": {
                "rectangle": location_restriction_rectangle
            },
        }
        if page_token:
            payload["pageToken"] = page_token

        resp = self._http.post(self._url, json=payload, headers=headers)
        resp.raise_for_status()
        return resp.json()
