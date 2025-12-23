from __future__ import annotations

# =============================================================================
# client.py
#
# Classification:
# - Role: Google Places API client (HTTP only).
# - Responsibilities:
#   - Load configuration from Settings (single source of truth)
#   - Execute Places Text Search requests
#   - Execute Place Details requests (optional enrichment)
#   - Raise typed provider errors for consistent API/job handling
# - Non-goals:
#   - Orchestration loops
#   - Sheets writing
#   - Persistence
# =============================================================================

import logging
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any

import httpx

from ....core.config import settings
from ....core.utils.text import norm
from ..errors import LocationConfigError, LocationProviderError

logger = logging.getLogger(__name__)


# =============================================================================
# Classification: Rate limiting
# =============================================================================

class GoogleRateLimiter:
    """
    Classification: simple throttler.

    Policy:
    - Blocking waits are acceptable for background jobs.
    - Keep conservative default to avoid quota spikes.
    """
    min_delay_seconds: float = 0.25

    def __init__(self) -> None:
        self._lock = Lock()
        self._last_ts: float | None = None

    def wait(self) -> None:
        with self._lock:
            now = time.time()
            if self._last_ts is not None:
                elapsed = now - self._last_ts
                remaining = self.min_delay_seconds - elapsed
                if remaining > 0:
                    time.sleep(remaining)
            self._last_ts = time.time()


# =============================================================================
# Classification: Configuration
# =============================================================================

@dataclass(frozen=True)
class GooglePlacesClientConfig:
    api_key: str
    base_url: str  # e.g. "https://maps.googleapis.com/maps/api/place"
    timeout_seconds: float = 20.0


# =============================================================================
# Classification: Client
# =============================================================================

class GooglePlacesClient:
    """
    Google Places API client.

    Supported endpoints (derived from Settings.google_places_base_url):
      - Text Search:  {base_url}/textsearch/json
      - Details:      {base_url}/details/json
    """

    def __init__(self, *, config: GooglePlacesClientConfig | None = None) -> None:
        cfg = config or self._default_config()

        self._api_key = cfg.api_key
        self._base_url = cfg.base_url.rstrip("/")
        self._http = httpx.Client(timeout=cfg.timeout_seconds)

        # Separate throttles allow different policies later if needed.
        self._rate_text = GoogleRateLimiter()
        self._rate_details = GoogleRateLimiter()

    @staticmethod
    def _default_config() -> GooglePlacesClientConfig:
        api_key = norm(getattr(settings, "google_maps_api_key", None))
        if not api_key:
            raise LocationConfigError("Google Maps API key is not configured (settings.google_maps_api_key).")

        base_url = norm(getattr(settings, "google_places_base_url", None))
        if not base_url:
            raise LocationConfigError("Google Places base URL is not configured (settings.google_places_base_url).")

        return GooglePlacesClientConfig(api_key=api_key, base_url=base_url)

    def close(self) -> None:
        """Classification: release HTTP resources."""
        try:
            self._http.close()
        except Exception:
            logger.debug("GOOGLE_HTTP_CLOSE_FAILED", exc_info=True)

    def __enter__(self) -> "GooglePlacesClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # -------------------------------------------------------------------------
    # Classification: Places Text Search
    # -------------------------------------------------------------------------

    def text_search(self, *, query: str, language: str, region: str) -> dict[str, Any]:
        """
        Execute one Places Text Search request.

        Args:
          query: fully composed query string ("<base> in <location>")
          language: best-effort language tag (from SearchPlan hl)
          region: iso2 (ccTLD bias), e.g. "jp", "de"
        """
        self._rate_text.wait()

        q = norm(query)
        if not q:
            raise ValueError("Google Places text_search requires non-empty query.")

        params: dict[str, Any] = {
            "key": self._api_key,
            "query": q,
            "language": norm(language) or "en",
            "region": norm(region).lower(),
        }

        url = f"{self._base_url}/textsearch/json"

        try:
            resp = self._http.get(url, params=params)
            resp.raise_for_status()
        except Exception as exc:
            raise LocationProviderError(f"Google Places Text Search HTTP error.") from exc

        data = resp.json() if resp.content else {}
        status = norm(str(data.get("status") or "")).upper()

        # Treat ZERO_RESULTS as successful request with no hits.
        if status in {"OK", "ZERO_RESULTS"}:
            return data

        msg = norm(data.get("error_message")) or status or "UNKNOWN"
        raise LocationProviderError(f"Google Places Text Search status={status} error={msg}")

    # -------------------------------------------------------------------------
    # Classification: Place Details (enrichment)
    # -------------------------------------------------------------------------

    def place_details(
        self,
        *,
        place_id: str,
        language: str,
        region: str,
        fields: str,
    ) -> dict[str, Any]:
        """
        Execute one Place Details request.

        fields: comma-separated list of required fields, e.g.:
          "website,url,formatted_phone_number,international_phone_number"
        """
        self._rate_details.wait()

        pid = norm(place_id)
        if not pid:
            raise ValueError("place_id must not be empty for place_details.")

        params: dict[str, Any] = {
            "key": self._api_key,
            "place_id": pid,
            "language": norm(language) or "en",
            "region": norm(region).lower(),
            "fields": norm(fields),
        }

        url = f"{self._base_url}/details/json"

        try:
            resp = self._http.get(url, params=params)
            resp.raise_for_status()
        except Exception as exc:
            raise LocationProviderError("Google Places Details HTTP error.") from exc

        data = resp.json() if resp.content else {}
        status = norm(str(data.get("status") or "")).upper()

        if status == "OK":
            return data

        msg = norm(data.get("error_message")) or status or "UNKNOWN"
        raise LocationProviderError(f"Google Places Details status={status} error={msg}")
