from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class SerpApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class SerpApiConfig:
    api_key: str
    base_url: str = "https://serpapi.com/search.json"
    timeout_s: int = 30


class SerpApiClient:
    """
    Minimal SerpAPI client for Google Search + Locations.

    - Search API: https://serpapi.com/search-api
    - Locations API: https://serpapi.com/locations-api

    IMPORTANT:
    - Under uvicorn, environment variables from .env are not guaranteed unless
      you load them in-process. We do that in from_env().
    """

    _google_domains: Optional[dict] = None
    _google_languages: Optional[dict] = None
    _google_countries: Optional[dict] = None

    def __init__(self, config: SerpApiConfig):
        if not config.api_key:
            raise ValueError("Missing SerpAPI api_key (SERPAPI_API_KEY).")
        self._cfg = config

    @classmethod
    def from_env(cls) -> "SerpApiClient":
        """
        Uvicorn-safe .env loading.
        """
        load_dotenv()  # loads .env from project root (or current working dir)
        api_key = os.getenv("SERPAPI_API_KEY", "").strip()

        if not api_key:
            # Make it explicit in logs why you got 0 hits / failures
            logger.error("SERPAPI_API_KEY is missing/empty. Check your .env and process env.")
            raise ValueError("Missing SerpAPI api_key (SERPAPI_API_KEY).")

        logger.info("SerpApiClient initialized from env (api_key_present=%s).", bool(api_key))
        return cls(SerpApiConfig(api_key=api_key))

    # ---------------------------
    # Locations
    # ---------------------------

    def resolve_location_id(self, q: str, *, limit: int = 1) -> Optional[str]:
        """
        Resolve a human location string to a SerpAPI location_id (preferred).
        """
        params = {
            "q": q,
            "limit": limit,
            "api_key": self._cfg.api_key,
        }

        try:
            r = requests.get(
                "https://serpapi.com/locations.json",
                params=params,
                timeout=self._cfg.timeout_s,
            )
        except requests.RequestException as e:
            raise SerpApiError(f"SerpAPI locations request failed: {e}") from e

        if r.status_code != 200:
            raise SerpApiError(f"SerpAPI locations HTTP {r.status_code}: {r.text[:300]}")

        data = r.json()
        if not isinstance(data, list) or not data:
            return None

        first = data[0] if isinstance(data[0], dict) else {}
        # SerpAPI commonly uses `id`
        loc_id = first.get("id") or first.get("location_id")
        if loc_id is None:
            return None
        return str(loc_id)

    # ---------------------------
    # Search
    # ---------------------------

    def search_google(
        self,
        q: str,
        *,
        engine: str = "google_light",
        location: Optional[str] = None,  # can be location_id string
        google_domain: Optional[str] = None,
        hl: Optional[str] = None,
        gl: Optional[str] = None,
        start: int = 0,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "engine": engine,
            "q": q,
            "api_key": self._cfg.api_key,
            "start": start,
        }
        if hl:
            params["hl"] = hl
        if gl:
            params["gl"] = gl
        if location:
            params["location"] = location
        if google_domain:
            params["google_domain"] = google_domain

        logger.info("SERPAPI_SEARCH | params=%s", params)

        try:
            r = requests.get(self._cfg.base_url, params=params, timeout=self._cfg.timeout_s)
        except requests.RequestException as e:
            raise SerpApiError(f"SerpAPI request failed: {e}") from e

        if r.status_code != 200:
            raise SerpApiError(f"SerpAPI HTTP {r.status_code}: {r.text[:300]}")

        data = r.json()

        # SerpAPI can return {"error": "..."}
        if isinstance(data, dict) and data.get("error"):
            raise SerpApiError(f"SerpAPI error: {data.get('error')}")

        # Diagnostics: see what key you need for parsing
        if isinstance(data, dict):
            logger.info(
                "SERPAPI_RESPONSE | keys=%s | organic_results_len=%s",
                list(data.keys()),
                len(data.get("organic_results") or []),
            )

        return data

    # ---------------------------
    # Country settings helpers
    # ---------------------------

    def resolve_google_domain(self, country_code: str) -> str:
        if self._google_domains is None:
            path = Path(__file__).with_name("google-domains.json")
            with path.open("r", encoding="utf-8") as f:
                raw = json.load(f)

            if isinstance(raw, list):
                m = {}
                for item in raw:
                    cc = (item.get("country_code") or item.get("country") or item.get("cc") or "").upper()
                    dom = item.get("domain") or item.get("google_domain") or item.get("tld")
                    if cc and dom:
                        m[cc] = dom
                self._google_domains = m
            elif isinstance(raw, dict):
                self._google_domains = raw
            else:
                self._google_domains = {}

        return self._google_domains.get((country_code or "").upper(), "google.com")

    def _load_google_languages(self) -> dict:
        if self._google_languages is None:
            path = Path(__file__).with_name("google-languages.json")
            with path.open("r", encoding="utf-8") as f:
                raw = json.load(f)

            if isinstance(raw, list):
                m = {}
                for item in raw:
                    lang_code = (item.get("language_code") or "").lower()
                    if lang_code:
                        m[lang_code] = lang_code
                self._google_languages = m
            elif isinstance(raw, dict):
                self._google_languages = raw
            else:
                self._google_languages = {}

        return self._google_languages

    def is_valid_language(self, language_code: str) -> bool:
        languages = self._load_google_languages()
        return (language_code or "").lower() in languages

    def resolve_hl(self, country_code: str, prompt_language: Optional[str] = None) -> str:
        languages = self._load_google_languages()

        if prompt_language:
            pl = prompt_language.lower()
            if pl in languages:
                return pl

        # fallback
        return "en"

    def resolve_gl(self, country_code: str) -> str:
        if self._google_countries is None:
            path = Path(__file__).with_name("google-countries.json")
            with path.open("r", encoding="utf-8") as f:
                raw = json.load(f)

            if isinstance(raw, list):
                m = {}
                for item in raw:
                    cc = (item.get("country_code") or "").upper()
                    if cc:
                        m[cc] = cc.lower()
                self._google_countries = m
            elif isinstance(raw, dict):
                self._google_countries = raw
            else:
                self._google_countries = {}

        return self._google_countries.get((country_code or "").upper(), "us")

    def resolve_country_settings(self, country_code: str, prompt_language: Optional[str] = None) -> Dict[str, str]:
        return {
            "google_domain": self.resolve_google_domain(country_code),
            "hl": self.resolve_hl(country_code, prompt_language),
            "gl": self.resolve_gl(country_code),
        }
