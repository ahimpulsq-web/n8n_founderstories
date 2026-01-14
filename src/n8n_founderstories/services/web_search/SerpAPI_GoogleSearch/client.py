from __future__ import annotations

# =============================================================================
# client.py
#
# Classification:
# - Role: SerpAPI Google Search client (HTTP only).
# - Responsibilities:
#   - Load configuration from Settings (single source of truth)
#   - Execute SerpAPI requests (engine=google)
#   - Apply conservative rate limiting
#   - Raise typed provider errors for consistent API/job handling
# - Non-goals:
#   - Orchestration loops
#   - Sheets writing
#   - Persistence
# =============================================================================

import logging
import random
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any

import httpx
from pydantic import BaseModel, Field

from ....core.config import settings
from ....core.utils.text import norm
from ..errors import WebSearchConfigError, WebSearchProviderError

logger = logging.getLogger(__name__)


# =============================================================================
# Classification: Rate limiting
# =============================================================================

class SerpApiRateLimiter:
    """
    Classification: simple throttler.

    Policy:
    - Blocking waits are acceptable for background jobs.
    - Conservative default reduces risk of quota spikes / 429.
    """
    min_delay_seconds: float = 1.0

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
# Classification: Client-local response models
# =============================================================================

class SerpOrganicResult(BaseModel):
    position: int | None = None
    title: str | None = None
    link: str | None = None
    displayed_link: str | None = None
    snippet: str | None = None
    source: str | None = None


class SerpApiResponse(BaseModel):
    organic_results: list[SerpOrganicResult] = Field(default_factory=list)
    raw: dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# Classification: Configuration
# =============================================================================

@dataclass(frozen=True)
class SerpApiClientConfig:
    api_key: str
    base_url: str
    engine: str = "google"
    timeout_seconds: float = 30.0

    # Retries
    max_attempts: int = 3
    backoff_base_seconds: float = 0.8
    backoff_max_seconds: float = 8.0


# =============================================================================
# Classification: Client
# =============================================================================

class SerpApiClient:
    """
    SerpAPI client configured via Settings.

    Endpoint:
      GET {base_url}?engine=google&q=...&api_key=...&google_domain=...&hl=...&gl=...

    Retry policy:
    - Retry transient errors: 429 and 5xx, plus timeouts.
    - Fail fast on non-transient 4xx.
    """

    def __init__(self, *, config: SerpApiClientConfig | None = None) -> None:
        cfg = config or self._default_config()

        self._api_key = cfg.api_key
        self._base_url = cfg.base_url
        self._engine = cfg.engine

        self._max_attempts = max(1, int(cfg.max_attempts))
        self._backoff_base = float(cfg.backoff_base_seconds)
        self._backoff_max = float(cfg.backoff_max_seconds)

        self._rate = SerpApiRateLimiter()
        self._http = httpx.Client(timeout=cfg.timeout_seconds)

    @staticmethod
    def _default_config() -> SerpApiClientConfig:
        api_key = norm(getattr(settings, "serpapi_api_key", None))
        if not api_key:
            raise WebSearchConfigError("SERPAPI_API_KEY is not configured (settings.serpapi_api_key).")

        base_url = norm(getattr(settings, "serpapi_base_url", None))
        if not base_url:
            raise WebSearchConfigError("SerpAPI base URL is not configured (settings.serpapi_base_url).")

        engine = norm(getattr(settings, "serpapi_engine", None)) or "google"

        return SerpApiClientConfig(api_key=api_key, base_url=base_url, engine=engine)

    def close(self) -> None:
        try:
            self._http.close()
        except Exception:
            logger.debug("SERPAPI_HTTP_CLOSE_FAILED", exc_info=True)

    def __enter__(self) -> "SerpApiClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # -------------------------------------------------------------------------
    # Classification: Search
    # -------------------------------------------------------------------------

    def search(
        self,
        *,
        query: str,
        google_domain: str,
        hl: str,
        gl: str,
        location: str | None = None,
        num: int = 10,
        start: int = 0,
        safe: str | None = None,
        request_id: str | None = None,
    ) -> SerpApiResponse:
        q = norm(query)
        if not q:
            raise ValueError("SerpAPI search requires non-empty query.")

        params: dict[str, Any] = {
            "engine": self._engine,
            "q": q,
            "api_key": self._api_key,
            "google_domain": norm(google_domain),
            "hl": norm(hl) or "en",
            "gl": norm(gl) or "us",
            "num": int(num),
            "start": int(start),
        }
        if safe:
            params["safe"] = norm(safe)
        if location and norm(location):
            params["location"] = norm(location)

        last_exc: Exception | None = None

        for attempt in range(1, self._max_attempts + 1):
            self._rate.wait()

            try:
                resp = self._http.get(self._base_url, params=params)

                # Explicit transient classification
                if resp.status_code == 429 or 500 <= resp.status_code <= 599:
                    raise WebSearchProviderError(f"SerpAPI transient HTTP {resp.status_code}")

                resp.raise_for_status()

                data = resp.json() if resp.content else {}
                if isinstance(data, dict) and data.get("error"):
                    raise WebSearchProviderError(str(data.get("error")))

                organic: list[SerpOrganicResult] = []
                if isinstance(data, dict):
                    for r in (data.get("organic_results", []) or []):
                        if not isinstance(r, dict):
                            continue
                        organic.append(
                            SerpOrganicResult(
                                position=r.get("position"),
                                title=r.get("title"),
                                link=r.get("link"),
                                displayed_link=r.get("displayed_link"),
                                snippet=r.get("snippet"),
                                source=r.get("source"),
                            )
                        )

                logger.debug(
                    "SERPAPI_OK | id=%s | q=%r | gl=%s | hl=%s | domain=%s | results=%d",
                    request_id,
                    q[:120],
                    params["gl"],
                    params["hl"],
                    params["google_domain"],
                    len(organic),
                )

                return SerpApiResponse(
                    organic_results=organic,
                    raw=data if isinstance(data, dict) else {"_raw": data},
                )

            except httpx.HTTPStatusError as exc:
                status = getattr(exc.response, "status_code", None)
                if status and 400 <= status <= 499:
                    raise WebSearchProviderError(f"SerpAPI non-retryable HTTP {status}") from exc
                last_exc = exc

            except (httpx.TimeoutException, WebSearchProviderError) as exc:
                last_exc = exc

            except Exception as exc:
                last_exc = exc

            if attempt >= self._max_attempts:
                break

            # Backoff with jitter
            delay = min(self._backoff_max, self._backoff_base * (2 ** (attempt - 1)))
            delay = delay * (0.8 + 0.4 * random.random())

            logger.warning(
                "SERPAPI_RETRY | id=%s | attempt=%d/%d | q=%r | delay=%.2fs | err=%s",
                request_id,
                attempt,
                self._max_attempts,
                q[:120],
                delay,
                last_exc,
            )
            time.sleep(delay)

        raise WebSearchProviderError(f"SerpAPI failed after retries: {last_exc}") from last_exc
