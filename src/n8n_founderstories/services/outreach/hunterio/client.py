from __future__ import annotations

# =============================================================================
# client.py
#
# Classification:
# - Role: Hunter Discover provider client (HTTP only).
# - Responsibilities:
#   - Build valid /discover payloads
#   - Apply rate limiting
#   - Parse response into HunterCompany
# - Non-goals:
#   - Orchestration loops
#   - Sheets writing
#   - Persistence
# =============================================================================

import logging
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, Iterable, Tuple, Optional

import httpx

from ....core.config import settings
from ....core.utils.text import norm
from .models import HunterCompany
from ..errors import OutreachConfigError, OutreachProviderError


logger = logging.getLogger(__name__)


class HunterRateLimiter:
    """
    Simple throttler.

    Policy:
    - Conservative default to protect API quotas.
    - Blocking wait is acceptable for background jobs.
    """
    min_delay_seconds: float = 1.25

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


@dataclass(frozen=True)
class HunterClientConfig:
    api_key: str
    base_url: str = "https://api.hunter.io/v2"
    timeout_seconds: float = 30.0


class HunterClient:
    """
    Hunter Discover API client.

    Endpoint:
      POST {base_url}/discover?api_key=...

    Key constraints:
    - headcount must be a list: ["11-50"]
    - if city is set, country must be set
    - state is only valid with country="US" (we do not use state filtering here)
    """

    def __init__(self, *, config: HunterClientConfig | None = None) -> None:
        cfg = config or self._default_config()
        self._api_key = cfg.api_key
        self._base_url = cfg.base_url.rstrip("/")
        self._rate = HunterRateLimiter()
        self._http = httpx.Client(timeout=cfg.timeout_seconds)

    @staticmethod
    def _default_config() -> HunterClientConfig:
        api_key = norm(getattr(settings, "hunter_api_key", None))
        if not api_key:
            raise OutreachConfigError("Hunter API key is not configured (settings.hunter_api_key).")

        base_url = norm(getattr(settings, "hunter_base_url", None)) or "https://api.hunter.io/v2"
        return HunterClientConfig(api_key=api_key, base_url=base_url)

    def close(self) -> None:
        try:
            self._http.close()
        except Exception:
            logger.debug("HUNTER_HTTP_CLOSE_FAILED", exc_info=True)

    def __enter__(self) -> "HunterClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def discover(
        self,
        *,
        headquarters_location: dict[str, Any] | None = None,
        headcount_bucket: str | None = None,
        query_text: str | None = None,
        keywords_include: Iterable[str] | None = None,
        keyword_match: str | None = None,
        limit: int = 100,
        offset: int = 0,
        ) -> Tuple[list[HunterCompany], int, dict[str, Any] | None]:

        """
        Execute one Hunter /discover call.

        Returns:
          (companies, meta.total)
        """
        self._rate.wait()

        payload: Dict[str, Any] = {
            "limit": int(limit),
            "offset": int(offset),
        }

        # Optional hard filters
        hc = norm(headcount_bucket) if headcount_bucket is not None else None
        if hc:
            payload["headcount"] = [hc]

        if isinstance(headquarters_location, dict) and headquarters_location.get("include"):
            payload["headquarters_location"] = headquarters_location

        q = norm(query_text)
        if q:
            payload["query"] = q

        kws = [norm(k) for k in (keywords_include or []) if norm(k)]
        if kws:
            km = (norm(keyword_match) or "all").lower()
            if km not in {"any", "all"}:
                km = "all"
            payload["keywords"] = {"include": kws, "match": km}

        # Hunter requires either a query or at least one filter
        if not payload.get("query") and not any(
            k in payload for k in ("headquarters_location", "headcount", "keywords", "industry")
        ):
            raise ValueError("Hunter discover requires either query or at least one filter.")

        url = f"{self._base_url}/discover"
        logger.debug("HUNTER_DISCOVER_REQUEST | payload=%s", payload)

        resp = self._http.post(url, params={"api_key": self._api_key}, json=payload)
        resp.raise_for_status()
        data = resp.json()

        applied_filters: dict[str, Any] | None = None
        if isinstance(data, dict):
            meta = data.get("meta")
            if isinstance(meta, dict):
                f = meta.get("filters")
                if isinstance(f, dict):
                    applied_filters = f

        companies, total = self._parse_response(data)
        return companies, total, applied_filters

    @staticmethod
    def _parse_response(data: Any) -> Tuple[list[HunterCompany], int]:
        items: list[dict[str, Any]] = []
        total = 0

        if isinstance(data, dict):
            meta = data.get("meta") or {}
            try:
                total = int(meta.get("total") or 0)
            except Exception:
                total = 0

            d = data.get("data")
            if isinstance(d, dict):
                raw_items = d.get("domains") or d.get("results") or []
                if isinstance(raw_items, list):
                    items = [x for x in raw_items if isinstance(x, dict)]
            elif isinstance(d, list):
                items = [x for x in d if isinstance(x, dict)]
                total = total or len(items)

        elif isinstance(data, list):
            items = [x for x in data if isinstance(x, dict)]
            total = len(items)

        companies: list[HunterCompany] = []
        for item in items:
            c = HunterCompany.from_hunter_item(item)
            if c.domain:
                companies.append(c)

        return companies, total
