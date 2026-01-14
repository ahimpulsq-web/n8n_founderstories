from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Dict
from urllib import robotparser
from urllib.parse import urlparse

import httpx

logger = logging.getLogger("n8n_founderstories")


def log_event(event: str, **fields: Any) -> None:
    payload: Dict[str, Any] = {"event": event, **fields}
    logger.info(json.dumps(payload, ensure_ascii=False))


def to_markdown(text: str) -> str:
    s = text or ""
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


@dataclass
class RateLimiter:
    min_interval_s: float = 10.0
    _last_hit: Dict[str, float] = None

    def __post_init__(self):
        if self._last_hit is None:
            self._last_hit = {}

    def allow(self, key: str) -> bool:
        now = time.time()
        last = self._last_hit.get(key)
        if last is None or (now - last) >= self.min_interval_s:
            self._last_hit[key] = now
            return True
        return False


@dataclass(frozen=True)
class RobotsDecision:
    allowed: bool
    reason: str


async def is_allowed_by_robots(url: str, user_agent: str) -> RobotsDecision:
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    rp = robotparser.RobotFileParser()
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(robots_url, headers={"User-Agent": user_agent})
            if resp.status_code >= 400:
                return RobotsDecision(True, f"robots.txt not accessible ({resp.status_code}); allow by default")
            rp.parse(resp.text.splitlines())
    except Exception as e:
        return RobotsDecision(True, f"robots.txt fetch failed ({type(e).__name__}); allow by default")

    allowed = rp.can_fetch(user_agent, url)
    return RobotsDecision(allowed, "allowed by robots.txt" if allowed else "disallowed by robots.txt")
