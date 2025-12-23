from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import requests


def normalize_domain(value: str | None) -> Optional[str]:
    """
    Normalize a noisy domain/URL into a bare hostname.
    Returns None if it does not look like a domain.
    """
    s = (value or "").strip()
    if not s:
        return None

    if "://" in s:
        try:
            host = urlparse(s).netloc
        except Exception:
            return None
    else:
        host = s.split("/")[0]

    host = host.strip().lower()
    if host.startswith("www."):
        host = host[4:]

    if "." not in host or " " in host:
        return None

    return host


@dataclass(frozen=True)
class FetchConfig:
    timeout_sec: int = 12
    user_agent: str = "n8n_founderstories/1.0 (email-extractor)"
    max_bytes: int = 800_000


def fetch_text(url: str, cfg: FetchConfig) -> Optional[str]:
    """Bounded fetch (timeouts, redirects, max bytes)."""
    try:
        r = requests.get(
            url,
            headers={"User-Agent": cfg.user_agent},
            timeout=cfg.timeout_sec,
            allow_redirects=True,
        )
        if r.status_code >= 400:
            return None

        content = (r.content or b"")[: cfg.max_bytes]
        r.encoding = r.apparent_encoding or r.encoding
        enc = r.encoding or "utf-8"
        return content.decode(enc, errors="ignore")
    except Exception:
        return None
