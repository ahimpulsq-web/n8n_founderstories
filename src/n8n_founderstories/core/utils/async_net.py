from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import httpx


@dataclass(frozen=True)
class AsyncFetchConfig:
    timeout_sec: float = 12.0
    user_agent: str = "n8n_founderstories/1.0 (email-extractor)"
    max_bytes: int = 800_000

    # Concurrency / politeness controls
    max_global_concurrency: int = 60
    per_host_concurrency: int = 1
    per_host_min_delay_sec: float = 0.3

    # Retries for transient statuses
    retry_statuses: tuple[int, ...] = (429, 503, 520, 522, 524)
    max_retries: int = 1
    retry_backoff_sec: float = 1.2


@dataclass(frozen=True)
class FetchResult:
    url: str
    final_url: Optional[str]
    status_code: Optional[int]
    text: Optional[str]
    error: Optional[str]  # timeout / request_error:... / http_403 / decode_error / unexpected:...


class AsyncFetcher:
    """
    Shared async HTTP client that:
    - Reuses connections (keep-alive)
    - Bounds total concurrency
    - Bounds per-host concurrency
    - Enforces per-host pacing
    - Avoids HTTP/2 protocol edge failures (http2=False)
    """
    def __init__(self, cfg: AsyncFetchConfig):
        self.cfg = cfg
        self._global_sem = asyncio.Semaphore(cfg.max_global_concurrency)
        self._host_sems: dict[str, asyncio.Semaphore] = {}
        self._host_next_allowed: dict[str, float] = {}
        self._lock = asyncio.Lock()

        timeout = httpx.Timeout(cfg.timeout_sec, connect=min(5.0, cfg.timeout_sec))
        limits = httpx.Limits(
            max_connections=cfg.max_global_concurrency,
            max_keepalive_connections=max(10, cfg.max_global_concurrency // 2),
        )

        # Keep headers honest and robust. Do NOT force brotli.
        headers = {
            "User-Agent": cfg.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
            "Connection": "keep-alive",
        }

        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            limits=limits,
            headers=headers,
            http2=False,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _host_sem(self, host: str) -> asyncio.Semaphore:
        async with self._lock:
            sem = self._host_sems.get(host)
            if sem is None:
                sem = asyncio.Semaphore(self.cfg.per_host_concurrency)
                self._host_sems[host] = sem
            return sem

    async def _pace(self, host: str) -> None:
        if not host or self.cfg.per_host_min_delay_sec <= 0:
            return
        now = asyncio.get_running_loop().time()
        async with self._lock:
            next_allowed = self._host_next_allowed.get(host, 0.0)
            delay = max(0.0, next_allowed - now)
            self._host_next_allowed[host] = max(next_allowed, now) + self.cfg.per_host_min_delay_sec
        if delay > 0:
            await asyncio.sleep(delay)

    def _decode_text(self, r: httpx.Response) -> Optional[str]:
        raw = (r.content or b"")[: self.cfg.max_bytes]
        enc = r.encoding or "utf-8"
        try:
            return raw.decode(enc, errors="ignore")
        except Exception:
            return None

    async def fetch(self, url: str) -> FetchResult:
        host = ""
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            host = ""

        async with self._global_sem:
            host_sem = await self._host_sem(host or "_")
            async with host_sem:
                await self._pace(host)

                tries = 0
                last_exc: Optional[Exception] = None

                while True:
                    tries += 1
                    try:
                        r = await self._client.get(url)
                        status = int(r.status_code)
                        final_url = str(r.url) if r.url else None

                        text = self._decode_text(r)

                        if status in self.cfg.retry_statuses and tries <= (1 + int(self.cfg.max_retries)):
                            # short backoff + retry once
                            await asyncio.sleep(self.cfg.retry_backoff_sec)
                            continue

                        if status >= 400:
                            # keep snippet for debugging
                            snippet = (text or "")[:50_000] if text else None
                            return FetchResult(
                                url=url,
                                final_url=final_url,
                                status_code=status,
                                text=snippet,
                                error=f"http_{status}",
                            )

                        if text is None:
                            return FetchResult(url=url, final_url=final_url, status_code=status, text=None, error="decode_error")

                        return FetchResult(url=url, final_url=final_url, status_code=status, text=text, error=None)

                    except httpx.TimeoutException as e:
                        last_exc = e
                        return FetchResult(url=url, final_url=None, status_code=None, text=None, error="timeout")
                    except httpx.RequestError as e:
                        last_exc = e
                        err = f"request_error:{type(e).__name__}:{str(e)[:200]}"
                        return FetchResult(url=url, final_url=None, status_code=None, text=None, error=err)
                    except httpx.HTTPError as e:
                        last_exc = e
                        err = f"http_error:{type(e).__name__}:{str(e)[:200]}"
                        return FetchResult(url=url, final_url=None, status_code=None, text=None, error=err)
                    except Exception as e:
                        last_exc = e
                        return FetchResult(
                            url=url,
                            final_url=None,
                            status_code=None,
                            text=None,
                            error=f"unexpected:{type(e).__name__}:{str(e)[:200]}",
                        )

    async def fetch_text(self, url: str) -> Optional[str]:
        res = await self.fetch(url)
        return res.text
