# src/n8n_founderstories/services/web_scrapers/company_enrichment/llm/client.py
from __future__ import annotations

import asyncio
import itertools
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

from .logger import logger


@dataclass(frozen=True)
class LLMClientConfig:
    api_keys: List[str]
    model: str
    timeout_s: float = 40.0
    max_concurrency: int = 6

    # Optional OpenRouter recommended headers
    http_referer: Optional[str] = None
    x_title: Optional[str] = None


class OpenRouterLLMClient:
    """
    Minimal OpenRouter chat-completions client with:
      - key rotation
      - bounded concurrency
      - robust logging without relying on private semaphore internals
    """

    def __init__(self, cfg: LLMClientConfig):
        if not cfg.api_keys:
            raise ValueError("LLMClientConfig.api_keys must not be empty")

        self._cfg = cfg
        self._keys = itertools.cycle(cfg.api_keys)
        self._sem = asyncio.Semaphore(max(1, int(cfg.max_concurrency)))
        self._client = httpx.AsyncClient(timeout=cfg.timeout_s)

        # inflight counter for logging
        self._inflight = 0
        self._inflight_lock = asyncio.Lock()

    def _next_key(self) -> str:
        return next(self._keys)

    async def _inc_inflight(self) -> int:
        async with self._inflight_lock:
            self._inflight += 1
            return self._inflight

    async def _dec_inflight(self) -> int:
        async with self._inflight_lock:
            self._inflight = max(0, self._inflight - 1)
            return self._inflight

    def _headers(self, api_key: str) -> Dict[str, str]:
        h = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        if self._cfg.http_referer:
            h["HTTP-Referer"] = self._cfg.http_referer
        if self._cfg.x_title:
            h["X-Title"] = self._cfg.x_title
        return h

    async def complete(
        self,
        *,
        prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 800,
    ) -> Dict[str, Any]:
        api_key = self._next_key()
        key_suffix = api_key[-6:] if len(api_key) >= 6 else api_key

        payload = {
            "model": self._cfg.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": float(temperature),
            "max_tokens": int(max_tokens),
        }

        start = time.perf_counter()

        async with self._sem:
            inflight = await self._inc_inflight()
            logger.info(
                f"[LLM START] model={self._cfg.model} key=...{key_suffix} inflight={inflight}"
            )

            try:
                r = await self._client.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers=self._headers(api_key),
                    json=payload,
                )
                r.raise_for_status()

                elapsed = time.perf_counter() - start
                logger.info(
                    f"[LLM OK] model={self._cfg.model} key=...{key_suffix} "
                    f"status={r.status_code} duration={elapsed:.2f}s"
                )
                return r.json()

            except httpx.HTTPStatusError as e:
                elapsed = time.perf_counter() - start
                body = ""
                try:
                    body = e.response.text or ""
                except Exception:
                    body = ""
                logger.error(
                    f"[LLM HTTP ERROR] model={self._cfg.model} key=...{key_suffix} "
                    f"status={e.response.status_code} duration={elapsed:.2f}s "
                    f"body={body[:500]}"
                )
                raise

            except Exception:
                elapsed = time.perf_counter() - start
                logger.exception(
                    f"[LLM EXCEPTION] model={self._cfg.model} key=...{key_suffix} "
                    f"duration={elapsed:.2f}s"
                )
                raise

            finally:
                inflight = await self._dec_inflight()
                logger.info(f"[LLM END] model={self._cfg.model} inflight={inflight}")

    async def close(self) -> None:
        await self._client.aclose()
