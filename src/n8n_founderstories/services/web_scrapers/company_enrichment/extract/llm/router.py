# src/n8n_founderstories/services/web_scrapers/company_enrichment/llm/router.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import httpx

from .client import LLMClientConfig, OpenRouterLLMClient


@dataclass(frozen=True)
class LLMRouterConfig:
    """
    Router configuration:
    - api_keys are shared across all models (OpenRouter key works for all)
    - models defines priority order: first is primary, rest are fallbacks
    """
    api_keys: List[str]
    models: List[str]  # priority order
    timeout_s: float = 40.0
    max_concurrency: int = 6

    # retries per model (not across models)
    max_retries_per_model: int = 1


class OpenRouterLLMRouter:
    """
    Model router with fallback.
    - Maintains one OpenRouterLLMClient per model.
    - Tries models in priority order until success.
    """

    def __init__(self, cfg: LLMRouterConfig):
        if not cfg.api_keys:
            raise ValueError("LLMRouterConfig.api_keys must not be empty")
        if not cfg.models:
            raise ValueError("LLMRouterConfig.models must not be empty")

        self._cfg = cfg
        self._clients: Dict[str, OpenRouterLLMClient] = {}
        self._lock = asyncio.Lock()

    async def _get_client(self, model: str) -> OpenRouterLLMClient:
        # lazy init to avoid creating httpx clients if unused
        if model in self._clients:
            return self._clients[model]

        async with self._lock:
            if model in self._clients:
                return self._clients[model]

            client_cfg = LLMClientConfig(
                api_keys=self._cfg.api_keys,
                model=model,
                timeout_s=self._cfg.timeout_s,
                max_concurrency=self._cfg.max_concurrency,
            )
            c = OpenRouterLLMClient(client_cfg)
            self._clients[model] = c
            return c

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        # Keep simple: network/timeouts and 429/5xx should be retryable.
        if isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
            return True

        if isinstance(exc, httpx.HTTPStatusError):
            code = exc.response.status_code
            if code in (408, 409, 425, 429):
                return True
            if 500 <= code <= 599:
                return True

        return False

    async def complete(
        self,
        *,
        prompt: str,
        models: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run a completion against:
          - provided `models` order, else router default cfg.models
        Returns the raw OpenRouter JSON response.
        """
        model_order = list(models) if models else list(self._cfg.models)

        last_error: Optional[Exception] = None

        for model in model_order:
            client = await self._get_client(model)

            attempts = 0
            max_attempts = 1 + max(0, int(self._cfg.max_retries_per_model))

            while attempts < max_attempts:
                attempts += 1
                try:
                    res = await client.complete(prompt=prompt)
                    # annotate for downstream debugging
                    res["_router"] = {"model": model, "attempt": attempts}
                    return res
                except Exception as e:
                    last_error = e
                    if attempts < max_attempts and self._is_retryable_error(e):
                        continue
                    break  # move to next model

        # Exhausted all models
        if last_error:
            raise last_error
        raise RuntimeError("OpenRouterLLMRouter: completion failed without exception")

    async def close(self) -> None:
        # Close all underlying httpx clients
        for c in list(self._clients.values()):
            await c.close()
        self._clients.clear()
