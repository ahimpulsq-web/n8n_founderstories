# src/n8n_founderstories/services/openrouter/openrouter_client.py
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Literal, Type, TypeVar

from pydantic import BaseModel

from ...core.config import settings
from .runner import OpenRouterClient, LLMRunSpec
from .run_log import start_run

T = TypeVar("T", bound=BaseModel)
Strategy = Literal["single", "fallback", "vote"]

_ctx = threading.local()


def set_run_context(*, module: str, request_id: str | None = None) -> None:
    _ctx.module = (module or "").strip()
    _ctx.request_id = request_id
    start_run(module=_ctx.module)


def _get_ctx() -> tuple[str | None, str | None]:
    return getattr(_ctx, "module", None), getattr(_ctx, "request_id", None)


@dataclass(frozen=True)
class PackageLLMSpec:
    tier: str  # "LLM_PREMIUM_MODELS" | "LLM_FREE_MODELS"
    strategy: Strategy = "fallback"
    temperature: float = 0.0
    vote_k: int = 3
    vote_min_wins: int = 2
    max_tokens: int | None = None


_client: OpenRouterClient | None = None


def get_client() -> OpenRouterClient:
    global _client
    if _client is None:
        _client = OpenRouterClient()
    return _client


def run_spec(pkg: PackageLLMSpec) -> LLMRunSpec:
    models = settings.resolve_tier_models(pkg.tier)
    return LLMRunSpec(
        models=models,
        strategy=pkg.strategy,
        temperature=pkg.temperature,
        vote_k=pkg.vote_k,
        vote_min_wins=pkg.vote_min_wins,
        max_tokens=pkg.max_tokens,
    )


def generate_structured(
    *,
    pkg: PackageLLMSpec,
    user_prompt: str,
    system_instructions: str,
    schema_model: Type[T],
) -> T:
    client = get_client()
    return client.generate_structured(
        user_prompt=user_prompt,
        system_instructions=system_instructions,
        schema_model=schema_model,
        spec=run_spec(pkg),
    )
