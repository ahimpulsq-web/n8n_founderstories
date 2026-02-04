# src/n8n_founderstories/core/utils/llm_selection.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..config import settings

PoolName = Literal["LLM_PREMIUM_MODELS", "LLM_FREE_MODELS"]
ModuleName = Literal["search_plan"]


@dataclass(frozen=True)
class LLMSelection:
    """
    Result of selecting credentials + model(s) for a module.

    - api_key: chosen key (currently first; deterministic)
    - pool: which pool was selected (FREE vs PREMIUM)
    - models: models to use (order preserved from env)
    """
    api_key: str
    pool: PoolName
    models: list[str]


_ALLOWED_POOLS: set[str] = {"LLM_PREMIUM_MODELS", "LLM_FREE_MODELS"}


def _normalize_pool(value: str) -> PoolName:
    v = (value or "").strip().upper()
    if v not in _ALLOWED_POOLS:
        raise ValueError(
            f"Invalid pool selector {value!r}. Allowed: {sorted(_ALLOWED_POOLS)}"
        )
    return v  # type: ignore[return-value]


def _pool_for_module(module: ModuleName) -> PoolName:
    if module == "search_plan":
        return _normalize_pool(settings.search_plan_tier)
    raise ValueError(f"Unknown module: {module!r}")


def _models_for_pool(pool: PoolName) -> list[str]:
    if pool == "LLM_PREMIUM_MODELS":
        models = list(settings.llm_premium_models)
    else:
        models = list(settings.llm_free_models)

    if not models:
        raise ValueError(f"Model pool {pool} is empty.")
    return models


def _first_api_key() -> str:
    keys = list(settings.llm_api_keys)
    if not keys:
        raise ValueError("LLM_API_KEYS is empty.")
    return keys[0]


def select_llm(
    *,
    module: ModuleName,
    mode: Literal["single", "all", "first_n"] = "single",
    n: int = 1,
) -> LLMSelection:
    """
    Selection policy:
    - module determines pool via <MODULE>_TIER env mapping
    - mode:
        - single: returns first model only
        - all: returns all models in pool
        - first_n: returns first N models in pool
    """
    api_key = _first_api_key()
    pool = _pool_for_module(module)
    models = _models_for_pool(pool)

    if mode == "all":
        chosen = models
    elif mode == "first_n":
        if n <= 0:
            raise ValueError("n must be > 0 when mode='first_n'.")
        chosen = models[:n]
    else:  # single
        chosen = [models[0]]

    return LLMSelection(api_key=api_key, pool=pool, models=chosen)


def select_single_model(*, module: ModuleName) -> tuple[str, str]:
    """
    Convenience: returns (api_key, model_name) for modules that must use one model.
    """
    sel = select_llm(module=module, mode="single")
    return sel.api_key, sel.models[0]
