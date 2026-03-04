# src/n8n_founderstories/services/openrouter/context.py
"""Context management for OpenRouter LLM calls."""
from __future__ import annotations

import threading
from typing import Any


_ctx = threading.local()


def set_run_context(*, module: str, request_id: str | None = None) -> None:
    """Set the current run context for LLM calls.
    
    Args:
        module: Module name making the LLM call
        request_id: Optional request ID for tracking
    """
    _ctx.module = (module or "").strip()
    _ctx.request_id = request_id


def _get_ctx() -> tuple[str | None, str | None]:
    """Get the current run context.
    
    Returns:
        Tuple of (module, request_id)
    """
    return getattr(_ctx, "module", None), getattr(_ctx, "request_id", None)


def append_llm_result(
    *,
    module: str | None,
    request_id: str | None,
    kind: str,
    schema_model: str,
    strategy: str,
    vote_k: int,
    vote_min_wins: int,
    prompt: str,
    duration_ms: int,
    models: list[dict[str, Any]],
    winner: dict[str, Any],
) -> None:
    """Append LLM result to logs (stub implementation).
    
    This function can be extended to log LLM results to a file or database.
    Currently it's a no-op to avoid breaking existing code.
    
    Args:
        module: Module name
        request_id: Request ID
        kind: Type of LLM call (e.g., "structured")
        schema_model: Name of the schema model
        strategy: Strategy used (e.g., "vote")
        vote_k: Number of models in vote
        vote_min_wins: Minimum wins required
        prompt: User prompt
        duration_ms: Duration in milliseconds
        models: List of model results
        winner: Winning result
    """
    # TODO: Implement actual logging if needed
    pass