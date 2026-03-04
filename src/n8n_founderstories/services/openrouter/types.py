"""Type definitions for OpenRouter client."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Strategy = Literal["fallback", "vote"]


@dataclass(frozen=True)
class LLMRunSpec:
    """Specification for an LLM run.
    
    Args:
        models: List of model names to use
        strategy: Execution strategy - "fallback" or "vote"
        temperature: Temperature parameter for generation (0.0-2.0)
        vote_k: Number of models to use in vote strategy (default: 3)
        vote_min_wins: Minimum wins required for consensus in vote (default: 2)
        max_tokens: Maximum tokens to generate (optional)
    """
    models: list[str]
    strategy: Strategy = "fallback"
    temperature: float = 0.1
    vote_k: int = 3
    vote_min_wins: int = 2
    max_tokens: int | None = None