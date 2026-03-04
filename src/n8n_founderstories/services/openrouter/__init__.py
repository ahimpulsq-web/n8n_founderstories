"""OpenRouter LLM client package.

This package provides a clean, standardized interface for OpenRouter API calls
with support for multiple execution strategies and structured outputs.

Public API:
    - OpenRouterClient: Main client class
    - LLMRunSpec: Specification for LLM runs
    - Strategy: Type for execution strategies ("fallback" or "vote")
    - set_run_context: Set context for request tracing
    - OpenRouterError and subclasses: Error types
"""
from __future__ import annotations

from .client import OpenRouterClient
from .context import set_run_context
from .errors import (
    OpenRouterAllModelsFailedError,
    OpenRouterAPIError,
    OpenRouterError,
    OpenRouterTimeoutError,
    OpenRouterValidationError,
)
from .types import LLMRunSpec, Strategy

__all__ = [
    # Main client
    "OpenRouterClient",
    # Types
    "LLMRunSpec",
    "Strategy",
    # Context
    "set_run_context",
    # Errors
    "OpenRouterError",
    "OpenRouterAPIError",
    "OpenRouterTimeoutError",
    "OpenRouterValidationError",
    "OpenRouterAllModelsFailedError",
]