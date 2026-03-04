"""Error types for OpenRouter client."""
from __future__ import annotations


class OpenRouterError(Exception):
    """Base exception for OpenRouter client errors."""
    pass


class OpenRouterAPIError(OpenRouterError):
    """Error from OpenRouter API response."""
    
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class OpenRouterTimeoutError(OpenRouterError):
    """Request to OpenRouter timed out."""
    pass


class OpenRouterValidationError(OpenRouterError):
    """Invalid response from OpenRouter (e.g., malformed JSON)."""
    pass


class OpenRouterAllModelsFailedError(OpenRouterError):
    """All models failed in a multi-model strategy."""
    
    def __init__(self, errors: dict[str, str]):
        self.errors = errors
        super().__init__(f"All LLM models failed. Errors: {errors}")