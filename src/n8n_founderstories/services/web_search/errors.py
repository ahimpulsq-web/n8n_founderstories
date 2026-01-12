from __future__ import annotations

# =============================================================================
# errors.py
#
# Classification:
# - Role: Web search error taxonomy (provider-agnostic).
# - Policy:
#   - WebSearchConfigError: misconfiguration (missing API key, invalid base URL, etc.)
#   - WebSearchProviderError: upstream/provider failure or unexpected payload
# - Consumers:
#   - provider clients
#   - runners / workers
#   - API endpoints (mapped to HTTPException)
# =============================================================================


from ...core.errors import BaseN8NError, ErrorCode


class WebSearchError(BaseN8NError):
    """Base exception for web search integrations."""
    pass


class WebSearchConfigError(WebSearchError):
    """Misconfiguration: missing API key, invalid base URL, etc."""
    
    def __init__(self, message: str, details: dict = None):
        super().__init__(
            message=message,
            error_code=ErrorCode.CONFIG_MISSING_API_KEY,
            details=details
        )


class WebSearchProviderError(WebSearchError):
    """Provider returned an error response or an unexpected payload."""
    
    def __init__(self, message: str, provider: str = "Web Search Provider", details: dict = None):
        super().__init__(
            message=message,
            error_code=ErrorCode.PROVIDER_API_ERROR,
            details={"provider": provider, **(details or {})}
        )
