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


class WebSearchError(Exception):
    """Base exception for web search integrations."""


class WebSearchConfigError(WebSearchError):
    """Misconfiguration: missing API key, invalid base URL, etc."""


class WebSearchProviderError(WebSearchError):
    """Provider returned an error response or an unexpected payload."""
