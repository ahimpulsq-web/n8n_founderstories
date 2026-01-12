from __future__ import annotations

# =============================================================================
# errors.py
#
# Classification:
# - Role: Location error taxonomy (provider-agnostic).
# - Policy:
#   - LocationConfigError: misconfiguration (missing API key, base URL, etc.)
#   - LocationProviderError: upstream/provider failure or unexpected payload
# - Consumers:
#   - provider clients
#   - runners / workers
#   - API endpoints (mapped to HTTPException)
# =============================================================================

from ...core.errors import BaseN8NError, ErrorCode


class LocationError(BaseN8NError):
    """Base exception for location integrations."""
    pass


class LocationConfigError(LocationError):
    """Misconfiguration: missing API key, invalid base URL, etc."""
    
    def __init__(self, message: str, details: dict = None):
        super().__init__(
            message=message,
            error_code=ErrorCode.CONFIG_MISSING_API_KEY,
            details=details
        )


class LocationProviderError(LocationError):
    """Provider returned an error response or an unexpected payload."""
    
    def __init__(self, message: str, provider: str = "Location Provider", details: dict = None):
        super().__init__(
            message=message,
            error_code=ErrorCode.PROVIDER_API_ERROR,
            details={"provider": provider, **(details or {})}
        )
