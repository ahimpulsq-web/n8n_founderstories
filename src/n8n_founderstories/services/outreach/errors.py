from __future__ import annotations

# =============================================================================
# Classification: Outreach error taxonomy
# =============================================================================

from ...core.errors import BaseN8NError, ErrorCode


class OutreachError(BaseN8NError):
    """Base exception for outreach integrations."""
    pass


class OutreachConfigError(OutreachError):
    """Misconfiguration: missing API key, invalid base URL, etc."""
    
    def __init__(self, message: str, details: dict = None):
        super().__init__(
            message=message,
            error_code=ErrorCode.CONFIG_MISSING_API_KEY,
            details=details
        )


class OutreachProviderError(OutreachError):
    """Provider returned an error response or an unexpected payload."""
    
    def __init__(self, message: str, provider: str = "Outreach Provider", details: dict = None):
        super().__init__(
            message=message,
            error_code=ErrorCode.PROVIDER_API_ERROR,
            details={"provider": provider, **(details or {})}
        )
