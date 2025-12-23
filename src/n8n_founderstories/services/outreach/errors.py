from __future__ import annotations

# =============================================================================
# Classification: Outreach error taxonomy
# =============================================================================

class OutreachError(Exception):
    """Base exception for outreach integrations."""


class OutreachConfigError(OutreachError):
    """Misconfiguration: missing API key, invalid base URL, etc."""


class OutreachProviderError(OutreachError):
    """Provider returned an error response or an unexpected payload."""
