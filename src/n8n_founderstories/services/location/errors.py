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

class LocationError(Exception):
    """Base exception for location integrations."""


class LocationConfigError(LocationError):
    """Misconfiguration: missing API key, invalid base URL, etc."""


class LocationProviderError(LocationError):
    """Provider returned an error response or an unexpected payload."""
