from __future__ import annotations

# =============================================================================
# Classification: Base interfaces and shared helpers for outreach providers
# =============================================================================

from typing import Protocol, Any

class OutreachClient(Protocol):
    """Minimal client protocol for provider integrations."""
    def close(self) -> None: ...
