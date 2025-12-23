from __future__ import annotations

# =============================================================================
# base.py
#
# Classification:
# - Role: Base interfaces for location providers (tool-agnostic).
# - Consumers:
#   - location provider packages (google_maps, etc.)
#   - orchestration runners
# - Non-goals:
#   - HTTP logic
#   - Sheets writing
# =============================================================================

from typing import Protocol


class LocationClient(Protocol):
    """Minimal client protocol for provider integrations."""
    def close(self) -> None: ...
