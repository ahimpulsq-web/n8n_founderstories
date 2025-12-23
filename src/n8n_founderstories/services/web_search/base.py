from __future__ import annotations

# =============================================================================
# base.py
#
# Classification:
# - Role: Base interfaces for web search providers (tool-agnostic).
# - Consumers:
#   - web search provider packages (SerpAPI_GoogleSearch, etc.)
#   - orchestration runners
# - Non-goals:
#   - HTTP logic
#   - Sheets writing
# =============================================================================

from typing import Protocol


class WebSearchClient(Protocol):
    """Minimal client protocol for provider integrations."""
    def close(self) -> None: ...
