# src/n8n_founderstories/core/logging.py

from __future__ import annotations

import logging

from .config import settings


# -------------------------------------------------------------------------
# Logging setup
# -------------------------------------------------------------------------
def setup_logging() -> None:
    """Configure application-wide logging."""
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )
