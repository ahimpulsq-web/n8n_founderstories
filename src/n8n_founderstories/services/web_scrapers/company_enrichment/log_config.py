from __future__ import annotations

import logging
import os
from typing import Optional

_DEFAULT_LOGGER_NAME = "n8n_founderstories.web_scrapers"


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Central logger accessor for web_scrapers.

    - Defaults to INFO (production-friendly).
    - Can be overridden via LOG_LEVEL env var (DEBUG/INFO/WARNING/ERROR).
    - Call sites should use: logger = get_logger(__name__)
    """
    logger_name = name or _DEFAULT_LOGGER_NAME
    logger = logging.getLogger(logger_name)

    # Configure root handler once (safe in libraries if you guard it)
    if not logging.getLogger().handlers:
        level = os.environ.get("LOG_LEVEL", "INFO").upper()
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        )

    return logger
