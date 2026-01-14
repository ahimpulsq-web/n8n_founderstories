# src/n8n_founderstories/core/logging.py

from __future__ import annotations

import logging
from .config import settings


class DropHttpxRequests(logging.Filter):
    """
    Drop noisy per-request logs emitted by httpx/httpcore.
    Keeps your application logs intact.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        return not record.name.startswith(("httpx", "httpcore"))


def setup_logging() -> None:
    """Configure application-wide logging with colored output."""

    class ColoredFormatter(logging.Formatter):
        """Custom formatter that adds colors to log levels and message categories."""

        # ANSI color codes
        LEVEL_COLORS = {
            "DEBUG": "\033[94m",     # Blue
            "INFO": "\033[92m",      # Green
            "WARNING": "\033[93m",   # Yellow
            "ERROR": "\033[91m",     # Red
            "CRITICAL": "\033[95m",  # Magenta
        }
        CYAN = "\033[96m"
        BLUE = "\033[94m"
        RESET = "\033[0m"

        def format(self, record: logging.LogRecord) -> str:
            # Color the level name
            levelname = record.levelname
            if levelname in self.LEVEL_COLORS:
                record.levelname = f"{self.LEVEL_COLORS[levelname]}{levelname}{self.RESET}"

            message = record.getMessage()

            # Check if message starts with a category (e.g., "SYSTEM |", "PROMPT |")
            if " | " in message:
                parts = message.split(" | ", 1)
                category = parts[0]
                rest = parts[1] if len(parts) > 1 else ""

                # Color URLs in the rest of the message
                import re
                url_pattern = r"(https?://[^\s\)]+)"
                rest = re.sub(url_pattern, f"{self.BLUE}\\1{self.RESET}", rest)

                # Reconstruct message with colored category
                record.msg = f"{self.CYAN}{category}{self.RESET} | {rest}"
                record.args = ()

            return super().format(record)

    handler = logging.StreamHandler()
    handler.addFilter(DropHttpxRequests())
    handler.setFormatter(ColoredFormatter("%(levelname)s:     %(message)s"))

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        handlers=[handler],
        force=True,
    )

    # Configure third-party library loggers (defense in depth)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
