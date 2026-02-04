# src/n8n_founderstories/core/logging/setup.py

from __future__ import annotations

import logging
from .live_status import LiveStatusLogger
from ..config import settings

# Global reference to live status logger for clearing before normal logs
_live_status_logger = None


def set_live_status_logger(logger) -> None:
    """Set the global live status logger reference."""
    global _live_status_logger
    _live_status_logger = logger

def get_live_status_logger() -> LiveStatusLogger:
    """Return the process-wide LiveStatusLogger singleton."""
    global _live_status_logger
    if _live_status_logger is None:
        _live_status_logger = LiveStatusLogger()
    return _live_status_logger

class DropHttpxRequests(logging.Filter):
    """
    Drop noisy per-request logs emitted by httpx/httpcore.
    Keeps your application logs intact.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        return not record.name.startswith(("httpx", "httpcore"))


class LiveStatusClearingHandler(logging.StreamHandler):
    """
    StreamHandler that clears any active live status line before writing logs,
    then reprints it after. This prevents normal logs from corrupting the live
    status display while keeping the progress visible.
    """
    
    def emit(self, record: logging.LogRecord) -> None:
        """Clear live status line, emit log, then reprint live status."""
        global _live_status_logger
        
        # Clear the live status line before the log
        if _live_status_logger is not None:
            try:
                _live_status_logger.clear_line()
            except Exception:
                pass  # Don't let clearing errors break logging
        
        # Emit the actual log
        super().emit(record)
        
        # Reprint the live status line after the log
        if _live_status_logger is not None:
            try:
                _live_status_logger.reprint_after_log()
            except Exception:
                pass  # Don't let reprinting errors break logging


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
            # Color the level name while preserving padding
            # The format string uses %(levelname)-8s which pads to 8 characters
            # We need to apply color to the actual level name, then pad the result
            levelname = record.levelname.strip()  # Remove any existing padding
            if levelname in self.LEVEL_COLORS:
                # Color the level name and pad to 8 characters
                colored_level = f"{self.LEVEL_COLORS[levelname]}{levelname}{self.RESET}"
                # Pad to 8 visible characters (levelname length + spaces)
                padding_needed = 8 - len(levelname)
                record.levelname = colored_level + (" " * padding_needed)
            
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

    handler = LiveStatusClearingHandler()
    handler.addFilter(DropHttpxRequests())
    # Use %-8s to left-align level name in 8-character field for proper alignment
    handler.setFormatter(ColoredFormatter("%(levelname)-8s %(message)s"))

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