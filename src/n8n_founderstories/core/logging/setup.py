# src/n8n_founderstories/core/logging/setup.py

from __future__ import annotations

import logging
import sys
from pathlib import Path
from .live_status import LiveStatusLogger
from ..config import settings

# Global reference to live status logger (now disabled/no-op)
_live_status_logger = None


def set_live_status_logger(logger) -> None:
    """Set the global live status logger reference (no-op for compatibility)."""
    global _live_status_logger
    _live_status_logger = logger

def get_live_status_logger() -> LiveStatusLogger:
    """Return a disabled LiveStatusLogger for compatibility."""
    global _live_status_logger
    if _live_status_logger is None:
        # Create disabled instance (no live updates)
        _live_status_logger = LiveStatusLogger(enabled=False)
    return _live_status_logger

class DropHttpxRequests(logging.Filter):
    """
    Drop noisy per-request logs emitted by httpx/httpcore.
    Keeps your application logs intact.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        return not record.name.startswith(("httpx", "httpcore"))


class MinimalConsoleFilter(logging.Filter):
    """
    Filter for console output - only show critical events:
    - SYSTEM startup
    - SEARCH PLAN start/complete
    - Service START/COMPLETED
    - DATABASE operations
    - SHEETS operations
    - Critical errors
    """
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        
        # Always show errors and warnings
        if record.levelno >= logging.WARNING:
            return True
        
        # Show these specific events
        allowed_patterns = [
            "SYSTEM |",
            "SEARCH PLAN | START",
            "SEARCH PLAN | COMPLETE",
            "| STATE=START |",
            "| STATE=COMPLETED |",
            "CRAWL |",  # Show crawl progress logs
            "EXTRACT |",  # Show extraction progress logs (matches CRAWL format)
            "RUN | DONE",
            "DATABASE |",
            "SHEETS |",
            "EMAIL_WORKER |",  # Show email worker logs
            "EMAIL_GEN |",  # Show email generation logs
            "MAIL CONTENT |",  # Show per-email logs
        ]
        
        return any(pattern in msg for pattern in allowed_patterns)


def setup_logging() -> None:
    """
    Configure application-wide logging with:
    - Minimal console output (critical events only)
    - Per-service file handlers (logs/hunteriov2.log, logs/googlemapsv2.log)
    - Colored console output
    """

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
            levelname = record.levelname.strip()
            if levelname in self.LEVEL_COLORS:
                colored_level = f"{self.LEVEL_COLORS[levelname]}{levelname}{self.RESET}"
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

    class PlainFormatter(logging.Formatter):
        """Plain formatter for file output (no colors)."""
        pass

    # Create logs directory
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers.clear()

    # Console handler (minimal, colored) with UTF-8 encoding
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.INFO)
    console_handler.addFilter(DropHttpxRequests())
    console_handler.addFilter(MinimalConsoleFilter())
    console_handler.setFormatter(ColoredFormatter("%(levelname)-8s %(message)s"))
    # Force UTF-8 encoding for console output to handle emojis and special characters
    if hasattr(console_handler.stream, 'reconfigure'):
        console_handler.stream.reconfigure(encoding='utf-8', errors='replace')
    root_logger.addHandler(console_handler)

    # HunterIOV2 file handler with UTF-8 encoding
    hunter_handler = logging.FileHandler(logs_dir / "hunteriov2.log", mode="a", encoding="utf-8")
    hunter_handler.setLevel(logging.DEBUG)
    hunter_handler.addFilter(lambda record: "hunteriov2" in record.name.lower() or "HUNTERIOV2" in record.getMessage())
    hunter_handler.setFormatter(PlainFormatter("%(levelname)-8s %(message)s"))
    root_logger.addHandler(hunter_handler)

    # GoogleMapsV2 file handler with UTF-8 encoding
    gmaps_handler = logging.FileHandler(logs_dir / "googlemapsv2.log", mode="a", encoding="utf-8")
    gmaps_handler.setLevel(logging.DEBUG)
    gmaps_handler.addFilter(lambda record: "google_mapsv2" in record.name.lower() or "GOOGLEMAPSV2" in record.getMessage())
    gmaps_handler.setFormatter(PlainFormatter("%(levelname)-8s %(message)s"))
    root_logger.addHandler(gmaps_handler)

    # Crawl file handler with UTF-8 encoding
    crawl_handler = logging.FileHandler(logs_dir / "crawl.log", mode="a", encoding="utf-8")
    crawl_handler.setLevel(logging.DEBUG)
    crawl_handler.addFilter(lambda record: "crawl" in record.name.lower() or "CRAWL |" in record.getMessage())
    crawl_handler.setFormatter(PlainFormatter("%(levelname)-8s %(message)s"))
    root_logger.addHandler(crawl_handler)

    # LLM Extract file handler with UTF-8 encoding
    llm_extract_handler = logging.FileHandler(logs_dir / "llm_extract.log", mode="a", encoding="utf-8")
    llm_extract_handler.setLevel(logging.DEBUG)
    llm_extract_handler.addFilter(lambda record: "llm_extract" in record.name.lower() or "LLM_EXTRACT |" in record.getMessage())
    llm_extract_handler.setFormatter(PlainFormatter("%(levelname)-8s %(message)s"))
    root_logger.addHandler(llm_extract_handler)

    # Email Generator file handler with UTF-8 encoding
    email_gen_handler = logging.FileHandler(logs_dir / "email_generator.log", mode="a", encoding="utf-8")
    email_gen_handler.setLevel(logging.DEBUG)
    email_gen_handler.addFilter(lambda record: "email_generator" in record.name.lower() or "EMAIL_WORKER |" in record.getMessage() or "EMAIL_GEN |" in record.getMessage() or "MAIL CONTENT |" in record.getMessage())
    email_gen_handler.setFormatter(PlainFormatter("%(levelname)-8s %(message)s"))
    root_logger.addHandler(email_gen_handler)

    # Optional: Full app log (everything) with UTF-8 encoding
    app_handler = logging.FileHandler(logs_dir / "app.log", mode="a", encoding="utf-8")
    app_handler.setLevel(logging.DEBUG)
    app_handler.addFilter(DropHttpxRequests())
    app_handler.setFormatter(PlainFormatter("%(levelname)-8s %(message)s"))
    root_logger.addHandler(app_handler)

    # Configure third-party library loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)