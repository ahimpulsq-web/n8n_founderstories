# src/n8n_founderstories/__main__.py

"""
Allows running via:
    python -m n8n_founderstories
"""

from __future__ import annotations

import signal
import sys
import uvicorn

from .core.config import settings


def main() -> None:
    """Run the API server with proper signal handling."""
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        """Handle Ctrl+C and other termination signals."""
        print("\n🛑 Shutting down gracefully...")
        sys.exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Termination signal
    
    try:
        uvicorn.run(
            "n8n_founderstories.main:app",
            host=settings.host,
            port=settings.port,
            reload=settings.reload,
            log_level="warning",
            access_log=False,
        )
    except KeyboardInterrupt:
        print("\n🛑 Shutting down gracefully...")
        sys.exit(0)


if __name__ == "__main__":
    main()
