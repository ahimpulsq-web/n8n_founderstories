# src/n8n_founderstories/__main__.py

"""
Allows running via:
    python -m n8n_founderstories
"""

from __future__ import annotations

import uvicorn

from .core.config import settings


def main() -> None:
    """Run the API server."""
    uvicorn.run(
        "n8n_founderstories.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.reload,
        log_level="warning",
        access_log=False,
    )


if __name__ == "__main__":
    main()
