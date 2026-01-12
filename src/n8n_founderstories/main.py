# src/n8n_founderstories/main.py

"""
FastAPI application wiring for n8n_founderstories.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .api.v1 import router as api_v1_router
from .core.config import settings
from .core.logging import setup_logging
from .core.errors import global_exception_handler, ErrorCode

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------------
# App Factory
# -------------------------------------------------------------------------
def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    setup_logging()

    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        description="Backend for N8N Founder Stories automation",
    )

    _register_exception_handlers(app)
    _register_routes(app)

    return app


# -------------------------------------------------------------------------
# Registration helpers
# -------------------------------------------------------------------------
def _register_exception_handlers(app: FastAPI) -> None:
    """Register global exception handlers."""
    from datetime import datetime, timezone

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        logger.warning(
            "REQUEST_VALIDATION_ERROR | path=%s | errors=%s",
            request.url.path,
            exc.errors(),
        )
        return JSONResponse(
            status_code=422,
            content={
                "detail": "Request validation failed. Please check your input.",
                "error_code": ErrorCode.VALIDATION_INVALID_FORMAT.value,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "validation_errors": exc.errors()
            }
        )

    # Use our centralized exception handler
    app.add_exception_handler(Exception, global_exception_handler)


def _register_routes(app: FastAPI) -> None:
    """Attach API routers."""
    app.include_router(api_v1_router, prefix=settings.api_v1_prefix)


# -------------------------------------------------------------------------
# ASGI entrypoint
# -------------------------------------------------------------------------
app = create_app()
