# src/n8n_founderstories/main.py

"""
FastAPI application wiring for n8n_founderstories.
"""

from __future__ import annotations

import logging
import os
import threading

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .api.v1 import router as api_v1_router
from .core.config import settings
from .core.logging import setup_logging
from .core.errors import global_exception_handler, ErrorCode

logger = logging.getLogger(__name__)

# Global references to background worker threads
_crawler_worker_thread: threading.Thread | None = None
_sheets_updater_thread: threading.Thread | None = None
_llm_worker_thread: threading.Thread | None = None
_aggregate_worker_thread: threading.Thread | None = None
_email_generator_worker_thread: threading.Thread | None = None


def _sync_env_vars() -> None:
    """
    Sync critical settings to OS environment variables.
    
    This ensures that modules using os.getenv() directly can access
    configuration values loaded by pydantic-settings from .env file.
    """
    # Sync Google Service Account file path
    if settings.google_service_account_file:
        os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"] = str(settings.service_account_path())
    
    # Sync other critical env vars that might be accessed via os.getenv()
    if settings.hunter_api_key:
        os.environ["HUNTER_API_KEY"] = settings.hunter_api_key
    
    if settings.postgres_password:
        os.environ["POSTGRES_PASSWORD"] = settings.postgres_password
        os.environ["POSTGRES_HOST"] = settings.postgres_host
        os.environ["POSTGRES_PORT"] = str(settings.postgres_port)
        os.environ["POSTGRES_DATABASE"] = settings.postgres_database
        os.environ["POSTGRES_USERNAME"] = settings.postgres_username


# -------------------------------------------------------------------------
# App Factory
# -------------------------------------------------------------------------
def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    # Sync settings to OS environment variables first
    _sync_env_vars()
    
    setup_logging()

    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        description="Backend for N8N Founder Stories automation",
    )

    _register_exception_handlers(app)
    _register_routes(app)
    _register_startup_events(app)

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


def _register_startup_events(app: FastAPI) -> None:
    """Register startup and shutdown event handlers."""
    @app.on_event("startup")
    async def startup_event():
        """Start application and background workers."""
        global _crawler_worker_thread, _sheets_updater_thread, _llm_worker_thread, _aggregate_worker_thread, _email_generator_worker_thread
        
        # Ensure database tables exist before starting workers
        import psycopg
        from .services.master import repo as master_repo
        from .services.mailer.email_generator import repo as email_gen_repo
        from .services.mailer.mail_tracker import repo as mail_tracker_repo
        try:
            with psycopg.connect(settings.postgres_dsn) as conn:
                master_repo.ensure_table(conn)
                email_gen_repo.ensure_table(conn, job_id="startup")
                mail_tracker_repo.ensure_table(conn)
                logger.info("SYSTEM | Database tables initialized")
        except Exception as e:
            logger.error(f"SYSTEM | Failed to initialize database tables: {e}")
            raise
        
        url = f"http://{settings.host}:{settings.port}"
        logger.info(f"SYSTEM | Application ready ({url})")
        
        # Start crawler worker in background
        from .services.enrichment.crawl.worker import run_worker as run_crawler
        
        _crawler_worker_thread = threading.Thread(
            target=lambda: run_crawler(poll_interval_s=5.0),
            name="CrawlerWorker",
            daemon=True
        )
        _crawler_worker_thread.start()
        logger.info("SYSTEM | Crawler worker started")
        
        # Start sheets updater worker in background
        from .services.sheets.updater_worker import run_worker as run_sheets_updater
        
        _sheets_updater_thread = threading.Thread(
            target=lambda: run_sheets_updater(poll_interval_s=30.0),
            name="SheetsUpdaterWorker",
            daemon=True
        )
        _sheets_updater_thread.start()
        logger.info("SYSTEM | Sheets updater worker started (updates every 30s)")
        
        # Start LLM extraction worker in background
        from .services.enrichment.extract.llm.worker import run_worker as run_llm_worker
        
        _llm_worker_thread = threading.Thread(
            target=lambda: run_llm_worker(poll_interval_s=5.0),
            name="LLMExtractionWorker",
            daemon=True
        )
        _llm_worker_thread.start()
        logger.info("SYSTEM | LLM extraction worker started")
        
        # Start aggregate worker in background
        from .services.enrichment.aggregate.worker import run_worker as run_aggregate_worker
        
        _aggregate_worker_thread = threading.Thread(
            target=lambda: run_aggregate_worker(poll_interval_s=5.0),
            name="AggregateWorker",
            daemon=True
        )
        _aggregate_worker_thread.start()
        logger.info("SYSTEM | Aggregate worker started")
        
        # Start email generator worker in background
        from .services.mailer.email_generator.worker import run_worker as run_email_generator
        
        _email_generator_worker_thread = threading.Thread(
            target=run_email_generator,
            name="EmailGeneratorWorker",
            daemon=True
        )
        _email_generator_worker_thread.start()
        logger.info("SYSTEM | Email generator worker started")
    
    @app.on_event("shutdown")
    async def shutdown_event():
        """Handle graceful shutdown."""
        logger.info("SYSTEM | Shutting down application...")
        
        # Crawler worker will stop automatically (daemon thread)
        if _crawler_worker_thread and _crawler_worker_thread.is_alive():
            logger.info("SYSTEM | Crawler worker will stop with application")
        
        # Sheets updater worker will stop automatically (daemon thread)
        if _sheets_updater_thread and _sheets_updater_thread.is_alive():
            logger.info("SYSTEM | Sheets updater worker will stop with application")
        
        # LLM extraction worker will stop automatically (daemon thread)
        if _llm_worker_thread and _llm_worker_thread.is_alive():
            logger.info("SYSTEM | LLM extraction worker will stop with application")
        
        # Aggregate worker will stop automatically (daemon thread)
        if _aggregate_worker_thread and _aggregate_worker_thread.is_alive():
            logger.info("SYSTEM | Aggregate worker will stop with application")
        
        # Email generator worker will stop automatically (daemon thread)
        if _email_generator_worker_thread and _email_generator_worker_thread.is_alive():
            logger.info("SYSTEM | Email generator worker will stop with application")
        
        logger.info("SYSTEM | Shutdown complete")


# -------------------------------------------------------------------------
# ASGI entrypoint
# -------------------------------------------------------------------------
app = create_app()
