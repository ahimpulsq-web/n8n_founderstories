# src/n8n_founderstories/core/config.py

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# =============================================================================
#  Default path resolvers
# =============================================================================

def _default_data_dir() -> str:
    """
    Resolve the default root directory for persisted artifacts.

    - Purpose: filesystem base for all saved outputs (plans, searches, enrichment).
    - Scope: local development and source-based execution.
    - Override: via environment variable N8N_DATA_DIR.

    Behavior:
    - Computes path relative to the repository root:
        <repo>/data
    - Keeps backward compatibility with current layout.
    """
    # core/config.py -> <repo>/src/n8n_founderstories/core/config.py
    # parents[3] -> <repo>
    repo_root = Path(__file__).resolve().parents[3]
    return str(repo_root / "data")


# =============================================================================
#  Application settings model
# =============================================================================


class Settings(BaseSettings):
    """
    Central application settings loaded from environment variables and/or .env.

    Design principles:
    - Single source of truth for configuration
    - Group settings by concern (App, Server, LLM, Integrations, etc.)
    - Keep defaults safe for local development; override via env in production
    """

    # -------------------------------------------------------------------------
    # Application
    # -------------------------------------------------------------------------
    app_name: str = Field(default="N8N-FounderStories", description="Application name")
    environment: str = Field(default="development", description="Environment name")
    api_v1_prefix: str = Field(default="/api/v1", description="API prefix for versioned routes")
    log_level: str = Field(default="INFO", description="Logging level")

    # -------------------------------------------------------------------------
    # Server (used by __main__.py / uvicorn)
    # -------------------------------------------------------------------------
    host: str = Field(default="0.0.0.0", description="Bind host for the API server")
    port: int = Field(default=8000, description="Bind port for the API server")
    reload: bool = Field(default=False, description="Enable auto-reload (development)")

    # -------------------------------------------------------------------------
    # Configuration loading (Pydantic Settings)
    # -------------------------------------------------------------------------
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # -------------------------------------------------------------------------
    # Configuration data directory
    # -------------------------------------------------------------------------
    data_dir: str = Field(
    default_factory=_default_data_dir,
    description="Root folder for persisted artifacts (env: N8N_DATA_DIR).",
    alias="N8N_DATA_DIR",
    )

    # -------------------------------------------------------------------------
    # LLM Provider selection
    # -------------------------------------------------------------------------
    llm_provider: str = Field(
        default="groq",
        description="Default LLM provider identifier (e.g., 'gemini', 'groq').",
    )

    # -------------------------------------------------------------------------
    # LLM Providers
    # -------------------------------------------------------------------------

    # Gemini
    gemini_api_key: str | None = Field(
        default=None,
        description="Gemini API key for Google GenAI (GEMINI_API_KEY).",
    )
    gemini_model_name: str = Field(
        default="gemini-2.0-flash",
        description="Gemini model name to use for structured generation.",
    )

    # Groq
    groq_api_key: str | None = Field(default=None, description="GROQ_API_KEY")
    groq_model_name: str = Field(
        default="None",
        description="GROQ_MODEL_NAME",
    )

    # -------------------------------------------------------------------------
    # Outreach integrations
    # -------------------------------------------------------------------------

    # Hunter.io
    hunter_api_key: str | None = Field(
        default=None,
        description="Hunter.io API key (HUNTERIO_API_KEY).",
    )
    hunter_base_url: str = Field(
        default="https://api.hunter.io/v2",
        description="Base URL for Hunter.io API.",
    )

    # -------------------------------------------------------------------------
    # Location integrations
    # -------------------------------------------------------------------------

    # Google Maps / Places
    google_maps_api_key: str | None = Field(
        default=None,
        description="Google Maps / Places API key (GOOGLE_MAPS_API_KEY).",
    )
    google_places_base_url: str = Field(
        default="https://maps.googleapis.com/maps/api/place",
        description="Base URL for Google Places API.",
    )

    # -------------------------------------------------------------------------
    # Search engine integrations
    # -------------------------------------------------------------------------

    # SerpAPI
    serpapi_api_key: str | None = Field(
        default=None,
        description="SerpAPI API key (SERPAPI_API_KEY).",
        alias="SERPAPI_API_KEY",
    )
    serpapi_base_url: str = Field(
        default="https://serpapi.com/search.json",
        description="Base URL for SerpAPI search endpoint.",
    )
    serpapi_engine: str = Field(
        default="google",
        description="Default SerpAPI search engine (e.g., 'google').",
    )

    # -------------------------------------------------------------------------
    # Google Sheets integration
    # -------------------------------------------------------------------------
    google_service_account_file: str = Field(
        default="C:/Projects/N8N-FounderStories/n8n-founderstories-f655fa6ecf68.json",
        description="Path to Google service account JSON (GOOGLE_SERVICE_ACCOUNT_FILE).",
    )

    google_sheets_scopes: list[str] = Field(
        default_factory=lambda: [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
        description="OAuth scopes for Google Sheets/Drive.",
    )

    # -------------------------------------------------------------------------
    # Google Sheets formatting settings
    # -------------------------------------------------------------------------
    google_sheets_header_row_height: int = Field(
        default=30,
        ge=15,
        le=100,
        description="Header row height in pixels (15-100).",
    )
    google_sheets_body_row_height: int = Field(
        default=21,
        ge=15,
        le=50,
        description="Body row height in pixels (15-50).",
    )
    google_sheets_wrap_strategy: str = Field(
        default="CLIP",
        description="Text wrap strategy for body cells: CLIP, OVERFLOW_CELL, or WRAP.",
    )

    # -------------------------------------------------------------------------
    # PostgreSQL Database Configuration
    # -------------------------------------------------------------------------
    postgres_host: str = Field(
        default="localhost",
        description="PostgreSQL host (POSTGRES_HOST).",
    )
    postgres_port: int = Field(
        default=5432,
        description="PostgreSQL port (POSTGRES_PORT).",
    )
    postgres_database: str = Field(
        default="n8n_founderstories",
        description="PostgreSQL database name (POSTGRES_DATABASE).",
    )
    postgres_username: str = Field(
        default="postgres",
        description="PostgreSQL username (POSTGRES_USERNAME).",
    )
    postgres_password: str = Field(
        default="",
        description="PostgreSQL password (POSTGRES_PASSWORD).",
    )
    
    # Connection pool settings
    postgres_min_connections: int = Field(
        default=5,
        ge=1,
        le=50,
        description="Minimum database connections in pool.",
    )
    postgres_max_connections: int = Field(
        default=20,
        ge=5,
        le=100,
        description="Maximum database connections in pool.",
    )
    postgres_pool_timeout: int = Field(
        default=30,
        ge=5,
        le=300,
        description="Connection pool timeout in seconds.",
    )
    
    # SSL and debugging
    postgres_ssl_mode: str = Field(
        default="prefer",
        description="PostgreSQL SSL mode (disable, allow, prefer, require).",
    )
    postgres_echo_sql: bool = Field(
        default=False,
        description="Echo SQL queries to logs (development only).",
    )
    
    # -------------------------------------------------------------------------
    # Database Migration and Hybrid Mode Settings
    # -------------------------------------------------------------------------
    enable_postgres: bool = Field(
        default=True,
        description="Enable PostgreSQL integration (ENABLE_POSTGRES).",
    )
    hybrid_mode: bool = Field(
        default=True,
        description="Run in hybrid mode (both Sheets and PostgreSQL).",
    )
    postgres_primary: bool = Field(
        default=True,
        description="Use PostgreSQL as primary storage (Sheets is export/view layer).",
    )
    
    # Data synchronization settings
    sync_to_sheets: bool = Field(
        default=True,
        description="Export PostgreSQL data to Google Sheets (DB→Sheets).",
    )
    sync_interval_minutes: int = Field(
        default=15,
        ge=1,
        le=1440,
        description="Sync interval in minutes for hybrid mode.",
    )
    
    # -------------------------------------------------------------------------
    # Database Feature Flags (safe-by-default)
    # -------------------------------------------------------------------------
    postgres_dsn: str | None = Field(
        default=None,
        description="Full PostgreSQL connection string (POSTGRES_DSN). If not provided, will be built from individual postgres_* fields.",
    )
    
    hunter_companies_db_enabled: bool = Field(
        default=False,
        description="Enable Hunter.io companies database writes (HUNTER_COMPANIES_DB_ENABLED).",
    )
    hunter_audit_db_enabled: bool = Field(
        default=False,
        description="Enable Hunter.io audit database writes (HUNTER_AUDIT_DB_ENABLED).",
    )
    google_maps_results_db_enabled: bool = Field(
        default=False,
        description="Enable Google Maps results database writes (GOOGLE_MAPS_RESULTS_DB_ENABLED).",
    )
    google_maps_enriched_db_enabled: bool = Field(
        default=False,
        description="Enable Google Maps enriched database writes (GOOGLE_MAPS_ENRICHED_DB_ENABLED).",
    )
    
    # -------------------------------------------------------------------------
    # Hunter.io Configuration
    # -------------------------------------------------------------------------
    hunter_sheets_export_enabled: bool = Field(
        default=True,
        description="Enable Hunter.io results export to Google Sheets (HUNTER_SHEETS_EXPORT_ENABLED).",
    )
    hunter_sheets_live_append_enabled: bool = Field(
        default=False,
        description="Enable live append to Google Sheets during Hunter.io discovery (HUNTER_SHEETS_LIVE_APPEND_ENABLED).",
    )
    hunter_db_batch_size_results: int = Field(
        default=200,
        ge=50,
        le=1000,
        description="Batch size for Hunter.io results database writes.",
    )
    hunter_db_batch_size_audit: int = Field(
        default=50,
        ge=10,
        le=200,
        description="Batch size for Hunter.io audit database writes.",
    )
    google_maps_db_batch_size_results: int = Field(
        default=200,
        ge=50,
        le=1000,
        description="Batch size for Google Maps results database writes.",
    )
    google_maps_db_batch_size_enriched: int = Field(
        default=100,
        ge=25,
        le=500,
        description="Batch size for Google Maps enriched database writes.",
    )
    
    # -------------------------------------------------------------------------
    # Google Maps Configuration
    # -------------------------------------------------------------------------
    google_maps_sheets_export_enabled: bool = Field(
        default=True,
        description="Enable Google Maps results export to Google Sheets (GOOGLE_MAPS_SHEETS_EXPORT_ENABLED).",
    )
    google_maps_sheets_live_append_enabled: bool = Field(
        default=False,
        description="Enable live append to Google Sheets during Google Maps discovery (GOOGLE_MAPS_SHEETS_LIVE_APPEND_ENABLED).",
    )
    
    # -------------------------------------------------------------------------
    # Master Data Configuration
    # -------------------------------------------------------------------------
    master_sheets_export_enabled: bool = Field(
        default=True,
        description="Enable Master results export to Google Sheets at job end (MASTER_SHEETS_EXPORT_ENABLED).",
    )
    
    # -------------------------------------------------------------------------
    # Computed properties
    # -------------------------------------------------------------------------
    
    def get_postgres_dsn(self) -> str | None:
        """
        Get PostgreSQL DSN, building it from components if not directly provided.
        
        Priority:
        1. Use postgres_dsn if explicitly set
        2. Build from postgres_host, postgres_port, postgres_database, postgres_username, postgres_password
        3. Return None if password is empty (indicates incomplete configuration)
        
        Returns:
            PostgreSQL connection string or None if not configured
        """
        # If explicit DSN provided, use it
        if self.postgres_dsn:
            return self.postgres_dsn
        
        # Build from components only if password is set (indicates intentional configuration)
        if not self.postgres_password:
            return None
        
        # Build DSN from individual components
        return (
            f"postgresql://{self.postgres_username}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"
        )


# Singleton settings object used throughout the application.
settings = Settings()
