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


# Singleton settings object used throughout the application.
settings = Settings()
