# src/n8n_founderstories/core/config.py

from __future__ import annotations

from pathlib import Path
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict



# =============================================================================
# Default path resolvers
# =============================================================================

def _default_repo_root() -> Path:
    """
    Calculate repository root directory.
    
    This function goes up 3 levels from this file:
    - config.py is in: src/n8n_founderstories/core/
    - parents[0] = core/
    - parents[1] = n8n_founderstories/
    - parents[2] = src/
    - parents[3] = project root
    """
    repo_root = Path(__file__).resolve().parents[3]
    
    # Verify .env file exists at calculated root
    env_file = repo_root / ".env"
    if not env_file.exists():
        import warnings
        warnings.warn(
            f"WARNING: .env file not found at calculated repo root: {repo_root}\n"
            f"Expected .env at: {env_file}\n"
            f"Current file: {__file__}\n"
            f"This may cause configuration issues!"
        )
    
    return repo_root


def _default_data_dir() -> str:
    repo_root = _default_repo_root()
    return str(repo_root / "data")


def _split_csv(raw: str | None) -> list[str]:
    s = (raw or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


# =============================================================================
# Application settings model
# =============================================================================

class Settings(BaseSettings):
    """
    Central application settings loaded from environment variables and/or .env.

    Project decision:
    - OpenRouter is the ONLY LLM entry point.
    - Models are selected via pools (premium/free) and per-module tier selectors.
    - No other providers are configured here.
    """

    model_config = SettingsConfigDict(
        env_file=str(_default_repo_root() / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # -------------------------------------------------------------------------
    # Application
    # -------------------------------------------------------------------------
    app_name: str = Field(default="N8N-FounderStories", alias="APP_NAME")
    environment: str = Field(default="development", alias="ENVIRONMENT")
    api_v1_prefix: str = Field(default="/api/v1", alias="API_V1_PREFIX")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # -------------------------------------------------------------------------
    # Server
    # -------------------------------------------------------------------------
    host: str = Field(default="0.0.0.0", alias="HOST")
    port: int = Field(default=8000, alias="PORT")
    reload: bool = Field(default=False, alias="RELOAD")

    # -------------------------------------------------------------------------
    # Data dir
    # -------------------------------------------------------------------------
    data_dir: str = Field(default_factory=_default_data_dir, alias="N8N_DATA_DIR")

    # -------------------------------------------------------------------------
    # Integrations: Hunter.io
    # -------------------------------------------------------------------------
    hunter_api_key: str | None = Field(default=None, alias="HUNTER_API_KEY")
    hunter_base_url: str = Field(default="https://api.hunter.io/v2", alias="HUNTER_BASE_URL")

    # -------------------------------------------------------------------------
    # Integrations: SerpAPI
    # -------------------------------------------------------------------------
    serpapi_api_key: str | None = Field(default=None, alias="SERPAPI_API_KEY")
    serpapi_base_url: str = Field(default="https://serpapi.com/search.json", alias="SERPAPI_BASE_URL")
    serpapi_engine: str = Field(default="google", alias="SERPAPI_ENGINE")

    # -------------------------------------------------------------------------
    # Integrations: Google Maps / Places
    # -------------------------------------------------------------------------
    google_maps_api_key: str | None = Field(default=None, alias="GOOGLE_MAPS_API_KEY")
    google_places_base_url: str = Field(
        default="https://maps.googleapis.com/maps/api/place",
        alias="GOOGLE_PLACES_BASE_URL",
    )

    # -------------------------------------------------------------------------
    # Integrations: Geocoding
    # -------------------------------------------------------------------------
    geocoding_api_key: str | None = Field(default=None, alias="GEOCODING_API_KEY")

    # -------------------------------------------------------------------------
    # Integrations: Google Sheets / Drive
    # -------------------------------------------------------------------------
    google_service_account_file: str = Field(
        default="./credentials/service-account.json",
        alias="GOOGLE_SERVICE_ACCOUNT_FILE",
    )
    google_sheets_scopes: list[str] = Field(
        default_factory=lambda: [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
        alias="GOOGLE_SHEETS_SCOPES",
    )
    
    # Global mail tracking sheet ID
    global_mail_tracking_sheet_id: str | None = Field(
        default=None,
        alias="GLOBAL_MAIL_TRACKING_SHEET_ID"
    )

    # -------------------------------------------------------------------------
    # PostgreSQL
    # -------------------------------------------------------------------------
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_database: str = Field(default="n8n_founderstories", alias="POSTGRES_DATABASE")
    postgres_username: str = Field(default="postgres", alias="POSTGRES_USERNAME")
    postgres_password: str = Field(default="", alias="POSTGRES_PASSWORD")

    postgres_min_connections: int = Field(default=5, ge=1, le=50, alias="POSTGRES_MIN_CONNECTIONS")
    postgres_max_connections: int = Field(default=20, ge=5, le=100, alias="POSTGRES_MAX_CONNECTIONS")
    postgres_pool_timeout: int = Field(default=30, ge=5, le=300, alias="POSTGRES_POOL_TIMEOUT")

    postgres_ssl_mode: str = Field(default="require", alias="POSTGRES_SSL_MODE")
    postgres_echo_sql: bool = Field(default=False, alias="POSTGRES_ECHO_SQL")

    postgres_dsn: str | None = Field(default=None, alias="POSTGRES_DSN")

    # -------------------------------------------------------------------------
    # DB / Hybrid mode flags
    # -------------------------------------------------------------------------
    enable_postgres: bool = Field(default=True, alias="ENABLE_POSTGRES")
    hybrid_mode: bool = Field(default=True, alias="HYBRID_MODE")
    postgres_primary: bool = Field(default=True, alias="POSTGRES_PRIMARY")


    # DB write toggles
    hunter_companies_db_enabled: bool = Field(default=False, alias="HUNTER_COMPANIES_DB_ENABLED")
    hunter_audit_db_enabled: bool = Field(default=False, alias="HUNTER_AUDIT_DB_ENABLED")
    google_maps_results_db_enabled: bool = Field(default=False, alias="GOOGLE_MAPS_RESULTS_DB_ENABLED")
    google_maps_enriched_db_enabled: bool = Field(default=False, alias="GOOGLE_MAPS_ENRICHED_DB_ENABLED")
    web_search_results_db_enabled: bool = Field(default=False, alias="WEB_SEARCH_RESULTS_DB_ENABLED")

    # -------------------------------------------------------------------------
    # DB batch sizes
    # -------------------------------------------------------------------------
    hunter_db_batch_size_results: int = Field(default=200, ge=50, le=1000, alias="HUNTER_DB_BATCH_SIZE_RESULTS")
    hunter_db_batch_size_audit: int = Field(default=50, ge=10, le=200, alias="HUNTER_DB_BATCH_SIZE_AUDIT")
    google_maps_db_batch_size_results: int = Field(default=200, ge=50, le=1000, alias="GOOGLE_MAPS_DB_BATCH_SIZE_RESULTS")
    google_maps_db_batch_size_enriched: int = Field(default=100, ge=25, le=500, alias="GOOGLE_MAPS_DB_BATCH_SIZE_ENRICHED")
    web_search_db_batch_size_results: int = Field(default=200, ge=50, le=1000, alias="WEB_SEARCH_DB_BATCH_SIZE_RESULTS")

    # -------------------------------------------------------------------------
    # Export toggles
    # -------------------------------------------------------------------------
    hunter_sheets_export_enabled: bool = Field(default=True, alias="HUNTER_SHEETS_EXPORT_ENABLED")
    hunter_sheets_live_append_enabled: bool = Field(default=False, alias="HUNTER_SHEETS_LIVE_APPEND_ENABLED")

    google_maps_sheets_export_enabled: bool = Field(default=True, alias="GOOGLE_MAPS_SHEETS_EXPORT_ENABLED")
    google_maps_sheets_live_append_enabled: bool = Field(default=False, alias="GOOGLE_MAPS_SHEETS_LIVE_APPEND_ENABLED")

    master_sheets_export_enabled: bool = Field(default=True, alias="MASTER_SHEETS_EXPORT_ENABLED")

    # -------------------------------------------------------------------------
    # Web Scraper / enrichment runtime
    # -------------------------------------------------------------------------
    domain_concurrency: int = Field(default=4, ge=1, le=100, alias="DOMAIN_CONCURRENCY")
    crawl4ai_max_concurrency: int = Field(default=3, ge=1, le=50, alias="CRAWL4AI_MAX_CONCURRENCY")
    llm_max_concurrency: int = Field(default=6, ge=1, le=100, alias="LLM_MAX_CONCURRENCY")

    top_k_pages: int = Field(default=4, ge=1, le=20, alias="TOP_K_PAGES")
    crawl_timeout_s: float = Field(default=40.0, ge=1.0, le=300.0, alias="CRAWL_TIMEOUT_S")
    wait_after_load_s: float = Field(default=0.0, ge=0.0, le=30.0, alias="WAIT_AFTER_LOAD_S")
    headless: bool = Field(default=True, alias="HEADLESS")
    user_agent: str = Field(default="Mozilla/5.0 (FounderStories LLM)", alias="USER_AGENT")
    language: str = Field(default="de", alias="LANGUAGE")
    max_chars_per_page: int = Field(default=18_000, ge=1000, le=200_000, alias="MAX_CHARS_PER_PAGE")


    # =========================================================================
    # LLM (OpenRouter-only)
    # =========================================================================

    llm_provider: str = Field(default="openrouter", alias="LLM_PROVIDER")

    # OpenRouter API key (single key)
    llm_api_key: str = Field(default="", alias="LLM_API_KEY")

    llm_premium_models_raw: str = Field(default="", alias="LLM_PREMIUM_MODELS")

    openrouter_base_url: str = Field(default="https://openrouter.ai/api/v1", alias="OPENROUTER_BASE_URL")
    openrouter_http_referer: str = Field(default="", alias="OPENROUTER_HTTP_REFERER")

    llm_timeout_s: float = Field(default=60.0, ge=1.0, le=300.0, alias="LLM_TIMEOUT_S")
    llm_max_retries: int = Field(default=2, ge=0, le=10, alias="LLM_MAX_RETRIES")

    # Module Tier Mappings (REQUIRED)
    prompt_tier: str = Field(..., alias="PROMPT_TIER")
    search_plan_tier: str = Field(..., alias="SEARCH_PLAN_TIER")
    link_classifier_tier: str = Field(..., alias="LINK_CLASSIFIER_TIER")
    blog_extractor_tier: str = Field(..., alias="BLOG_EXTRACTOR_TIER")

    @field_validator("llm_provider")
    @classmethod
    def validate_llm_provider(cls, v: str) -> str:
        if (v or "").strip().lower() != "openrouter":
            raise ValueError("Only 'openrouter' is supported (project decision).")
        return "openrouter"

    @field_validator("prompt_tier", "search_plan_tier", "link_classifier_tier", "blog_extractor_tier")
    @classmethod
    def validate_tier(cls, v: str) -> str:
        valid = {"LLM_PREMIUM_MODELS"}
        if v not in valid:
            raise ValueError(f"Tier must be one of {valid}, got: {v}")
        return v
    
    # -------------------------------------------------------------------------
    # Embeddings (OpenRouter)
    # -------------------------------------------------------------------------
    embedding_model: str = Field(
        default="openai/text-embedding-3-large",
        alias="EMBEDDING_MODEL",
    )


    # -------------------------------------------------------------------------
    # Computed properties / helpers
    # -------------------------------------------------------------------------

    def get_postgres_dsn(self) -> str | None:
        if self.postgres_dsn:
            return self.postgres_dsn

        if not self.postgres_password:
            return None

        return (
            f"postgresql://{self.postgres_username}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"
        )

    @property
    def llm_premium_models(self) -> List[str]:
        return _split_csv(self.llm_premium_models_raw)

    @property
    def openrouter_api_key(self) -> str:
        if not self.llm_api_key:
            raise ValueError("LLM_API_KEYS is missing or empty.")
        return self.llm_api_key

    def resolve_tier_models(self, tier: str) -> list[str]:
        t = (tier or "").strip()
        if t == "LLM_PREMIUM_MODELS":
            models = self.llm_premium_models
        else:
            raise ValueError(f"Unknown tier: {tier!r}")

        if not models:
            raise ValueError(f"{tier} resolved to empty model list.")
        return models

    def service_account_path(self) -> Path:
        """Resolve service account file path relative to repo root if not absolute."""
        path = Path(self.google_service_account_file)
        if not path.is_absolute():
            return _default_repo_root() / path
        return path


# Singleton settings object
settings = Settings()
