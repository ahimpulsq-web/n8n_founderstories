"""
Database configuration for PostgreSQL integration.

Purpose:
- Single source of truth for DB feature flags and DB-tuning knobs (batch sizes).
- Loaded exclusively from core.config.settings (Pydantic Settings + .env).
- Safe-by-default: a feature is active only if its flag is true AND DSN exists.

Notes:
- Do not duplicate DSN construction logic here. That belongs in settings.get_postgres_dsn().
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ...core.config import settings


def _enabled(flag: bool, dsn: Optional[str]) -> bool:
    """A DB feature is active only if its flag is true AND DSN exists."""
    return bool(flag) and bool(dsn)


@dataclass(frozen=True)
class DatabaseConfig:
    """
    Database configuration snapshot (read-only).
    Derived exclusively from `core.config.settings`.
    """

    # Connection
    postgres_dsn: Optional[str]

    # Feature flags (raw from settings; gated by properties below)
    hunter_companies_db_enabled: bool
    hunter_audit_db_enabled: bool
    google_maps_results_db_enabled: bool
    google_maps_enriched_db_enabled: bool
    web_search_results_db_enabled: bool

    # Batch sizes
    hunter_db_batch_size_results: int
    hunter_db_batch_size_audit: int
    google_maps_db_batch_size_results: int
    google_maps_db_batch_size_enriched: int
    web_search_db_batch_size_results: int

    @classmethod
    def from_settings(cls) -> "DatabaseConfig":
        """Build config from global Settings."""
        dsn = settings.get_postgres_dsn()

        return cls(
            postgres_dsn=dsn,
            hunter_companies_db_enabled=settings.hunter_companies_db_enabled,
            hunter_audit_db_enabled=settings.hunter_audit_db_enabled,
            google_maps_results_db_enabled=settings.google_maps_results_db_enabled,
            google_maps_enriched_db_enabled=settings.google_maps_enriched_db_enabled,
            web_search_results_db_enabled=settings.web_search_results_db_enabled,
            hunter_db_batch_size_results=settings.hunter_db_batch_size_results,
            hunter_db_batch_size_audit=settings.hunter_db_batch_size_audit,
            google_maps_db_batch_size_results=settings.google_maps_db_batch_size_results,
            google_maps_db_batch_size_enriched=settings.google_maps_db_batch_size_enriched,
            web_search_db_batch_size_results=settings.web_search_db_batch_size_results,
        )

    # ---------------------------------------------------------------------
    # Enabled checks (dsn-gated)
    # ---------------------------------------------------------------------

    @property
    def is_hunter_companies_enabled(self) -> bool:
        return _enabled(self.hunter_companies_db_enabled, self.postgres_dsn)

    @property
    def is_hunter_audit_enabled(self) -> bool:
        return _enabled(self.hunter_audit_db_enabled, self.postgres_dsn)

    @property
    def is_google_maps_results_enabled(self) -> bool:
        return _enabled(self.google_maps_results_db_enabled, self.postgres_dsn)

    @property
    def is_google_maps_enriched_enabled(self) -> bool:
        return _enabled(self.google_maps_enriched_db_enabled, self.postgres_dsn)

    @property
    def is_web_search_results_enabled(self) -> bool:
        return _enabled(self.web_search_results_db_enabled, self.postgres_dsn)

    @property
    def is_enabled(self) -> bool:
        """True if any DB integration is active (and DSN exists)."""
        return bool(self.postgres_dsn) and (
            self.is_hunter_companies_enabled
            or self.is_hunter_audit_enabled
            or self.is_google_maps_results_enabled
            or self.is_google_maps_enriched_enabled
            or self.is_web_search_results_enabled
        )

    # ---------------------------------------------------------------------
    # Validation
    # ---------------------------------------------------------------------

    def validate(self) -> tuple[bool, Optional[str]]:
        """
        Validate configuration.
        Returns: (is_valid, error_message)
        """
        any_flag = any(
            [
                self.hunter_companies_db_enabled,
                self.hunter_audit_db_enabled,
                self.google_maps_results_db_enabled,
                self.google_maps_enriched_db_enabled,
                self.web_search_results_db_enabled,
            ]
        )

        # DB disabled is valid
        if not any_flag:
            return True, None

        if not self.postgres_dsn:
            return False, "PostgreSQL DSN is required when any *_DB_ENABLED flag is true"

        if not self.postgres_dsn.startswith(("postgresql://", "postgres://")):
            return False, "POSTGRES_DSN must start with postgresql:// or postgres://"

        return True, None


# Global configuration snapshot
db_config = DatabaseConfig.from_settings()
