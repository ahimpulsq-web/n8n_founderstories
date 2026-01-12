"""
Database configuration module for PostgreSQL integration.

This module provides configuration management for PostgreSQL connections,
including feature flags and connection validation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ...core.config import settings


@dataclass
class DatabaseConfig:
    """
    Database configuration with validation and feature flags.
    
    This configuration is safe-by-default: if PostgreSQL is not properly
    configured, the feature will be disabled without breaking the application.
    """
    postgres_dsn: Optional[str]
    hunter_companies_db_enabled: bool
    hunter_audit_db_enabled: bool
    google_maps_results_db_enabled: bool
    google_maps_enriched_db_enabled: bool
    
    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """
        Create database configuration from core settings.
        
        This method now reads from core.config.settings, which automatically
        loads values from .env files. This ensures consistent environment
        variable handling across the application.
        
        Environment variables (loaded via Settings):
        - POSTGRES_DSN: Full PostgreSQL connection string (or built from components)
        - HUNTER_COMPANIES_DB_ENABLED: Enable Hunter companies DB writes (default: false)
        - HUNTER_AUDIT_DB_ENABLED: Enable Hunter audit DB writes (default: false)
        - GOOGLE_MAPS_RESULTS_DB_ENABLED: Enable Google Maps results DB writes (default: false)
        - GOOGLE_MAPS_ENRICHED_DB_ENABLED: Enable Google Maps enriched DB writes (default: false)
        
        Returns:
            DatabaseConfig instance with validated settings
        """
        # Get postgres_dsn from settings (either explicit or built from components)
        postgres_dsn = settings.get_postgres_dsn()
        
        # Get feature flags from settings (safe-by-default: False)
        hunter_companies_db_enabled = settings.hunter_companies_db_enabled
        hunter_audit_db_enabled = settings.hunter_audit_db_enabled
        google_maps_results_db_enabled = settings.google_maps_results_db_enabled
        google_maps_enriched_db_enabled = settings.google_maps_enriched_db_enabled
        
        # If features are enabled but no DSN provided, disable the features (safety check)
        if hunter_companies_db_enabled and not postgres_dsn:
            hunter_companies_db_enabled = False
        if hunter_audit_db_enabled and not postgres_dsn:
            hunter_audit_db_enabled = False
        if google_maps_results_db_enabled and not postgres_dsn:
            google_maps_results_db_enabled = False
        if google_maps_enriched_db_enabled and not postgres_dsn:
            google_maps_enriched_db_enabled = False
        
        return cls(
            postgres_dsn=postgres_dsn,
            hunter_companies_db_enabled=hunter_companies_db_enabled,
            hunter_audit_db_enabled=hunter_audit_db_enabled,
            google_maps_results_db_enabled=google_maps_results_db_enabled,
            google_maps_enriched_db_enabled=google_maps_enriched_db_enabled
        )
    
    @property
    def is_enabled(self) -> bool:
        """Check if any database integration is enabled and properly configured."""
        return (
            self.hunter_companies_db_enabled or
            self.hunter_audit_db_enabled or
            self.google_maps_results_db_enabled or
            self.google_maps_enriched_db_enabled
        ) and bool(self.postgres_dsn)
    
    @property
    def is_hunter_companies_enabled(self) -> bool:
        """Check if Hunter companies database integration is enabled."""
        return self.hunter_companies_db_enabled and bool(self.postgres_dsn)
    
    @property
    def is_hunter_audit_enabled(self) -> bool:
        """Check if Hunter audit database integration is enabled."""
        return self.hunter_audit_db_enabled and bool(self.postgres_dsn)
    
    @property
    def is_google_maps_results_enabled(self) -> bool:
        """Check if Google Maps results database integration is enabled."""
        return self.google_maps_results_db_enabled and bool(self.postgres_dsn)
    
    @property
    def is_google_maps_enriched_enabled(self) -> bool:
        """Check if Google Maps enriched database integration is enabled."""
        return self.google_maps_enriched_db_enabled and bool(self.postgres_dsn)
    
    def validate(self) -> tuple[bool, Optional[str]]:
        """
        Validate the database configuration.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not (self.hunter_companies_db_enabled or self.hunter_audit_db_enabled or
                self.google_maps_results_db_enabled or self.google_maps_enriched_db_enabled):
            return True, None  # Disabled is valid
            
        if not self.postgres_dsn:
            enabled_features = []
            if self.hunter_companies_db_enabled:
                enabled_features.append("HUNTER_COMPANIES_DB_ENABLED")
            if self.hunter_audit_db_enabled:
                enabled_features.append("HUNTER_AUDIT_DB_ENABLED")
            if self.google_maps_results_db_enabled:
                enabled_features.append("GOOGLE_MAPS_RESULTS_DB_ENABLED")
            if self.google_maps_enriched_db_enabled:
                enabled_features.append("GOOGLE_MAPS_ENRICHED_DB_ENABLED")
            return False, f"POSTGRES_DSN is required when {' or '.join(enabled_features)}=true"
            
        if not self.postgres_dsn.startswith(("postgresql://", "postgres://")):
            return False, "POSTGRES_DSN must start with postgresql:// or postgres://"
            
        return True, None


# Global configuration instance
db_config = DatabaseConfig.from_env()