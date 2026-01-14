"""
Simple migration runner for PostgreSQL schema changes.

This module provides a lightweight migration system without requiring
external dependencies like Alembic.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List, Tuple

from ..connection import get_connection_context, DatabaseConnectionError

logger = logging.getLogger(__name__)


class MigrationError(Exception):
    """Raised when migration operations fail."""
    pass


def ensure_migrations_table(dsn: str) -> None:
    """
    Ensure the schema_migrations table exists.
    
    Args:
        dsn: PostgreSQL connection string
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        id SERIAL PRIMARY KEY,
        filename VARCHAR(255) NOT NULL UNIQUE,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        checksum VARCHAR(64)
    );
    
    CREATE INDEX IF NOT EXISTS idx_schema_migrations_filename 
    ON schema_migrations(filename);
    """
    
    try:
        with get_connection_context(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                conn.commit()
        logger.debug("Schema migrations table ready")
    except Exception as e:
        raise MigrationError(f"Failed to create migrations table: {e}") from e


def get_applied_migrations(dsn: str) -> set[str]:
    """
    Get list of already applied migrations.
    
    Args:
        dsn: PostgreSQL connection string
        
    Returns:
        Set of applied migration filenames
    """
    try:
        with get_connection_context(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT filename FROM schema_migrations ORDER BY applied_at")
                return {row[0] for row in cur.fetchall()}
    except Exception as e:
        raise MigrationError(f"Failed to get applied migrations: {e}") from e


def get_migration_files(migrations_dir: Path) -> List[Tuple[str, Path]]:
    """
    Get sorted list of migration files.
    
    Args:
        migrations_dir: Directory containing migration files
        
    Returns:
        List of (filename, filepath) tuples sorted by filename
    """
    if not migrations_dir.exists():
        logger.warning("Migrations directory does not exist: %s", migrations_dir)
        return []
    
    migration_files = []
    for file_path in migrations_dir.rglob("*.sql"):
        if file_path.is_file():
            migration_files.append((file_path.name, file_path))
    
    # Sort by filename to ensure proper order
    migration_files.sort(key=lambda x: x[0])
    return migration_files


def apply_migration(dsn: str, filename: str, filepath: Path) -> None:
    """
    Apply a single migration file.
    
    Args:
        dsn: PostgreSQL connection string
        filename: Migration filename
        filepath: Path to migration file
    """
    try:
        # Read migration content
        migration_sql = filepath.read_text(encoding='utf-8')
        
        with get_connection_context(dsn) as conn:
            with conn.cursor() as cur:
                # Execute migration
                cur.execute(migration_sql)
                
                # Record migration as applied
                cur.execute(
                    "INSERT INTO schema_migrations (filename) VALUES (%s)",
                    (filename,)
                )
                
                conn.commit()
        
        logger.debug("Applied migration: %s", filename)
        
    except Exception as e:
        raise MigrationError(f"Failed to apply migration {filename}: {e}") from e


def run_migrations(dsn: str, migrations_dir: Path | str | None = None) -> int:
    """
    Run all pending migrations.
    
    Args:
        dsn: PostgreSQL connection string
        migrations_dir: Directory containing migration files. 
                       If None, uses default migrations directory.
        
    Returns:
        Number of migrations applied
    """
    if migrations_dir is None:
        # Enforce canonical location: services/database/migrations
        migrations_dir = Path(__file__).resolve().parent
    elif isinstance(migrations_dir, str):
        migrations_dir = Path(migrations_dir).resolve()
    else:
        migrations_dir = migrations_dir.resolve()
    
    logger.debug("Running migrations from: %s", migrations_dir)
    
    try:
        # Ensure migrations table exists
        ensure_migrations_table(dsn)
        
        # Get applied migrations
        applied = get_applied_migrations(dsn)
        logger.debug("Found %d already applied migrations", len(applied))
        
        # Get available migration files
        migration_files = get_migration_files(migrations_dir)
        logger.debug("Found %d migration files", len(migration_files))
        
        # Apply pending migrations
        applied_count = 0
        for filename, filepath in migration_files:
            if filename not in applied:
                logger.debug("Applying migration: %s", filename)
                apply_migration(dsn, filename, filepath)
                applied_count += 1
            else:
                logger.debug("Skipping already applied migration: %s", filename)
        
        if applied_count == 0:
            logger.debug("No pending migrations to apply")
        else:
            logger.debug("Successfully applied %d migrations", applied_count)
        
        return applied_count
        
    except Exception as e:
        logger.error("Migration failed: %s", e)
        raise


def main():
    """CLI entry point for running migrations."""
    import sys
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get DSN from environment
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        logger.error("POSTGRES_DSN environment variable is required")
        sys.exit(1)
    
    try:
        applied_count = run_migrations(dsn)
        logger.debug("Migration completed successfully. Applied %d migrations.", applied_count)
    except Exception as e:
        logger.error("Migration failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()