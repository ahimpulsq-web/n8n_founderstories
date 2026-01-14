"""
Database connection module using psycopg v3.

This module provides safe connection management for PostgreSQL operations
without creating global connections at import time.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator, Optional

import psycopg
from psycopg import Connection

from .config import DatabaseConfig

logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Raised when database connection fails."""
    pass


def get_connection(dsn: Optional[str] = None) -> Connection:
    """
    Create a new PostgreSQL connection using psycopg v3.
    
    Args:
        dsn: PostgreSQL connection string. If None, uses config from environment.
        
    Returns:
        psycopg Connection instance
        
    Raises:
        DatabaseConnectionError: If connection fails
    """
    if dsn is None:
        from .config import db_config
        dsn = db_config.postgres_dsn
    
    if not dsn:
        raise DatabaseConnectionError("No PostgreSQL DSN provided")
    
    try:
        conn = psycopg.connect(dsn)
        # Test the connection
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return conn
    except Exception as e:
        raise DatabaseConnectionError(f"Failed to connect to PostgreSQL: {e}") from e


@contextmanager
def get_connection_context(dsn: Optional[str] = None) -> Generator[Connection, None, None]:
    """
    Context manager for database connections.
    
    Automatically handles connection cleanup and error handling.
    
    Args:
        dsn: PostgreSQL connection string. If None, uses config from environment.
        
    Yields:
        psycopg Connection instance
        
    Example:
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM hunter_companies")
                results = cur.fetchall()
    """
    conn = None
    try:
        conn = get_connection(dsn)
        yield conn
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass  # Connection might already be closed
        logger.error("Database operation failed: %s", e)
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass  # Connection might already be closed


def test_connection(dsn: Optional[str] = None) -> tuple[bool, Optional[str]]:
    """
    Test database connection without raising exceptions.
    
    Args:
        dsn: PostgreSQL connection string. If None, uses config from environment.
        
    Returns:
        Tuple of (success, error_message)
    """
    try:
        with get_connection_context(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()
                logger.debug("PostgreSQL connection successful: %s", version[0] if version else "Unknown version")
        return True, None
    except Exception as e:
        error_msg = f"PostgreSQL connection failed: {e}"
        logger.warning(error_msg)
        return False, error_msg


def ensure_database_ready(config: Optional[DatabaseConfig] = None) -> bool:
    """
    Ensure database is ready for operations.
    
    This function validates configuration and tests connectivity.
    
    Args:
        config: Database configuration. If None, uses global config.
        
    Returns:
        True if database is ready, False otherwise
    """
    if config is None:
        from .config import db_config
        config = db_config
    
    if not config.is_enabled:
        logger.debug("Database integration is disabled")
        return False
    
    is_valid, error = config.validate()
    if not is_valid:
        logger.warning("Database configuration invalid: %s", error)
        return False
    
    is_connected, error = test_connection(config.postgres_dsn)
    if not is_connected:
        logger.warning("Database connection test failed: %s", error)
        return False
    
    return True