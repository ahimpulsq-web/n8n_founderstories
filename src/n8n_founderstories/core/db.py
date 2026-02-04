"""
Simple PostgreSQL connection helper for HunterIOV2 persistence.
Uses psycopg3 with minimal configuration.
"""
from __future__ import annotations

import os
import logging
from typing import Any

import psycopg

logger = logging.getLogger(__name__)


def get_conn() -> psycopg.Connection[Any]:
    """
    Get a PostgreSQL connection using environment variables.
    
    Priority:
    1. POSTGRES_DSN if present
    2. Build DSN from POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE,
       POSTGRES_USERNAME, POSTGRES_PASSWORD
    
    Returns:
        psycopg.Connection with autocommit enabled
    
    Raises:
        ValueError: If required environment variables are missing
        psycopg.Error: If connection fails
    """
    dsn = os.getenv("POSTGRES_DSN")
    
    if not dsn:
        # Build DSN from individual components
        host = os.getenv("POSTGRES_HOST")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DATABASE")
        username = os.getenv("POSTGRES_USERNAME")
        password = os.getenv("POSTGRES_PASSWORD")
        
        if not all([host, database, username, password]):
            raise ValueError(
                "Missing required Postgres environment variables. "
                "Provide either POSTGRES_DSN or all of: "
                "POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD"
            )
        
        dsn = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    
    conn = psycopg.connect(dsn, autocommit=True)
    logger.debug("PostgreSQL connection established")
    return conn