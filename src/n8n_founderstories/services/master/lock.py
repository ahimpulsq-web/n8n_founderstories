"""
PostgreSQL advisory lock for master consolidation.

Ensures only one master sync runs at a time to prevent race conditions
when multiple jobs finish simultaneously.
"""

from __future__ import annotations
from contextlib import contextmanager
from typing import Any, Generator
import logging

import psycopg

logger = logging.getLogger(__name__)

# Stable lock key for masterv2 (hash of "masterv2_master_leads" truncated to int64)
# Using a simple constant for clarity
MASTER_LOCK_KEY = 987654321


@contextmanager
def advisory_lock(conn: psycopg.Connection[Any]) -> Generator[None, None, None]:
    """
    Acquire PostgreSQL advisory lock for master sync.
    
    Uses pg_advisory_lock/unlock to serialize master merges.
    This prevents race conditions when hunter and maps jobs finish simultaneously.
    
    Args:
        conn: Active psycopg connection
        
    Yields:
        None (lock is held during context)
        
    Example:
        with advisory_lock(conn):
            # Only one process can be here at a time
            upsert_candidates(...)
    """
    try:
        with conn.cursor() as cur:
            logger.debug("Acquiring advisory lock %d", MASTER_LOCK_KEY)
            cur.execute("SELECT pg_advisory_lock(%s)", (MASTER_LOCK_KEY,))
            logger.debug("Advisory lock %d acquired", MASTER_LOCK_KEY)
        
        yield
        
    finally:
        with conn.cursor() as cur:
            logger.debug("Releasing advisory lock %d", MASTER_LOCK_KEY)
            cur.execute("SELECT pg_advisory_unlock(%s)", (MASTER_LOCK_KEY,))
            logger.debug("Advisory lock %d released", MASTER_LOCK_KEY)