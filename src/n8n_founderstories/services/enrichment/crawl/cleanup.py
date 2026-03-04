"""
Cleanup utilities for crawler worker to prevent hanging issues.
"""

from __future__ import annotations
import logging
from typing import Any
from datetime import datetime, timedelta, timezone

import psycopg

logger = logging.getLogger(__name__)


def cleanup_stale_processing_status(
    conn: psycopg.Connection[Any],
    timeout_minutes: int = 5
) -> int:
    """
    Clean up domains stuck in 'processing' status for too long.
    
    This prevents the system from hanging when a worker crashes or gets stuck
    while processing a domain. Domains stuck in 'processing' for longer than
    the timeout will be reset to NULL so they can be retried.
    
    Args:
        conn: Active psycopg connection
        timeout_minutes: Minutes after which a 'processing' status is considered stale
        
    Returns:
        Number of domains reset
    """
    # First, add a timestamp column if it doesn't exist
    with conn.cursor() as cur:
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'mstr_results'
                    AND column_name = 'crawl_processing_started_at'
                ) THEN
                    ALTER TABLE mstr_results ADD COLUMN crawl_processing_started_at TIMESTAMP WITH TIME ZONE;
                END IF;
            END $$;
        """)
    conn.commit()
    
    # Reset domains that are in 'processing' but have no timestamp
    # These are legacy domains from before the timestamp column existed
    # We can't know when they started, so we reset them to be safe
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE mstr_results
            SET crawl_status = NULL,
                crawl_processing_started_at = NULL
            WHERE crawl_status = 'processing'
              AND crawl_processing_started_at IS NULL
            RETURNING domain, request_id
        """)
        legacy_domains = cur.fetchall()
        legacy_count = len(legacy_domains)
        
        if legacy_count > 0:
            logger.warning(
                "CRAWLER_CLEANUP | Reset %d legacy 'processing' domains (no timestamp)",
                legacy_count
            )
    conn.commit()
    
    # Reset stale processing statuses
    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)
    
    with conn.cursor() as cur:
        # Reset domains that have been processing for too long
        cur.execute("""
            UPDATE mstr_results
            SET crawl_status = NULL,
                crawl_processing_started_at = NULL
            WHERE crawl_status = 'processing'
              AND crawl_processing_started_at IS NOT NULL
              AND crawl_processing_started_at < %s
            RETURNING domain, request_id
        """, (cutoff_time,))
        
        reset_domains = cur.fetchall()
        reset_count = len(reset_domains)
    
    if reset_count > 0:
        logger.warning(
            "CRAWLER_CLEANUP | Reset %d stale 'processing' domains (timeout: %d minutes)",
            reset_count,
            timeout_minutes
        )
        for domain, request_id in reset_domains:
            logger.info(
                "CRAWLER_CLEANUP | RESET | domain=%s | request_id=%s",
                domain,
                request_id
            )
    
    conn.commit()
    return reset_count


def get_processing_stats(conn: psycopg.Connection[Any]) -> dict:
    """
    Get statistics about domains in various processing states.
    
    Args:
        conn: Active psycopg connection
        
    Returns:
        Dict with stats: {
            "null": int,  # Domains waiting to be processed
            "processing": int,  # Domains currently being processed
            "succeeded": int,  # Successfully crawled domains
            "failed": int,  # Failed domains
            "reused": int  # Reused domains
        }
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                crawl_status,
                COUNT(*) as count
            FROM mstr_results
            GROUP BY crawl_status
        """)
        
        stats = {
            "null": 0,
            "processing": 0,
            "succeeded": 0,
            "failed": 0,
            "reused": 0
        }
        
        for row in cur.fetchall():
            status = row[0]
            count = row[1]
            
            if status is None:
                stats["null"] = count
            elif status in stats:
                stats[status] = count
    
    return stats