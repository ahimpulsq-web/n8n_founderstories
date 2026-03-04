"""
Google Maps source adapter for MasterV2.

Fetches normalized candidates from gmaps_results table.
Sorting is handled at the sheets export level.
"""

from __future__ import annotations
import logging
from typing import Any

import psycopg

from ..models import LeadCandidate

logger = logging.getLogger(__name__)


def fetch_candidates(job_id: str, conn: psycopg.Connection[Any], sheet_id: str | None = None) -> list[LeadCandidate]:
    """
    Fetch lead candidates from gmaps_results for a given job_id.
    
    Returns normalized candidates without sorting.
    Sorting is applied at the sheets export level to match Google Maps sheet ordering.
    
    Args:
        job_id: Job identifier to filter results
        conn: Active psycopg connection
        sheet_id: Google Sheets ID for tracking (optional)
        
    Returns:
        List of LeadCandidate instances
    """
    candidates = []
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT domain, organization, request_id
            FROM gmaps_results
            WHERE job_id = %s
              AND domain IS NOT NULL
              AND domain != ''
        """, (job_id,))
        
        for domain, organization, request_id in cur.fetchall():
            # Skip if domain is empty after strip
            if not domain or not domain.strip():
                continue
            
            try:
                candidate = LeadCandidate(
                    domain=domain.strip().lower(),
                    organization=organization.strip() if organization else None,
                    request_id=request_id,
                    job_id=job_id,
                    sheet_id=sheet_id
                )
                candidates.append(candidate)
            except ValueError as e:
                logger.warning(
                    "MASTER | source=google_maps | job_id=%s | skip_invalid_domain=%s | error=%s",
                    job_id,
                    domain,
                    str(e)
                )
                continue
    
    logger.info(
        "MASTER | source=google_maps | job_id=%s | fetched=%d candidates",
        job_id,
        len(candidates)
    )
    
    return candidates