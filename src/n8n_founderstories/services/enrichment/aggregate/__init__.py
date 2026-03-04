"""
═══════════════════════════════════════════════════════════════════════════════
AGGREGATE MODULE - Domain-Level Result Aggregation
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [WORKER] - Background daemon for result aggregation

PURPOSE:
    Aggregates page-level LLM extraction results into domain-level enrichment
    results. Monitors extraction_status and creates consolidated records per domain.

ARCHITECTURE:
    - Worker runs in daemon thread (started at app startup)
    - Polls for domains with extraction_status = 'succeeded' or 'reused'
    - Aggregates all llm_ext_results for each domain
    - Normalizes and deduplicates emails and contacts
    - Stores results in enrichment_results table

INTEGRATION:
    Started in main.py startup event alongside crawler and LLM workers

═══════════════════════════════════════════════════════════════════════════════
"""

from .repository import ensure_table, upsert_enrichment_result
from .worker import run_worker

__all__ = [
    "ensure_table",
    "upsert_enrichment_result",
    "run_worker",
]