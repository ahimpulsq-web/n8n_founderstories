"""
═══════════════════════════════════════════════════════════════════════════════
AGGREGATE REPOSITORY - Database Persistence for Enrichment Results
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [INFRASTRUCTURE] - Database access layer for enrichment results

PURPOSE:
    Manages the enrichment_results table which stores domain-level aggregated
    data from llm_ext_results. One row per domain per request.

TABLE SCHEMA (enrichment_results):
    ┌─────────────────┬──────────┬─────────────────────────────────────────┐
    │ Column          │ Type     │ Description                             │
    ├─────────────────┼──────────┼─────────────────────────────────────────┤
    │ id              │ UUID     │ Primary key (auto-generated)            │
    │ request_id      │ TEXT     │ Request identifier                      │
    │ job_id          │ TEXT     │ Job identifier                          │
    │ sheet_id        │ TEXT     │ Google Sheet ID                         │
    │ organization    │ TEXT     │ Organization name                       │
    │ domain          │ TEXT     │ Normalized domain (lowercase)           │
    │ company_json    │ TEXT     │ Aggregated company data with evidence   │
    │ description_json│ TEXT     │ Aggregated description with evidence    │
    │ emails_json     │ TEXT     │ Deduplicated emails with evidence       │
    │ contacts_json   │ TEXT     │ Deduplicated contacts with evidence     │
    │ created_at      │ TIMESTAMP│ When record was created                 │
    │ updated_at      │ TIMESTAMP│ When record was last updated            │
    └─────────────────┴──────────┴─────────────────────────────────────────┘
    
    UNIQUE constraint: (request_id, domain) - ONE ROW PER DOMAIN PER REQUEST

DATA FORMAT:
    All JSON fields follow the aggregated evidence format:
    
    company_json:
    {
        "value": "Company Name",
        "evidence": [
            {"url": "https://...", "page_type": "impressum", "quote": "..."},
            {"url": "https://...", "page_type": "home", "quote": "..."}
        ]
    }
    
    emails_json:
    [
        {
            "email": "info@example.com",
            "evidence": [
                {"url": "https://...", "page_type": "impressum", "quote": "..."},
                {"url": "https://...", "page_type": "home", "quote": "..."}
            ]
        }
    ]
    
    contacts_json:
    [
        {
            "name": "John Doe",
            "role": "CEO",
            "evidence": [
                {"url": "https://...", "page_type": "impressum", "quote": "..."}
            ]
        }
    ]

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import psycopg

logger = logging.getLogger(__name__)


# =============================================================================
# TABLE MANAGEMENT
# =============================================================================

def ensure_table(conn: psycopg.Connection[Any]) -> None:
    """
    Create the enrichment_results table if it doesn't exist.
    
    This function is idempotent and safe to call on every run.
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
    """
    with conn.cursor() as cur:
        # Wrap table creation in exception handling to handle concurrent creation
        cur.execute("""
            DO $$
            BEGIN
                CREATE TABLE IF NOT EXISTS enrichment_results (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                request_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                sheet_id TEXT,
                organization TEXT,
                domain TEXT NOT NULL,
                company_json TEXT,
                description_json TEXT,
                emails_json TEXT,
                det_emails_json TEXT,
                contacts_json TEXT,
                company TEXT,
                email TEXT,
                emails TEXT,
                description TEXT,
                contacts TEXT,
                status TEXT NOT NULL DEFAULT 'succeeded',
                error TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                    UNIQUE(request_id, domain)
                );
            EXCEPTION
                WHEN duplicate_object THEN NULL;
                WHEN unique_violation THEN NULL;
            END $$;
        """)
        
        # Add det_emails_json column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'enrichment_results'
                    AND column_name = 'det_emails_json'
                ) THEN
                    ALTER TABLE enrichment_results ADD COLUMN det_emails_json TEXT;
                END IF;
            END $$;
        """)
        
        # Add company column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'enrichment_results'
                    AND column_name = 'company'
                ) THEN
                    ALTER TABLE enrichment_results ADD COLUMN company TEXT;
                END IF;
            END $$;
        """)
        
        # Add description column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'enrichment_results'
                    AND column_name = 'description'
                ) THEN
                    ALTER TABLE enrichment_results ADD COLUMN description TEXT;
                END IF;
            END $$;
        """)
        
        # Add emails column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'enrichment_results'
                    AND column_name = 'emails'
                ) THEN
                    ALTER TABLE enrichment_results ADD COLUMN emails TEXT;
                END IF;
            END $$;
        """)
        
        # Add email column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'enrichment_results'
                    AND column_name = 'email'
                ) THEN
                    ALTER TABLE enrichment_results ADD COLUMN email TEXT;
                END IF;
            END $$;
        """)
        
        # Add contacts column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'enrichment_results'
                    AND column_name = 'contacts'
                ) THEN
                    ALTER TABLE enrichment_results ADD COLUMN contacts TEXT;
                END IF;
            END $$;
        """)
        
        # Add status column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'enrichment_results'
                    AND column_name = 'status'
                ) THEN
                    ALTER TABLE enrichment_results ADD COLUMN status TEXT NOT NULL DEFAULT 'succeeded';
                END IF;
            END $$;
        """)
        
        # Add error column if it doesn't exist (for existing tables)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'enrichment_results'
                    AND column_name = 'error'
                ) THEN
                    ALTER TABLE enrichment_results ADD COLUMN error TEXT;
                END IF;
            END $$;
        """)
        
        # Create indexes for efficient queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_enrichment_results_request_id
            ON enrichment_results(request_id)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_enrichment_results_domain
            ON enrichment_results(domain)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_enrichment_results_job_id
            ON enrichment_results(job_id)
        """)
        
        conn.commit()
    
    logger.info("enrichment_results table ensured")


# =============================================================================
# DATA PERSISTENCE
# =============================================================================

def upsert_enrichment_result(
    conn: psycopg.Connection[Any],
    request_id: str,
    job_id: str,
    sheet_id: Optional[str],
    organization: Optional[str],
    domain: str,
    company_json: Optional[str],
    description_json: Optional[str],
    emails_json: Optional[str],
    det_emails_json: Optional[str],
    contacts_json: Optional[str],
    company: Optional[str],
    email: Optional[str],
    emails: Optional[str],
    description: Optional[str],
    contacts: Optional[str],
    status: str = 'succeeded',
    error: Optional[str] = None,
) -> None:
    """
    Insert or update enrichment result for a domain.
    
    This function uses UPSERT (INSERT ... ON CONFLICT DO UPDATE) to ensure
    idempotency. If the (request_id, domain) already exists, the row is updated.
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
        request_id: Request identifier
        job_id: Job identifier
        sheet_id: Google Sheet ID
        organization: Organization name
        domain: Normalized domain
        company_json: Aggregated company data as JSON string
        description_json: Aggregated description as JSON string
        emails_json: Deduplicated LLM emails as JSON string
        det_emails_json: Deduplicated deterministic emails as JSON string
        contacts_json: Deduplicated contacts as JSON string
        company: Final selected company (JSON with name, score, page_type)
        email: Final selected email (JSON with email, score, page_type)
        emails: All emails with scores (JSON array)
        description: Final selected description (TEXT)
        contacts: Final selected contacts (TEXT)
        status: Status of enrichment ('succeeded' or 'failed')
        error: Error message if status is 'failed'
    
    Notes:
        - Caller must commit the transaction
        - Safe to call multiple times with same (request_id, domain)
        - Last write wins for conflicts
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO enrichment_results (
                request_id,
                job_id,
                sheet_id,
                organization,
                domain,
                company_json,
                description_json,
                emails_json,
                det_emails_json,
                contacts_json,
                company,
                email,
                emails,
                description,
                contacts,
                status,
                error,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
            ON CONFLICT (request_id, domain)
            DO UPDATE SET
                job_id = EXCLUDED.job_id,
                sheet_id = EXCLUDED.sheet_id,
                organization = EXCLUDED.organization,
                company_json = EXCLUDED.company_json,
                description_json = EXCLUDED.description_json,
                emails_json = EXCLUDED.emails_json,
                det_emails_json = EXCLUDED.det_emails_json,
                contacts_json = EXCLUDED.contacts_json,
                company = EXCLUDED.company,
                email = EXCLUDED.email,
                emails = EXCLUDED.emails,
                description = EXCLUDED.description,
                contacts = EXCLUDED.contacts,
                status = EXCLUDED.status,
                error = EXCLUDED.error,
                updated_at = now()
        """, (
            request_id,
            job_id,
            sheet_id,
            organization,
            domain,
            company_json,
            description_json,
            emails_json,
            det_emails_json,
            contacts_json,
            company,
            email,
            emails,
            description,
            contacts,
            status,
            error,
        ))
    
    logger.debug(
        "ENRICHMENT_RESULT_UPSERTED request_id=%s domain=%s",
        request_id,
        domain,
    )


# =============================================================================
# WORKER QUERIES
# =============================================================================

def get_next_unaggregated_domain(conn: psycopg.Connection[Any]) -> Optional[dict[str, Any]]:
    """
    Get the next domain that needs aggregation.
    
    Finds domains where:
    - extraction_status = 'succeeded', 'reused', or 'failed'
    - enrichment_status IS NULL (not yet processed)
    
    Args:
        conn: Active psycopg connection
    
    Returns:
        Dictionary with domain data or None if no domains need aggregation:
        {
            "request_id": str,
            "job_id": str,
            "sheet_id": str,
            "organization": str,
            "domain": str,
            "extraction_status": str,
        }
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                request_id,
                job_id,
                sheet_id,
                organization,
                domain,
                extraction_status
            FROM mstr_results
            WHERE extraction_status IN ('succeeded', 'reused', 'failed')
              AND enrichment_status IS NULL
            ORDER BY domain ASC
            LIMIT 1
        """)
        
        row = cur.fetchone()
        if not row:
            return None
        
        return {
            "request_id": row[0],
            "job_id": row[1],
            "sheet_id": row[2],
            "organization": row[3],
            "domain": row[4],
            "extraction_status": row[5],
        }


def get_llm_results_for_domain(
    conn: psycopg.Connection[Any],
    request_id: str,
    domain: str,
) -> list[dict[str, Any]]:
    """
    Get all LLM extraction results for a domain.
    
    Args:
        conn: Active psycopg connection
        request_id: Request identifier
        domain: Domain to get results for
    
    Returns:
        List of dictionaries with page-level extraction results:
        [
            {
                "url": str,
                "page_type": str,
                "company_json": str,
                "description_json": str,
                "emails_json": str,
                "contacts_json": str,
            }
        ]
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                url,
                page_type,
                company_json,
                description_json,
                emails_json,
                contacts_json
            FROM llm_ext_results
            WHERE request_id = %s
              AND domain = %s
              AND status = 'succeeded'
            ORDER BY created_at ASC
        """, (request_id, domain))
        
        results = []
        for row in cur.fetchall():
            results.append({
                "url": row[0],
                "page_type": row[1],
                "company_json": row[2],
                "description_json": row[3],
                "emails_json": row[4],
                "contacts_json": row[5],
            })
        
        return results


def get_det_results_for_domain(
    conn: psycopg.Connection[Any],
    request_id: str,
    domain: str,
) -> list[dict[str, Any]]:
    """
    Get all deterministic extraction results for a domain.
    
    Args:
        conn: Active psycopg connection
        request_id: Request identifier
        domain: Domain to get results for
    
    Returns:
        List of dictionaries with page-level deterministic extraction results:
        [
            {
                "url": str,
                "page_type": str,
                "emails_json": str,  # JSON array like ["email1@x.com", "email2@x.com"]
            }
        ]
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                url,
                page_type,
                emails_json
            FROM det_ext_results
            WHERE request_id = %s
              AND domain = %s
            ORDER BY created_at ASC
        """, (request_id, domain))
        
        results = []
        for row in cur.fetchall():
            results.append({
                "url": row[0],
                "page_type": row[1],
                "emails_json": row[2],
            })
        
def get_extraction_error_for_domain(
    conn: psycopg.Connection[Any],
    request_id: str,
    domain: str,
) -> Optional[str]:
    """
    Get the error message from failed extraction for a domain.
    
    Args:
        conn: Active psycopg connection
        request_id: Request identifier
        domain: Domain to get error for
    
    Returns:
        Error message string or None if no error found
    """
    with conn.cursor() as cur:
        # Get first error from llm_ext_results
        cur.execute("""
            SELECT error
            FROM llm_ext_results
            WHERE request_id = %s
              AND domain = %s
              AND status = 'failed'
              AND error IS NOT NULL
            ORDER BY created_at ASC
            LIMIT 1
        """, (request_id, domain))
        
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
        
        return None
        return results