from __future__ import annotations

from dataclasses import asdict
from typing import Optional
from uuid import UUID

from ..database.connection import get_connection_context
from ...core.config import settings

from .models import CompanyEnrichmentResultCreate, CompanyEnrichmentResultRow


class CompanyEnrichmentResultsRepository:
    """
    PostgreSQL repository for company_enrichment_results.

    Idempotent upserts keyed by (request_id, master_result_id).
    """

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = dsn or settings.postgres_dsn  # adapt if your settings name differs

    def upsert(self, payload: CompanyEnrichmentResultCreate) -> CompanyEnrichmentResultRow:
        sql = """
        INSERT INTO company_enrichment_results (
            request_id,
            master_result_id,
            organization,
            domain,
            source,
            emails,
            contacts,
            extraction_status,
            debug_message
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (request_id, master_result_id)
        DO UPDATE SET
            updated_at = NOW(),
            organization = EXCLUDED.organization,
            domain = EXCLUDED.domain,
            source = EXCLUDED.source,
            emails = EXCLUDED.emails,
            contacts = EXCLUDED.contacts,
            extraction_status = EXCLUDED.extraction_status,
            debug_message = EXCLUDED.debug_message
        RETURNING
            id, created_at, updated_at,
            request_id, master_result_id,
            organization, domain, source,
            emails, contacts, extraction_status, debug_message;
        """

        with get_connection_context(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        payload.request_id,
                        payload.master_result_id,
                        payload.organization,
                        payload.domain,
                        payload.source,
                        payload.emails,
                        payload.contacts,
                        payload.extraction_status,
                        payload.debug_message,
                    ),
                )
                row = cur.fetchone()
                conn.commit()

        return CompanyEnrichmentResultRow(
            id=row[0],
            created_at=row[1],
            updated_at=row[2],
            request_id=row[3],
            master_result_id=row[4],
            organization=row[5],
            domain=row[6],
            source=row[7],
            emails=row[8],
            contacts=row[9],
            extraction_status=row[10],
            debug_message=row[11],
        )

    def get_by_request_and_master(self, request_id: str, master_result_id: UUID) -> Optional[CompanyEnrichmentResultRow]:
        sql = """
        SELECT
            id, created_at, updated_at,
            request_id, master_result_id,
            organization, domain, source,
            emails, contacts, extraction_status, debug_message
        FROM company_enrichment_results
        WHERE request_id = %s AND master_result_id = %s
        LIMIT 1;
        """

        with get_connection_context(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (request_id, master_result_id))
                row = cur.fetchone()

        if not row:
            return None

        return CompanyEnrichmentResultRow(
            id=row[0],
            created_at=row[1],
            updated_at=row[2],
            request_id=row[3],
            master_result_id=row[4],
            organization=row[5],
            domain=row[6],
            source=row[7],
            emails=row[8],
            contacts=row[9],
            extraction_status=row[10],
            debug_message=row[11],
        )
    
    def list_candidates_for_email_extraction(
        self,
        *,
        request_id: str,
        limit: int = 200,
    ) -> list[dict]:
        """
        Read-only: returns candidate rows for email extraction.
        We only return what Step 2 needs: request_id, master_result_id, domain.
        
        Note: This query always selects the first N remaining candidates.
        After processing a batch, those rows will have emails populated and
        will no longer match the filter, so the next call will naturally
        select the next batch of unprocessed rows.
        
        IMPORTANT: Do not use OFFSET pagination - it causes skipping when
        the candidate set shrinks as rows are updated.
        """
        sql = """
        SELECT request_id, master_result_id, domain
        FROM company_enrichment_results
        WHERE request_id = %s
        AND domain IS NOT NULL AND domain <> ''
        AND (emails IS NULL OR emails = '')
        AND (extraction_status IS NULL OR extraction_status = '')
        ORDER BY master_result_id ASC
        LIMIT %s;
        """

        with get_connection_context(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (request_id, limit))
                rows = cur.fetchall()

        return [{"request_id": r[0], "master_result_id": r[1], "domain": r[2]} for r in rows]
    
    def update_email_extraction_result(
        self,
        *,
        request_id: str,
        master_result_id: UUID,
        emails: Optional[str],
        contacts: Optional[str],
        extraction_status: Optional[str],
        debug_message: Optional[str],
    ) -> None:
        """
        Update enrichment results with extracted emails and contact names.
        
        This method updates both emails and contacts (names with roles) in a single
        transaction, ensuring data consistency.
        
        Args:
            request_id: Request identifier
            master_result_id: Master result UUID
            emails: Formatted emails string: (email: url),(email2: url2)
            contacts: Contact names with roles: Name1 (Role1); Name2 (Role2)
            extraction_status: Status of extraction (ok, no_emails_found, error, etc.)
            debug_message: Debug information for troubleshooting
        """
        sql = """
        UPDATE company_enrichment_results
        SET
            updated_at = NOW(),
            emails = %s,
            contacts = %s,
            extraction_status = %s,
            debug_message = %s
        WHERE request_id = %s
        AND master_result_id = %s
        AND domain IS NOT NULL AND domain <> ''
        AND (emails IS NULL OR emails = '')
        AND (extraction_status IS NULL OR extraction_status = '');
        """
        with get_connection_context(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (emails, contacts, extraction_status, debug_message, request_id, master_result_id))
                conn.commit()

    def get_all_by_request(self, request_id: str) -> list[CompanyEnrichmentResultRow]:
        """
        Fetch all enrichment results for a given request_id.
        
        Args:
            request_id: The request identifier
            
        Returns:
            List of CompanyEnrichmentResultRow objects
        """
        sql = """
        SELECT
            id, created_at, updated_at,
            request_id, master_result_id,
            organization, domain, source,
            emails, contacts, extraction_status, debug_message
        FROM company_enrichment_results
        WHERE request_id = %s
        ORDER BY created_at ASC;
        """
        
        with get_connection_context(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (request_id,))
                rows = cur.fetchall()
        
        results = []
        for row in rows:
            results.append(CompanyEnrichmentResultRow(
                id=row[0],
                created_at=row[1],
                updated_at=row[2],
                request_id=row[3],
                master_result_id=row[4],
                organization=row[5],
                domain=row[6],
                source=row[7],
                emails=row[8],
                contacts=row[9],
                extraction_status=row[10],
                debug_message=row[11],
            ))
        
        return results


