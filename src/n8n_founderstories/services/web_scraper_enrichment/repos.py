from __future__ import annotations

import json
from typing import Optional
from uuid import UUID

from ..database.connection import get_connection_context
from ...core.config import settings
from .models import WebScraperEnrichmentResultCreate, WebScraperEnrichmentResultRow


def _json_dumps(v) -> str:
    return json.dumps(v or [], ensure_ascii=False)


class WebScraperEnrichmentResultsRepository:
    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = dsn or settings.postgres_dsn

    # ---------------------------
    # CLONE
    # ---------------------------
    def upsert(self, payload: WebScraperEnrichmentResultCreate) -> WebScraperEnrichmentResultRow:
        """
        Upsert a web scraper enrichment result.
        On conflict, resets all pipeline state to ensure idempotent reprocessing.
        """
        sql = """
        INSERT INTO web_scraper_enrichment_results (
            request_id, master_result_id, organization, domain, source,
            extraction_status, debug_message
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (request_id, master_result_id)
        DO UPDATE SET
            updated_at = NOW(),
            -- Reset pipeline state for idempotent reprocessing
            extraction_status = 'pending',
            contact_case = NULL,
            about_case = NULL,
            crawl_homepage = NULL,
            crawl_pages = NULL,
            det_status = NULL,
            det_emails = NULL,
            det_error = NULL,
            llm_status = NULL,
            llm_company = NULL,
            llm_emails = NULL,
            llm_contacts = NULL,
            llm_about = NULL,
            llm_error = NULL,
            debug_message = NULL,
            contact_links = NULL,
            about_links = NULL,
            -- Reset combine fields to prevent stale data
            combine_status = NULL,
            combined_emails = NULL,
            combined_company = NULL,
            combined_descriptions = NULL,
            combined_people = NULL,
            combine_debug = NULL
        RETURNING *;
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
                        payload.extraction_status,
                        payload.debug_message,
                    ),
                )
                row = cur.fetchone()
                conn.commit()

                return WebScraperEnrichmentResultRow(
                    id=row[0],
                    created_at=row[1],
                    updated_at=row[2],
                    request_id=row[3],
                    master_result_id=row[4],
                    organization=row[5],
                    domain=row[6],
                    source=row[7],
                    contact_links=row[8],
                    contact_case=row[9],
                    about_links=row[10],
                    about_case=row[11],
                    extraction_status=row[12],
                    debug_message=row[13],
                    crawl_homepage=row[14],
                    crawl_pages=row[15],
                    det_status=row[16],
                    det_emails=row[17],
                    det_error=row[18],
                    llm_status=row[19],
                    llm_company=row[20],
                    llm_emails=row[21],
                    llm_contacts=row[22],
                    llm_about=row[23],
                    llm_error=row[24],
                )


    # ---------------------------
    # CRAWL
    # ---------------------------
    def claim_candidates_for_crawl(self, request_id: str, limit: int) -> list[dict]:
        print(f"[REPO] claim_candidates_for_crawl: request_id={request_id}, limit={limit}", flush=True)
        sql = """
        WITH c AS (
            SELECT id, master_result_id, domain
            FROM web_scraper_enrichment_results
            WHERE request_id = %s
              AND domain IS NOT NULL
              AND extraction_status = 'pending'
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE web_scraper_enrichment_results t
        SET extraction_status='crawl_running'
        FROM c
        WHERE t.id=c.id
        RETURNING t.id, t.master_result_id, t.domain;
        """
        print(f"[REPO] Getting DB connection...", flush=True)
        with get_connection_context(self._dsn) as conn:
            print(f"[REPO] Executing query...", flush=True)
            with conn.cursor() as cur:
                cur.execute(sql, (request_id, limit))
                rows = cur.fetchall()
                print(f"[REPO] Fetched {len(rows)} rows, committing...", flush=True)
                conn.commit()
                print(f"[REPO] Committed successfully", flush=True)
        result = [{"id": r[0], "master_result_id": r[1], "domain": r[2]} for r in rows]
        print(f"[REPO] Returning {len(result)} candidates", flush=True)
        return result

    def update_crawl_results_batch(self, *, request_id: str, updates: list[dict]) -> None:
        sql = """
        UPDATE web_scraper_enrichment_results
        SET contact_links=%s, contact_case=%s,
            about_links=%s, about_case=%s,
            crawl_homepage=%s, crawl_pages=%s,
            extraction_status=%s, debug_message=%s
        WHERE request_id=%s AND master_result_id=%s;
        """
        with get_connection_context(self._dsn) as conn:
            with conn.cursor() as cur:
                for u in updates:
                    cur.execute(
                        sql,
                        (
                            _json_dumps(u["contact_links"]),
                            u["contact_case"],
                            _json_dumps(u["about_links"]),
                            u["about_case"],
                            u.get("crawl_homepage"),  # Already JSON string or None
                            u.get("crawl_pages"),     # Already JSON string or None
                            u["status"],
                            u["debug_message"],
                            request_id,
                            u["master_result_id"],
                        ),
                    )
                conn.commit()

    # ---------------------------
    # DETERMINISTIC
    # ---------------------------
    def claim_candidates_for_det(self, request_id: str, limit: int) -> list[dict]:
        sql = """
        WITH c AS (
            SELECT id, master_result_id, domain, crawl_homepage, crawl_pages
            FROM web_scraper_enrichment_results
            WHERE request_id=%s
              AND extraction_status = 'crawl_ok'
              AND det_status IS NULL
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE web_scraper_enrichment_results t
        SET det_status='running'
        FROM c
        WHERE t.id=c.id
        RETURNING t.id, t.master_result_id, t.domain, t.crawl_homepage, t.crawl_pages;
        """
        with get_connection_context(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (request_id, limit))
                rows = cur.fetchall()
                conn.commit()
        return [
            {
                "id": r[0],
                "master_result_id": r[1],
                "domain": r[2],
                "crawl_homepage": r[3],  # JSONB or None
                "crawl_pages": r[4],     # JSONB or None
            }
            for r in rows
        ]

    def update_det_results_batch(self, *, request_id: str, updates: list[dict]) -> None:
        sql = """
        UPDATE web_scraper_enrichment_results
        SET det_emails=%s, det_status=%s, det_error=%s
        WHERE request_id=%s AND master_result_id=%s;
        """
        with get_connection_context(self._dsn) as conn:
            with conn.cursor() as cur:
                for u in updates:
                    cur.execute(
                        sql,
                        (
                            _json_dumps(u["det_emails"]),
                            u["status"],
                            u.get("error"),
                            request_id,
                            u["master_result_id"],
                        ),
                    )
                conn.commit()

    # ---------------------------
    # LLM
    # ---------------------------
    def claim_candidates_for_llm(self, request_id: str, limit: int) -> list[dict]:
        sql = """
        WITH c AS (
            SELECT id, master_result_id, domain, crawl_homepage, crawl_pages,
                   contact_links, contact_case, about_links, about_case
            FROM web_scraper_enrichment_results
            WHERE request_id=%s
              AND extraction_status = 'crawl_ok'
              AND llm_status IS NULL
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE web_scraper_enrichment_results t
        SET llm_status='running'
        FROM c
        WHERE t.id=c.id
        RETURNING t.id, t.master_result_id, t.domain, t.crawl_homepage, t.crawl_pages,
                  t.contact_links, t.contact_case, t.about_links, t.about_case;
        """
        with get_connection_context(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (request_id, limit))
                rows = cur.fetchall()
                conn.commit()
        return [
            {
                "id": r[0],
                "master_result_id": r[1],
                "domain": r[2],
                "crawl_homepage": r[3],
                "crawl_pages": r[4],
                "contact_links": r[5],
                "contact_case": r[6],
                "about_links": r[7],
                "about_case": r[8],
            }
            for r in rows
        ]

    def update_llm_results_batch(self, *, request_id: str, updates: list[dict]) -> None:
        sql = """
        UPDATE web_scraper_enrichment_results
        SET llm_company=%s, llm_emails=%s, llm_contacts=%s, llm_about=%s,
            llm_status=%s, llm_error=%s
        WHERE request_id=%s AND master_result_id=%s;
        """
        with get_connection_context(self._dsn) as conn:
            with conn.cursor() as cur:
                for u in updates:
                    cur.execute(
                        sql,
                        (
                            u.get("llm_company"),
                            u.get("llm_emails"),
                            u.get("llm_contacts"),
                            u.get("llm_about"),
                            u["status"],
                            u.get("error"),
                            request_id,
                            u["master_result_id"],
                        ),
                    )
                conn.commit()

    # ---------------------------
    # COMBINE
    # ---------------------------
    def claim_candidates_for_combine(self, request_id: str, limit: int) -> list[dict]:
        """
        Claim rows eligible for COMBINE stage.
        
        Eligibility:
        - extraction_status = 'crawl_ok'
        - det_status = 'ok' (must be successful, not just completed)
        - llm_status = 'ok' (must be successful, not just completed)
        - combine_status IS NULL
        """
        sql = """
        WITH c AS (
            SELECT id, master_result_id, domain,
                   det_emails, llm_company, llm_emails, llm_contacts, llm_about
            FROM web_scraper_enrichment_results
            WHERE request_id=%s
              AND extraction_status = 'crawl_ok'
              AND det_status = 'ok'
              AND llm_status = 'ok'
              AND combine_status IS NULL
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE web_scraper_enrichment_results t
        SET combine_status='running'
        FROM c
        WHERE t.id=c.id
        RETURNING t.id, t.master_result_id, t.domain,
                  t.det_emails, t.llm_company, t.llm_emails, t.llm_contacts, t.llm_about;
        """
        with get_connection_context(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (request_id, limit))
                rows = cur.fetchall()
                conn.commit()
        return [
            {
                "id": r[0],
                "master_result_id": r[1],
                "domain": r[2],
                "det_emails": r[3],
                "llm_company": r[4],
                "llm_emails": r[5],
                "llm_contacts": r[6],
                "llm_about": r[7],
            }
            for r in rows
        ]

    def update_combine_results_batch(self, *, request_id: str, updates: list[dict]) -> None:
        """
        Batch update COMBINE results.
        
        Updates:
        - combined_emails (JSON)
        - combined_company (JSON)
        - combined_descriptions (JSON)
        - combined_people (JSON)
        - combine_status (ok | partial | error)
        - combine_debug (error message or NULL)
        """
        sql = """
        UPDATE web_scraper_enrichment_results
        SET combined_emails=%s, combined_company=%s, combined_descriptions=%s, combined_people=%s,
            combine_status=%s, combine_debug=%s
        WHERE request_id=%s AND master_result_id=%s;
        """
        with get_connection_context(self._dsn) as conn:
            with conn.cursor() as cur:
                for u in updates:
                    cur.execute(
                        sql,
                        (
                            u.get("combined_emails"),
                            u.get("combined_company"),
                            u.get("combined_descriptions"),
                            u.get("combined_people"),
                            u["status"],
                            u.get("debug"),
                            request_id,
                            u["master_result_id"],
                        ),
                    )
                conn.commit()
