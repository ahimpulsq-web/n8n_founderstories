from __future__ import annotations

import logging
from typing import Optional, List
from uuid import UUID

from ..jobs.logging import job_logger
from ..jobs.sheets_status import ToolStatusWriter
from ..master_data.repos import MasterResultsRepository

from .models import CompanyEnrichmentResultCreate
from .repos import CompanyEnrichmentResultsRepository

import asyncio

from ..web_scrapers.company_data_extractor.async_extractor import (
    extract_emails_for_domain_async,
    format_emails_best_first,
    AsyncExtractionConfig,
)

from ...core.utils.async_net import AsyncFetchConfig, AsyncFetcher
from ...core.utils.email import pick_best_email


logger = logging.getLogger(__name__)


def _step1_clone_from_master(
    *,
    repo: CompanyEnrichmentResultsRepository,
    rid: str,
    master_rows: list[dict],
    log,
) -> int:
    """
    Step 1: Clone master-owned fields into company_enrichment_results.

    Writes (idempotent upsert):
      - request_id, master_result_id, organization, domain, source

    Leaves extractor-owned fields empty:
      - emails, contacts, extraction_status, debug_message

    Returns:
      Count of rows upserted (eligible rows with a domain + id).
    """
    written = 0
    skipped = 0

    for r in master_rows:
        master_result_id = r.get("id")
        domain = (r.get("domain") or "").strip()

        if not master_result_id or not domain:
            skipped += 1
            continue

        payload = CompanyEnrichmentResultCreate(
            request_id=rid,
            master_result_id=master_result_id,
            organization=(r.get("company") or r.get("organization") or "").strip() or None,
            domain=domain or None,
            source=(r.get("source_tool") or r.get("source") or "").strip() or None,
            emails=None,
            contacts=None,
            extraction_status=None,
            debug_message=None,
        )

        repo.upsert(payload)
        written += 1

    log.info(
        "ENRICHMENT | STEP1_CLONE_DONE | request_id=%s | written=%d | skipped=%d | total_master=%d",
        rid,
        written,
        skipped,
        len(master_rows),
    )
    return written


def _step2_select_rows_to_extract(
    *,
    repo: CompanyEnrichmentResultsRepository,
    rid: str,
    log,
) -> list[dict]:
    """
    Step 2 (pre-extraction): Read from company_enrichment_results (our own table)
    and select rows that need extraction.

    NOTE: This is a stub selector until we implement the actual extraction.
    For now, we select rows with:
      - domain present
      - emails is NULL/empty (not yet extracted)
    """
    # We do not yet have a "list by request" method on the repo; the simplest way
    # to keep things consistent is to add it in the next step.
    #
    # For now, return an empty list and log intent.
    log.info("ENRICHMENT | STEP2_SELECT_SKIPPED | request_id=%s | reason=list_method_not_implemented", rid)
    return []


def run_company_enrichment_for_request(
    *,
    request_id: str,
    job_id: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
) -> None:
    """
    Triggered by Master after Master ingestion is complete.

    Required workflow:
      1) Clone master_results -> company_enrichment_results (idempotent)
      2) Read company_enrichment_results -> perform extraction (emails, later contacts, etc.)
         and update the same rows (idempotent)
      3) After each batch, sync enrichment data to Sheets (incremental updates)
    
    Args:
        request_id: Request identifier
        job_id: Optional job identifier for logging
        spreadsheet_id: Optional spreadsheet ID for Sheets sync
    """
    rid = (request_id or "").strip()
    log = job_logger(__name__, tool="company_enrichment", request_id=rid, job_id=job_id or "company_enrichment")

    # Load master rows (source of truth) for cloning step
    master_repo = MasterResultsRepository()
    ok, err, master_rows = master_repo.get_results_by_request(request_id=rid)
    if not ok:
        raise RuntimeError(f"Failed to load master_results for request_id={rid}: {err}")

    if not master_rows:
        log.info("ENRICHMENT | NO_MASTER_ROWS | request_id=%s", rid)
        return

    repo = CompanyEnrichmentResultsRepository()

    # Initialize Sheets client and ToolStatusWriter (if spreadsheet_id provided)
    sheets_client = None
    status: Optional[ToolStatusWriter] = None
    
    if spreadsheet_id:
        try:
            from ..exports.sheets import SheetsClient, default_sheets_config
            sheets_client = SheetsClient(config=default_sheets_config(spreadsheet_id=spreadsheet_id))
            
            # Initialize ToolStatusWriter for Tool_Status sheet updates
            status = ToolStatusWriter(sheets=sheets_client, spreadsheet_id=spreadsheet_id)
            status.ensure_ready()
            
            log.info("ENRICHMENT | SHEETS_CLIENT_INITIALIZED | request_id=%s", rid)
        except Exception as e:
            log.warning("ENRICHMENT | SHEETS_CLIENT_INIT_FAILED | error=%s", e)
            sheets_client = None
            status = None

    # Write initial status to Tool_Status sheet
    if status and job_id:
        try:
            status.write(
                job_id=job_id,
                tool="company_enrichment",
                request_id=rid,
                state="RUNNING",
                phase="enrichment_clone",
                current=0,
                total=len(master_rows),
                message=f"Starting enrichment for {len(master_rows)} companies",
                meta={"master_rows": len(master_rows)},
                force=True,
            )
        except Exception as e:
            log.warning("ENRICHMENT | TOOL_STATUS_WRITE_FAILED | error=%s", e)

    # Step 1: Clone (always runs; safe/idempotent)
    _step1_clone_from_master(repo=repo, rid=rid, master_rows=master_rows, log=log)

    # Step 2.1: Read domains from enrichment table in batches (inputs for extractor)
    batch_size = 10
    total_loaded = 0

    while True:
        # Always select first N remaining candidates (no offset)
        # After processing, those rows will have emails and won't match filter anymore
        batch = repo.list_candidates_for_email_extraction(
            request_id=rid,
            limit=batch_size,
        )
        if not batch:
            break

        domains = [b["domain"] for b in batch]
        total_loaded += len(domains)

        log.info(
            "ENRICHMENT | STEP2_1_BATCH_LOADED | request_id=%s | batch=%d | total=%d",
            rid,
            len(domains),
            total_loaded,
        )
        
        # Update progress in Tool_Status
        if status and job_id:
            try:
                status.write(
                    job_id=job_id,
                    tool="company_enrichment",
                    request_id=rid,
                    state="RUNNING",
                    phase="enrichment_extraction",
                    current=total_loaded,
                    total=len(master_rows),
                    message=f"Processing batch {total_loaded // batch_size + 1}: {len(domains)} domains",
                    meta={"total_processed": total_loaded, "batch_size": len(domains)},
                )
            except Exception as e:
                log.warning("ENRICHMENT | TOOL_STATUS_UPDATE_FAILED | error=%s", e)
        
        # Step 2.2: Email extraction
        fetcher = AsyncFetcher(AsyncFetchConfig())
        cfg = AsyncExtractionConfig()
        
        # Track master_result_ids affected in this batch for Sheets sync
        batch_master_result_ids: List[UUID] = []

        for b in batch:
            mrid = b["master_result_id"]
            domain = (b.get("domain") or "").strip()

            log.info(
                "ENRICHMENT | STEP2_2_EXTRACT_START | request_id=%s | master_result_id=%s | domain=%s",
                rid,
                mrid,
                domain,
            )

            try:
                (
                    _emails_unique,
                    _best_email,
                    _best_source_url,
                    emails_with_sources,
                    contact_name,
                    _company_desc,
                    reason,
                    debug,
                ) = asyncio.run(
                    extract_emails_for_domain_async(domain, fetcher=fetcher, cfg=cfg)
                )

                emails_str = format_emails_best_first(domain=domain, pairs=emails_with_sources or [])
                
                # Format contacts: contact_name already comes in format "Name1 (Role1); Name2 (Role2)"
                # from extract_contact_name function in imprint_extractor.py
                contacts_str = contact_name or None

                repo.update_email_extraction_result(
                    request_id=rid,
                    master_result_id=mrid,
                    emails=emails_str,
                    contacts=contacts_str,
                    extraction_status=reason,
                    debug_message=debug,
                )
                
                # Track this master_result_id for Sheets sync
                batch_master_result_ids.append(mrid)

            except Exception as e:
                repo.update_email_extraction_result(
                    request_id=rid,
                    master_result_id=mrid,
                    emails=None,  # keep retryable
                    contacts=None,
                    extraction_status="error",
                    debug_message=str(e)[:500],
                )
                
                # Track this master_result_id for Sheets sync (even on error)
                batch_master_result_ids.append(mrid)

        # Step 2.3: Sync enrichment data to Sheets after batch completion
        if sheets_client and batch_master_result_ids:
            try:
                from ..exports.enrichment_sheets_sync import sync_enrichment_to_sheets
                
                log.info(
                    "ENRICHMENT | SHEETS_SYNC_START | request_id=%s | batch_size=%d",
                    rid,
                    len(batch_master_result_ids)
                )
                
                sync_enrichment_to_sheets(
                    client=sheets_client,
                    request_id=rid,
                    master_result_ids=batch_master_result_ids,
                )
                
                log.info(
                    "ENRICHMENT | SHEETS_SYNC_COMPLETE | request_id=%s | batch_size=%d",
                    rid,
                    len(batch_master_result_ids)
                )
            except Exception as e:
                # Log error but do not fail enrichment - Sheets sync is non-blocking
                log.error(
                    "ENRICHMENT | SHEETS_SYNC_FAILED | request_id=%s | error=%s",
                    rid,
                    e,
                    exc_info=True
                )

    log.info("ENRICHMENT | STEP2_1_COMPLETE | request_id=%s | total_domains=%d", rid, total_loaded)
    
    # Write final success status to Tool_Status sheet
    if status and job_id:
        try:
            status.write(
                job_id=job_id,
                tool="company_enrichment",
                request_id=rid,
                state="SUCCEEDED",
                phase="enrichment_complete",
                current=total_loaded,
                total=total_loaded,
                message=f"Enrichment completed: processed {total_loaded} domains",
                meta={"total_processed": total_loaded},
                force=True,
            )
        except Exception as e:
            log.warning("ENRICHMENT | TOOL_STATUS_FINAL_WRITE_FAILED | error=%s", e)
    
