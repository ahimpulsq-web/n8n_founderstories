from __future__ import annotations

import asyncio
import json
import os
from typing import Optional

# Fix Windows console encoding issue for Crawl4AI
# This must be set before importing crawl4ai
os.environ['PYTHONIOENCODING'] = 'utf-8'
if os.name == 'nt':  # Windows
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')

from ..jobs import JobsSheetWriter
from ..jobs.store import mark_failed, mark_running, mark_succeeded
from ..master_data.repos import MasterResultsRepository
from ..exports.sheets import SheetsClient, default_sheets_config

from .models import WebScraperEnrichmentResultCreate
from .repos import WebScraperEnrichmentResultsRepository

from ..web_scrapers.company_enrichment.crawl.crawl4ai_client import (
    Crawl4AIClient,
    Crawl4AIClientConfig,
)
from ..web_scrapers.company_enrichment.crawl.service import (
    DomainCrawlerService,
    DomainCrawlConfig,
)
from ..web_scrapers.company_enrichment.models import PageArtifact
from ..web_scrapers.company_enrichment.extract.deterministic.extract import extract as det_extract
from ..web_scrapers.company_enrichment.extract.llm.extract import extract as llm_extract
from ..web_scrapers.company_enrichment.extract.llm.router import OpenRouterLLMRouter, LLMRouterConfig
from ...core.config import settings

import logging

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

# Enable crawl4ai and web scraper event logging
logging.getLogger("n8n_founderstories.web_scrapers.events").setLevel(logging.INFO)
logging.getLogger("crawl4ai").setLevel(logging.INFO)



def _serialize_page_artifact(page: PageArtifact) -> dict:
    """Serialize PageArtifact to dict for JSON storage."""
    return {
        "url": str(page.url),
        "final_url": str(page.final_url) if page.final_url else None,
        "status_code": page.status_code,
        "cleaned_html": page.cleaned_html,
        "markdown": page.markdown,
        "title": page.title,
        "fetched_at_utc": page.fetched_at_utc,
        "error": page.error,
        "links": page.links,
        "meta": page.meta or {},
    }


def _deserialize_page_artifact(data: dict) -> PageArtifact:
    """Deserialize dict to PageArtifact."""
    return PageArtifact(
        url=data["url"],
        final_url=data.get("final_url"),
        status_code=data.get("status_code"),
        cleaned_html=data.get("cleaned_html", ""),
        markdown=data.get("markdown", ""),
        title=data.get("title"),
        fetched_at_utc=data.get("fetched_at_utc"),
        error=data.get("error"),
        links=data.get("links", []),
        meta=data.get("meta", {}),
    )


def _serialize_evidence(evidence) -> dict:
    """Serialize Evidence to dict."""
    if not evidence:
        return {}
    return {
        "url": str(evidence.url),
        "quote": evidence.quote,
    }

print("WEB_SCRAPER_ENRICHMENT_STARTED", flush=True)

def run_web_scraper_enrichment_for_request(
    request_id: str,
    job_id: Optional[str] = None,
    spreadsheet_id: Optional[str] = None
) -> None:
    """
    Run web scraper enrichment pipeline for a request.
    
    Pipeline stages:
    1. Crawl: extraction_status (pending -> crawl_running -> crawl_ok/crawl_error)
    2. Deterministic: det_status (NULL -> running -> ok/error)
    3. LLM: llm_status (NULL -> running -> ok/error)
    4. Combine: combine_status (NULL -> running -> ok/partial/error)
    """
    rid = request_id
    # Services own their logging - use standard logger
    log = logger

    repo = WebScraperEnrichmentResultsRepository()
    
    # Batch sizes from environment
    crawl_batch_size = int(os.getenv("WEB_CRAWL_BATCH_SIZE", "5"))
    det_batch_size = int(os.getenv("WEB_DET_BATCH_SIZE", "5"))
    llm_batch_size = int(os.getenv("WEB_LLM_BATCH_SIZE", "5"))
    combine_batch_size = int(os.getenv("WEB_COMBINE_BATCH_SIZE", "5"))

    status = None
    
    if job_id:
        mark_running(job_id)
        
        # Initialize Tool_Status tracking if spreadsheet_id provided
        if spreadsheet_id:
            try:
                status = JobsSheetWriter(sheet_id=spreadsheet_id)
                
                status.write(
                    job_id=job_id,
                    tool="web_scraper_enrichment",
                    request_id=rid,
                    state="RUNNING",
                    current=0,
                    total=0,
                )
            except Exception as e:
                log.warning("Failed to initialize Tool_Status: %s", e)

    # Clone master results to web_scraper_enrichment_results
    master_repo = MasterResultsRepository()
    ok, err, rows = master_repo.get_results_by_request(request_id=rid)
    if not ok:
        raise RuntimeError(err)

    for r in rows:
        repo.upsert(
            WebScraperEnrichmentResultCreate(
                request_id=rid,
                master_result_id=r["id"],
                domain=r["domain"],
                organization=r.get("company"),
                source=r.get("source"),
                extraction_status="pending",
            )
        )
    
    # MANDATORY: Log cloned row count before async execution
    log.info("WEB_ENRICHMENT_CLONED_ROWS count=%d", len(rows))

    async def pipeline():
        log.info("PIPELINE_STARTED")
        crawl_cfg = Crawl4AIClientConfig(
            headless=settings.headless,
            timeout_s=45,
            max_concurrency=4,
            user_agent=settings.user_agent,
        )
        log.info("PIPELINE_CONFIG_CREATED")
        
        # Pre-initialize browser BEFORE starting workers to avoid resource contention
        log.info("PRE_INITIALIZING_BROWSER")
        print("[PIPELINE] Pre-initializing browser to avoid worker contention...", flush=True)
        crawl_client = None
        try:
            crawl_client = Crawl4AIClient(crawl_cfg)
            await asyncio.wait_for(crawl_client.start(), timeout=90.0)
            log.info("BROWSER_PRE_INITIALIZED")
            print("[PIPELINE] ✓ Browser pre-initialized successfully", flush=True)
        except asyncio.TimeoutError:
            log.error("BROWSER_PRE_INIT_TIMEOUT timeout=90s")
            print("[PIPELINE] ✗ Browser pre-initialization timed out", flush=True)
            if crawl_client:
                await crawl_client.close()
            raise RuntimeError("Browser initialization timed out - cannot proceed with crawling")
        except Exception as e:
            log.error("BROWSER_PRE_INIT_ERROR error=%s type=%s", str(e), type(e).__name__)
            print(f"[PIPELINE] ✗ Browser pre-initialization failed: {e}", flush=True)
            if crawl_client:
                await crawl_client.close()
            raise

        # Events to signal worker completion
        crawl_done = asyncio.Event()
        det_done = asyncio.Event()
        llm_done = asyncio.Event()
        combine_done = asyncio.Event()

        async def crawl_worker():
            """
            Worker A: Crawl domains and persist page artifacts.
            State: extraction_status (pending -> crawl_running -> crawl_ok/crawl_error)
            """
            log.info("CRAWL_WORKER_STARTED batch_size=%d", crawl_batch_size)
            
            total_processed = 0
            
            # Use pre-initialized client
            log.info("CRAWL_WORKER_USING_PREINIT_CLIENT")
            
            try:
                crawler = DomainCrawlerService(crawl_client)
                log.info("CRAWL_WORKER_READY")
                
                while True:
                    log.info("CRAWL_WORKER_CLAIMING_BATCH limit=%d", crawl_batch_size)
                    print(f"[DEBUG] About to claim batch for crawl, limit={crawl_batch_size}", flush=True)
                    try:
                        crawl_batch = await asyncio.wait_for(
                            asyncio.to_thread(repo.claim_candidates_for_crawl, rid, crawl_batch_size),
                            timeout=30.0
                        )
                        print(f"[DEBUG] Claimed {len(crawl_batch)} rows for crawl", flush=True)
                        log.info("CRAWL_WORKER_CLAIMED count=%d", len(crawl_batch))
                    except asyncio.TimeoutError:
                        print("[DEBUG] TIMEOUT claiming batch for crawl", flush=True)
                        log.error("CRAWL_WORKER_CLAIM_TIMEOUT timeout=30s")
                        break
                    except Exception as e:
                        print(f"[DEBUG] ERROR claiming batch: {e}", flush=True)
                        log.error("CRAWL_WORKER_CLAIM_ERROR error=%s type=%s", str(e), type(e).__name__)
                        break
                    
                    if not crawl_batch:
                        log.info("CRAWL_WORKER_NO_CANDIDATES")
                        break
                    
                    log.info(
                        "CRAWL_BATCH_START size=%d",
                        len(crawl_batch),
                    )
                    
                    updates = []
                    for c in crawl_batch:
                        try:
                            crawl = await crawler.crawl_domain(c["domain"], DomainCrawlConfig())
                            meta = crawl.meta or {}
                            
                            # Serialize homepage
                            crawl_homepage = None
                            if crawl.homepage:
                                crawl_homepage = json.dumps(
                                    _serialize_page_artifact(crawl.homepage),
                                    ensure_ascii=False
                                )
                            
                            # Serialize pages
                            crawl_pages = None
                            if crawl.pages:
                                crawl_pages = json.dumps(
                                    [_serialize_page_artifact(p) for p in crawl.pages],
                                    ensure_ascii=False
                                )
                            
                            updates.append(
                                {
                                    "master_result_id": c["master_result_id"],
                                    "contact_links": meta.get("contact_selected_links", []),
                                    "contact_case": meta.get("contact_case", ""),
                                    "about_links": meta.get("about_selected_links", []),
                                    "about_case": meta.get("about_case", ""),
                                    "crawl_homepage": crawl_homepage,
                                    "crawl_pages": crawl_pages,
                                    "status": "crawl_ok",
                                    "debug_message": None,
                                }
                            )
                        except Exception as e:
                            log.error(
                                "CRAWL_ERROR domain=%s error=%s",
                                c["domain"],
                                str(e)[:200],
                            )
                            updates.append(
                                {
                                    "master_result_id": c["master_result_id"],
                                    "contact_links": [],
                                    "contact_case": "",
                                    "about_links": [],
                                    "about_case": "",
                                    "crawl_homepage": None,
                                    "crawl_pages": None,
                                    "status": "crawl_error",
                                    "debug_message": str(e)[:200],
                                }
                            )
                    
                    await asyncio.to_thread(
                        repo.update_crawl_results_batch, request_id=rid, updates=updates
                    )
                    
                    total_processed += len(updates)
                    
                    log.info(
                        "CRAWL_BATCH_END size=%d total=%d",
                        len(updates),
                        total_processed,
                    )
            
            finally:
                # Don't close the client here - it will be closed by the pipeline
                pass
            
            crawl_done.set()
            log.info(
                "CRAWL_WORKER_DONE total_processed=%d",
                total_processed,
            )

        async def det_worker():
            """
            Worker B: Run deterministic extraction on crawl_ok rows.
            State: det_status (NULL -> running -> ok/error)
            """
            log.info("DET_WORKER_STARTED batch_size=%d", det_batch_size)
            
            empty_polls = 0
            max_empty_polls = 3
            total_processed = 0
            
            while True:
                det_batch = await asyncio.to_thread(
                    repo.claim_candidates_for_det, rid, det_batch_size
                )
                
                if not det_batch:
                    empty_polls += 1
                    if crawl_done.is_set() and empty_polls >= max_empty_polls:
                        break
                    await asyncio.sleep(1.0)
                    continue
                
                empty_polls = 0
                
                log.info(
                    "DET_BATCH_START size=%d",
                    len(det_batch),
                )
                
                updates = []
                for c in det_batch:
                    try:
                        # Deserialize crawl artifacts
                        pages = []
                        
                        # Add homepage first if present
                        if c.get("crawl_homepage"):
                            homepage_data = c["crawl_homepage"]
                            if isinstance(homepage_data, str):
                                homepage_data = json.loads(homepage_data)
                            pages.append(_deserialize_page_artifact(homepage_data))
                        
                        # Add crawl pages
                        if c.get("crawl_pages"):
                            pages_data = c["crawl_pages"]
                            if isinstance(pages_data, str):
                                pages_data = json.loads(pages_data)
                            for page_data in pages_data:
                                pages.append(_deserialize_page_artifact(page_data))
                        
                        # Run deterministic extraction
                        det = det_extract(domain=c["domain"], pages=pages)
                        
                        # Format det_emails with source_url
                        det_emails = [
                            {
                                "email": e.email,
                                "source_url": str(e.source_url) if e.source_url else None,
                            }
                            for e in det.emails
                        ]
                        
                        updates.append(
                            {
                                "master_result_id": c["master_result_id"],
                                "det_emails": det_emails,
                                "status": "ok",
                                "error": None,
                            }
                        )
                    except Exception as e:
                        log.error(
                            "DET_ERROR domain=%s error=%s",
                            c["domain"],
                            str(e)[:200],
                        )
                        updates.append(
                            {
                                "master_result_id": c["master_result_id"],
                                "det_emails": [],
                                "status": "error",
                                "error": str(e)[:200],
                            }
                        )
                
                await asyncio.to_thread(
                    repo.update_det_results_batch, request_id=rid, updates=updates
                )
                
                total_processed += len(updates)
                
                log.info(
                    "DET_BATCH_END size=%d total=%d",
                    len(updates),
                    total_processed,
                )
            
            det_done.set()
            log.info(
                "DET_WORKER_DONE total_processed=%d",
                total_processed,
            )

        async def llm_worker():
            """
            Worker C: Run LLM extraction on crawl_ok rows.
            State: llm_status (NULL -> running -> ok/error)
            """
            log.info("LLM_WORKER_STARTED batch_size=%d", llm_batch_size)
            
            empty_polls = 0
            max_empty_polls = 3
            total_processed = 0
            
            # Initialize LLM router
            llm_router_cfg = LLMRouterConfig(
                api_keys=settings.openrouter_api_keys,
                models=[settings.openrouter_model],
                timeout_s=settings.llm_timeout_s,
                max_concurrency=settings.llm_max_concurrency,
            )
            router = OpenRouterLLMRouter(llm_router_cfg)
            
            while True:
                llm_batch = await asyncio.to_thread(
                    repo.claim_candidates_for_llm, rid, llm_batch_size
                )
                
                if not llm_batch:
                    empty_polls += 1
                    if crawl_done.is_set() and empty_polls >= max_empty_polls:
                        break
                    await asyncio.sleep(1.0)
                    continue
                
                empty_polls = 0
                
                log.info(
                    "LLM_BATCH_START size=%d",
                    len(llm_batch),
                )
                
                updates = []
                for c in llm_batch:
                    try:
                        # Deserialize crawl artifacts
                        pages = []
                        
                        # Add homepage first if present
                        if c.get("crawl_homepage"):
                            homepage_data = c["crawl_homepage"]
                            if isinstance(homepage_data, str):
                                homepage_data = json.loads(homepage_data)
                            pages.append(_deserialize_page_artifact(homepage_data))
                        
                        # Add crawl pages
                        if c.get("crawl_pages"):
                            pages_data = c["crawl_pages"]
                            if isinstance(pages_data, str):
                                pages_data = json.loads(pages_data)
                            for page_data in pages_data:
                                pages.append(_deserialize_page_artifact(page_data))
                        
                        # Build crawl_meta from stored data
                        # contact_links and about_links are stored as JSON strings in DB
                        contact_links = c.get("contact_links")
                        if contact_links and isinstance(contact_links, str):
                            contact_links = json.loads(contact_links)
                        
                        about_links = c.get("about_links")
                        if about_links and isinstance(about_links, str):
                            about_links = json.loads(about_links)
                        
                        crawl_meta = {
                            "contact_case": c.get("contact_case"),
                            "about_case": c.get("about_case"),
                            "contact_selected_links": contact_links or [],
                            "about_selected_links": about_links or [],
                        }
                        
                        # Run LLM extraction
                        llm = await llm_extract(
                            domain=c["domain"],
                            crawl_meta=crawl_meta,
                            pages=pages,
                            router=router,
                        )
                        
                        # Serialize LLM outputs
                        llm_company = None
                        if llm.company:
                            llm_company = json.dumps(
                                {
                                    "name": llm.company.name,
                                    "evidence_url": str(llm.company.evidence.url),
                                    "evidence_quote": llm.company.evidence.quote,
                                },
                                ensure_ascii=False,
                            )
                        
                        llm_emails = json.dumps(
                            [
                                {
                                    "email": e.email,
                                    "evidence_url": str(e.evidence.url),
                                    "evidence_quote": e.evidence.quote,
                                }
                                for e in llm.emails
                            ],
                            ensure_ascii=False,
                        )
                        
                        llm_contacts = json.dumps(
                            [
                                {
                                    "name": c.name,
                                    "role": c.role,
                                    "evidence_url": str(c.evidence.url),
                                    "evidence_quote": c.evidence.quote,
                                }
                                for c in llm.contacts
                            ],
                            ensure_ascii=False,
                        )
                        
                        llm_about = None
                        if llm.about:
                            llm_about = json.dumps(
                                {
                                    "short_description": llm.about.short_description,
                                    "short_evidence_url": str(llm.about.short_evidence.url) if llm.about.short_evidence else None,
                                    "short_evidence_quote": llm.about.short_evidence.quote if llm.about.short_evidence else None,
                                    "long_description": llm.about.long_description,
                                    "long_evidence_url": str(llm.about.long_evidence.url) if llm.about.long_evidence else None,
                                    "long_evidence_quote": llm.about.long_evidence.quote if llm.about.long_evidence else None,
                                },
                                ensure_ascii=False,
                            )
                        
                        updates.append(
                            {
                                "master_result_id": c["master_result_id"],
                                "llm_company": llm_company,
                                "llm_emails": llm_emails,
                                "llm_contacts": llm_contacts,
                                "llm_about": llm_about,
                                "status": "ok",
                                "error": None,
                            }
                        )
                    except Exception as e:
                        log.error(
                            "LLM_ERROR domain=%s error=%s",
                            c["domain"],
                            str(e)[:200],
                        )
                        updates.append(
                            {
                                "master_result_id": c["master_result_id"],
                                "llm_company": None,
                                "llm_emails": None,
                                "llm_contacts": None,
                                "llm_about": None,
                                "status": "error",
                                "error": str(e)[:200],
                            }
                        )
                
                await asyncio.to_thread(
                    repo.update_llm_results_batch, request_id=rid, updates=updates
                )
                
                total_processed += len(updates)
                
                log.info(
                    "LLM_BATCH_END size=%d total=%d",
                    len(updates),
                    total_processed,
                )
            
            llm_done.set()
            log.info(
                "LLM_WORKER_DONE total_processed=%d",
                total_processed,
            )

        async def combine_worker():
            """
            Worker D: Combine deterministic and LLM results using the proper combine service.
            State: combine_status (NULL -> running -> ok/partial/error)
            
            Starts as soon as both DET and LLM are finished for a row.
            """
            log.info("COMBINE_WORKER_STARTED batch_size=%d", combine_batch_size)
            
            # Import combine service and models
            from ..web_scrapers.company_enrichment.combine.service import combine_enrichment
            from ..web_scrapers.company_enrichment.models import (
                CrawlArtifacts,
                PageArtifact,
                DeterministicExtraction,
                DeterministicEmail,
                LLMExtraction,
                LLMCompany,
                LLMEmail,
                LLMContact,
                LLMAbout,
                Evidence,
            )
            
            empty_polls = 0
            max_empty_polls = 3
            total_processed = 0
            
            while True:
                combine_batch = await asyncio.to_thread(
                    repo.claim_candidates_for_combine, rid, combine_batch_size
                )
                
                if not combine_batch:
                    empty_polls += 1
                    # Stop when crawl, det, and llm are all done AND no more candidates
                    if crawl_done.is_set() and det_done.is_set() and llm_done.is_set() and empty_polls >= max_empty_polls:
                        break
                    await asyncio.sleep(1.0)
                    continue
                
                empty_polls = 0
                
                log.info(
                    "COMBINE_BATCH_START size=%d",
                    len(combine_batch),
                )
                
                updates = []
                for c in combine_batch:
                    try:
                        # =====================================================
                        # RECONSTRUCT CRAWL ARTIFACTS
                        # =====================================================
                        # Note: We don't have crawl_homepage and crawl_pages in the combine query
                        # We'll need to fetch them or pass empty artifacts
                        # For now, create minimal crawl artifacts
                        crawl = CrawlArtifacts(
                            domain=c["domain"],
                            homepage=None,
                            pages=[],
                            meta={}
                        )
                        
                        # =====================================================
                        # RECONSTRUCT DETERMINISTIC EXTRACTION
                        # =====================================================
                        det_emails_data = c.get("det_emails")
                        # Handle both JSONB (list) and JSON string formats
                        if det_emails_data:
                            if isinstance(det_emails_data, str):
                                det_emails_data = json.loads(det_emails_data)
                        
                        det_emails = []
                        for email_data in (det_emails_data or []):
                            det_emails.append(
                                DeterministicEmail(
                                    email=email_data["email"],
                                    source_url=email_data.get("source_url"),
                                )
                            )
                        
                        deterministic = DeterministicExtraction(emails=det_emails)
                        
                        # =====================================================
                        # RECONSTRUCT LLM EXTRACTION
                        # =====================================================
                        # Parse llm_company
                        llm_company = None
                        llm_company_data = c.get("llm_company")
                        # Handle both JSONB (dict) and JSON string formats
                        if llm_company_data:
                            if isinstance(llm_company_data, str):
                                llm_company_data = json.loads(llm_company_data)
                            llm_company = LLMCompany(
                                name=llm_company_data["name"],
                                evidence=Evidence(
                                    url=llm_company_data["evidence_url"],
                                    quote=llm_company_data.get("evidence_quote", ""),
                                )
                            )
                        
                        # Parse llm_emails
                        llm_emails = []
                        llm_emails_data = c.get("llm_emails")
                        # Handle both JSONB (list/dict) and JSON string formats
                        if llm_emails_data:
                            if isinstance(llm_emails_data, str):
                                llm_emails_data = json.loads(llm_emails_data)
                        for email_data in (llm_emails_data or []):
                            llm_emails.append(
                                LLMEmail(
                                    email=email_data["email"],
                                    evidence=Evidence(
                                        url=email_data["evidence_url"],
                                        quote=email_data.get("evidence_quote", ""),
                                    )
                                )
                            )
                        
                        # Parse llm_contacts
                        llm_contacts = []
                        llm_contacts_data = c.get("llm_contacts")
                        # Handle both JSONB (list/dict) and JSON string formats
                        if llm_contacts_data:
                            if isinstance(llm_contacts_data, str):
                                llm_contacts_data = json.loads(llm_contacts_data)
                        for contact_data in (llm_contacts_data or []):
                            llm_contacts.append(
                                LLMContact(
                                    name=contact_data["name"],
                                    role=contact_data.get("role"),
                                    evidence=Evidence(
                                        url=contact_data["evidence_url"],
                                        quote=contact_data.get("evidence_quote", ""),
                                    )
                                )
                            )
                        
                        # Parse llm_about
                        llm_about = None
                        llm_about_data = c.get("llm_about")
                        # Handle both JSONB (dict) and JSON string formats
                        if llm_about_data:
                            if isinstance(llm_about_data, str):
                                llm_about_data = json.loads(llm_about_data)
                            short_evidence = None
                            if llm_about_data.get("short_evidence_url"):
                                short_evidence = Evidence(
                                    url=llm_about_data["short_evidence_url"],
                                    quote=llm_about_data.get("short_evidence_quote", ""),
                                )
                            
                            long_evidence = None
                            if llm_about_data.get("long_evidence_url"):
                                long_evidence = Evidence(
                                    url=llm_about_data["long_evidence_url"],
                                    quote=llm_about_data.get("long_evidence_quote", ""),
                                )
                            
                            llm_about = LLMAbout(
                                short_description=llm_about_data.get("short_description"),
                                short_evidence=short_evidence,
                                long_description=llm_about_data.get("long_description"),
                                long_evidence=long_evidence,
                            )
                        
                        llm = LLMExtraction(
                            company=llm_company,
                            emails=llm_emails,
                            contacts=llm_contacts,
                            about=llm_about,
                        )
                        
                        # =====================================================
                        # CALL COMBINE SERVICE
                        # =====================================================
                        email_results, company_result, descriptions, people = await asyncio.to_thread(
                            combine_enrichment,
                            domain=c["domain"],
                            crawl=crawl,
                            deterministic=deterministic,
                            llm=llm,
                        )
                        
                        # =====================================================
                        # SERIALIZE RESULTS
                        # =====================================================
                        combined_emails = [
                            {
                                "email": e.email,
                                "frequency": e.frequency,
                                "source_agreement": e.source_agreement,
                                "confidence": e.confidence,
                                "sources": e.sources,
                            }
                            for e in email_results
                        ]
                        
                        combined_company = None
                        if company_result:
                            combined_company = {
                                "name": company_result.name,
                                "frequency": company_result.frequency,
                                "confidence": company_result.confidence,
                                "sources": company_result.sources,
                            }
                        
                        combined_descriptions = [
                            {
                                "kind": d.kind,
                                "text": d.text,
                                "source_url": str(d.source_url) if d.source_url else None,
                            }
                            for d in descriptions
                        ]
                        
                        combined_people = [
                            {
                                "name": p.name,
                                "role": p.role,
                                "sources": p.sources,
                            }
                            for p in people
                        ]
                        
                        # Determine status
                        status = "ok"
                        if not email_results and not company_result:
                            status = "partial"
                        
                        updates.append({
                            "master_result_id": c["master_result_id"],
                            "combined_emails": json.dumps(combined_emails, ensure_ascii=False),
                            "combined_company": json.dumps(combined_company, ensure_ascii=False) if combined_company else None,
                            "combined_descriptions": json.dumps(combined_descriptions, ensure_ascii=False),
                            "combined_people": json.dumps(combined_people, ensure_ascii=False),
                            "status": status,
                            "debug": None,
                        })
                        
                    except Exception as e:
                        log.error(
                            "COMBINE_ERROR domain=%s error=%s",
                            c["domain"],
                            str(e)[:200],
                        )
                        updates.append({
                            "master_result_id": c["master_result_id"],
                            "combined_emails": None,
                            "combined_company": None,
                            "combined_descriptions": None,
                            "combined_people": None,
                            "status": "error",
                            "debug": str(e)[:200],
                        })
                
                await asyncio.to_thread(
                    repo.update_combine_results_batch, request_id=rid, updates=updates
                )
                
                total_processed += len(updates)
                
                log.info(
                    "COMBINE_BATCH_END size=%d total=%d",
                    len(updates),
                    total_processed,
                )
                
                # Sync to Google Sheets after each batch (DB is source of truth)
                if spreadsheet_id:
                    try:
                        from ..exports.web_enrichment_sheets_sync import sync_combined_web_enrichment_to_master_sheet
                        from ..exports.email_sheets_sync import sync_combined_to_mail_content_sheet

                        sync_combined_web_enrichment_to_master_sheet(
                            request_id=rid,
                            spreadsheet_id=spreadsheet_id
                        )
                        log.info("COMBINE_BATCH_SHEETS_SYNC_DONE batch_size=%d", len(updates))

                        sync_combined_to_mail_content_sheet(
                            request_id=rid,
                            spreadsheet_id=spreadsheet_id
                        )
                        log.info("EMAIL_SHEETS_SYNC batch_size=%d", len(updates))

                        
                        # Update Tool_Status after sheets sync
                        if status:
                            try:
                                status.write(
                                    job_id=job_id,
                                    tool="web_scraper_enrichment",
                                    request_id=rid,
                                    state="RUNNING",
                                    current=total_processed,
                                    total=len(rows),
                                )
                            except Exception as status_err:
                                log.warning("Failed to update Tool_Status after sync: %s", status_err)
                    except Exception as sync_err:
                        log.error("COMBINE_BATCH_SHEETS_SYNC_ERROR error=%s", sync_err, exc_info=True)
                        # Don't fail the job if sheets sync fails - DB is source of truth
            
            combine_done.set()
            log.info(
                "COMBINE_WORKER_DONE total_processed=%d",
                total_processed,
            )

        # Run all four workers concurrently
        try:
            await asyncio.gather(
                crawl_worker(),
                det_worker(),
                llm_worker(),
                combine_worker(),
            )
        finally:
            # Ensure browser is closed
            if crawl_client:
                log.info("PIPELINE_CLOSING_BROWSER")
                await crawl_client.close()
                log.info("PIPELINE_BROWSER_CLOSED")

    # Async safety: Check if we're already in an event loop
    try:
        loop = asyncio.get_running_loop()
        # Already in event loop - fail fast with clear error
        raise RuntimeError(
            "run_web_scraper_enrichment_for_request cannot be called from within "
            "an existing event loop. Use 'await pipeline()' directly or run in a separate thread."
        )
    except RuntimeError as e:
        if "no running event loop" in str(e).lower():
            # No event loop - safe to use asyncio.run()
            asyncio.run(pipeline())
        else:
            # Re-raise if it's our custom error or another RuntimeError
            raise

    # Sync combined results to Master sheet
    try:
        from ..exports.web_enrichment_sheets_sync import sync_combined_web_enrichment_to_master_sheet
        
        log.info("SHEETS_SYNC_START")
        sync_combined_web_enrichment_to_master_sheet(request_id=rid)
        log.info("SHEETS_SYNC_DONE")
    except Exception as e:
        log.error("SHEETS_SYNC_ERROR error=%s type=%s", str(e), type(e).__name__)
        # Don't fail the job if sheets sync fails - it's presentation only
        # The DB remains the source of truth
    
    if job_id:
        # Final Tool_Status update
        if status:
            try:
                status.write(
                    job_id=job_id,
                    tool="web_scraper_enrichment",
                    request_id=rid,
                    state="SUCCEEDED",
                    current=len(rows),
                    total=len(rows),
                )
            except Exception as e:
                log.warning("Failed to update final Tool_Status: %s", e)
        
        mark_succeeded(job_id, "Web enrichment complete")
