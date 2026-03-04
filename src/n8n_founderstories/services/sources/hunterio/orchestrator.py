"""
Hunter.io orchestrator module.

Coordinates the complete Hunter.io lead discovery workflow:
1. Input validation
2. Job lifecycle management (running/succeeded/failed)
3. API calls via client + policy
4. Data persistence to database
5. Export coordination (delegates to sheets.exports)

This module focuses ONLY on Hunter.io business logic and does NOT:
- Define sheet layouts (see sheets.specs.hunter_leads)
- Fetch data for sheets (see sheets.data_fetchers.hunterio)
- Write to Google Sheets directly (see sheets.exports.hunterio)
- Sync to master (delegates to masterv2.service)

Architecture:
    ┌─────────────────────────────────────────┐
    │  orchestrator.py (THIS MODULE)          │
    │  - Validate input                       │
    │  - Manage job lifecycle                 │
    │  - Call Hunter API                      │
    │  - Persist to DB                        │
    │  - Coordinate export                    │
    └─────────────────────────────────────────┘
                    │
                    ├──> client.py (HTTP calls)
                    ├──> policy.py (retry + rate limit)
                    ├──> repo.py (DB persistence)
                    └──> sheets.exports.hunterio (export coordination)
"""

from __future__ import annotations

import logging
import os
from typing import Any

from .client import HunterClient
from .models import HunterInput
from .policy import HunterAPIPolicy
from .repo import ensure_table, upsert_batch_rows, finalize_request_job_id
from n8n_founderstories.core.logging.tags import log_db
from n8n_founderstories.core.utils.domain_eligibility import is_social_domain
from n8n_founderstories.services.jobs.status_writer import StatusWriterLike, safe_status_write
from ...jobs.store import mark_running, update_progress, mark_succeeded, mark_failed
from n8n_founderstories.core.db import get_conn

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

HUNTER_HEADCOUNT_BUCKETS = (
    "1-10",
    "11-50",
    "51-200",
    "201-500",
    # "501-1000",
    # "1001-5000",
    # "5001-10000",
    # "10001+",
)
"""
Headcount buckets to query in Hunter.io API.

These buckets are used to segment company searches by employee count.
Commented buckets are disabled to reduce API calls and focus on smaller companies.
"""

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def _extract_total_results(data: dict[str, Any]) -> int:
    """
    Extract total result count from Hunter.io API response.
    
    Args:
        data: Hunter.io API response dictionary
        
    Returns:
        Total number of results, or 0 if not found
    """
    meta = data.get("meta")
    if isinstance(meta, dict):
        for key in ("results", "total"):
            try:
                val = meta.get(key)
                if val is not None:
                    return int(val)
            except Exception:
                pass
    return 0


def _extract_domains(data: dict[str, Any]) -> list[str]:
    """
    Extract domain list from Hunter.io API response.
    
    Args:
        data: Hunter.io API response dictionary
        
    Returns:
        List of normalized domain strings
    """
    out: list[str] = []
    items = data.get("data") or []
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict):
                dom = (item.get("domain") or "").strip().lower()
                if dom:
                    out.append(dom)
    return out


def _fmt_location(location: dict[str, Any] | None) -> str:
    """
    Format location for database storage and sheet export.
    
    Produces compact location strings suitable for database storage
    and Google Sheets display.
    
    Args:
        location: Location dictionary with country/city keys
        
    Returns:
        Formatted string: "-" (none), "DE" (country), or "DE/Berlin" (country/city)
    """
    if not location:
        return "-"
    
    country = (location.get("country") or "").strip()
    city = (location.get("city") or "").strip()
    
    if not country:
        return "-"
    
    if city:
        return f"{country}/{city}"
    
    return country


def _export_to_sheets(inp: HunterInput, job_id: str, keyword_mode: bool) -> int:
    """
    Coordinate export of Hunter.io results to Google Sheets.
    
    This function delegates to the sheets.exports.hunterio module,
    which handles data fetching, formatting, and writing.
    
    Separation of concerns:
    - This function: Prepares export context (term_order, country_order)
    - sheets.exports.hunterio: Orchestrates fetch + write
    - sheets.data_fetchers.hunterio: Fetches and sorts data by request_id
    - sheets.specs.hunter_leads: Defines sheet layout
    - sheets.writer: Writes to Google Sheets API
    
    Args:
        inp: HunterInput with search parameters, request_id, and sheet_id
        job_id: Job identifier (for logging only)
        keyword_mode: Whether running in keyword mode (vs query mode)
        
    Returns:
        Number of rows written to sheet, or 0 if skipped/failed
    """
    # Check if export is configured
    sheet_id = (inp.sheet_id or os.getenv("HUNTERIO_SHEET_ID", "")).strip()
    if not sheet_id:
        logger.debug("Export skipped: no sheet_id provided (inp.sheet_id or HUNTERIO_SHEET_ID)")
        return 0
    
    try:
        # Build term order for sorting
        if keyword_mode:
            # Use keywords in their original order
            term_order = [k.strip() for k in inp.keywords if isinstance(k, str) and k.strip()]
        else:
            # Query mode: single term
            query = (inp.target_prompt or "").strip()
            term_order = [query] if query else []
        
        # Build country order from locations
        country_order = []
        seen_countries = set()
        for loc in (inp.locations or []):
            if isinstance(loc, dict):
                country = (loc.get("country") or "").strip()
                if country and country not in seen_countries:
                    country_order.append(country)
                    seen_countries.add(country)
        
        # Delegate to export module (handles fetch + write)
        # IMPORTANT: Export by request_id (not job_id) to avoid coupling with finalize_request_job_id()
        from n8n_founderstories.services.sheets.exports import hunterio as export_module
        
        row_count = export_module.export_to_sheet(
            request_id=inp.request_id,
            sheet_id=sheet_id,
            context={
                "term_order": term_order,
                "country_order": country_order,
            },
            job_id=job_id,  # For logging only
        )
        
        logger.debug(f"Successfully exported {row_count} rows to sheet {sheet_id} (request_id={inp.request_id})")
        return row_count
        
    except Exception as e:
        # Log error but don't fail the job
        logger.error(f"Failed to export to Google Sheets: {e}", exc_info=True)
        return 0


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

def run_hunter(
    *,
    inp: HunterInput,
    job_id: str | None = None,
    use_industries_filter: bool = True,
    status_writer: StatusWriterLike = None,
) -> list[dict[str, str]]:
    """
    Main orchestrator for Hunter.io lead discovery workflow.
    
    Coordinates the complete workflow:
    1. Input validation
    2. Database table setup
    3. Job lifecycle management (RUNNING → SUCCEEDED/FAILED)
    4. API calls via client + policy (with retry + rate limiting)
    5. Data cleaning and persistence to database
    6. Export to Google Sheets (if configured)
    7. Sync to master consolidation table
    
    This function focuses ONLY on Hunter.io business logic.
    It does NOT define sheet layouts, fetch data for sheets,
    or write to Google Sheets directly.
    
    Args:
        inp: HunterInput with validated search parameters
        job_id: Optional job identifier (generated if not provided)
        use_industries_filter: Whether to apply industries filter
        status_writer: Optional JobsSheetWriter for live status updates
        
    Returns:
        List of cleaned row dictionaries with keys:
        - organization: Company name
        - domain: Company domain
        - location: Formatted location string
        - headcount: Headcount bucket
        - query: Search query that found this lead
        
    Raises:
        Exception: If validation, API calls, or database operations fail
    """
    policy = HunterAPIPolicy()
    inp.validate()
    
    # Open database connection for this run
    conn = get_conn()
    
    try:
        # Generate job_id if not provided
        if not job_id:
            from uuid import uuid4
            job_id = f"htrio__{uuid4().hex}"
        
        # job_id is now guaranteed to be set (non-optional)
        
        # Ensure database table exists
        ensure_table(conn, job_id)

        cleaned_rows: list[dict[str, str]] = []
        locations = list(inp.locations) if inp.locations else [None]
        social_skipped_count = 0  # Track social domains filtered out

        keyword_list = [k.strip() for k in inp.keywords if isinstance(k, str) and k.strip()]
        keyword_mode = bool(keyword_list)

        # Determine which industries to use based on configuration
        industries_to_use = inp.industries if use_industries_filter else None

        # Compute total discover calls
        headcounts = HUNTER_HEADCOUNT_BUCKETS
        total_calls = len(locations) * len(headcounts) * (len(keyword_list) if keyword_mode else 1)
        current = 0
        
        # Initialize progress throttler (log every 5 seconds)
        from n8n_founderstories.core.logging.progress import ProgressThrottler
        progress_throttler = ProgressThrottler(interval_s=5.0)

        # Mark job as RUNNING and initialize progress
        if job_id:
            try:
                mark_running(job_id)
                update_progress(
                    job_id,
                    phase="hunteriov2",
                    current=0,
                    total=total_calls,
                    message="Starting Hunter run",
                    metrics={
                        "mode": "keywords" if keyword_mode else "query",
                        "total_calls": total_calls,
                    }
                )
                
            except Exception as e:
                logger.warning("Failed to update job progress: %s", e)
        
        # Calculate queries count based on mode
        queries_count = len(keyword_list) if keyword_mode else 1
        
        # Live status: STARTED
        logger.info(
            "HUNTERIOV2 | STATE=START | job_id=%s | request_id=%s | locations=%d | queries=%d | headcount_buckets=%d | max_calls=%d",
            job_id,
            inp.request_id,
            len(locations),
            queries_count,
            len(headcounts),
            total_calls,
        )
        
        # Track cumulative found results for progress logging
        cumulative_found = 0
        
        with HunterClient() as client:
            for location in locations:
                for headcount_bucket in HUNTER_HEADCOUNT_BUCKETS:

                    if keyword_mode:
                        # -------- KEYWORD MODE (NO QUERY) --------
                        for kw in keyword_list:
                            # Update progress before each call (increment before call to start at 1)
                            current += 1
                            if job_id:
                                try:
                                    update_progress(
                                        job_id,
                                        phase="hunteriov2",
                                        current=current,
                                        total=total_calls,
                                        message=f"Calling Hunter ({current}/{total_calls})",
                                        metrics={
                                            "mode": "keywords",
                                            "country": (location or {}).get("country", "") if location else "",
                                            "headcount": headcount_bucket,
                                            "keyword": kw,
                                        }
                                    )
                                except Exception as e:
                                    logger.warning("Failed to update progress: %s", e)
        
                            data = policy.call_discover(
                                request_id=inp.request_id,
                                fn=lambda loc=location, k=kw, hc=headcount_bucket, ind=industries_to_use: client.discover(
                                    query=None,                    # ✅ critical
                                    keywords=[k],
                                    location=loc,
                                    headcount=[hc],
                                    industries=ind,
                                    limit=100,
                                ),
                            )

                            total = _extract_total_results(data)
                            domains = _extract_domains(data)
                            cumulative_found += len(domains)

                            # Throttled progress logging (every ~5 seconds)
                            if progress_throttler.should_log():
                                loc_str = (location or {}).get("country", "") if location else ""
                                logger.info(
                                    "HUNTERIOV2 | PROGRESS | current=%d | total=%d | found=%d | loc=%s | term=%s",
                                    current,
                                    total_calls,
                                    cumulative_found,
                                    loc_str,
                                    kw,
                                )

                            # Extract cleaned rows from data
                            batch_rows: list[dict[str, str]] = []
                            for item in data.get("data", []):
                                if not isinstance(item, dict):
                                    continue
                                
                                domain = (item.get("domain") or "").strip().lower()
                                if not domain:
                                    continue
                                
                                organization = (item.get("organization") or "").strip()
                                if not organization:
                                    organization = domain
                                
                                term = kw.strip()
                                if not term:
                                    continue
                                
                                # Skip social media domains (Facebook, Instagram)
                                if is_social_domain(domain):
                                    social_skipped_count += 1
                                    continue
                                
                                row = {
                                    "organization": organization,
                                    "domain": domain,
                                    "location": _fmt_location(location),
                                    "headcount": headcount_bucket.strip(),
                                    "query": term,
                                }
                                batch_rows.append(row)
                                cleaned_rows.append(row)
                            
                            # Persist to database incrementally (optimized: only UPSERT, no job_id finalization yet)
                            if batch_rows:
                                try:
                                    upsert_batch_rows(conn, inp.request_id, job_id, batch_rows)
                                except Exception as e:
                                    logger.error(f"Failed to persist {len(batch_rows)} rows to database: {e}")
                                    raise

                    else:
                        # -------- QUERY MODE (NO KEYWORD) --------
                        query = (inp.target_prompt or "").strip()
                        if not query:
                            continue

                        # Update progress before each call (increment before call to start at 1)
                        current += 1
                        if job_id:
                            try:
                                update_progress(
                                    job_id,
                                    phase="hunteriov2",
                                    current=current,
                                    total=total_calls,
                                    message=f"Calling Hunter ({current}/{total_calls})",
                                    metrics={
                                        "mode": "query",
                                        "country": (location or {}).get("country", "") if location else "",
                                        "headcount": headcount_bucket,
                                    }
                                )
                            except Exception as e:
                                logger.warning("Failed to update progress: %s", e)

                        data = policy.call_discover(
                            request_id=inp.request_id,
                            fn=lambda loc=location, q=query, hc=headcount_bucket, ind=industries_to_use: client.discover(
                                query=q,
                                keywords=None,
                                location=loc,
                                headcount=[hc],
                                industries=ind,
                                limit=100,
                            ),
                        )

                        total = _extract_total_results(data)
                        domains = _extract_domains(data)
                        cumulative_found += len(domains)

                        # Throttled progress logging (every ~5 seconds)
                        if progress_throttler.should_log():
                            loc_str = (location or {}).get("country", "") if location else ""
                            logger.info(
                                "HUNTERIOV2 | PROGRESS | current=%d | total=%d | found=%d | loc=%s | term=%s",
                                current,
                                total_calls,
                                cumulative_found,
                                loc_str,
                                query,
                            )

                        # Extract cleaned rows from data
                        batch_rows: list[dict[str, str]] = []
                        for item in data.get("data", []):
                            if not isinstance(item, dict):
                                continue
                            
                            domain = (item.get("domain") or "").strip().lower()
                            if not domain:
                                continue
                            
                            organization = (item.get("organization") or "").strip()
                            if not organization:
                                organization = domain
                            
                            term = query.strip()
                            if not term:
                                continue
                            
                            # Skip social media domains (Facebook, Instagram)
                            if is_social_domain(domain):
                                social_skipped_count += 1
                                continue
                            
                            row = {
                                "organization": organization,
                                "domain": domain,
                                "location": _fmt_location(location),
                                "headcount": headcount_bucket.strip(),
                                "query": term,
                            }
                            batch_rows.append(row)
                            cleaned_rows.append(row)
                        
                        # Persist to database incrementally (optimized: only UPSERT, no job_id finalization yet)
                        if batch_rows:
                            try:
                                upsert_batch_rows(conn, inp.request_id, job_id, batch_rows)
                            except Exception as e:
                                logger.error(f"Failed to persist {len(batch_rows)} rows to database: {e}")
                                raise

        # Finalize: Update all rows for this request to latest job_id (called once at end)
        # This also returns the final row count, so no need for separate SELECT
        if job_id:
            try:
                db_count = finalize_request_job_id(conn, inp.request_id, job_id)
                logger.info(f"HUNTERIOV2 | FINALIZED | request_id={inp.request_id} | job_id={job_id} | total_rows={db_count}")
            except Exception as e:
                logger.error(f"Failed to finalize job_id for request: {e}")
                # Fallback: get count manually
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM htr_results WHERE request_id=%s",
                        (inp.request_id,),
                    )
                    db_count = int(cur.fetchone()[0])
        else:
            db_count = 0
        
        # Calculate unique domains extracted (before DB deduplication)
        extracted_unique_domains = len({r["domain"] for r in cleaned_rows})
        
        # Mark job as SUCCEEDED
        if job_id:
            try:
                mark_succeeded(
                    job_id,
                    message="Hunter run completed",
                    metrics={
                        "total_calls": total_calls,
                        "calls_done": current,
                        "extracted_rows": len(cleaned_rows),
                        "extracted_unique_domains": extracted_unique_domains,
                        "db_rows": db_count,
                    }
                )
                
                # Update Tool_Status sheet with SUCCEEDED state
                safe_status_write(
                    status_writer,
                    job_id=job_id,
                    tool="hunter",
                    request_id=inp.request_id,
                    state="SUCCEEDED",
                )
            except Exception as e:
                logger.warning("Failed to mark job as succeeded: %s", e)
        
        # DATABASE log
        log_db(logger, service="HUNTERIOV2", table="htr_results", rows=db_count)
        
        # =========================================================================
        # Classification: Export coordination
        # =========================================================================
        # Export to Google Sheets (write_rows handles SHEETS logging)
        # Note: Export is keyed by request_id, job_id is only for logging
        sheets_count = _export_to_sheets(inp, job_id or "-", keyword_mode)
        
        # COMPLETED log (end-of-job summary with pre-dedupe count)
        # IMPORTANT: Log this BEFORE master sync so it appears first in logs
        logger.info(
            "HUNTERIOV2 | STATE=COMPLETED | job_id=%s | extracted_rows=%d | extracted_unique_domains=%d | db_rows=%d | sheets_rows=%d | social_domains_skipped=%d",
            job_id, len(cleaned_rows), extracted_unique_domains, db_count, sheets_count, social_skipped_count
        )
        
        # Sync to master consolidation table
        if job_id:
            try:
                from ...master.service import sync_from_source
                # Pass sheet_id and request_id to enable automatic master sheet export
                master_stats = sync_from_source(
                    "hunter",
                    job_id,
                    sheet_id=inp.sheet_id,
                    request_id=inp.request_id
                )
                logger.info(
                    "HUNTERIOV2 | MASTER_SYNC | job_id=%s | seen=%d | upserted=%d",
                    job_id,
                    master_stats.get("seen", 0),
                    master_stats.get("upserted", 0)
                )
            except Exception as e:
                # Log error but don't fail the job
                logger.error("HUNTERIOV2 | MASTER_SYNC_ERROR | job_id=%s | error=%s", job_id, str(e))

        return cleaned_rows


    except Exception as exc:
        # Mark job as FAILED
        if job_id:
            try:
                mark_failed(job_id, error=str(exc), message="Hunter run failed")
                
                # Update Tool_Status sheet with FAILED state
                safe_status_write(
                    status_writer,
                    job_id=job_id,
                    tool="hunter",
                    request_id=inp.request_id,
                    state="FAILED",
                )
            except Exception as e:
                logger.warning("Failed to mark job as failed: %s", e)
        
        # Log failure
        logger.error(
            "HUNTERIOV2 | STATE=FAILED | request_id=%s | error=%s",
            inp.request_id,
            str(exc),
        )
        
        raise
    
    finally:
        # Always close the database connection
        conn.close()
