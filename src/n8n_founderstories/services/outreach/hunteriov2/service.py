from __future__ import annotations

import logging
import os
from typing import Any

from .client import HunterClient
from .models import HunterInput
from .api_policy import HunterAPIPolicy
from .run_log import append_hunter_run
from .repository import ensure_table, upsert_rows
from ....core.logging.tags import log_db
from . import sheets_spec
from ...jobs.store import mark_running, update_progress, mark_succeeded, mark_failed
from ....core.db import get_conn
from ....core.logging.live_status import LiveStatusLogger
from ....core.logging import set_live_status_logger
from ....exportsv2.writer import write_rows

logger = logging.getLogger(__name__)

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


def _extract_total_results(data: dict[str, Any]) -> int:
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
    out: list[str] = []
    items = data.get("data") or []
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict):
                dom = (item.get("domain") or "").strip().lower()
                if dom:
                    out.append(dom)
    return out


def _format_location(location: dict[str, Any] | None) -> str:
    """Format location for display in live status."""
    if not location:
        return "NONE"
    
    parts = []
    country = location.get("country", "").strip()
    city = location.get("city", "").strip() if location.get("city") else ""
    
    if country:
        parts.append(f"COUNTRY:{country}")
    if city:
        parts.append(f"CITY:{city}")
    
    return ",".join(parts) if parts else "NONE"


def _fmt_location(location: dict[str, Any] | None) -> str:
    """Format location for cleaned output: '-', 'DE', or 'DE/Berlin'."""
    if not location:
        return "-"
    
    country = (location.get("country") or "").strip()
    city = (location.get("city") or "").strip()
    
    if not country:
        return "-"
    
    if city:
        return f"{country}/{city}"
    
    return country


def _export_to_sheets(conn, inp: HunterInput, job_id: str, keyword_mode: bool) -> None:
    """
    Export HunterIO results to Google Sheets after job completion.
    
    Args:
        conn: Active database connection
        inp: HunterInput with search parameters
        job_id: Job identifier
        keyword_mode: Whether running in keyword mode (vs query mode)
    """
    # Check if export is configured
    sheet_id = (inp.sheet_id or os.getenv("HUNTERIO_SHEET_ID", "")).strip()
    if not sheet_id:
        logger.debug("Export skipped: no sheet_id provided (inp.sheet_id or HUNTERIO_SHEET_ID)")
        return
    
    # Get tab name (with optional override)
    tab_name = os.getenv("HUNTERIO_SHEET_TAB", sheets_spec.TAB_NAME).strip()
    
    try:
        # Build term order
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
        
        # Fetch sorted rows from database
        from . import data_fetcher
        
        rows = data_fetcher.fetch_rows_for_sheet(
            conn,
            job_id=job_id,
            term_order=term_order,
            country_order=country_order,
        )
        
        logger.debug(f"Exporting {len(rows)} rows to Google Sheets")
        
        # Write to Google Sheets
        write_rows(
            sheet_id=sheet_id,
            tab_name=tab_name,
            headers=sheets_spec.HEADERS,
            rows=rows,
            mode="replace",
            service="HUNTERIOV2",
            job_id=job_id,
        )
        
        logger.debug(f"Successfully exported {len(rows)} rows to sheet {sheet_id}, tab '{tab_name}'")
        
    except Exception as e:
        # Log error but don't fail the job
        logger.error(f"Failed to export to Google Sheets: {e}", exc_info=True)


def run_hunter(
    *,
    inp: HunterInput,
    job_id: str | None = None,
    use_industries_filter: bool = True,
    status_writer = None,
) -> list[dict[str, str]]:
    policy = HunterAPIPolicy()
    inp.validate()
    
    # Open database connection for this run
    conn = get_conn()
    
    try:
        # Generate job_id if not provided
        if not job_id:
            from uuid import uuid4
            job_id = f"htrio__{uuid4().hex}"
        
        # Ensure database table exists
        ensure_table(conn, job_id)

        results: list[dict[str, Any]] = []
        cleaned_rows: list[dict[str, str]] = []
        locations = list(inp.locations) if inp.locations else [None]

        keyword_list = [k.strip() for k in inp.keywords if isinstance(k, str) and k.strip()]
        keyword_mode = bool(keyword_list)

        # Determine which industries to use based on configuration
        industries_to_use = inp.industries if use_industries_filter else None

        # Compute total discover calls
        headcounts = HUNTER_HEADCOUNT_BUCKETS
        total_calls = len(locations) * len(headcounts) * (len(keyword_list) if keyword_mode else 1)
        current = 0
        
        # Initialize live status logger and register it globally
        live_status = LiveStatusLogger()
        set_live_status_logger(live_status)

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
                
                # Write initial status to Tool_Status sheet
                if status_writer:
                    status_writer.write(
                        job_id=job_id,
                        tool="hunter",
                        request_id=inp.request_id,
                        state="RUNNING",
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
        
        # Track last search details for final log
        last_search_details = {}
        
        with HunterClient() as client:
            for location in locations:
                for headcount_bucket in HUNTER_HEADCOUNT_BUCKETS:

                    if keyword_mode:
                            # -------- KEYWORD MODE (NO QUERY) --------
                            for kw in keyword_list:
                                # Update progress before each call
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

                                # Store details for final log
                                last_search_details = {
                                    "completed_total": f"{current}/{total_calls}",
                                    "loc": _format_location(location),
                                    "headcount": headcount_bucket,
                                    "kw": kw,
                                    "found": cumulative_found,
                                }
                                
                                # Live status: SEARCHING with progress format
                                live_status.update(
                                    service="HUNTERIOV2",
                                    state="SEARCHING",
                                    **last_search_details,
                                )

                                append_hunter_run(
                                    request_id=inp.request_id,
                                    target=inp.target_prompt or "",
                                    country=(location or {}).get("country", "") if location else "",
                                    city=(location or {}).get("city") if location else None,
                                    headcount=headcount_bucket,
                                    keyword=kw,
                                    total_results=total,
                                    domains=domains,
                                    response=data,
                                )

                                results.append(
                                    {
                                        "location": location,
                                        "headcount": headcount_bucket,
                                        "keyword": kw,
                                        "response": data,
                                    }
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
                                    
                                    row = {
                                        "organization": organization,
                                        "domain": domain,
                                        "location": _fmt_location(location),
                                        "headcount": headcount_bucket.strip(),
                                        "term": term,
                                    }
                                    batch_rows.append(row)
                                    cleaned_rows.append(row)
                                
                                # Persist to database incrementally
                                if batch_rows:
                                    try:
                                        upsert_rows(conn, job_id or "", inp.request_id, batch_rows)
                                    except Exception as e:
                                        logger.error(f"Failed to persist {len(batch_rows)} rows to database: {e}")
                                        raise

                    else:
                        # -------- QUERY MODE (NO KEYWORD) --------
                        query = (inp.target_prompt or "").strip()
                        if not query:
                            continue

                        # Update progress before each call
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

                        # Store details for final log
                        last_search_details = {
                            "completed_total": f"{current}/{total_calls}",
                            "loc": _format_location(location),
                            "headcount": headcount_bucket,
                            "kw": query,
                            "found": cumulative_found,
                        }
                        
                        # Live status: SEARCHING with progress format
                        live_status.update(
                            service="HUNTERIOV2",
                            state="SEARCHING",
                            **last_search_details,
                        )

                        append_hunter_run(
                            request_id=inp.request_id,
                            target=query,
                            country=(location or {}).get("country", "") if location else "",
                            city=(location or {}).get("city") if location else None,
                            headcount=headcount_bucket,
                            keyword="",
                            total_results=total,
                            domains=domains,
                            response=data,
                        )

                        results.append(
                            {
                                "location": location,
                                "headcount": headcount_bucket,
                                "keyword": None,
                                "response": data,
                            }
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
                            
                            row = {
                                "organization": organization,
                                "domain": domain,
                                "location": _fmt_location(location),
                                "headcount": headcount_bucket.strip(),
                                "term": term,
                            }
                            batch_rows.append(row)
                            cleaned_rows.append(row)
                        
                        # Persist to database incrementally
                        if batch_rows:
                            try:
                                upsert_rows(conn, job_id or "", inp.request_id, batch_rows)
                            except Exception as e:
                                logger.error(f"Failed to persist {len(batch_rows)} rows to database: {e}")
                                raise

        # Final SEARCH status: make last search log permanent with all details
        if last_search_details:
            live_status.done(
                service="HUNTERIOV2",
                state="SEARCH",
                **last_search_details,
            )
        
        # Mark job as SUCCEEDED
        if job_id:
            try:
                mark_succeeded(
                    job_id,
                    message="Hunter run completed",
                    metrics={
                        "total_calls": total_calls,
                        "calls_done": current,
                        "results": len(results),
                    }
                )
                
                # Update Tool_Status sheet with SUCCEEDED state
                if status_writer:
                    status_writer.write(
                        job_id=job_id,
                        tool="hunter",
                        request_id=inp.request_id,
                        state="SUCCEEDED",
                    )
            except Exception as e:
                logger.warning("Failed to mark job as succeeded: %s", e)
        
        # Export to Google Sheets (after job success)
        if job_id:
            _export_to_sheets(conn, inp, job_id, keyword_mode)

            # DATABASE: log final persisted row count AFTER sheets export
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM hunterio_results WHERE job_id=%s",
                    (job_id,),
                )
                db_count = int(cur.fetchone()[0])

            logger.info(
                "HUNTERIOV2 | DATABASE | job_id=%s | rows=%d",
                job_id, db_count
            )

        return cleaned_rows


    except Exception as exc:
        # Mark job as FAILED
        if job_id:
            try:
                mark_failed(job_id, error=str(exc), message="Hunter run failed")
                
                # Update Tool_Status sheet with FAILED state
                if status_writer:
                    status_writer.write(
                        job_id=job_id,
                        tool="hunter",
                        request_id=inp.request_id,
                        state="FAILED",
                    )
            except Exception as e:
                logger.warning("Failed to mark job as failed: %s", e)
        
        # Live status: FAILED
        live_status.done(
            service="HUNTERIOV2",
            state="FAILED",
            request_id=inp.request_id,
            error=str(exc),
        )
        
        raise
    
    finally:
        # Always close the database connection
        conn.close()
