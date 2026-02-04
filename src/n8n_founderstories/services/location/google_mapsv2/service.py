from __future__ import annotations

import logging
import os
from typing import Any, Optional

from .client import GoogleGeocodingClient, GooglePlacesClient
from .models import GooglePlacesInput
from .api_policy import GoogleMapsAPIPolicy
from .run_log import append_places_page, append_places_location_summary, append_geocode_result
from .repository import ensure_table, upsert_rows
from ....core.logging.tags import log_db
from . import sheets_spec
from ...jobs.store import mark_running, update_progress, mark_succeeded, mark_failed
from ....core.db import get_conn
from ....core.logging import get_live_status_logger
from ....exportsv2.writer import write_rows

logger = logging.getLogger(__name__)

FIELD_MASK = ",".join([
    "places.id",
    "places.displayName",
    "places.websiteUri",
    "places.editorialSummary",
    "places.formattedAddress",
    "nextPageToken",
])


def _location_to_geocode_address(loc: dict[str, Any]) -> Optional[str]:
    """
    Build geocoding address from location dict.
    
    Priority:
    - city + country_name => "city, country_name"
    - state + country_name => "state, country_name"
    - country_name => "country_name"
    
    Args:
        loc: Location dict with city, state, country_name fields
        
    Returns:
        Geocoding address string or None if insufficient data
    """
    city = (loc.get("city") or "").strip() if isinstance(loc.get("city"), str) else ""
    state = (loc.get("state") or "").strip() if isinstance(loc.get("state"), str) else ""
    country_name = (loc.get("country_name") or "").strip() if isinstance(loc.get("country_name"), str) else ""

    if city and country_name:
        return f"{city}, {country_name}"
    if state and country_name:
        return f"{state}, {country_name}"
    if country_name:
        return country_name

    # continent-only is too vague → skip
    return None


def _viewport_to_rectangle(viewport: dict[str, Any]) -> dict[str, Any]:
    """Convert geocoding viewport to Places API rectangle format."""
    ne = viewport.get("northeast") or {}
    sw = viewport.get("southwest") or {}
    return {
        "low":  {"latitude": float(sw["lat"]), "longitude": float(sw["lng"])},
        "high": {"latitude": float(ne["lat"]), "longitude": float(ne["lng"])},
    }


def _extract_viewport(geo_json: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Extract viewport from geocoding response."""
    results = geo_json.get("results") or []
    if not isinstance(results, list) or not results:
        return None
    r0 = results[0]
    if not isinstance(r0, dict):
        return None
    geom = r0.get("geometry") or {}
    if not isinstance(geom, dict):
        return None
    viewport = geom.get("viewport")
    return viewport if isinstance(viewport, dict) else None


def _format_location_path(loc: dict[str, Any]) -> str:
    """
    Format location as hierarchical path: country/state/city.
    
    Examples:
        {"country": "FR"} -> "FR"
        {"country": "FR", "state": "Île-de-France"} -> "FR/Île-de-France"
        {"country": "FR", "state": "Île-de-France", "city": "Paris"} -> "FR/Île-de-France/Paris"
        {} -> ""
    
    Args:
        loc: Location dict
        
    Returns:
        Formatted location path
    """
    parts = []
    
    country = (loc.get("country") or "").strip()
    if country:
        parts.append(country)
    
    state = (loc.get("state") or "").strip()
    if state:
        parts.append(state)
    
    city = (loc.get("city") or "").strip()
    if city:
        parts.append(city)
    
    return "/".join(parts)


def _format_location_display(loc: dict[str, Any] | None) -> str:
    """Format location for live status display."""
    if not loc:
        return "NONE"
    
    parts = []
    country = (loc.get("country") or "").strip()
    state = (loc.get("state") or "").strip()
    city = (loc.get("city") or "").strip()
    
    if country:
        parts.append(f"COUNTRY:{country}")
    if state:
        parts.append(f"STATE:{state}")
    if city:
        parts.append(f"CITY:{city}")
    
    return ",".join(parts) if parts else "NONE"


def _to_lead(place: dict[str, Any], text_query: str, location_path: str) -> dict[str, str] | None:
    """
    Convert Places API result to cleaned lead dict.
    Returns None if website is missing (filtering out no-website leads).
    
    Args:
        place: Place dict from API
        text_query: The search query used
        location_path: Formatted location path (e.g., "FR/Île-de-France/Paris")
        
    Returns:
        Dict with keys: text_query, location, organization, website, description
        or None if website is missing
    """
    # Extract fields
    name = ""
    dn = place.get("displayName")
    if isinstance(dn, dict):
        name = (dn.get("text") or "").strip()

    desc = ""
    es = place.get("editorialSummary")
    if isinstance(es, dict):
        desc = (es.get("text") or "").strip()
    
    website = (place.get("websiteUri") or "").strip()
    
    # Filter out leads without website
    if not website:
        return None
    
    # Use name as organization, fallback to "Unknown" if no name
    organization = name if name else "Unknown"
    
    return {
        "text_query": text_query,
        "location": location_path,
        "organization": organization,
        "website": website,
        "description": desc,
    }


def _export_to_sheets(conn, inp: GooglePlacesInput, job_id: str) -> None:
    """
    Export Google Maps Places results to Google Sheets after job completion.
    
    Args:
        conn: Active database connection
        inp: GooglePlacesInput with search parameters
        job_id: Job identifier
    """
    # Check if export is configured
    sheet_id = (inp.sheet_id or os.getenv("GOOGLEMAPS_SHEET_ID", "")).strip()
    if not sheet_id:
        logger.debug("Export skipped: no sheet_id provided (inp.sheet_id or GOOGLEMAPS_SHEET_ID)")
        return
    
    # Get tab name (with optional override)
    tab_name = os.getenv("GOOGLEMAPS_SHEET_TAB", sheets_spec.TAB_NAME).strip()
    
    try:
        # Build query order from input
        query_order = [q.strip() for q in inp.places_text_queries if isinstance(q, str) and q.strip()]
        
        # Build country order from locations
        country_order = []
        seen_countries = set()
        for loc in (inp.resolved_locations or []):
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
            query_order=query_order,
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
            service="GOOGLEMAPSV2",
            job_id=job_id,
        )
        
        logger.debug(f"Successfully exported {len(rows)} rows to sheet {sheet_id}, tab '{tab_name}'")
        
    except Exception as e:
        # Log error but don't fail the job
        logger.error(f"Failed to export to Google Sheets: {e}", exc_info=True)


def run_google_places(
    *,
    inp: GooglePlacesInput,
    job_id: str | None = None,
    status_writer = None,
) -> list[dict[str, str]]:
    """
    Run Google Maps Places search with full HunterIOV2-style features:
    - API policy (rate limiting + retries)
    - PostgreSQL persistence with upsert
    - Google Sheets export
    - Job lifecycle management
    - Live status logging
    
    Flow:
    1. Phase 1: Geocode all locations first
    2. Phase 2: Search Google Places using geocoded locations
    
    Args:
        inp: GooglePlacesInput with search parameters
        job_id: Optional job ID (generated if not provided)
        status_writer: Optional JobsSheetWriter for live status updates (unused - service manages its own state)
        
    Returns:
        List of cleaned lead dicts
    """
    policy = GoogleMapsAPIPolicy()
    inp.validate()
    
    # Open database connection for this run
    conn = get_conn()
    
    try:
        # Generate job_id if not provided
        if not job_id:
            from uuid import uuid4
            job_id = f"gmap__{uuid4().hex}"
        
        # Ensure database table exists
        ensure_table(conn, job_id)

        cleaned_rows: list[dict[str, str]] = []
        locations = list(inp.resolved_locations) if inp.resolved_locations else []

        # Compute total API calls for progress tracking
        # Geocoding: 1 per location
        # Places searches: len(locations) * len(queries) * max_pages (worst case)
        total_geocode_calls = len(locations)
        total_search_calls = len(locations) * len(inp.places_text_queries) * inp.max_pages
        total_calls = total_geocode_calls + total_search_calls
        current = 0
        kept_total = 0  # Track cumulative kept results
        
        # Initialize live status logger and register it globally
        live_status = get_live_status_logger()

        # Mark job as RUNNING and initialize progress
        if job_id:
            try:
                mark_running(job_id)
                update_progress(
                    job_id,
                    phase="googlemapsv2",
                    current=0,
                    total=total_calls,
                    message="Starting Google Maps Places run",
                    metrics={
                        "locations": len(locations),
                        "queries": len(inp.places_text_queries),
                        "total_calls": total_calls,
                    }
                )
                
                # Write initial status to Tool_Status sheet
                if status_writer:
                    status_writer.write(
                        job_id=job_id,
                        tool="google_maps",
                        request_id=inp.request_id,
                        state="RUNNING",
                    )
            except Exception as e:
                logger.warning("Failed to update job progress: %s", e)
        
        # Live status: STARTED
        logger.info(
            "GOOGLEMAPSV2 | STATE=START | job_id=%s | request_id=%s | locations=%d | queries=%d | max_pages=%d | max_search_calls=%d",
            job_id,
            inp.request_id,
            len(locations),
            len(inp.places_text_queries),
            inp.max_pages,
            total_search_calls,
        )
        
        # ========================================
        # PHASE 1: GEOCODING (all locations first)
        # ========================================
        
        geocoded = []
        geocode_failed = 0
        
        with GoogleGeocodingClient() as geo:
            for idx, loc in enumerate(locations, start=1):
                if not isinstance(loc, dict):
                    continue

                addr = _location_to_geocode_address(loc)
                if not addr:
                    continue

                # Update progress for geocoding
                current += 1
                if job_id:
                    try:
                        update_progress(
                            job_id,
                            phase="googlemapsv2",
                            current=current,
                            total=total_calls,
                            message=f"Geocoding locations ({idx}/{len(locations)})",
                            metrics={
                                "location": _format_location_display(loc),
                            }
                        )
                    except Exception as e:
                        logger.warning("Failed to update progress: %s", e)

                # Extract location parts for logging
                country = (loc.get("country") or "").strip()
                state = (loc.get("state") or "").strip() or None
                city = (loc.get("city") or "").strip() or None
                
                # Live status: GEOCODE (live update on same line)
                live_status.update(
                    service="GOOGLEMAPSV2",
                    state="GEOCODE",
                    location=_format_location_display(loc),
                    locations=f"{idx}/{len(locations)}",
                )

                # Geocode with API policy
                geo_json = policy.call(
                    fn=lambda a=addr: geo.geocode(address=a),
                    request_id=inp.request_id,
                    label="GEOCODE",
                )
                
                viewport = _extract_viewport(geo_json)
                
                if not viewport:
                    logger.warning(f"No viewport for location: {loc}")
                    geocode_failed += 1
                    
                    # Log geocoding failure
                    append_geocode_result(
                        request_id=inp.request_id,
                        country=country,
                        state=state,
                        city=city,
                        address=addr,
                        success=False,
                        error_msg="no viewport",
                    )
                    continue

                rectangle = _viewport_to_rectangle(viewport)
                location_path = _format_location_path(loc)
                
                # Log successful geocoding
                append_geocode_result(
                    request_id=inp.request_id,
                    country=country,
                    state=state,
                    city=city,
                    address=addr,
                    success=True,
                )
                
                # Store successful geocoding result
                geocoded.append({
                    "loc": loc,
                    "rectangle": rectangle,
                    "location_path": location_path,
                })
        
        # Live status: GEOCODE DONE (final summary)
        live_status.done(
            service="GOOGLEMAPSV2",
            state="GEOCODE",
            ok=len(geocoded),
            failed=geocode_failed,
            total=len(locations),
        )
        
        # ========================================
        # PHASE 2: SEARCH (Google Places)
        # ========================================
        
        # Track last search details for final log
        last_search_details = {}
        
        with GooglePlacesClient() as places:
            for geo_item in geocoded:
                loc = geo_item["loc"]
                rectangle = geo_item["rectangle"]
                location_path = geo_item["location_path"]
                
                # Loop over each text query
                for text_query in inp.places_text_queries:
                    query_location_returned = 0
                    query_location_kept = 0

                    page_token: str | None = None
                    for page_idx in range(int(inp.max_pages)):
                        page_no = page_idx + 1
                        
                        # Update progress for search
                        current += 1
                        if job_id:
                            try:
                                update_progress(
                                    job_id,
                                    phase="googlemapsv2",
                                    current=current,
                                    total=total_calls,
                                    message=f"Searching places ({current}/{total_calls})",
                                    metrics={
                                        "location": _format_location_display(loc),
                                        "query": text_query,
                                        "page": page_no,
                                    }
                                )
                            except Exception as e:
                                logger.warning("Failed to update progress: %s", e)
                        
                        # Search with API policy
                        data = policy.call(
                            fn=lambda: places.search_text(
                                text_query=text_query,
                                language_code=inp.language,
                                include_pure_service_area=inp.include_pure_service_area,
                                page_size=inp.page_size,
                                page_token=page_token,
                                location_restriction_rectangle=rectangle,
                                field_mask=FIELD_MASK,
                            ),
                            request_id=inp.request_id,
                            label="SEARCH_TEXT",
                        )

                        places_list = data.get("places") or []
                        page_leads = []
                        page_returned = 0
                        page_kept = 0

                        if isinstance(places_list, list):
                            page_returned = len(places_list)
                            batch_rows: list[dict[str, str]] = []
                            
                            for p in places_list:
                                if not isinstance(p, dict):
                                    continue
                                
                                lead = _to_lead(p, text_query, location_path)
                                
                                # Skip leads without website
                                if lead is None:
                                    continue
                                
                                batch_rows.append(lead)
                                cleaned_rows.append(lead)
                                page_leads.append(lead)
                                page_kept += 1
                            
                            # Persist to database incrementally
                            if batch_rows:
                                try:
                                    upsert_rows(conn, job_id or "", inp.request_id, batch_rows)
                                except Exception as e:
                                    logger.error(f"Failed to persist {len(batch_rows)} rows to database: {e}")
                                    raise

                        query_location_returned += page_returned
                        query_location_kept += page_kept
                        kept_total += page_kept

                        # Log page results
                        country = (loc.get("country") or "").strip()
                        state = (loc.get("state") or "").strip() or None
                        city = (loc.get("city") or "").strip() or None
                        
                        append_places_page(
                            request_id=inp.request_id,
                            text_query=text_query,
                            language=inp.language,
                            page_size=inp.page_size,
                            max_pages=inp.max_pages,
                            country=country,
                            state=state,
                            city=city,
                            page_no=page_no,
                            returned=page_returned,
                            kept=page_kept,
                            leads_preview=page_leads,
                            response=None,
                        )

                        # Live status: SEARCH page results
                        # Calculate search call number (current - geocode calls)
                        search_call_num = current - total_geocode_calls
                        
                        # Store details for final log
                        last_search_details = {
                            "completed_total": f"{search_call_num}/{total_search_calls}",
                            "loc": _format_location_display(loc),
                            "query": text_query,
                            "page": f"{page_no}/{inp.max_pages}",
                            "found": kept_total,
                        }
                        
                        live_status.update(
                            service="GOOGLEMAPSV2",
                            state="SEARCH",
                            **last_search_details,
                        )

                        page_token = (data.get("nextPageToken") or "").strip() or None
                        if not page_token:
                            break

                    # Log location summary
                    append_places_location_summary(
                        request_id=inp.request_id,
                        text_query=text_query,
                        country=country,
                        state=state,
                        city=city,
                        total_returned=query_location_returned,
                        total_kept=query_location_kept,
                    )
        
        # Final search status: make last search log permanent with all details
        if last_search_details:
            live_status.done(
                service="GOOGLEMAPSV2",
                state="SEARCH",
                **last_search_details,
            )

        # Mark job as SUCCEEDED
        if job_id:
            try:
                mark_succeeded(
                    job_id,
                    message="Google Maps Places run completed",
                    metrics={
                        "total_calls": total_calls,
                        "calls_done": current,
                        "results": len(cleaned_rows),
                    }
                )
                
                # Update Tool_Status sheet with SUCCEEDED state
                if status_writer:
                    status_writer.write(
                        job_id=job_id,
                        tool="google_maps",
                        request_id=inp.request_id,
                        state="SUCCEEDED",
                    )
            except Exception as e:
                logger.warning("Failed to mark job as succeeded: %s", e)
        
        # Export to Google Sheets (after job success)
        if job_id:
            _export_to_sheets(conn, inp, job_id)

            # DATABASE COMPLETED: log final persisted row count AFTER sheets export
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM googlemaps_places_results WHERE job_id=%s",
                    (job_id,),
                )
                db_count = int(cur.fetchone()[0])

            logger.info(
                "GOOGLEMAPSV2 | DATABASE | job_id=%s | rows=%d",
                job_id, db_count
            )

        return cleaned_rows

    except Exception as exc:
        # Mark job as FAILED
        if job_id:
            try:
                mark_failed(job_id, error=str(exc), message="Google Maps Places run failed")
                
                # Update Tool_Status sheet with FAILED state
                if status_writer:
                    status_writer.write(
                        job_id=job_id,
                        tool="google_maps",
                        request_id=inp.request_id,
                        state="FAILED",
                    )
            except Exception as e:
                logger.warning("Failed to mark job as failed: %s", e)
        
        # Live status: FAILED
        live_status.done(
            service="GOOGLEMAPSV2",
            state="FAILED",
            request_id=inp.request_id,
            error=str(exc),
        )
        
        raise
    
    finally:
        # Always close the database connection
        conn.close()
