"""
Google Maps Places orchestrator module.

Coordinates the complete Google Maps Places discovery workflow:
1. Input validation
2. Job lifecycle management (running/succeeded/failed)
3. API calls via client + policy
4. Data persistence to database
5. Export coordination (delegates to sheets.exports)

This module focuses ONLY on Google Maps business logic and does NOT:
- Define sheet layouts (see sheets.specs.google_maps_leads)
- Fetch data for sheets (see sheets.data_fetchers.google_maps)
- Write to Google Sheets directly (see sheets.exports.google_maps)
- Sync to master (delegates to masterv2.service)

Architecture:
    ┌─────────────────────────────────────────┐
    │  orchestrator.py (THIS MODULE)          │
    │  - Validate input                       │
    │  - Manage job lifecycle                 │
    │  - Call Google Maps API                 │
    │  - Persist to DB                        │
    │  - Coordinate export                    │
    └─────────────────────────────────────────┘
                    │
                    ├──> client.py (HTTP calls)
                    ├──> policy.py (retry + rate limit)
                    ├──> repo.py (DB persistence)
                    └──> sheets.exports.google_maps (export coordination)
"""

from __future__ import annotations

import logging
import os
from typing import Any

from .client import GooglePlacesClient
from .models import GooglePlacesInput
from .policy import GoogleMapsAPIPolicy
from .repo import (
    ensure_table,
    upsert_batch_rows,
    finalize_request_job_id,
    append_places_page,
    append_places_location_summary,
)
from n8n_founderstories.core.logging.tags import log_db
from n8n_founderstories.core.utils.domain_eligibility import is_social_domain
from n8n_founderstories.core.utils.domain import normalize_domain
from n8n_founderstories.services.jobs.status_writer import StatusWriterLike, safe_status_write
from ...jobs.store import mark_running, update_progress, mark_succeeded, mark_failed
from n8n_founderstories.core.db import get_conn

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

FIELD_MASK = ",".join([
    "places.id",
    "places.displayName",
    "places.websiteUri",
    "places.formattedAddress",
    "places.types",
    "places.primaryType",
    "nextPageToken",
])

# Excluded place types - retail/service businesses that should be filtered out
EXCLUDED_PLACE_TYPES = {
    # Retail / marketplaces
    "supermarket",
    "grocery_store",
    "convenience_store",
    "department_store",
    "shopping_mall",
    "electronics_store",
    "clothing_store",
    "home_goods_store",

    # Automotive services
    "car_repair",
    "car_dealer",
    "gas_station",

    # Food service (not manufacturing)
    "restaurant",
    "cafe",
    "bakery",
    "bar",

    # Fitness / wellness services
    "gym",
    "fitness_center",
    "health_club",
    "yoga_studio",

    # Health services
    "hospital",
    "clinic",
    "pharmacy",
    "doctor",
    "dentist",

    # General services
    "real_estate_agency",
    "travel_agency",
    "bank",
    "atm",
    "lodging",
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


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


def _is_excluded_place(place: dict) -> bool:
    """
    Returns True if place is a retail/service business based on primaryType or types list.

    Checks against EXCLUDED_PLACE_TYPES which includes retail stores, service businesses,
    restaurants, gyms, health services, etc. - anything that's not a product manufacturer
    or brand company.

    Args:
        place: Place dict from API with primaryType and types fields

    Returns:
        True if place should be excluded (is a retail/service business)
    """
    # Check primaryType (case-insensitive)
    primary = (place.get("primaryType") or "").strip().lower()
    if primary in EXCLUDED_PLACE_TYPES:
        return True

    # Check types list (case-insensitive)
    types = place.get("types") or []
    if not isinstance(types, list):
        return False

    for t in types:
        if isinstance(t, str) and t.strip().lower() in EXCLUDED_PLACE_TYPES:
            return True

    return False


def _to_lead(place: dict[str, Any], text_query: str, location_path: str) -> dict[str, str] | None:
    """
    Convert Places API result to cleaned lead dict.
    Returns None if website is missing (filtering out no-website leads).

    Args:
        place: Place dict from API
        text_query: The search query used
        location_path: Formatted location path (e.g., "FR/Île-de-France/Paris")

    Returns:
        Dict with keys: query, location, organization, domain
        or None if website is missing
    """
    # Extract fields
    name = ""
    dn = place.get("displayName")
    if isinstance(dn, dict):
        name = (dn.get("text") or "").strip()

    website = (place.get("websiteUri") or "").strip()

    # Filter out leads without website
    if not website:
        return None

    # Normalize domain (following HunterIO pattern)
    domain = normalize_domain(website) or website.strip().lower()

    # Use name as organization, fallback to "Unknown" if no name
    organization = name if name else "Unknown"

    return {
        "query": text_query,
        "location": location_path,
        "organization": organization,
        "domain": domain,
    }


def _export_to_sheets(inp: GooglePlacesInput, job_id: str) -> None:
    """
    Coordinate export of Google Maps Places results to Google Sheets.

    This function delegates to the sheets.exports.google_maps module,
    which handles data fetching, formatting, and writing.

    Separation of concerns:
    - This function: Prepares export context (query_order, country_order)
    - sheets.exports.google_maps: Orchestrates fetch + write
    - sheets.data_fetchers.google_maps: Fetches and sorts data
    - sheets.specs.google_maps_leads: Defines sheet layout
    - sheets.writer: Writes to Google Sheets API

    Args:
        inp: GooglePlacesInput with search parameters and sheet_id
        job_id: Job identifier
    """
    # Check if export is configured
    sheet_id = (inp.sheet_id or os.getenv("GOOGLEMAPS_SHEET_ID", "")).strip()
    if not sheet_id:
        logger.debug("Export skipped: no sheet_id provided (inp.sheet_id or GOOGLEMAPS_SHEET_ID)")
        return

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

        # Delegate to export module (handles fetch + write)
        from n8n_founderstories.services.sheets.exports import google_maps as export_module

        row_count = export_module.export_to_sheet(
            job_id=job_id,
            sheet_id=sheet_id,
            context={
                "query_order": query_order,
                "country_order": country_order,
            }
        )

        logger.debug(f"Successfully exported {row_count} rows to sheet {sheet_id}")

    except Exception as e:
        # Log error but don't fail the job
        logger.error(f"Failed to export to Google Sheets: {e}", exc_info=True)


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

def run_google_places(
    *,
    inp: GooglePlacesInput,
    job_id: str | None = None,
    status_writer: StatusWriterLike = None,
) -> list[dict[str, str]]:
    """
    Run Google Maps Places search with full HunterIOV2-style features:
    - API policy (rate limiting + retries)
    - PostgreSQL persistence with merge logic
    - Google Sheets export
    - Job lifecycle management
    - Live status logging

    Flow:
    1. Phase 1: Build searchable locations from resolved_locations.geo.rectangle
    2. Phase 2: Search Google Places using Places API Text Search

    Args:
        inp: GooglePlacesInput with search parameters
        job_id: Optional job ID (generated if not provided)
        status_writer: Optional JobsSheetWriter for live status updates

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
        
        # job_id is now guaranteed to be set (non-optional)

        # Ensure database table exists
        ensure_table(conn, job_id)

        seen_map: dict[str, dict[str, str]] = {}        # Dedupe by domain (latest wins)
        locations = list(inp.resolved_locations) if inp.resolved_locations else []
        social_skipped_count = 0                        # Track social domains filtered out
        excluded_by_type = 0                            # Track places excluded by type
        duplicates_ignored = 0                          # Track identical duplicates (no change)
        duplicates_updated = 0                          # Track duplicates with data changes
        unique_added = 0                                # Track unique domains added
        current = 0                                     # Call counter (search calls only)

        # ========================================
        # PHASE 1: BUILD SEARCHABLE LOCATIONS
        # ========================================

        searchable: list[dict[str, Any]] = []
        geo_missing = 0

        for loc in locations:
            if not isinstance(loc, dict):
                continue

            geo = loc.get("geo") or {}
            if not isinstance(geo, dict):
                geo = {}

            rectangle = geo.get("rectangle")
            if not isinstance(rectangle, dict):
                geo_missing += 1
                logger.warning(
                    "GOOGLEMAPSV2 | SKIP_LOCATION_NO_RECTANGLE | request_id=%s | loc=%s",
                    inp.request_id,
                    _format_location_display(loc),
                )
                continue

            # Basic structural sanity check for rectangle format
            try:
                _ = float(rectangle["low"]["latitude"])
                _ = float(rectangle["low"]["longitude"])
                _ = float(rectangle["high"]["latitude"])
                _ = float(rectangle["high"]["longitude"])
            except Exception:
                geo_missing += 1
                logger.warning(
                    "GOOGLEMAPSV2 | SKIP_LOCATION_BAD_RECTANGLE | request_id=%s | loc=%s | rectangle=%r",
                    inp.request_id,
                    _format_location_display(loc),
                    rectangle,
                )
                continue

            searchable.append({
                "loc": loc,
                "rectangle": rectangle,
                "location_path": _format_location_path(loc),
            })

        logger.info(
            "GOOGLEMAPSV2 | GEO_READY | ok=%d | skipped_no_geo=%d | total=%d",
            len(searchable),
            geo_missing,
            len(locations),
        )

        # Compute total API calls for progress tracking
        # Places searches: len(searchable) * len(queries) * max_pages (worst case)
        total_search_calls = len(searchable) * len(inp.places_text_queries) * int(inp.max_pages)
        total_calls = total_search_calls  # geocoding removed: rectangles are precomputed in search_plan

        # Initialize progress throttler (log every 5 seconds)
        from n8n_founderstories.core.logging.progress import ProgressThrottler
        progress_throttler = ProgressThrottler(interval_s=5.0)

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
                        "locations": len(searchable),
                        "queries": len(inp.places_text_queries),
                        "total_calls": total_calls,
                    }
                )

            except Exception as e:
                logger.warning("Failed to update job progress: %s", e)

        # Live status: STARTED
        logger.info(
            "GOOGLEMAPSV2 | STATE=START | job_id=%s | request_id=%s | locations=%d | queries=%d | max_pages=%d | max_search_calls=%d",
            job_id,
            inp.request_id,
            len(searchable),
            len(inp.places_text_queries),
            inp.max_pages,
            total_search_calls,
        )

        # ========================================
        # PHASE 2: SEARCH (Google Places)
        # ========================================

        with GooglePlacesClient() as places:
            for item in searchable:
                loc = item["loc"]
                rectangle = item["rectangle"]
                location_path = item["location_path"]
                
                # Extract location components once per location
                country = (loc.get("country") or "").strip()
                state = (loc.get("state") or "").strip() or None
                city = (loc.get("city") or "").strip() or None

                # Loop over each text query
                for text_query in inp.places_text_queries:
                    query_location_returned = 0
                    query_location_kept = 0

                    page_token: str | None = None
                    for page_idx in range(int(inp.max_pages)):
                        page_no = page_idx + 1

                        # Update progress for search (increment before call to start at 1)
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

                                # Skip excluded place types (retail/service businesses)
                                if _is_excluded_place(p):
                                    excluded_by_type += 1
                                    continue

                                lead = _to_lead(p, text_query, location_path)

                                # Skip leads without website
                                if lead is None:
                                    continue

                                # Get normalized domain from lead
                                domain = lead.get("domain", "")
                                if not domain:
                                    continue
                                
                                # Skip social media domains (Facebook, Instagram)
                                if is_social_domain(domain):
                                    social_skipped_count += 1
                                    continue
                                
                                # Dedupe in-memory: latest wins, but only UPSERT if data changed
                                prev = seen_map.get(domain)
                                if prev is not None:
                                    # Check if data is identical (skip UPSERT if so)
                                    if prev == lead:
                                        duplicates_ignored += 1
                                        continue
                                    # Data changed: update seen_map and UPSERT to DB
                                    duplicates_updated += 1
                                    seen_map[domain] = lead
                                    batch_rows.append(lead)
                                    # Don't add to page_leads (not a new unique domain)
                                    continue
                                
                                # New unique domain: add to all collections
                                unique_added += 1
                                seen_map[domain] = lead
                                batch_rows.append(lead)
                                page_leads.append(lead)
                                page_kept += 1

                            # Persist to database using UPSERT
                            if batch_rows:
                                try:
                                    upsert_batch_rows(conn, inp.request_id, job_id, batch_rows)
                                except Exception as e:
                                    logger.error(f"Failed to persist {len(batch_rows)} rows to database: {e}")
                                    raise

                        query_location_returned += page_returned
                        query_location_kept += page_kept

                        # Log page results
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

                        # Throttled progress logging (every ~5 seconds)
                        if progress_throttler.should_log():
                            logger.info(
                                "GOOGLEMAPSV2 | PROGRESS | current=%d | total=%d | unique=%d | loc=%s | query=%r",
                                current,
                                total_calls,
                                unique_added,
                                country,
                                text_query,
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
        
        # Convert seen_map to cleaned_rows (deduped output)
        cleaned_rows = list(seen_map.values())

        # Finalize database: update all rows to latest job_id
        db_count = 0
        if job_id:
            try:
                db_count = finalize_request_job_id(conn, inp.request_id, job_id)
                logger.info(f"GOOGLEMAPSV2 | FINALIZED | request_id={inp.request_id} | job_id={job_id} | total_rows={db_count}")
            except Exception as e:
                logger.error(f"Failed to finalize database: {e}")
                raise

        # Mark job as SUCCEEDED (metrics match HunterIO naming for consistency)
        if job_id:
            try:
                # Calculate extracted_rows: all rows that passed filters (including duplicates)
                extracted_rows = unique_added + duplicates_ignored + duplicates_updated
                
                mark_succeeded(
                    job_id,
                    message="Google Maps Places run completed",
                    metrics={
                        "total_calls": total_calls,
                        "calls_done": current,
                        "extracted_rows": extracted_rows,
                        "extracted_unique_domains": len(cleaned_rows),
                        "db_rows": db_count,
                        "excluded_by_type": excluded_by_type,
                        "social_domains_skipped": social_skipped_count,
                        "duplicates_ignored": duplicates_ignored,
                        "duplicates_updated": duplicates_updated,
                    }
                )

                # Update Tool_Status sheet with SUCCEEDED state
                safe_status_write(
                    status_writer,
                    job_id=job_id,
                    tool="google_maps",
                    request_id=inp.request_id,
                    state="SUCCEEDED",
                )
            except Exception as e:
                logger.warning("Failed to mark job as succeeded: %s", e)

        # DATABASE log
        log_db(logger, service="GOOGLEMAPSV2", table="gmaps_results", rows=db_count)

        # Export to Google Sheets (write_rows handles SHEETS logging)
        sheets_count = 0
        if job_id:
            _export_to_sheets(inp, job_id)
            # Get sheets count (same as db_count for this service)
            sheets_count = db_count

        # COMPLETED log (end-of-job summary with deduplication stats)
        # IMPORTANT: Log this BEFORE master sync so it appears first in logs
        logger.info(
            "GOOGLEMAPSV2 | STATE=COMPLETED | job_id=%s | unique_domains=%d | db_rows=%d | sheets_rows=%d | excluded_by_type=%d | social_skipped=%d | duplicates_ignored=%d | duplicates_updated=%d",
            job_id, len(cleaned_rows), db_count, sheets_count, excluded_by_type, social_skipped_count, duplicates_ignored, duplicates_updated
        )

        # Sync to master consolidation table
        if job_id:
            try:
                from ...master.service import sync_from_source
                # Pass sheet_id and request_id to enable automatic master sheet export
                master_stats = sync_from_source(
                    "google_maps",
                    job_id,
                    sheet_id=inp.sheet_id,
                    request_id=inp.request_id
                )
                logger.info(
                    "GOOGLEMAPSV2 | MASTER_SYNC | job_id=%s | seen=%d | upserted=%d",
                    job_id,
                    master_stats.get("seen", 0),
                    master_stats.get("upserted", 0)
                )
            except Exception as e:
                # Log error but don't fail the job
                logger.error("GOOGLEMAPSV2 | MASTER_SYNC_ERROR | job_id=%s | error=%s", job_id, str(e))

        return cleaned_rows

    except Exception as exc:
        # Mark job as FAILED
        if job_id:
            try:
                mark_failed(job_id, error=str(exc), message="Google Maps Places run failed")

                # Update Tool_Status sheet with FAILED state
                safe_status_write(
                    status_writer,
                    job_id=job_id,
                    tool="google_maps",
                    request_id=inp.request_id,
                    state="FAILED",
                )
            except Exception as e:
                logger.warning("Failed to mark job as failed: %s", e)

        # Log failure
        logger.error(
            "GOOGLEMAPSV2 | STATE=FAILED | request_id=%s | error=%s",
            inp.request_id,
            str(exc),
        )

        raise

    finally:
        # Always close the database connection
        conn.close()
