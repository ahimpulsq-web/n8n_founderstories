from __future__ import annotations

import json
import logging
import math
from datetime import datetime
from typing import Any

from ....core.utils.text import norm
from ....core.config import settings
from ....services.exports.sheets import SheetsClient, default_sheets_config
from ....services.exports.sheets_exporter import export_google_maps_results
from ....services.exports.sheets_schema import TAB_STATUS
from ....services.jobs.logging import job_logger
from ....services.jobs.sheets_status import ToolStatusWriter
from ....services.jobs.store import mark_failed, mark_running, mark_succeeded, update_progress
from ....services.search_plan import SearchPlan
from ....services.storage import save_google_maps_output
from ..errors import LocationProviderError
from .client import GooglePlacesClient
from .models import GoogleMapsJobResult, GoogleMapsPlace, GoogleMapsRunResult
from .repos import (
    GoogleMapsResultRow,
    GoogleMapsResultsRepository,
    GoogleMapsEnrichQueueRow,
    GoogleMapsAuditRow,
    safe_insert_google_maps_results,
    safe_insert_google_maps_enrich_queue,
    safe_insert_google_maps_audit,
    convert_db_results_to_sheets_format,
    convert_db_audit_to_sheets_format,
)

logger = logging.getLogger(__name__)

# Note: TAB_STATUS imported from sheets_schema.py
# Note: GoogleMaps_v2 and GoogleMaps_Audit_v2 tabs are created only at export time

HARD_MAX_PER_RUN = 20
STATUS_EVERY_N = 5

# Note: Headers are now defined in sheets_exporter.py (single source of truth)


def _utc_iso() -> str:
    return datetime.utcnow().isoformat()


def _types_csv(types: Any) -> str:
    if not isinstance(types, list):
        return norm(types)
    return ", ".join([norm(x) for x in types if norm(x)])


def _maps_url_from_place_id(place_id: str) -> str:
    pid = norm(place_id)
    if not pid:
        return ""
    return f"https://www.google.com/maps/search/?api=1&query_place_id={pid}"


def _looks_like_language_tag(v: str) -> bool:
    s = norm(v)
    if not s or len(s) > 10:
        return False
    return all(ch.isalpha() or ch == "-" for ch in s)


def _resolve_language(*, hl_plan: str | None, default_lang: str = "en") -> str:
    hl = norm(hl_plan)
    return hl if hl and _looks_like_language_tag(hl) else default_lang


def _should_write_status(step: int, total: int | None, every_n: int = STATUS_EVERY_N) -> bool:
    if step <= 0:
        return True
    if total and step >= total:
        return True
    return step % max(1, int(every_n)) == 0


def _select_locations_from_geo(*, geo_location_keywords: Any, max_locations: int) -> list[tuple[str, str | None, str]]:
    out: list[tuple[str, str | None, str]] = []
    seen: set[str] = set()
    if not isinstance(geo_location_keywords, dict):
        return out

    cap = max(0, int(max_locations))

    for iso2_raw, bucket in geo_location_keywords.items():
        iso2 = norm(str(iso2_raw)).lower()
        if len(iso2) != 2 or not iso2.isalpha():
            continue
        if not isinstance(bucket, dict):
            continue

        hl_plan = norm(bucket.get("hl")) or None
        locs = bucket.get("locations")
        if not isinstance(locs, list) or not locs:
            continue

        cleaned = [norm(str(x)) for x in locs if norm(str(x))]
        if not cleaned:
            continue

        selected: list[str] = [cleaned[0]]
        if cap > 0:
            selected.extend(cleaned[1 : 1 + cap])

        for loc in selected:
            key = f"{iso2}|{loc.lower()}"
            if key in seen:
                continue
            seen.add(key)
            out.append((iso2, hl_plan, loc))

    return out


def run_google_maps_discover_job(
    *,
    job_id: str,
    plan: SearchPlan,
    spreadsheet_id: str,
    max_queries: int = 5,
    max_locations: int = 3,
    max_results: int = 250,
    dedupe_places: bool = True,
    enqueue_for_enrich: bool = True,
    trigger_master: bool = True,
) -> None:
    rid = norm(getattr(plan, "request_id", None))
    log = job_logger(__name__, tool="google_maps_discover", request_id=rid, job_id=job_id)

    runs_done = 0
    total_runs_est: int | None = None
    unique_new_places = 0

    sheets: SheetsClient | None = None
    status: ToolStatusWriter | None = None

    seen_place_ids_run: set[str] = set()

    try:
        mark_running(job_id)

        raw_prompt = norm(getattr(plan, "raw_prompt", None))
        provider_name = norm(getattr(plan, "provider_name", None)) or None
        geo = norm(getattr(plan, "geo", None)) or None

        if not rid:
            raise ValueError("search_plan.request_id must not be empty.")
        if not raw_prompt:
            raise ValueError("search_plan.raw_prompt must not be empty.")

        sid = norm(spreadsheet_id)
        if not sid:
            raise ValueError("spreadsheet_id must not be empty.")

        mq = max(1, int(max_queries))
        ml = max(0, int(max_locations))
        mr = int(max_results)
        if mr <= 0:
            raise ValueError("max_results must be >= 1.")

        base_queries = [norm(q) for q in (getattr(plan, "maps_queries", None) or []) if norm(q)]
        base_queries = base_queries[:mq]
        if not base_queries:
            raise ValueError("search_plan.maps_queries must not be empty.")

        locations = _select_locations_from_geo(
            geo_location_keywords=getattr(plan, "geo_location_keywords", None),
            max_locations=ml,
        )
        if not locations:
            raise ValueError("search_plan.geo_location_keywords is missing/invalid; cannot build locations.")

        total_runs_est = len(base_queries) * len(locations)

        # Initialize sheets client - only for Tool_Status
        sheets = SheetsClient(config=default_sheets_config(spreadsheet_id=sid))
        # Only create Tool_Status tab at runtime (exception to export-only rule)
        sheets.ensure_tab(TAB_STATUS)
        
        # Always initialize status writer for Tool_Status updates
        status = ToolStatusWriter(sheets=sheets, spreadsheet_id=sid)

        result = GoogleMapsJobResult(
            request_id=rid,
            raw_prompt=raw_prompt,
            provider_name=provider_name,
            geo=geo,
            max_queries=mq,
            max_locations=ml,
            max_results=mr,
            dedupe_places=bool(dedupe_places),
            enrich=False,
            queries_used=list(base_queries),
            locations_used=[loc for (_, _, loc) in locations],
            iso2_used=sorted({iso2 for (iso2, _, _) in locations}),
            total_runs_est=total_runs_est,
            runs_done=0,
            total_unique_places=0,
            total_enriched_places=0,
        )

        update_progress(
            job_id,
            phase="discover",
            current=0,
            total=total_runs_est,
            message="Starting Google Maps discovery.",
            metrics={"queries": len(base_queries), "locations": len(locations), "max_results": mr},
        )

        status.write(
            job_id=job_id,
            tool="google_maps_discover",
            request_id=rid,
            state="RUNNING",
            phase="discover",
            current=0,
            total=mr,
            message="Starting Google Maps discovery.",
            meta={"enqueue_for_enrich": enqueue_for_enrich, "total_runs_est": total_runs_est, "runs_done": runs_done},
        )

        # Database buffers (always used - DB-first approach)
        db_result_rows: list[GoogleMapsResultRow] = []
        db_queue_rows: list[GoogleMapsEnrichQueueRow] = []
        db_audit_rows: list[GoogleMapsAuditRow] = []

        with GooglePlacesClient() as client:
            total_runs_remaining = total_runs_est

            for iso2, hl_plan, loc_label in locations:
                if unique_new_places >= mr:
                    break

                language_used = _resolve_language(hl_plan=hl_plan, default_lang="en")
                region_param = iso2

                for base_q in base_queries:
                    if unique_new_places >= mr:
                        break

                    take_n = 0
                    returned_count = 0
                    eligible_after = 0
                    appended_this_run = 0
                    run_err = ""
                    stop_reason = ""

                    try:
                        remaining_needed = mr - unique_new_places
                        total_runs_remaining = max(1, int(total_runs_remaining))

                        take_n = int(math.ceil(remaining_needed / max(1, total_runs_remaining)))
                        take_n = min(max(1, take_n), HARD_MAX_PER_RUN)
                        take_n = min(take_n, remaining_needed)

                        final_query = f"{base_q} in {loc_label}"

                        run = GoogleMapsRunResult(
                            iso2=iso2.upper(),
                            hl_plan=hl_plan,
                            language_used=language_used,
                            location_label=loc_label,
                            base_query=base_q,
                            final_query=final_query,
                            region_param=region_param,
                            take_n=take_n,
                        )

                        data = client.text_search(query=final_query, language=language_used, region=region_param)

                        raw_results = data.get("results") or []
                        if not isinstance(raw_results, list):
                            raw_results = []

                        returned_count = len(raw_results)
                        run.returned_count = returned_count

                        eligible: list[GoogleMapsPlace] = []
                        for item in raw_results:
                            if not isinstance(item, dict):
                                continue

                            p = GoogleMapsPlace(
                                name=item.get("name"),
                                place_id=item.get("place_id"),
                                formatted_address=item.get("formatted_address"),
                                types=item.get("types") or [],
                                business_status=item.get("business_status"),
                                rating=item.get("rating"),
                                user_ratings_total=item.get("user_ratings_total"),
                                geometry=item.get("geometry"),
                                google_maps_url=_maps_url_from_place_id(item.get("place_id") or ""),
                                raw=item,
                            )

                            # CRITICAL: Use canonical place_id for storage, separate dedupe key
                            # place_id must NEVER be mutated - it's an opaque identifier from Google
                            canonical_place_id = norm(p.place_id)
                            dedupe_key = canonical_place_id.lower() if canonical_place_id else ""
                            
                            if dedupe_places and dedupe_key and dedupe_key in seen_place_ids_run:
                                continue

                            eligible.append(p)
                            if len(eligible) >= take_n:
                                break

                        eligible_after = len(eligible)
                        run.eligible_after_dedupe = eligible_after

                        # Buffer main rows + queue rows with CORRECT iso2/hl
                        ts = _utc_iso()
                        for p in eligible:
                            # CRITICAL: Use canonical place_id, separate dedupe key
                            canonical_place_id = norm(p.place_id)
                            dedupe_key = canonical_place_id.lower() if canonical_place_id else ""
                            
                            if dedupe_places and dedupe_key:
                                seen_place_ids_run.add(dedupe_key)

                            # Extract lat/lng from geometry
                            lat, lng = None, None
                            if p.geometry and isinstance(p.geometry, dict):
                                location = p.geometry.get("location", {})
                                if isinstance(location, dict):
                                    lat = location.get("lat")
                                    lng = location.get("lng")

                            # Database row (always buffer for DB-first)
                            # Use new column names: organisation, type, search_query
                            # CRITICAL: Store canonical place_id exactly as received from Google
                            db_result_rows.append(
                                GoogleMapsResultRow.from_runner_data(
                                    job_id=job_id,
                                    request_id=rid,
                                    place_id=canonical_place_id,
                                    organisation=norm(p.name),  # Renamed from 'name'
                                    address=norm(p.formatted_address),
                                    type=_types_csv(p.types),  # Renamed from 'category'
                                    lat=lat,
                                    lng=lng,
                                    phone="",  # Will be filled during enrichment
                                    website="",  # Will be filled during enrichment
                                    search_query=final_query,  # Renamed from 'query_text'
                                    intended_location=loc_label,
                                    source="google_maps",
                                    raw_json=p.raw,
                                    location_label=loc_label,
                                    domain="",  # Will be filled during enrichment
                                    business_status=norm(p.business_status),
                                    google_maps_url=norm(p.google_maps_url),
                                    country=iso2.upper(),  # Add ISO2 country code
                                )
                            )

                            # DB Queue row (only if enrichment enabled)
                            # CRITICAL: Enqueue canonical place_id, NOT the lowercased dedupe key
                            if enqueue_for_enrich and canonical_place_id:
                                db_queue_rows.append(
                                    GoogleMapsEnrichQueueRow.from_runner_data(
                                        job_id=job_id,
                                        request_id=rid,
                                        place_id=canonical_place_id,
                                        iso2=iso2.upper(),
                                        hl=norm(hl_plan),
                                    )
                                )

                        appended_this_run = len(eligible)
                        unique_new_places += appended_this_run

                        run.appended_rows = appended_this_run
                        run.unique_places_job = unique_new_places
                        run.meta = {"status": norm(data.get("status"))}

                        result.places.extend(eligible)
                        result.runs.append(run)

                    except LocationProviderError as exc:
                        run_err = str(exc)

                    except Exception as exc:
                        run_err = str(exc)

                    finally:
                        runs_done += 1
                        result.runs_done = runs_done
                        result.total_unique_places = unique_new_places
                        total_runs_remaining = max(0, int(total_runs_remaining) - 1)

                        msg = f"{iso2.upper()} | {loc_label} | {base_q} | appended={appended_this_run} | unique={unique_new_places}"

                        update_progress(
                            job_id,
                            phase="discover",
                            current=runs_done,
                            total=total_runs_est,
                            message=msg,
                            metrics={"unique_places": unique_new_places},
                        )

                        if status and _should_write_status(runs_done, total_runs_est):
                            status.write(
                                job_id=job_id,
                                tool="google_maps_discover",
                                request_id=rid,
                                state="RUNNING",
                                phase="discover",
                                current=unique_new_places,
                                total=mr,
                                message=msg,
                                meta={"unique_places": unique_new_places, "runs_done": runs_done},
                            )

                        # DB-first: Buffer audit row for database insert
                        db_audit_rows.append(
                            GoogleMapsAuditRow(
                                job_id=job_id,
                                request_id=rid,
                                phase="discover",
                                country_iso2=iso2.upper(),
                                hl_plan=norm(hl_plan),
                                language_used=language_used,
                                location_label=loc_label,
                                base_query=base_q,
                                final_query=f"{base_q} in {loc_label}",
                                region_param=region_param,
                                take_n=take_n,
                                returned_count=returned_count,
                                eligible_after_dedupe=eligible_after,
                                appended_rows=appended_this_run,
                                unique_places_job=unique_new_places,
                                stop_reason=norm(stop_reason),
                                error=norm(run_err),
                                raw_meta_json={"status": "OK" if not run_err else "ERROR", "error": run_err} if run_err else {"status": "OK"},
                            )
                        )

        # Step 1: Always write to database first (DB-first approach)
        if db_result_rows:
            log.info(f"DB_WRITE | tool=google_maps_discover | results={len(db_result_rows)}")
            safe_insert_google_maps_results(job_id, rid, db_result_rows)

        # Step 2: Write audit rows to database (DB-first audit)
        if db_audit_rows:
            log.info(f"DB_AUDIT_WRITE | tool=google_maps_discover | audit_rows={len(db_audit_rows)}")
            safe_insert_google_maps_audit(job_id, rid, db_audit_rows)

        # Step 3: Write enrichment queue to database (if enrichment enabled)
        if enqueue_for_enrich and db_queue_rows:
            log.info(f"DB_QUEUE_ENQUEUE | tool=google_maps_discover | queue_items={len(db_queue_rows)}")
            safe_insert_google_maps_enrich_queue(job_id, rid, db_queue_rows)

        # Step 4: Export to Google Sheets from database (only if export enabled)
        if settings.google_maps_sheets_export_enabled:
            try:
                log.info("SHEETS_EXPORT | Starting DB→Sheets export")
                
                # Query database for results (OPERATIONAL only)
                repo = GoogleMapsResultsRepository()
                success, error, db_results = repo.get_results_by_job(job_id)
                
                if success and db_results:
                    # Filter to OPERATIONAL only
                    operational_results = [r for r in db_results if r.get('business_status') == 'OPERATIONAL']
                    
                    # Convert DB results to sheets format (simplified columns)
                    sheets_rows = convert_db_results_to_sheets_format(operational_results)
                    
                    # Get audit rows from database
                    from .repos import GoogleMapsAuditRepository
                    audit_repo = GoogleMapsAuditRepository()
                    audit_success, audit_error, db_audit_records = audit_repo.get_audit_by_job(job_id)
                    audit_sheets_rows = convert_db_audit_to_sheets_format(db_audit_records) if audit_success else []
                    
                    # Export using lightweight exporter
                    export_google_maps_results(
                        client=sheets,
                        job_id=job_id,
                        request_id=rid,
                        results_rows=sheets_rows,
                        audit_rows=audit_sheets_rows if audit_sheets_rows else None,
                    )
                    log.info(f"SHEETS_EXPORT | tool=google_maps_discover | rows={len(sheets_rows)} | audit={len(audit_sheets_rows)}")
                else:
                    log.warning(f"SHEETS_EXPORT | Failed to retrieve results: {error}")
                    
            except Exception as e:
                log.error(f"SHEETS_EXPORT | Failed: {e}")
                # Don't fail the job if sheets export fails

        try:
            save_google_maps_output(
                request_id=rid,
                prompt=raw_prompt,
                payload=result.model_dump(mode="json"),
                provider="google_maps",
                kind="discover",
            )
        except Exception:
            logger.exception("GOOGLE_MAPS_DISCOVER_ARTIFACT_SAVE_FAILED")

        msg = "Google Maps discovery completed."
        if unique_new_places >= mr:
            msg = "Google Maps discovery completed (stopped early: max_results reached)."

        mark_succeeded(job_id, message=msg, metrics={"unique_places": unique_new_places, "runs_done": runs_done, "queued": len(db_queue_rows)})
        log.info("JOB_SUCCEEDED | runs_done=%d | unique_places=%d | queued=%d", runs_done, unique_new_places, len(db_queue_rows))

        if status:
            status.write(
                job_id=job_id,
                tool="google_maps_discover",
                request_id=rid,
                state="SUCCEEDED",
                phase="discover",
                current=unique_new_places,
                total=mr,
                message=msg,
                meta={"unique_places": unique_new_places, "runs_done": runs_done, "queued": len(db_queue_rows)},
                force=True,
            )
        
        # Trigger Master ingestion after successful completion (if enabled)
        if trigger_master:
            try:
                from ....services.master_data.orchestration import trigger_master_job
                
                success, error, master_job_id = trigger_master_job(
                    request_id=rid,
                    spreadsheet_id=sid,
                    source_tool="google_maps_discover"
                )
                
                if success:
                    log.info("MASTER_TRIGGERED | master_job_id=%s | trigger_master=true", master_job_id)
                else:
                    log.warning("MASTER_TRIGGER_FAILED | error=%s", error)
            except Exception as e:
                log.error("MASTER_TRIGGER_ERROR | error=%s", e, exc_info=True)
                # Don't fail the Google Maps job if Master trigger fails
        else:
            log.info("MASTER_TRIGGER_SKIPPED | reason=trigger_master_disabled | tool=google_maps_discover")

    except Exception as exc:
        mark_failed(job_id, error=str(exc), message="Google Maps discovery job failed.")
        log.exception("JOB_FAILED | error=%s", exc)

        try:
            if status:
                status.write(
                    job_id=job_id,
                    tool="google_maps_discover",
                    request_id=rid or "",
                    state="FAILED",
                    phase="discover",
                    current=unique_new_places,
                    total=mr if 'mr' in locals() else 250,
                    message=str(exc),
                    meta={"unique_places": unique_new_places, "runs_done": runs_done},
                    force=True,
                )
        except Exception:
            logger.exception("TOOL_STATUS_WRITE_FAILED")
