from __future__ import annotations

# =============================================================================
# enrich_runner.py
#
# DB queue-driven enrichment (fully DB-first):
# - Reads from gmaps_enrich_queue table (no Sheets queue)
# - Uses FOR UPDATE SKIP LOCKED for concurrency safety
# - Updates gmaps_results and google_maps_enriched tables
# - Sheets is only an export layer (DB→Sheets)
# =============================================================================

import logging
import time
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

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
from .client import GooglePlacesClient
from .repos import (
    GoogleMapsResultsRepository,
    GoogleMapsEnrichQueueRepository,
    convert_db_results_to_sheets_format,
)

logger = logging.getLogger(__name__)

# Note: TAB_STATUS imported from sheets_schema.py
# Note: GoogleMaps_v2 and GoogleMaps_Audit_v2 tabs are created only at export time

DETAILS_FIELDS = "website,url,formatted_phone_number,international_phone_number"


def _utc_iso() -> str:
    return datetime.utcnow().isoformat()


def _domain_from_website(website: str) -> str:
    w = norm(website)
    if not w:
        return ""
    try:
        host = urlparse(w).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""


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


def _resolve_language(hl: str | None, default_lang: str = "en") -> str:
    h = norm(hl)
    return h if h and _looks_like_language_tag(h) else default_lang


def run_google_maps_enrich_job(
    *,
    job_id: str,
    plan: SearchPlan,
    spreadsheet_id: str,
    max_items: int = 500,
    batch_size: int = 50,
    linger_seconds: float = 3.0,
    max_empty_batches: int = 10,
    max_retry_attempts: int = 3,
    trigger_master: bool = True,
) -> None:
    rid = norm(getattr(plan, "request_id", None))
    log = job_logger(__name__, tool="google_maps_enrich", request_id=rid, job_id=job_id)

    sheets: SheetsClient | None = None
    status: ToolStatusWriter | None = None

    processed = 0
    succeeded = 0
    failed = 0

    empty_batches = 0

    try:
        mark_running(job_id)

        sid = norm(spreadsheet_id)
        if not sid:
            raise ValueError("spreadsheet_id must not be empty.")
        if not rid:
            raise ValueError("search_plan.request_id must not be empty.")

        max_items_eff = max(1, int(max_items))
        batch_eff = max(1, int(batch_size))
        linger_s = max(0.0, float(linger_seconds))
        max_empty = max(0, int(max_empty_batches))
        max_retries = max(1, int(max_retry_attempts))

        # Initialize sheets client - only for Tool_Status
        sheets = SheetsClient(config=default_sheets_config(spreadsheet_id=sid))
        # Only create Tool_Status tab at runtime (exception to export-only rule)
        sheets.ensure_tab(TAB_STATUS)
        
        # Always initialize status writer for Tool_Status updates
        status = ToolStatusWriter(sheets=sheets, spreadsheet_id=sid)

        # Initialize repositories
        queue_repo = GoogleMapsEnrichQueueRepository()
        results_repo = GoogleMapsResultsRepository()

        # Get initial queue summary
        success, error, queue_summary = queue_repo.get_queue_summary(job_id, rid)
        if success:
            total_queue_items = queue_summary.get('total', 0)
            log.info(
                "QUEUE_SUMMARY | total=%d pending=%d processing=%d done=%d failed=%d failed_final=%d retry_eligible=%d",
                queue_summary.get('total', 0),
                queue_summary.get('pending', 0),
                queue_summary.get('processing', 0),
                queue_summary.get('done', 0),
                queue_summary.get('failed', 0),
                queue_summary.get('failed_final', 0),
                queue_summary.get('retry_eligible', 0),
            )
        else:
            total_queue_items = max_items_eff
            log.warning(f"Failed to get queue summary: {error}")

        update_progress(
            job_id,
            phase="enrich",
            current=0,
            total=min(max_items_eff, total_queue_items),
            message="Starting enrichment from DB queue.",
            metrics={"batch_size": batch_eff, "linger_seconds": linger_s, "max_empty_batches": max_empty},
        )

        status.write(
            job_id=job_id,
            tool="google_maps_enrich",
            request_id=rid,
            state="RUNNING",
            phase="enrich",
            current=0,
            total=min(max_items_eff, total_queue_items),
            message="Starting enrichment from DB queue.",
            meta={"batch_size": batch_eff, "linger_seconds": linger_s, "max_empty_batches": max_empty},
        )

        with GooglePlacesClient() as client:
            while processed < max_items_eff:
                # Fetch next batch from DB queue (concurrency-safe with retry gating)
                success, error, queue_items = queue_repo.fetch_next_batch(
                    job_id=job_id,
                    request_id=rid,
                    batch_size=batch_eff,
                    max_attempts=max_retries,
                )

                if not success:
                    log.error(f"Failed to fetch queue batch: {error}")
                    break

                if not queue_items:
                    empty_batches += 1

                    msg = (
                        f"Queue idle (empty={empty_batches}/{max_empty}). "
                        f"processed={processed} ok={succeeded} failed={failed}"
                    )
                    update_progress(
                        job_id,
                        phase="enrich",
                        current=processed,
                        total=min(max_items_eff, total_queue_items),
                        message=msg,
                        metrics={"ok": succeeded, "failed": failed, "empty_batches": empty_batches},
                    )
                    status.write(
                        job_id=job_id,
                        tool="google_maps_enrich",
                        request_id=rid,
                        state="RUNNING",
                        phase="enrich",
                        current=processed,
                        total=min(max_items_eff, total_queue_items),
                        message=msg,
                        meta={"empty_batches": empty_batches, "ok": succeeded, "failed": failed},
                    )

                    if max_empty == 0 or empty_batches >= max_empty:
                        log.info("Stopping: max empty batches reached")
                        break

                    if linger_s > 0:
                        time.sleep(linger_s)

                    continue

                empty_batches = 0

                # Process batch
                done_ids = []
                failed_ids = []

                for item in queue_items:
                    if processed >= max_items_eff:
                        # Mark remaining items as pending for next run
                        remaining_ids = [i['id'] for i in queue_items[queue_items.index(item):]]
                        queue_repo.mark_failed(
                            remaining_ids,
                            "Stopped: max_items reached",
                            max_attempts=max_retries
                        )
                        break

                    processed += 1
                    item_id = item['id']
                    place_id = item['place_id']  # CRITICAL: Canonical place_id from queue
                    iso2 = item.get('iso2', '')
                    hl = item.get('hl', '')
                    attempts = item.get('attempts', 0)

                    # Resolve language/region from queue item
                    hl_used = _resolve_language(hl, default_lang="en")
                    region = norm(iso2).lower() or "xx"

                    try:
                        # Call Places Details API with canonical place_id
                        # CRITICAL: place_id must NOT be mutated before API call
                        data = client.place_details(
                            place_id=place_id,  # Use canonical value from queue
                            language=hl_used,
                            region=region,
                            fields=DETAILS_FIELDS,
                        )

                        res = data.get("result") or {}
                        if not isinstance(res, dict):
                            res = {}

                        # Extract fields
                        website = norm(res.get("website"))
                        g_url = norm(res.get("url")) or _maps_url_from_place_id(place_id)
                        phone = norm(res.get("international_phone_number")) or norm(res.get("formatted_phone_number"))
                        domain = _domain_from_website(website)

                        # Update google_maps_results with contact fields (DB-first)
                        # No separate enriched table - all data goes into google_maps_results
                        results_repo.update_contact_fields(
                            job_id=job_id,
                            request_id=rid,
                            place_id=place_id,
                            website=website,
                            domain=domain,
                            phone=phone,
                            google_maps_url=g_url,
                        )

                        succeeded += 1
                        done_ids.append(item_id)

                    except Exception as exc:
                        failed += 1
                        failed_ids.append(item_id)
                        error_msg = str(exc)
                        log.warning(
                            "ENRICH_FAILED | place_id=%s attempts=%d error=%s",
                            place_id,
                            attempts,
                            error_msg[:200]
                        )

                        # Mark as failed with exponential backoff retry gating
                        # Will set FAILED (retry eligible) or FAILED_FINAL (terminal)
                        queue_repo.mark_failed(
                            [item_id],
                            error_msg,
                            max_attempts=max_retries
                        )

                    if processed % 10 == 0:
                        msg = f"Enrich processed={processed} ok={succeeded} failed={failed}"
                        update_progress(
                            job_id,
                            phase="enrich",
                            current=processed,
                            total=min(max_items_eff, total_queue_items),
                            message=msg,
                            metrics={"ok": succeeded, "failed": failed}
                        )
                        status.write(
                            job_id=job_id,
                            tool="google_maps_enrich",
                            request_id=rid,
                            state="RUNNING",
                            phase="enrich",
                            current=processed,
                            total=min(max_items_eff, total_queue_items),
                            message=msg,
                            meta={"ok": succeeded, "failed": failed}
                        )

                # Mark queue items as done
                if done_ids:
                    queue_repo.mark_done(done_ids)
                    log.info(f"QUEUE_UPDATE | marked_done={len(done_ids)}")

        # Final export to Google Sheets (DB→Sheets) - only if export enabled
        if settings.google_maps_sheets_export_enabled:
            try:
                log.info("SHEETS_EXPORT | Starting DB→Sheets export")
                
                # Export main results (with enriched contact fields) - OPERATIONAL only
                # Use request_id to fetch results (discover job_id creates rows, enrich job_id has none)
                success, error, db_results_all = results_repo.get_results_by_request(rid)
                
                if not success:
                    log.warning(f"SHEETS_EXPORT | Failed to retrieve results: {error}")
                    results_operational = []
                elif not db_results_all:
                    log.info("SHEETS_EXPORT | No results to export (0 rows)")
                    results_operational = []
                else:
                    # Filter to OPERATIONAL only with normalized business_status
                    results_operational = [
                        r for r in db_results_all
                        if norm(r.get('business_status')).upper() == 'OPERATIONAL'
                    ]
                
                results_to_export = convert_db_results_to_sheets_format(results_operational)
                
                # Export using lightweight exporter
                export_google_maps_results(
                    client=sheets,
                    job_id=job_id,
                    request_id=rid,
                    results_rows=results_to_export,
                    audit_rows=None,  # Optional: include audit if needed
                )
                log.info(f"SHEETS_EXPORT | tool=google_maps_enrich | main_rows={len(results_to_export)}")
                    
            except Exception as e:
                log.error(f"SHEETS_EXPORT | Failed: {e}")
                # Don't fail the job if sheets export fails

        # Save artifact summary
        try:
            save_google_maps_output(
                request_id=rid,
                prompt=norm(getattr(plan, "raw_prompt", None)),
                payload={
                    "job_id": job_id,
                    "kind": "enrich_summary",
                    "processed": processed,
                    "succeeded": succeeded,
                    "failed": failed,
                    "empty_batches": empty_batches,
                    "linger_seconds": linger_s,
                    "max_empty_batches": max_empty,
                },
                provider="google_maps",
                kind="enrich_summary",
            )
        except Exception:
            logger.exception("GOOGLE_MAPS_ENRICH_ARTIFACT_SAVE_FAILED")

        msg = f"Google Maps enrichment completed. processed={processed} ok={succeeded} failed={failed}"
        if processed >= max_items_eff:
            msg += " (stopped: max_items reached)"
        elif max_empty > 0 and empty_batches >= max_empty:
            msg += " (stopped: queue idle)"

        mark_succeeded(job_id, message=msg, metrics={"processed": processed, "ok": succeeded, "failed": failed})
        log.info("JOB_SUCCEEDED | %s", msg)

        if status:
            status.write(
                job_id=job_id,
                tool="google_maps_enrich",
                request_id=rid,
                state="SUCCEEDED",
                phase="enrich",
                current=processed,
                total=min(max_items_eff, total_queue_items),
                message=msg,
                meta={"processed": processed, "ok": succeeded, "failed": failed},
                force=True,
            )
        
        # Trigger Master ingestion after successful completion (if enabled)
        if trigger_master:
            try:
                from ....services.master_data.orchestration import trigger_master_job
                
                success, error, master_job_id = trigger_master_job(
                    request_id=rid,
                    spreadsheet_id=sid,
                    source_tool="google_maps_enrich"
                )
                
                if success:
                    log.info("MASTER_TRIGGERED | master_job_id=%s | trigger_master=true", master_job_id)
                else:
                    log.warning("MASTER_TRIGGER_FAILED | error=%s", error)
            except Exception as e:
                log.error("MASTER_TRIGGER_ERROR | error=%s", e, exc_info=True)
                # Don't fail the Google Maps job if Master trigger fails
        else:
            log.info("MASTER_TRIGGER_SKIPPED | reason=trigger_master_disabled | tool=google_maps_enrich")

    except Exception as exc:
        mark_failed(job_id, error=str(exc), message="Google Maps enrichment job failed.")
        log.exception("JOB_FAILED | error=%s", exc)

        try:
            if status:
                status.write(
                    job_id=job_id,
                    tool="google_maps_enrich",
                    request_id=rid or "",
                    state="FAILED",
                    phase="enrich",
                    current=processed,
                    total=max_items if isinstance(max_items, int) else 500,
                    message=str(exc),
                    meta={"processed": processed, "ok": succeeded, "failed": failed},
                    force=True,
                )
        except Exception:
            logger.exception("TOOL_STATUS_WRITE_FAILED")
