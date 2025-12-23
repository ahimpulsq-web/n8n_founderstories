from __future__ import annotations

import json
import logging
import math
from datetime import datetime
from typing import Any

from ....core.utils.text import norm
from ....services.exports.sheets import SheetsClient, default_sheets_config
from ....services.exports.sheets_manager import GoogleSheetsManager
from ....services.jobs.logging import job_logger
from ....services.jobs.sheets_status import ToolStatusWriter
from ....services.jobs.store import mark_failed, mark_running, mark_succeeded, update_progress
from ....services.search_plan import SearchPlan
from ....services.storage import save_google_maps_output
from ..errors import LocationProviderError
from .client import GooglePlacesClient
from .models import GoogleMapsJobResult, GoogleMapsPlace, GoogleMapsRunResult

logger = logging.getLogger(__name__)

TAB_STATUS = "Tool_Status"
TAB_MAIN = "GoogleMaps"
TAB_AUDIT = "GoogleMaps_Audit"

TAB_ENRICH_QUEUE = "GoogleMaps_EnrichQueue"
HEADERS_ENRICH_QUEUE = ["Place ID", "Sheet Row", "ISO2", "HL", "State", "Updated At", "Error"]

Q_STATE_PENDING = "PENDING"

HARD_MAX_PER_RUN = 20
STATUS_EVERY_N = 5

HEADERS_MAIN = [
    "Place Name",
    "Location Label",
    "Address",
    "Place ID",
    "Type",
    "Website",
    "Domain",
    "Phone",
    "Search Query",
    "Business Status",
    "Google Maps URL",
]

HEADERS_AUDIT = [
    "Job ID",
    "Request ID",
    "Phase",
    "Country (ISO2)",
    "HL (Plan)",
    "Language Used",
    "Location Label",
    "Base Query",
    "Final Query",
    "Region Param",
    "Take N",
    "Returned Count",
    "Eligible After Dedupe",
    "Appended Rows",
    "Unique Places (Job)",
    "Stop Reason",
    "Error",
    "Timestamp",
    "Raw Meta (JSON)",
]


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

        sheets = SheetsClient(config=default_sheets_config(spreadsheet_id=sid))
        gsm = GoogleSheetsManager(client=sheets)
        status = ToolStatusWriter(sheets=sheets, spreadsheet_id=sid)

        sheets.ensure_tab("Tool_Status")
        sheets.ensure_tab(TAB_MAIN)
        sheets.ensure_tab(TAB_AUDIT)
        if enqueue_for_enrich:
            sheets.ensure_tab(TAB_ENRICH_QUEUE)

        headers = [
            ("Tool_Status", ["Job ID", "Tool", "Request ID", "State", "Phase", "Current", "Total", "Percent", "Message", "Updated At", "Spreadsheet ID", "Meta (JSON)"]),
            (TAB_MAIN, HEADERS_MAIN),
            (TAB_AUDIT, HEADERS_AUDIT),
        ]
        if enqueue_for_enrich:
            headers.append((TAB_ENRICH_QUEUE, HEADERS_ENRICH_QUEUE))

        hide_tabs = [TAB_AUDIT, "HunterIO_Audit", "GoogleSearch_Audit"]
        if enqueue_for_enrich:
            hide_tabs.append(TAB_ENRICH_QUEUE)

        gsm.setup_tabs(
            headers=headers,
            hide_tabs=hide_tabs,
            tab_order=["Tool_Status", "HunterIO", "GoogleMaps", "GoogleSearch", "Master", TAB_AUDIT],
            overwrite_headers_for_owned_tabs=True,
        )

        if enqueue_for_enrich:
            try:
                sheets.hide_tab(tab_name=TAB_ENRICH_QUEUE)
            except Exception:
                logger.exception("GOOGLE_MAPS_DISCOVER_HIDE_QUEUE_FAILED")

        # ONE read per job to compute row numbers for queue
        last_row_before = sheets.get_last_row(tab_name=TAB_MAIN, signal_col=0)
        start_row_for_append = max(1, int(last_row_before)) + 1

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
            total=total_runs_est,
            message="Starting Google Maps discovery.",
            meta={"append_start_row": start_row_for_append, "enqueue_for_enrich": enqueue_for_enrich},
        )

        main_rows: list[list[str]] = []
        audit_rows: list[list[str]] = []
        queue_rows: list[list[str]] = []

        # Track how many main rows we have appended so far (for sheet_row calculation)
        appended_main_count = 0

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

                            pid = norm(p.place_id).lower()
                            if dedupe_places and pid and pid in seen_place_ids_run:
                                continue

                            eligible.append(p)
                            if len(eligible) >= take_n:
                                break

                        eligible_after = len(eligible)
                        run.eligible_after_dedupe = eligible_after

                        # Buffer main rows + queue rows with CORRECT iso2/hl and CORRECT sheet_row
                        ts = _utc_iso()
                        for p in eligible:
                            pid = norm(p.place_id).lower()
                            if dedupe_places and pid:
                                seen_place_ids_run.add(pid)

                            # main row
                            main_rows.append(
                                [
                                    norm(p.name),
                                    loc_label,
                                    norm(p.formatted_address),
                                    norm(p.place_id),
                                    _types_csv(p.types),
                                    "",  # website (enrich)
                                    "",  # domain (enrich)
                                    "",  # phone (enrich)
                                    final_query,
                                    norm(p.business_status),
                                    norm(p.google_maps_url),
                                ]
                            )

                            # queue row (computed from job-start start_row + how many main rows already buffered)
                            if enqueue_for_enrich and pid:
                                sheet_row = start_row_for_append + appended_main_count
                                queue_rows.append(
                                    [
                                        pid,
                                        str(sheet_row),
                                        iso2.upper(),
                                        norm(hl_plan),
                                        Q_STATE_PENDING,
                                        ts,
                                        "",
                                    ]
                                )

                            appended_main_count += 1

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
                                current=runs_done,
                                total=total_runs_est,
                                message=msg,
                                meta={"unique_places": unique_new_places, "runs_done": runs_done},
                            )

                        audit_rows.append(
                            [
                                job_id,
                                rid,
                                "TEXT_SEARCH",
                                iso2.upper(),
                                norm(hl_plan),
                                language_used,
                                loc_label,
                                base_q,
                                f"{base_q} in {loc_label}",
                                region_param,
                                str(take_n),
                                str(returned_count),
                                str(eligible_after),
                                str(appended_this_run),
                                str(unique_new_places),
                                norm(stop_reason),
                                norm(run_err),
                                _utc_iso(),
                                json.dumps({"status": "OK" if not run_err else "ERROR", "error": run_err} if run_err else {"status": "OK"}, ensure_ascii=False),
                            ]
                        )

        # Single append: main
        if main_rows:
            sheets.append_rows(tab_name=TAB_MAIN, rows=main_rows)

        # Single append: queue
        if enqueue_for_enrich and queue_rows:
            sheets.append_rows(tab_name=TAB_ENRICH_QUEUE, rows=queue_rows)

        # Single append: audit
        if audit_rows:
            sheets.append_rows(tab_name=TAB_AUDIT, rows=audit_rows)

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

        mark_succeeded(job_id, message=msg, metrics={"unique_places": unique_new_places, "runs_done": runs_done, "queued": len(queue_rows)})
        log.info("JOB_SUCCEEDED | runs_done=%d | unique_places=%d | queued=%d", runs_done, unique_new_places, len(queue_rows))

        if status:
            status.write(
                job_id=job_id,
                tool="google_maps_discover",
                request_id=rid,
                state="SUCCEEDED",
                phase="discover",
                current=runs_done,
                total=total_runs_est,
                message=msg,
                meta={"unique_places": unique_new_places, "runs_done": runs_done, "queued": len(queue_rows)},
                force=True,
            )

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
                    current=runs_done,
                    total=total_runs_est,
                    message=str(exc),
                    meta={"unique_places": unique_new_places, "runs_done": runs_done},
                    force=True,
                )
        except Exception:
            logger.exception("TOOL_STATUS_WRITE_FAILED")
