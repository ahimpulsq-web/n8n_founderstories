from __future__ import annotations

import json
import logging
from itertools import combinations
from typing import Any

from ....core.utils.text import norm
from ....core.config import settings
from ....services.exports.sheets import SheetsClient, default_sheets_config
from ....services.exports.sheets_exporter import export_hunter_results
from ....services.exports.sheets_schema import TAB_STATUS
from ....services.jobs.logging import job_logger
from ....services.jobs.sheets_status import ToolStatusWriter
from ....services.jobs.store import mark_failed, mark_running, mark_succeeded, update_progress
from ....services.search_plan import SearchPlan
from ....services.storage import save_enrichment_output
from .client import HunterClient
from .models import HunterCompany, HunterJobResult, HunterQueryType, HunterRunResult
from .repos import (
    HunterIOBatchProcessor,
    HunterIOResultRow,
    HunterIOResultsRepository,
    HunterAuditRepository,
    convert_db_results_to_sheets_format,
    convert_db_audit_to_sheets_format
)

logger = logging.getLogger(__name__)

# Note: TAB_STATUS imported from sheets_schema.py
# Note: HunterIO_v2 and HunterIO_Audit_v2 tabs are created only at export time

HEADCOUNT_BUCKETS: list[str] = [
    "1-10",
    "11-50",
    "51-200",
    "201-500",
    "501-1000",
    "1001-5000",
    "5001-10000",
    "10001+",
]

# Legacy constants - kept for backward compatibility but not used for new exports
# New exports use constants from sheets_schema.py
HUNTER_DOMAIN_KEY_COL = 1  # Domain is column 1 (0-indexed)


def _company_to_row(c: HunterCompany) -> list[str]:
    return [
        norm(c.organization),  # Organisation (first column in new format)
        norm(c.domain),        # Domain (second column in new format)
        norm(c.location),      # Location
        norm(c.headcount_bucket),  # Headcount
        norm(c.source_query),  # Search Query
        f"{norm(getattr(c, 'intended_location', None))} | {norm(getattr(c, 'intended_headcount_bucket', None))}",  # Debug Filters
    ]


def _audit_to_row(*, job_id: str, request_id: str, run: HunterRunResult, appended: int) -> list[str]:
    return [
        norm(job_id),
        norm(request_id),
        run.query_type.value if run.query_type else "",
        norm(run.location),
        norm(run.headcount_bucket),
        norm(run.applied_location),
        norm(run.applied_headcount_bucket),
        norm(run.query_text),
        ", ".join(run.keywords or []),
        norm(run.keyword_match),
        str(run.total_results or ""),
        str(run.returned_count),
        str(appended),
        json.dumps(run.applied_filters or {}, ensure_ascii=False),
    ]


def _applied_location_label(applied_filters: dict[str, Any] | None) -> str:
    if not isinstance(applied_filters, dict):
        return ""
    hq = applied_filters.get("headquarters_location")
    if not isinstance(hq, dict):
        return ""
    inc = hq.get("include")
    if not isinstance(inc, list) or not inc:
        return ""
    first = inc[0]
    if not isinstance(first, dict):
        return ""

    country = norm(first.get("country"))
    city = norm(first.get("city"))
    continent = norm(first.get("continent"))
    business_region = norm(first.get("business_region"))

    if city and country:
        return f"{city}, {country}"
    if country:
        return country
    if business_region:
        return business_region
    if continent:
        return continent
    return ""


def _applied_headcount_bucket(applied_filters: dict[str, Any] | None) -> str:
    if not isinstance(applied_filters, dict):
        return ""
    hc = applied_filters.get("headcount")
    if isinstance(hc, list) and hc:
        return norm(hc[0])
    return ""


def build_hunter_headquarters_locations(*, geo_location_keywords: dict, max_cities_per_country: int) -> list[tuple[str, dict]]:
    out: list[tuple[str, dict]] = []
    if not isinstance(geo_location_keywords, dict):
        return out

    cap = max(0, int(max_cities_per_country))

    for iso2, bucket in geo_location_keywords.items():
        iso2 = norm(str(iso2)).upper()
        if len(iso2) != 2 or not iso2.isalpha():
            continue
        if not isinstance(bucket, dict):
            continue

        locs = bucket.get("locations")
        if not isinstance(locs, list) or not locs:
            continue

        region = norm(str(locs[0]))
        if region:
            out.append((region, {"include": [{"country": iso2}]}))

        cities = [norm(str(x)) for x in locs[1:] if norm(str(x))]
        for city in cities[:cap]:
            out.append((city, {"include": [{"country": iso2}]}))

    final: list[tuple[str, dict]] = []
    seen: set[str] = set()
    for label, hq in out:
        k = label.lower()
        if k in seen:
            continue
        seen.add(k)
        final.append((label, hq))
    return final


def run_hunter_job(
    *,
    job_id: str,
    plan: SearchPlan,
    spreadsheet_id: str,
    max_web_queries: int | None = None,
    max_keywords: int | None = None,
    target_unique_domains: int = 250,
    max_cities_per_country: int = 4,
) -> None:
    rid = norm(getattr(plan, "request_id", None))
    log = job_logger(__name__, tool="hunter", request_id=rid, job_id=job_id)

    runs_done = 0
    total_runs_est: int | None = None

    # within-run dedupe only (no cross-run sheet reads)
    unique_domains_run: set[str] = set()

    sheets: SheetsClient | None = None
    gsm: GoogleSheetsManager | None = None
    status: ToolStatusWriter | None = None
    batch_processor: HunterIOBatchProcessor | None = None

    last_msg = "Starting Hunter discovery."

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

        # Inputs (capped)
        all_web_queries = [norm(q) for q in (getattr(plan, "web_queries", None) or []) if norm(q)]
        all_keywords = [norm(k) for k in (getattr(plan, "keywords", None) or []) if norm(k)]

        if isinstance(max_web_queries, int) and max_web_queries >= 0:
            web_queries = all_web_queries[:max_web_queries]
        else:
            web_queries = all_web_queries

        if isinstance(max_keywords, int) and max_keywords >= 0:
            keywords = all_keywords[:max_keywords]
        else:
            keywords = all_keywords

        # Locations
        geo_buckets = getattr(plan, "geo_location_keywords", None)
        locations = build_hunter_headquarters_locations(
            geo_location_keywords=geo_buckets if isinstance(geo_buckets, dict) else {},
            max_cities_per_country=max_cities_per_country,
        )
        if not locations:
            raise ValueError("search_plan.geo_location_keywords is missing/invalid; cannot build headquarters_location.")

        # Initialize batch processor for DB-first approach
        batch_processor = HunterIOBatchProcessor(job_id=job_id, request_id=rid)

        # Initialize sheets client - only for Tool_Status (exception to export-only rule)
        sheets = SheetsClient(config=default_sheets_config(spreadsheet_id=sid))
        sheets.ensure_tab(TAB_STATUS)
        status = ToolStatusWriter(sheets=sheets, spreadsheet_id=sid)

        # Estimate total runs from EFFECTIVE lists
        single_kw_runs = len(keywords)
        full_kw_runs = 1 if keywords else 0
        pair_kw_runs = (len(keywords) * (len(keywords) - 1) // 2) if len(keywords) >= 2 else 0
        runs_per_loc_hc = len(web_queries) + single_kw_runs + full_kw_runs + pair_kw_runs
        total_runs_est = len(locations) * len(HEADCOUNT_BUCKETS) * runs_per_loc_hc

        # Artifact skeleton
        result = HunterJobResult(
            request_id=rid,
            raw_prompt=raw_prompt,
            provider_name=provider_name,
            geo=geo,
            web_queries_used=list(web_queries),
            keywords_used=list(keywords),
            locations_used=[label for (label, _) in locations],
            headcount_buckets_used=list(HEADCOUNT_BUCKETS),
            target_unique_domains=int(target_unique_domains),
            max_cities_per_country=int(max_cities_per_country),
        )

        # Initial progress + Tool_Status
        update_progress(
            job_id,
            phase="discover",
            current=0,
            total=total_runs_est,
            message=last_msg,
            metrics={
                "web_queries": len(web_queries),
                "keywords": len(keywords),
                "locations": len(locations),
                "headcount_buckets": len(HEADCOUNT_BUCKETS),
                "target_unique_domains": target_unique_domains,
                "unique_domains_run": len(unique_domains_run),
                "plan_web_queries": len(all_web_queries),
                "plan_keywords": len(all_keywords),
            },
        )

        status.write(
            job_id=job_id,
            tool="hunter",
            request_id=rid,
            state="RUNNING",
            phase="discover",
            current=len(unique_domains_run),
            total=target_unique_domains,
            message=last_msg,
            meta={
                "target_unique_domains": target_unique_domains,
                "web_queries": len(web_queries),
                "keywords": len(keywords),
                "locations": len(locations),
                "headcount_buckets": len(HEADCOUNT_BUCKETS),
                "unique_domains_run": len(unique_domains_run),
                "plan_web_queries": len(all_web_queries),
                "plan_keywords": len(all_keywords),
                "user_max_web_queries": max_web_queries,
                "user_max_keywords": max_keywords,
                "runs_per_loc_hc": runs_per_loc_hc,
                "total_runs_est": total_runs_est,
                "runs_done": runs_done,
            },
        )

        # Execute
        with HunterClient() as client:
            # PHASE 1: KEYWORD PROCESSING (Single keywords, pairs, then all keywords)
            for hc_i, hc in enumerate(HEADCOUNT_BUCKETS, start=1):
                if target_unique_domains > 0 and len(unique_domains_run) >= target_unique_domains:
                    break

                for loc_i, (loc_label, hq_payload) in enumerate(locations, start=1):
                    if target_unique_domains > 0 and len(unique_domains_run) >= target_unique_domains:
                        break

                    headquarters_location = {"include": hq_payload["include"]}

                    if not keywords:
                        continue

                    # 1) Single keyword match=any (FIRST)
                    for kw in keywords:
                        if target_unique_domains > 0 and len(unique_domains_run) >= target_unique_domains:
                            break

                        companies, total, applied_filters = client.discover(
                            headquarters_location=headquarters_location,
                            headcount_bucket=hc,
                            keywords_include=[kw],
                            keyword_match="any",
                        )
                        runs_done += 1

                        qt = HunterQueryType.KW_ANY_SINGLE
                        source_query = f'KW_ANY:["{kw}"] | HQ={loc_label} | headcount="{hc}"'

                        applied_loc = _applied_location_label(applied_filters)
                        applied_hc = _applied_headcount_bucket(applied_filters)

                        tagged = [
                            c.model_copy(
                                update={
                                    "location": applied_loc,
                                    "headcount_bucket": applied_hc,
                                    "intended_location": loc_label,
                                    "intended_headcount_bucket": hc,
                                    "source_query": source_query,
                                    "query_type": qt,
                                }
                            )
                            for c in companies
                        ]

                        rows_main = [_company_to_row(c) for c in tagged]

                        appended = 0
                        main_to_append: list[list[str]] = []
                        for r in rows_main:
                            key = norm(r[HUNTER_DOMAIN_KEY_COL]).lower()
                            if not key:
                                continue
                            if key in unique_domains_run:
                                continue
                            unique_domains_run.add(key)
                            main_to_append.append(r)
                            appended += 1

                        # DB-first approach: write to database immediately
                        if main_to_append:
                            db_rows = []
                            for r in main_to_append:
                                db_row = HunterIOResultRow.from_runner_data(
                                    job_id=job_id,
                                    request_id=rid,
                                    organisation=r[0],  # Organisation (new format)
                                    domain=r[1],        # Domain (new format)
                                    location=applied_loc,
                                    headcount=applied_hc,
                                    search_query=f"{kw} | {qt.value}",
                                    debug_filters=f"{loc_label} | {hc}"
                                )
                                db_rows.append(db_row)
                            
                            # Add to batch processor
                            if batch_processor:
                                batch_processor.add_results(db_rows)
                            
                            # Note: No live append - export happens at job completion

                        run = HunterRunResult(
                            query_type=qt,
                            location=loc_label,
                            headcount_bucket=hc,
                            query_text=None,
                            keywords=[kw],
                            keyword_match="any",
                            applied_filters=applied_filters or {},
                            applied_location=applied_loc or None,
                            applied_headcount_bucket=applied_hc or None,
                            returned_count=len(tagged),
                            total_results=total,
                        )
                        result.runs.append(run)
                        result.companies.extend(tagged)

                        audit_row = _audit_to_row(job_id=job_id, request_id=rid, run=run, appended=appended)
                        
                        # Add audit record to batch processor
                        if batch_processor:
                            batch_processor.add_audit_records([audit_row])
                        
                        # Note: No live append - export happens at job completion

                        last_msg = (
                            f"[HC {hc_i}/{len(HEADCOUNT_BUCKETS)}] {hc} | "
                            f"[LOC {loc_i}/{len(locations)}] {loc_label} | "
                            f"KW_ANY appended={appended} | unique_run={len(unique_domains_run)}"
                        )

                        update_progress(
                            job_id,
                            phase="discover",
                            current=runs_done,
                            total=total_runs_est,
                            message=last_msg,
                            metrics={"unique_domains_run": len(unique_domains_run)},
                        )

                        status.write(
                            job_id=job_id,
                            tool="hunter",
                            request_id=rid,
                            state="RUNNING",
                            phase="discover",
                            current=len(unique_domains_run),
                            total=target_unique_domains,
                            message=last_msg,
                            meta={"unique_domains_run": len(unique_domains_run), "runs_done": runs_done},
                        )

                    if target_unique_domains > 0 and len(unique_domains_run) >= target_unique_domains:
                        break

                    # 2) Keyword pairs match=all (SECOND)
                    if len(keywords) >= 2:
                        for k1, k2 in combinations(keywords, 2):
                            if target_unique_domains > 0 and len(unique_domains_run) >= target_unique_domains:
                                break

                            pair = [k1, k2]
                            companies, total, applied_filters = client.discover(
                                headquarters_location=headquarters_location,
                                headcount_bucket=hc,
                                keywords_include=pair,
                                keyword_match="all",
                            )
                            runs_done += 1

                            qt = HunterQueryType.KW_ALL_PAIRS
                            source_query = f"KW_PAIR:{pair} | HQ={loc_label} | headcount=\"{hc}\""

                            applied_loc = _applied_location_label(applied_filters)
                            applied_hc = _applied_headcount_bucket(applied_filters)

                            tagged = [
                                c.model_copy(
                                    update={
                                        "location": applied_loc,
                                        "headcount_bucket": applied_hc,
                                        "intended_location": loc_label,
                                        "intended_headcount_bucket": hc,
                                        "source_query": source_query,
                                        "query_type": qt,
                                    }
                                )
                                for c in companies
                            ]

                            rows_main = [_company_to_row(c) for c in tagged]

                            appended = 0
                            main_to_append: list[list[str]] = []
                            for r in rows_main:
                                key = norm(r[HUNTER_DOMAIN_KEY_COL]).lower()
                                if not key:
                                    continue
                                if key in unique_domains_run:
                                    continue
                                unique_domains_run.add(key)
                                main_to_append.append(r)
                                appended += 1

                            # DB-first approach: write to database immediately
                            if main_to_append:
                                db_rows = []
                                for r in main_to_append:
                                    db_row = HunterIOResultRow.from_runner_data(
                                        job_id=job_id,
                                        request_id=rid,
                                        organisation=r[0],  # Organisation (new format)
                                        domain=r[1],        # Domain (new format)
                                        location=applied_loc,
                                        headcount=applied_hc,
                                        search_query=f"{k1},{k2} | {qt.value}",
                                        debug_filters=f"{loc_label} | {hc}"
                                    )
                                    db_rows.append(db_row)
                                
                                # Add to batch processor
                                if batch_processor:
                                    batch_processor.add_results(db_rows)
                                
                                # Note: No live append - export happens at job completion

                            run = HunterRunResult(
                                query_type=qt,
                                location=loc_label,
                                headcount_bucket=hc,
                                query_text=None,
                                keywords=pair,
                                keyword_match="all",
                                applied_filters=applied_filters or {},
                                applied_location=applied_loc or None,
                                applied_headcount_bucket=applied_hc or None,
                                returned_count=len(tagged),
                                total_results=total,
                            )
                            result.runs.append(run)
                            result.companies.extend(tagged)

                            audit_row = _audit_to_row(job_id=job_id, request_id=rid, run=run, appended=appended)
                            
                            # Add audit record to batch processor
                            if batch_processor:
                                batch_processor.add_audit_records([audit_row])
                            
                            # Note: No live append - export happens at job completion

                            last_msg = (
                                f"[HC {hc_i}/{len(HEADCOUNT_BUCKETS)}] {hc} | "
                                f"[LOC {loc_i}/{len(locations)}] {loc_label} | "
                                f"KW_PAIR appended={appended} | unique_run={len(unique_domains_run)}"
                            )

                            update_progress(
                                job_id,
                                phase="discover",
                                current=runs_done,
                                total=total_runs_est,
                                message=last_msg,
                                metrics={"unique_domains_run": len(unique_domains_run)},
                            )

                            status.write(
                                job_id=job_id,
                                tool="hunter",
                                request_id=rid,
                                state="RUNNING",
                                phase="discover",
                                current=len(unique_domains_run),
                                total=target_unique_domains,
                                message=last_msg,
                                meta={"unique_domains_run": len(unique_domains_run), "runs_done": runs_done},
                            )

                    if target_unique_domains > 0 and len(unique_domains_run) >= target_unique_domains:
                        break

                    # 3) All keywords match=all (THIRD)
                    companies, total, applied_filters = client.discover(
                        headquarters_location=headquarters_location,
                        headcount_bucket=hc,
                        keywords_include=keywords,
                        keyword_match="all",
                    )
                    runs_done += 1

                    qt = HunterQueryType.KW_ALL_FULL
                    source_query = f"KW_ALL:{list(keywords)} | HQ={loc_label} | headcount=\"{hc}\""

                    applied_loc = _applied_location_label(applied_filters)
                    applied_hc = _applied_headcount_bucket(applied_filters)

                    tagged = [
                        c.model_copy(
                            update={
                                "location": applied_loc,
                                "headcount_bucket": applied_hc,
                                "intended_location": loc_label,
                                "intended_headcount_bucket": hc,
                                "source_query": source_query,
                                "query_type": qt,
                            }
                        )
                        for c in companies
                    ]

                    rows_main = [_company_to_row(c) for c in tagged]

                    appended = 0
                    main_to_append: list[list[str]] = []
                    for r in rows_main:
                        key = norm(r[HUNTER_DOMAIN_KEY_COL]).lower()
                        if not key:
                            continue
                        if key in unique_domains_run:
                            continue
                        unique_domains_run.add(key)
                        main_to_append.append(r)
                        appended += 1

                    # DB-first approach: write to database immediately
                    if main_to_append:
                        db_rows = []
                        for r in main_to_append:
                            db_row = HunterIOResultRow.from_runner_data(
                                job_id=job_id,
                                request_id=rid,
                                organisation=r[0],  # Organisation (new format)
                                domain=r[1],        # Domain (new format)
                                location=applied_loc,
                                headcount=applied_hc,
                                search_query=f"{','.join(keywords)} | {qt.value}",
                                debug_filters=f"{loc_label} | {hc}"
                            )
                            db_rows.append(db_row)
                        
                        # Add to batch processor
                        if batch_processor:
                            batch_processor.add_results(db_rows)
                        
                        # Note: No live append - export happens at job completion

                    run = HunterRunResult(
                        query_type=qt,
                        location=loc_label,
                        headcount_bucket=hc,
                        query_text=None,
                        keywords=list(keywords),
                        keyword_match="all",
                        applied_filters=applied_filters or {},
                        applied_location=applied_loc or None,
                        applied_headcount_bucket=applied_hc or None,
                        returned_count=len(tagged),
                        total_results=total,
                    )
                    result.runs.append(run)
                    result.companies.extend(tagged)

                    audit_row = _audit_to_row(job_id=job_id, request_id=rid, run=run, appended=appended)
                    
                    # Add audit record to batch processor
                    if batch_processor:
                        batch_processor.add_audit_records([audit_row])
                    
                    # Note: No live append - export happens at job completion

                    last_msg = (
                        f"[HC {hc_i}/{len(HEADCOUNT_BUCKETS)}] {hc} | "
                        f"[LOC {loc_i}/{len(locations)}] {loc_label} | "
                        f"KW_ALL appended={appended} | unique_run={len(unique_domains_run)}"
                    )

                    update_progress(
                        job_id,
                        phase="discover",
                        current=runs_done,
                        total=total_runs_est,
                        message=last_msg,
                        metrics={"unique_domains_run": len(unique_domains_run)},
                    )

                    status.write(
                        job_id=job_id,
                        tool="hunter",
                        request_id=rid,
                        state="RUNNING",
                        phase="discover",
                        current=len(unique_domains_run),
                        total=target_unique_domains,
                        message=last_msg,
                        meta={"unique_domains_run": len(unique_domains_run), "runs_done": runs_done},
                    )

            # PHASE 2: WEB_QUERY PROCESSING (LAST)
            for hc_i, hc in enumerate(HEADCOUNT_BUCKETS, start=1):
                if target_unique_domains > 0 and len(unique_domains_run) >= target_unique_domains:
                    break

                for loc_i, (loc_label, hq_payload) in enumerate(locations, start=1):
                    if target_unique_domains > 0 and len(unique_domains_run) >= target_unique_domains:
                        break

                    headquarters_location = {"include": hq_payload["include"]}

                    # WEB_QUERY passes
                    for q in web_queries:
                        if target_unique_domains > 0 and len(unique_domains_run) >= target_unique_domains:
                            break

                        composed_query = f"{q} in {loc_label} with company size {hc}"
                        companies, total, applied_filters = client.discover(query_text=composed_query)
                        runs_done += 1

                        qt = HunterQueryType.WEB_QUERY
                        source_query = f'WEB_AI:"{composed_query}"'

                        applied_loc = _applied_location_label(applied_filters)
                        applied_hc = _applied_headcount_bucket(applied_filters)

                        tagged: list[HunterCompany] = [
                            c.model_copy(
                                update={
                                    "location": applied_loc,
                                    "headcount_bucket": applied_hc,
                                    "intended_location": loc_label,
                                    "intended_headcount_bucket": hc,
                                    "source_query": source_query,
                                    "query_type": qt,
                                }
                            )
                            for c in companies
                        ]

                        rows_main = [_company_to_row(c) for c in tagged]

                        appended = 0
                        main_to_append: list[list[str]] = []
                        for r in rows_main:
                            key = norm(r[HUNTER_DOMAIN_KEY_COL]).lower()
                            if not key:
                                continue
                            if key in unique_domains_run:
                                continue
                            unique_domains_run.add(key)
                            main_to_append.append(r)
                            appended += 1

                        # DB-first approach: write to database immediately
                        if main_to_append:
                            db_rows = []
                            for r in main_to_append:
                                db_row = HunterIOResultRow.from_runner_data(
                                    job_id=job_id,
                                    request_id=rid,
                                    organisation=r[0],  # Organisation (new format)
                                    domain=r[1],        # Domain (new format)
                                    location=applied_loc,
                                    headcount=applied_hc,
                                    search_query=f"{composed_query} | {qt.value}",
                                    debug_filters=f"{loc_label} | {hc}"
                                )
                                db_rows.append(db_row)
                            
                            # Add to batch processor
                            if batch_processor:
                                batch_processor.add_results(db_rows)
                            
                            # Note: No live append - export happens at job completion

                        run = HunterRunResult(
                            query_type=qt,
                            location=loc_label,
                            headcount_bucket=hc,
                            query_text=composed_query,
                            keywords=[],
                            keyword_match=None,
                            applied_filters=applied_filters or {},
                            applied_location=applied_loc or None,
                            applied_headcount_bucket=applied_hc or None,
                            returned_count=len(tagged),
                            total_results=total,
                        )
                        result.runs.append(run)
                        result.companies.extend(tagged)

                        audit_row = _audit_to_row(job_id=job_id, request_id=rid, run=run, appended=appended)
                        
                        # Add audit record to batch processor
                        if batch_processor:
                            batch_processor.add_audit_records([audit_row])
                        
                        # Note: No live append - export happens at job completion

                        last_msg = (
                            f"[HC {hc_i}/{len(HEADCOUNT_BUCKETS)}] {hc} | "
                            f"[LOC {loc_i}/{len(locations)}] {loc_label} | "
                            f"WEB_AI appended={appended} | unique_run={len(unique_domains_run)}"
                        )

                        update_progress(
                            job_id,
                            phase="discover",
                            current=runs_done,
                            total=total_runs_est,
                            message=last_msg,
                            metrics={"unique_domains_run": len(unique_domains_run)},
                        )

                        status.write(
                            job_id=job_id,
                            tool="hunter",
                            request_id=rid,
                            state="RUNNING",
                            phase="discover",
                            current=len(unique_domains_run),
                            total=target_unique_domains,
                            message=last_msg,
                            meta={"unique_domains_run": len(unique_domains_run), "runs_done": runs_done},
                        )

        # Flush any remaining batched DB writes
        if batch_processor:
            batch_processor.flush_all()
            log.info("Flushed all remaining batch processor buffers")

        # Export to Google Sheets (DB-first approach)
        if settings.hunter_sheets_export_enabled:
            try:
                log.info("SHEETS_EXPORT | Starting DB→Sheets export")
                
                # Query database for results
                results_repo = HunterIOResultsRepository()
                success, error, db_results = results_repo.get_companies_by_job(job_id)
                
                sheets_rows = []
                if success and db_results:
                    sheets_rows = convert_db_results_to_sheets_format(db_results)
                else:
                    log.warning(f"SHEETS_EXPORT | Failed to retrieve results: {error}")
                
                # Query database for audit records
                audit_repo = HunterAuditRepository()
                success_audit, error_audit, db_audit = audit_repo.get_audit_by_job(job_id)
                
                audit_sheets_rows = []
                if success_audit and db_audit:
                    audit_sheets_rows = convert_db_audit_to_sheets_format(db_audit)
                
                # Export using lightweight exporter
                export_hunter_results(
                    client=sheets,
                    job_id=job_id,
                    request_id=rid,
                    results_rows=sheets_rows,
                    audit_rows=audit_sheets_rows if audit_sheets_rows else None,
                )
                log.info(f"HUNTERIO | sheets_export | results={len(sheets_rows)} | audit={len(audit_sheets_rows)}")
                    
            except Exception as e:
                log.error(f"SHEETS_EXPORT | Failed: {e}")
                # Don't fail the job if sheets export fails

        # Persist JSON artifact
        result.total_unique_domains = len(unique_domains_run)
        save_enrichment_output(
            provider="hunter",
            kind="discover",
            request_id=rid,
            payload=result,
            raw_prompt=raw_prompt,
        )

        mark_succeeded(
            job_id,
            message="Hunter job completed.",
            metrics={"runs_done": runs_done, "unique_domains_run": len(unique_domains_run)},
        )
        log.info("JOB_SUCCEEDED | runs_done=%d | unique_domains_run=%d", runs_done, len(unique_domains_run))
        
        # Trigger Master ingestion after successful completion
        try:
            from ...master_data.orchestration import trigger_master_job
            
            success, error, master_job_id = trigger_master_job(
                request_id=rid,
                spreadsheet_id=sid,
                source_tool="hunter"
            )
            
            if success:
                log.info("MASTER_TRIGGERED | master_job_id=%s", master_job_id)
            else:
                log.warning("MASTER_TRIGGER_FAILED | error=%s", error)
        except Exception as e:
            log.error("MASTER_TRIGGER_ERROR | error=%s", e, exc_info=True)
            # Don't fail the Hunter job if Master trigger fails

        if status:
            status.write(
                job_id=job_id,
                tool="hunter",
                request_id=rid,
                state="SUCCEEDED",
                phase="discover",
                current=len(unique_domains_run),
                total=target_unique_domains,
                message="Hunter job completed.",
                meta={"runs_done": runs_done, "unique_domains_run": len(unique_domains_run)},
                force=True,
            )

    except Exception as exc:
        try:
            # Flush any remaining batched DB writes on failure
            if batch_processor:
                batch_processor.flush_all()
                log.info("Flushed remaining batch processor buffers on failure")
        except Exception:
            logger.exception("BATCH_PROCESSOR_FINAL_FLUSH_FAILED")
        

        mark_failed(job_id, error=str(exc), message="Hunter job failed.")
        log.exception("JOB_FAILED | error=%s", exc)

        try:
            if status:
                status.write(
                    job_id=job_id,
                    tool="hunter",
                    request_id=rid or "",
                    state="FAILED",
                    phase="discover",
                    current=len(unique_domains_run),
                    total=target_unique_domains,
                    message=str(exc),
                    meta={"runs_done": runs_done, "unique_domains_run": len(unique_domains_run)},
                    force=True,
                )
            if gsm:
                gsm.flush()
        except Exception:
            logger.exception("TOOL_STATUS_WRITE_FAILED")
