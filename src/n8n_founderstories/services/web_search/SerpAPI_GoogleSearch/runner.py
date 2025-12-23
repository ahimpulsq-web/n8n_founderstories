from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from ....core.utils.text import norm
from ....services.exports.sheets import SheetsClient, default_sheets_config
from ....services.exports.sheets_manager import GoogleSheetsManager
from ....services.jobs.logging import job_logger
from ....services.jobs.sheets_status import ToolStatusWriter
from ....services.jobs.store import mark_failed, mark_running, mark_succeeded, update_progress
from ....services.search_plan import SearchPlan
from ....services.storage import save_google_search_output
from .client import SerpApiClient
from .models import GoogleSearchJobResult, GoogleSearchRunResult

logger = logging.getLogger(__name__)

TAB_MAIN = "GoogleSearch"
TAB_AUDIT = "GoogleSearch_Audit"
COL_KEY = 0

HEADERS_MAIN = [
    "key",
    "domain",
    "possible_company_name",
    "google_search_location",
    "query_executed",
    "source_type",
    "timestamp",
]

HEADERS_AUDIT = [
    "Job ID",
    "Request ID",
    "Phase",
    "Country (ISO2)",
    "HL (Plan)",
    "HL Used",
    "GL",
    "Google Domain",
    "Location (SerpAPI)",
    "Search Model",
    "Query",
    "Query Executed",
    "Num",
    "Start",
    "Returned Count",
    "Domains (CSV)",
    "Appended Rows",
    "Unique Domains (Job)",
    "Error",
    "Timestamp",
    "Raw Meta (JSON)",
]


def _extract_domain(url: str | None) -> str | None:
    try:
        host = urlparse(str(url or "")).netloc.lower()
        host = host[4:] if host.startswith("www.") else host
        return host or None
    except Exception:
        return None


def _domain_norm(d: str | None) -> str:
    v = norm(d).lower()
    return v[4:] if v.startswith("www.") else v


def _query_norm(q: str | None) -> str:
    return norm(q).replace("_", " ")


def _looks_like_language_tag(v: str) -> bool:
    s = norm(v)
    if not s or len(s) > 10:
        return False
    return all(ch.isalpha() or ch == "-" for ch in s)


def _resolve_hl(*, hl_plan: str | None, default_lang: str = "en") -> str:
    hl = norm(hl_plan)
    return hl if hl and _looks_like_language_tag(hl) else default_lang


def google_domain_for_gl(gl: str) -> str:
    gln = norm(gl).lower()
    mapping = {
        "de": "google.de",
        "at": "google.at",
        "ch": "google.ch",
        "gb": "google.co.uk",
        "fr": "google.fr",
        "it": "google.it",
        "es": "google.es",
        "in": "google.co.in",
        "jp": "google.co.jp",
        "us": "google.com",
    }
    return mapping.get(gln, "google.com")


def _locations_from_bucket(bucket: dict[str, Any]) -> list[str]:
    locs = (bucket or {}).get("locations") or []
    locs = [norm(x) for x in locs if norm(x)]
    seen: set[str] = set()
    out: list[str] = []
    for x in locs:
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def _geoify_query(q: str, *, location: str | None) -> str:
    qn = _query_norm(q)
    loc = norm(location)
    if not qn or not loc:
        return qn
    return f"{qn} in {loc}"


def _build_key(*, domain: str, google_search_location: str, query_executed: str) -> str:
    return f"{domain}|{google_search_location}|{query_executed}".lower()


def _classify_source_type(source: str | None, url: str | None) -> str:
    s = norm(source).lower()
    if s:
        if "ad" in s:
            return "ad"
        return s

    u = norm(url).lower()
    if "/blog" in u:
        return "blog"
    if any(x in u for x in ["/news", "/press", "/media"]):
        return "news"
    return "organic"


def run_google_search_job(
    *,
    job_id: str,
    plan: SearchPlan,
    spreadsheet_id: str,
    max_queries: int = 10,
    max_results_per_query: int = 10,
    max_total_results: int = 250,
    dedupe_in_run: bool = True,
    use_cache: bool = True,
    search_model: str = "geo_in_query",  # geo_in_query | location_routing | hybrid
) -> None:
    rid = norm(getattr(plan, "request_id", None))
    log = job_logger(__name__, tool="google_search", request_id=rid, job_id=job_id)

    runs_done = 0
    total_runs_est: int | None = None
    unique_domains: set[str] = set()
    seen_keys_run: set[str] = set()

    sheets: SheetsClient | None = None
    gsm: GoogleSheetsManager | None = None
    status: ToolStatusWriter | None = None

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

        if search_model not in {"geo_in_query", "location_routing", "hybrid"}:
            raise ValueError("search_model must be one of: geo_in_query | location_routing | hybrid")

        mq = max(1, int(max_queries))
        m_rpq = max(1, int(max_results_per_query))
        m_total = max(1, int(max_total_results))

        all_queries = [norm(q) for q in (getattr(plan, "web_queries", None) or []) if norm(q)]
        queries = [_query_norm(q) for q in all_queries if _query_norm(q)]
        queries = queries[:mq]
        if not queries:
            raise ValueError("search_plan.web_queries must not be empty.")

        geo_cfg = getattr(plan, "geo_location_keywords", None) or {}

        region_items: list[tuple[str, str | None, dict[str, Any]]] = []
        if isinstance(geo_cfg, dict) and geo_cfg:
            for iso2_raw, bucket_raw in geo_cfg.items():
                iso2 = norm(str(iso2_raw)).upper()
                if len(iso2) != 2 or not iso2.isalpha():
                    continue
                bucket = bucket_raw if isinstance(bucket_raw, dict) else {}
                hl_plan = norm(bucket.get("hl")) or None
                region_items.append((iso2, hl_plan, bucket))
        else:
            region_items.append(("US", None, {}))

        location_triplets: list[tuple[str, str | None, str | None]] = []
        for iso2, hl_plan, bucket in region_items:
            locs = _locations_from_bucket(bucket)
            if not locs:
                location_triplets.append((iso2, hl_plan, None))
            else:
                for loc in locs:
                    location_triplets.append((iso2, hl_plan, loc))

        total_runs_est = len(queries) * len(location_triplets)
        if search_model == "hybrid":
            total_runs_est *= 2

        # Sheets + Manager + Status
        sheets = SheetsClient(config=default_sheets_config(spreadsheet_id=sid))
        gsm = GoogleSheetsManager(client=sheets)
        status = ToolStatusWriter(sheets=sheets, spreadsheet_id=sid, manager=gsm)

        sheets.ensure_tab("Tool_Status")
        sheets.ensure_tab(TAB_MAIN)
        sheets.ensure_tab(TAB_AUDIT)

        gsm.setup_tabs(
            headers=[
                ("Tool_Status", status.header()),
                (TAB_MAIN, HEADERS_MAIN),
                (TAB_AUDIT, HEADERS_AUDIT),
            ],
            hide_tabs=[TAB_AUDIT],
            tab_order=["Tool_Status", "HunterIO", "HunterIO_Audit", "GoogleMaps", "GoogleMaps_Audit", TAB_MAIN, TAB_AUDIT],
            overwrite_headers_for_owned_tabs=True,
        )

        result = GoogleSearchJobResult(
            request_id=rid,
            raw_prompt=raw_prompt,
            provider_name=provider_name,
            geo=geo,
            max_queries=mq,
            max_results_per_query=m_rpq,
            max_total_results=m_total,
            dedupe_domains=bool(dedupe_in_run),
            use_cache=bool(use_cache),
            search_model=search_model,
            queries_used=list(queries),
            iso2_used=sorted({iso2 for (iso2, _, _) in location_triplets}),
            locations_used=[norm(loc) for (_, _, loc) in location_triplets if norm(loc)],
            total_runs_est=total_runs_est,
            runs_done=0,
            total_unique_domains=0,
        )

        update_progress(
            job_id,
            phase="search",
            current=0,
            total=total_runs_est,
            message="Starting Google Search discovery.",
            metrics={
                "queries": len(queries),
                "regions": len(region_items),
                "locations": len(location_triplets),
                "search_model": search_model,
                "dedupe_in_run": bool(dedupe_in_run),
            },
        )

        status.write(
            job_id=job_id,
            tool="google_search",
            request_id=rid,
            state="RUNNING",
            phase="search",
            current=0,
            total=total_runs_est,
            message="Starting Google Search discovery.",
            meta={
                "queries": len(queries),
                "regions": len(region_items),
                "locations": len(location_triplets),
                "search_model": search_model,
                "dedupe_in_run": bool(dedupe_in_run),
            },
        )

        ts = datetime.utcnow().isoformat()

        with SerpApiClient() as client:

            def _run_one(*, q_original: str, iso2: str, hl_plan: str | None, loc: str | None, model: str) -> None:
                nonlocal runs_done, ts

                hl_used = _resolve_hl(hl_plan=hl_plan, default_lang="en")
                gl = iso2.lower()
                google_domain = google_domain_for_gl(gl)

                q_exec = _query_norm(q_original) if model == "location_routing" else _geoify_query(q_original, location=loc)

                run = GoogleSearchRunResult(
                    iso2=iso2,
                    hl_plan=hl_plan,
                    hl_used=hl_used,
                    gl=gl,
                    google_domain=google_domain,
                    location_for_serp=loc,
                    query=_query_norm(q_original),
                    query_executed=q_exec,
                    search_model=model,
                    num=m_rpq,
                    start=0,
                )

                run_domains: set[str] = set()
                appended = 0
                err = ""

                main_rows: list[list[str]] = []

                try:
                    resp = client.search(
                        query=q_exec,
                        google_domain=google_domain,
                        hl=hl_used,
                        gl=gl,
                        location=loc,
                        num=m_rpq,
                        start=0,
                        request_id=rid,
                    )
                    run.returned_count = len(resp.organic_results)

                    for r in resp.organic_results:
                        d = _domain_norm(_extract_domain(r.link))
                        if not d:
                            continue

                        google_search_location = norm(loc) or ""
                        key = _build_key(domain=d, google_search_location=google_search_location, query_executed=q_exec)

                        if dedupe_in_run and key in seen_keys_run:
                            continue

                        if dedupe_in_run:
                            seen_keys_run.add(key)

                        unique_domains.add(d)
                        run_domains.add(d)

                        main_rows.append([
                            key,
                            d,
                            "",  # possible_company_name
                            google_search_location,
                            q_exec,
                            _classify_source_type(r.source, r.link),
                            ts,
                        ])

                        appended += 1
                        if len(unique_domains) >= m_total:
                            break

                except Exception as exc:
                    err = str(exc)

                # Append buffered rows via manager (one append per tab per flush)
                if gsm and main_rows:
                    gsm.queue_append_rows(tab_name=TAB_MAIN, rows=main_rows)

                run.domains = sorted(run_domains)
                run.appended_rows = appended
                run.unique_domains_job = len(unique_domains)
                run.error = err or None
                run.meta = {"model": model}

                # Audit row (also buffered)
                audit_row = [
                    job_id,
                    rid,
                    run.phase,
                    run.iso2,
                    norm(run.hl_plan),
                    run.hl_used,
                    run.gl,
                    run.google_domain,
                    norm(run.location_for_serp),
                    run.search_model,
                    run.query,
                    run.query_executed,
                    str(run.num),
                    str(run.start),
                    str(run.returned_count),
                    ", ".join(run.domains),
                    str(run.appended_rows),
                    str(run.unique_domains_job),
                    norm(run.error),
                    datetime.utcnow().isoformat(),
                    json.dumps(run.meta or {}, ensure_ascii=False),
                ]
                if gsm:
                    gsm.queue_append_rows(tab_name=TAB_AUDIT, rows=[audit_row])

                result.runs.append(run)

                runs_done += 1
                result.runs_done = runs_done
                result.total_unique_domains = len(unique_domains)

                msg = f"{iso2} | {norm(loc) or 'NO_LOC'} | {model} | appended={appended} | unique={len(unique_domains)}"
                update_progress(
                    job_id,
                    phase="search",
                    current=runs_done,
                    total=total_runs_est,
                    message=msg,
                    metrics={"unique_domains": len(unique_domains)},
                )

                if status:
                    status.write(
                        job_id=job_id,
                        tool="google_search",
                        request_id=rid,
                        state="RUNNING",
                        phase="search",
                        current=runs_done,
                        total=total_runs_est,
                        message=msg,
                        meta={"unique_domains": len(unique_domains), "runs_done": runs_done},
                    )

            for q in queries:
                if len(unique_domains) >= m_total:
                    break

                for iso2, hl_plan, loc in location_triplets:
                    if len(unique_domains) >= m_total:
                        break

                    if search_model in {"location_routing", "hybrid"}:
                        _run_one(q_original=q, iso2=iso2, hl_plan=hl_plan, loc=loc, model="location_routing")
                        if len(unique_domains) >= m_total:
                            break

                    if search_model in {"geo_in_query", "hybrid"}:
                        _run_one(q_original=q, iso2=iso2, hl_plan=hl_plan, loc=loc, model="geo_in_query")
                        if len(unique_domains) >= m_total:
                            break

        # Ensure all buffered writes are persisted
        if gsm:
            gsm.flush()

        save_google_search_output(
            request_id=rid,
            prompt=raw_prompt,
            payload=result.model_dump(mode="json"),
            provider="google_search",
            kind="results",
        )

        msg = "Google Search job completed."
        if len(unique_domains) >= m_total:
            msg = "Google Search job completed (stopped early: max_total_results reached)."

        mark_succeeded(
            job_id,
            message=msg,
            metrics={"runs_done": runs_done, "unique_domains": len(unique_domains)},
        )
        log.info("JOB_SUCCEEDED | runs_done=%d | unique_domains=%d", runs_done, len(unique_domains))

        if status:
            status.write(
                job_id=job_id,
                tool="google_search",
                request_id=rid,
                state="SUCCEEDED",
                phase="search",
                current=runs_done,
                total=total_runs_est,
                message=msg,
                meta={"runs_done": runs_done, "unique_domains": len(unique_domains)},
                force=True,
            )
        if gsm:
            gsm.flush()

    except Exception as exc:
        if gsm:
            try:
                gsm.flush()
            except Exception:
                logger.exception("SHEETS_MANAGER_FINAL_FLUSH_FAILED")

        mark_failed(job_id, error=str(exc), message="Google Search job failed.")
        log.exception("JOB_FAILED | error=%s", exc)

        try:
            if status:
                status.write(
                    job_id=job_id,
                    tool="google_search",
                    request_id=rid or "",
                    state="FAILED",
                    phase="search",
                    current=runs_done,
                    total=total_runs_est,
                    message=str(exc),
                    meta={"runs_done": runs_done, "unique_domains": len(unique_domains)},
                    force=True,
                )
            if gsm:
                gsm.flush()
        except Exception:
            logger.exception("TOOL_STATUS_WRITE_FAILED")
