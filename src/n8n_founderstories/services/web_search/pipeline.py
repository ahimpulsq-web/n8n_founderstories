from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from .factory import WebSearchDeps
from .log_websearch import append_pipeline_result


def _classifier_items_from_hits(hits: List[Any]) -> List[Dict[str, Any]]:
    return [
        {"url": h.link, "title": h.title, "snippet": h.snippet}
        for h in hits
        if getattr(h, "link", None) and getattr(h, "title", None)
    ]


async def run_pipeline(
    *,
    deps: WebSearchDeps,
    search_plan: Dict[str, Any],
    max_pages: int = 1,
    engine: str = "google_light",
    resolve_locations: bool = True,
    classify_workers: int = 5,
    extract_blog_limit: int = 10,
    job_id: str | None = None,
) -> Dict[str, Any]:
    request_id = search_plan.get("request_id")
    prompt_language = search_plan.get("prompt_language")
    query = (search_plan.get("target_search") or "").strip()

    geo = search_plan.get("geo_location_keywords") or {}

    try:
        # 1) Google search (sync -> thread)
        hits = await asyncio.to_thread(
            deps.google.run_search_plan,
            web_queries=[query] if query else [],
            geo_location_keywords=geo,
            max_pages=max_pages,
            engine=engine,
            resolve_locations=resolve_locations,
            prompt_language=prompt_language,  # assumes your run_search_plan supports it
        )

        # 2) Classify (sync -> thread; internally threaded)
        classified = await asyncio.to_thread(
            deps.classifier.classify_many,
            _classifier_items_from_hits(hits),
            max_workers=classify_workers,
            fail_type="other",
        )

        # 3) Blog extract (async)
        blog_urls = [
            x["url"]
            for x in classified
            if (x.get("classification") or {}).get("type") == "blog"
        ][: max(0, extract_blog_limit)]

        blog_extractions: List[Dict[str, Any]] = []
        for url in blog_urls:
            try:
                blog_extractions.append(await deps.blog.extract_from_url(url))
            except Exception as e:
                blog_extractions.append({"source_url": url, "companies": [], "error": str(e)})

        out: Dict[str, Any] = {
            "request_id": request_id,
            "query": query,
            "prompt_language": prompt_language,
            "hits_count": len(hits),
            "hits": [
                {
                    "title": h.title,
                    "url": h.link,
                    "snippet": h.snippet,
                    "source_country": getattr(h, "source_country", None),
                    "source_location": getattr(h, "source_location", None),
                    "source_language": getattr(h, "source_language", None),
                    "source_domain": getattr(h, "source_domain", None),
                    "source_page": getattr(h, "source_page", None),
                }
                for h in hits
            ],
            "classified": classified,
            "blog_extractions": blog_extractions,
        }

        # Log once per pipeline run (complete log)
        append_pipeline_result(
            request_id=request_id,
            query=query,
            prompt_language=prompt_language,
            payload=out,
            status="ok",
        )

        return out

    except Exception as e:
        append_pipeline_result(
            request_id=request_id,
            query=query,
            prompt_language=prompt_language,
            payload={"error": str(e)},
            status="error",
        )
        raise
