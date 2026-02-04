"""
Mapper for converting pipeline output to database rows.

This module provides functions to map web search pipeline output
to WebSearchResultRow instances for database persistence.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from .models import WebSearchResultRow

logger = logging.getLogger(__name__)


def _lookup_hit_by_url(hits: List[Dict[str, Any]], url: str) -> Dict[str, Any] | None:
    """
    Lookup a hit by URL to get geo fields.
    
    Args:
        hits: List of hit dictionaries from pipeline output
        url: URL to lookup
        
    Returns:
        Hit dictionary if found, None otherwise
    """
    if not url:
        return None
    
    url_normalized = url.lower().strip()
    for hit in hits:
        hit_url = (hit.get("url") or "").lower().strip()
        if hit_url == url_normalized:
            return hit
    
    return None


def build_web_search_rows(
    job_id: str | None,
    search_plan: Dict[str, Any],
    pipeline_out: Dict[str, Any]
) -> List[WebSearchResultRow]:
    """
    Build WebSearchResultRow instances from pipeline output.
    
    This function maps the pipeline output to database rows by:
    1. Processing company hits (classified as type=company)
    2. Processing blog extracted companies
    3. Looking up geo fields from hits for each row
    
    Args:
        job_id: Job identifier (optional)
        search_plan: Search plan dictionary with request_id, query, etc.
        pipeline_out: Pipeline output dictionary with hits, classified, blog_extractions
        
    Returns:
        List of WebSearchResultRow instances ready for database insertion
    """
    request_id = pipeline_out.get("request_id") or search_plan.get("request_id")
    query = pipeline_out.get("query") or search_plan.get("target_search")
    
    if not request_id:
        logger.warning("No request_id found in pipeline output or search plan")
        return []
    
    hits = pipeline_out.get("hits", [])
    classified = pipeline_out.get("classified", [])
    blog_extractions = pipeline_out.get("blog_extractions", [])
    
    rows: List[WebSearchResultRow] = []
    
    # A) Process company hits
    for item in classified:
        classification = item.get("classification") or {}
        if classification.get("type") != "company":
            continue
        
        url = item.get("url")
        if not url:
            continue
        
        # Lookup geo fields from hits
        hit = _lookup_hit_by_url(hits, url)
        country = hit.get("source_country") if hit else None
        location = hit.get("source_location") if hit else None
        language = hit.get("source_language") if hit else None
        domain = hit.get("source_domain") if hit else None
        
        try:
            row = WebSearchResultRow.from_company_hit(
                job_id=job_id,
                request_id=request_id,
                url=url,
                title=item.get("title"),
                snippet=item.get("snippet"),
                classification=classification,
                country=country,
                location=location,
                language=language,
                domain=domain,
                query=query,
            )
            rows.append(row)
        except Exception as e:
            logger.warning(
                "Failed to create company hit row for url=%s: %s",
                url,
                e
            )
    
    # B) Process blog extracted companies
    for blog_extraction in blog_extractions:
        blog_url = blog_extraction.get("source_url")
        if not blog_url:
            continue
        
        companies = blog_extraction.get("companies", [])
        if not companies:
            continue
        
        # Lookup geo fields from hits using blog URL
        hit = _lookup_hit_by_url(hits, blog_url)
        country = hit.get("source_country") if hit else None
        location = hit.get("source_location") if hit else None
        language = hit.get("source_language") if hit else None
        domain = hit.get("source_domain") if hit else None
        
        for company in companies:
            company_name = company.get("name")
            if not company_name:
                continue
            
            try:
                row = WebSearchResultRow.from_blog_company(
                    job_id=job_id,
                    request_id=request_id,
                    blog_url=blog_url,
                    company_name=company_name,
                    company_website=company.get("website"),
                    company_evidence=company.get("evidence"),
                    country=country,
                    location=location,
                    language=language,
                    domain=domain,
                    query=query,
                )
                rows.append(row)
            except Exception as e:
                logger.warning(
                    "Failed to create blog company row for company=%s from blog=%s: %s",
                    company_name,
                    blog_url,
                    e
                )
    
    logger.debug(
        "Built %d web search rows from pipeline output (request_id=%s)",
        len(rows),
        request_id
    )
    
    return rows