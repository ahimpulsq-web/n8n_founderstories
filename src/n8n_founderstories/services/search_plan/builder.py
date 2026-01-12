from __future__ import annotations

import logging
from typing import Optional
from uuid import uuid4

from ...core.utils.text import norm
from ...core.utils.collections import dedupe_sources_keep_order

from ..geo_locator.geo_locator import resolve_geo
from ..llm.base import LLMClient, get_llm_client
from ..storage import save_search_plan_output
from .enforce_base import enforce_alternates, enforce_keywords
from .enforce_maps import enforce_maps_queries
from .enforce_web import enforce_web_queries
from .models import SearchPlan, SearchPlanMeta, SearchPlanPayload, SearchPlanPayloadLLM
from .prompts import (
    _SYSTEM_INSTRUCTIONS,
    MAX_ALTERNATES,
    MAX_KEYWORDS,
    MAX_WEB_QUERIES,
    MAX_MAPS_QUERIES,
)

logger = logging.getLogger(__name__)


def build_search_plan(
    prompt: str,
    *,
    request_id: Optional[str] = None,
    llm_client: Optional[LLMClient] = None,
    region: str = "DACH",
) -> SearchPlan:
    """
    Build a SearchPlan from the raw prompt.

    This function is the single authority for:
    - request_id creation
    - search_plan construction
    """

    if not prompt or not str(prompt).strip():
        raise ValueError("Prompt must be a non-empty string.")

    # -------------------------------------------------------------------------
    # 0) Root job identity (authoritative)
    # -------------------------------------------------------------------------
    if request_id is None:
        request_id = str(uuid4())

    client = llm_client or get_llm_client()

    # -------------------------------------------------------------------------
    # 1) System-owned geo resolution (deterministic)
    # -------------------------------------------------------------------------
    resolved_geo = resolve_geo(
        prompt=prompt,
        region=region,
        llm_client=client,  # kept for signature compatibility
    )

    # -------------------------------------------------------------------------
    # 2) LLM generation (LLM-owned fields ONLY; no geo fields)
    # -------------------------------------------------------------------------
    user_prompt = (
        f"Raw prompt: {prompt}\n\n"
        f"Geo scope (system-owned, for context only): {resolved_geo.resolved_geo}\n"
        f"Geo mode (system-owned, for context only): {resolved_geo.geo_mode}\n\n"
        "Fill the schema consistently with the above. "
        "Remember: keywords and web_queries must be geo-neutral."
    )

    try:
        llm_payload: SearchPlanPayloadLLM = client.generate_structured(
            user_prompt=user_prompt,
            system_instructions=_SYSTEM_INSTRUCTIONS,
            schema_model=SearchPlanPayloadLLM,
        )
    except Exception:
        logger.exception(
            "SEARCH_PLAN_FAILED | provider=%s | request_id=%s | prompt=%r",
            getattr(client, "provider_name", type(client).__name__),
            request_id,
            prompt,
        )
        raise

    # -------------------------------------------------------------------------
    # 3) Compose final payload (LLM-owned + system-owned geo)
    # -------------------------------------------------------------------------
    payload = SearchPlanPayload(
        industry=norm(llm_payload.industry),
        category=norm(llm_payload.category) if llm_payload.category else None,
        alternates=llm_payload.alternates or [],
        keywords=llm_payload.keywords or [],
        web_queries=llm_payload.web_queries or [],
        maps_queries=llm_payload.maps_queries or [],
        geo=norm(resolved_geo.resolved_geo),
        geo_location_keywords=dict(resolved_geo.geo_location_keywords),
    )

    # -------------------------------------------------------------------------
    # 4) Deterministic enforcement
    # -------------------------------------------------------------------------
    enforce_alternates(payload, max_alternates=MAX_ALTERNATES)
    enforce_keywords(payload, raw_prompt=prompt, max_keywords=MAX_KEYWORDS)
    enforce_web_queries(payload, max_web_queries=MAX_WEB_QUERIES)
    enforce_maps_queries(payload, raw_prompt=prompt, max_total=MAX_MAPS_QUERIES)

    # -------------------------------------------------------------------------
    # 5) Attach meta and build SearchPlan
    # -------------------------------------------------------------------------
    meta = SearchPlanMeta(
        raw_prompt=prompt,
        request_id=request_id,
        provider_name=getattr(
            client, "source_id", getattr(client, "provider_name", "llm")
        ),
    )

    plan = SearchPlan.model_validate(
        {**meta.model_dump(), **payload.model_dump()}
    )

    logger.info(
        "SEARCH_PLAN_BUILT | request_id=%s | provider=%s | industry=%s | geo=%s | geo_mode=%s | sources=%s",
        request_id,
        plan.provider_name,
        plan.industry,
        plan.geo,
        resolved_geo.geo_mode,
        ",".join(plan.sources_to_use),
    )

    # -------------------------------------------------------------------------
    # 6) Persist artifact (guaranteed request_id)
    # -------------------------------------------------------------------------
    save_search_plan_output(
        provider=plan.provider_name,
        request_id=request_id,
        payload=plan,
    )

    return plan
