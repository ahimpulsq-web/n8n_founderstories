from __future__ import annotations

# ============================================================================
# service.py
#
# Role:
# - Orchestration layer for prompt interpretation
# - Calls LLM for structured intent extraction
# - Applies deterministic post-validation
# - Resolves locations deterministically (via geo.py)
# - Matches industries deterministically (via embeddings)
# ============================================================================

import logging
from uuid import uuid4

from .models import PromptInterpretation
from .prompts import SEARCH_PLAN_GENERATION_INSTRUCTIONS
from .validators import post_validate_prompt_interpretation
from .geo import resolve_locations

from ..openrouter import (
    OpenRouterClient,
    LLMRunSpec,
    set_run_context,
)

from ...core.config import settings
from .industry_matching import match_industries

logger = logging.getLogger(__name__)

# Reuse client instance across calls
_client = OpenRouterClient()


def interpret_prompt(*, request_id: str | None, raw_prompt: str) -> PromptInterpretation:
    """
    Convert a raw user prompt into a structured PromptInterpretation.

    Orchestration flow:
    1. Validate input
    2. Call LLM for structured intent extraction
    3. Post-validate LLM output (deterministic cleanup)
    4. Resolve locations deterministically (via geo.py)
    5. Match industries deterministically (via embeddings)
    
    Guarantees:
    - request_id is always present (generated if missing)
    - deterministic keyword cleanup
    - deterministic location resolution (no LLM)
    - deterministic industry matching (no LLM)
    
    Location resolution logic:
    - global_search == True → resolved_locations = None (no filtering)
    - global_search == False and prompt_location is None/empty → resolved_locations = DACH_DEFAULT
    - prompt_location has tokens → geocode tokens (with fallback to DACH)
    """
    # ------------------------------------------------------------------------
    # 1) Input validation
    # ------------------------------------------------------------------------
    if not raw_prompt or not raw_prompt.strip():
        raise ValueError("raw_prompt must be a non-empty string")

    # ------------------------------------------------------------------------
    # 2) Ensure request_id exists (for tracing/correlation)
    # ------------------------------------------------------------------------
    request_id = request_id or str(uuid4())

    # ------------------------------------------------------------------------
    # 3) Run context (OpenRouter layer uses this for tracing/log correlation)
    # ------------------------------------------------------------------------
    set_run_context(module="search_plan", request_id=request_id)

    # ------------------------------------------------------------------------
    # 4) LLM run specification (stable + deterministic)
    # ------------------------------------------------------------------------
    spec = LLMRunSpec(
        models=list(settings.llm_premium_models),
        strategy="vote",
        vote_k=3,
        vote_min_wins=2,
        temperature=0.0,
    )

    # ------------------------------------------------------------------------
    # 5) LLM call -> structured schema output
    # ------------------------------------------------------------------------
    logger.debug("INTERPRET_PROMPT | request_id=%s | calling_llm", request_id)
    out = _client.generate_structured(
        user_prompt=raw_prompt,
        system_instructions=SEARCH_PLAN_GENERATION_INSTRUCTIONS,
        schema_model=PromptInterpretation,
        spec=spec,
    )

    # ------------------------------------------------------------------------
    # 6) Deterministic post-processing (sanity + cleanup of LLM fields)
    # ------------------------------------------------------------------------
    logger.debug("INTERPRET_PROMPT | request_id=%s | post_validating", request_id)
    pi = post_validate_prompt_interpretation(out)

    # Set metadata fields
    pi.request_id = request_id
    pi.raw_prompt = raw_prompt

    # ------------------------------------------------------------------------
    # 7) Deterministic location resolution (via geo.py)
    # ------------------------------------------------------------------------
    logger.debug(
        "INTERPRET_PROMPT | request_id=%s | resolving_locations | global_search=%s | prompt_location=%r",
        request_id,
        pi.global_search,
        pi.prompt_location,
    )
    pi.resolved_locations = resolve_locations(
        prompt_location=pi.prompt_location,
        global_search=pi.global_search,
    )
    
    if pi.resolved_locations:
        logger.info(
            "INTERPRET_PROMPT | request_id=%s | resolved_locations_count=%d",
            request_id,
            len(pi.resolved_locations),
        )
    else:
        logger.info("INTERPRET_PROMPT | request_id=%s | resolved_locations=None (global_search)", request_id)

    # ------------------------------------------------------------------------
    # 8) Deterministic industry matching (no LLM)
    # ------------------------------------------------------------------------
    logger.debug("INTERPRET_PROMPT | request_id=%s | matching_industries", request_id)
    pi.matched_industries = match_industries(
        prompt_target=pi.prompt_target,
        top_k=20,
    )

    logger.info(
        "INTERPRET_PROMPT | request_id=%s | complete | industries=%d",
        request_id,
        len(pi.matched_industries) if pi.matched_industries else 0,
    )

    return pi
