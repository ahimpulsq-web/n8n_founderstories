from __future__ import annotations

# ============================================================================
# service.py
#
# Role:
# - Interpret a raw user prompt into a structured PromptInterpretation
# - Uses LLM for: normalization, translation, target/keywords, location extraction
# - Applies deterministic post-validation
# - Adds deterministic industry matching (no LLM)
# ============================================================================

from uuid import uuid4

from .models import PromptInterpretation
from .prompts import SEARCH_PLAN_GENERATION_INSTRUCTIONS
from .validators import post_validate_prompt_interpretation

from ..openrouter.openrouter_client import (
    PackageLLMSpec,
    generate_structured,
    set_run_context,
)

from ...core.config import settings
from ...services.embeddings.match_industry import match_industries


def interpret_prompt(*, request_id: str | None, raw_prompt: str) -> PromptInterpretation:
    """
    Convert a raw user prompt into a structured PromptInterpretation.

    Guarantees:
    - request_id is always present (generated if missing)
    - deterministic keyword cleanup
    - deterministic industry matching appended
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
    set_run_context(module="llm_searchplan", request_id=request_id)

    # ------------------------------------------------------------------------
    # 4) LLM package strategy (stable + deterministic)
    # ------------------------------------------------------------------------
    pkg = PackageLLMSpec(
        tier=settings.prompt_tier,
        strategy="vote",
        vote_k=3,
        vote_min_wins=2,
        temperature=0.0,
    )

    # ------------------------------------------------------------------------
    # 5) LLM call -> structured schema output
    # ------------------------------------------------------------------------
    out = generate_structured(
        pkg=pkg,
        user_prompt=raw_prompt,
        system_instructions=SEARCH_PLAN_GENERATION_INSTRUCTIONS,
        schema_model=PromptInterpretation,
    )

    # ------------------------------------------------------------------------
    # 6) Deterministic post-processing (sanity + cleanup)
    # ------------------------------------------------------------------------
    pi = post_validate_prompt_interpretation(out)

    # Enforce request_id on returned object
    pi.request_id = request_id

    # ------------------------------------------------------------------------
    # 7) Deterministic industry matching (no LLM)
    # ------------------------------------------------------------------------
    pi.matched_industries = match_industries(
        prompt_target=pi.prompt_target,
        top_k=20,
    )

    return pi
