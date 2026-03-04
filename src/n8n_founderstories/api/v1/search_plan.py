from __future__ import annotations

# ============================================================================
# search_plan.py
# API endpoint: POST /search_plan
#
# Role:
# - Accept a raw user prompt (typically from n8n)
# - Normalize + validate input
# - Produce a spreadsheet-friendly sheet title (string only; no sheet creation)
# - Generate a structured "search plan" via the LLM interpreter pipeline
# - Return a stable response envelope for downstream automation
# ============================================================================

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ...core.utils.text import norm
from ...services.search_plan.service import interpret_prompt
from ...services.search_plan.models import PromptInterpretation

logger = logging.getLogger(__name__)
router = APIRouter()

# ============================================================================
# Constants (endpoint-local configuration)
# ============================================================================
SHEET_TITLE_PREFIX = "FounderStories - "
SHEET_TITLE_MAX_LEN = 90  # conservative limit for spreadsheet/tab naming systems


# ============================================================================
# Request / Response Models (stable contract to n8n)
# ============================================================================
class PromptRequest(BaseModel):
    """Minimal request payload from n8n."""
    prompt: str = Field(..., description="User-entered search or discovery prompt.")


class PromptResponse(BaseModel):
    """Stable response envelope returned to n8n."""
    sheet_title: str
    search_plan: PromptInterpretation


# ============================================================================
# Helpers (pure functions; no side-effects)
# ============================================================================
def _build_sheet_title(prompt: str) -> str:
    """
    Produce a spreadsheet-friendly sheet title.

    Constraints:
    - Adds stable prefix for consistent grouping in sheet lists
    - Trims whitespace
    - Bounds the length to avoid downstream sheet/tab naming failures
    """
    base = f"{SHEET_TITLE_PREFIX}{prompt}".strip()
    if len(base) <= SHEET_TITLE_MAX_LEN:
        return base
    return base[: (SHEET_TITLE_MAX_LEN - 1)].rstrip() + "…"


# ============================================================================
# Endpoint
# ============================================================================
@router.post("/search_plan", response_model=PromptResponse)
async def receive_prompt(payload: PromptRequest) -> PromptResponse:
    """
    Receive a raw prompt and produce a structured search plan.

    Error strategy:
    - Invalid input -> HTTP 400
    - LLM/interpreter failures -> HTTP 500 (let n8n handle retries)
    """

    # ------------------------------------------------------------------------
    # 1) Input normalization + validation
    # ------------------------------------------------------------------------
    # norm() is "text hygiene" only: trims/collapses whitespace for stability.
    prompt = norm(payload.prompt)
    if not prompt:
        raise HTTPException(
            status_code=400,
            detail="Prompt cannot be empty after trimming whitespace.",
        )

    # Lifecycle: request accepted and validated
    logger.info("SEARCH PLAN | START | prompt=%r", prompt)

    # ------------------------------------------------------------------------
    # 2) Spreadsheet title suggestion (string only; no sheet creation here)
    # ------------------------------------------------------------------------
    sheet_title = _build_sheet_title(prompt)

    # ------------------------------------------------------------------------
    # 3) Core operation: interpret prompt into a structured plan
    # ------------------------------------------------------------------------
    # interpret_prompt() performs:
    # - LLM-based normalization + keyword extraction + location resolution
    # - deterministic industry matching (no LLM)
    try:
        search_plan = interpret_prompt(
            request_id=None,   # interpreter will generate one if missing
            raw_prompt=prompt, # use normalized prompt for clean inputs
        )
    except Exception as exc:
        # Lifecycle: failed during plan generation (include stacktrace)
        logger.exception("SEARCH PLAN | ERROR | prompt=%r | error=%s", prompt, exc)
        raise HTTPException(
            status_code=500,
            detail="Failed to generate search plan. Please try again.",
        ) from exc

    # Lifecycle: plan generated successfully (correlate by request_id)
    logger.info(
        "SEARCH PLAN | COMPLETE | request_id=%s",
        search_plan.request_id,
    )

    # ------------------------------------------------------------------------
    # 4) Response envelope (stable contract for n8n)
    # ------------------------------------------------------------------------
    return PromptResponse(
        sheet_title=sheet_title,
        search_plan=search_plan,
    )
