from __future__ import annotations

# ============================================================================
# prompt.py
# API v1 endpoint: /prompt
#
# Role:
# - Accept raw user prompt (typically from n8n)
# - Build a SearchPlan (single source of truth) using the service layer
# - Return a stable response envelope for downstream automation
# ============================================================================

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ...core.config import settings
from ...core.utils.text import norm
from ...services.llm.base import get_llm_client
from ...services.search_plan import SearchPlan, build_search_plan

logger = logging.getLogger(__name__)
router = APIRouter()

DEFAULT_REGION = "DACH"
SHEET_TITLE_PREFIX = "FounderStories - "
SHEET_TITLE_MAX_LEN = 90  # keep conservative for spreadsheet systems
DEFAULT_LLM_PROVIDER = "groq"  # safe fallback matching config.py default


# ============================================================================
# Request/Response Models
# ============================================================================

class PromptRequest(BaseModel):
    """Minimal request payload from n8n."""
    prompt: str = Field(..., description="User-entered search or discovery prompt.")
    llm_provider: str | None = Field(default=None, description="Override default LLM provider.")


class PromptResponse(BaseModel):
    """Stable response returned to n8n."""
    sheet_title: str
    search_plan: SearchPlan


# ============================================================================
# Small helpers (endpoint-local)
# ============================================================================

def _build_sheet_title(prompt: str) -> str:
    """
    Produce a spreadsheet-friendly sheet title.

    Constraints:
    - bounded length
    - avoids accidental whitespace bloat
    """
    base = f"{SHEET_TITLE_PREFIX}{prompt}".strip()
    if len(base) <= SHEET_TITLE_MAX_LEN:
        return base
    return base[: (SHEET_TITLE_MAX_LEN - 1)].rstrip() + "…"


def _get_llm_provider(requested: str | None) -> str:
    """
    Resolve LLM provider with safe fallback.
    
    Priority:
    1. Requested provider (if provided and not empty)
    2. Settings default (if configured)
    3. Hard-coded safe default
    """
    provider = norm(requested) or norm(settings.llm_provider) or DEFAULT_LLM_PROVIDER
    return provider


# ============================================================================
# Endpoint
# ============================================================================

@router.post("/prompt", response_model=PromptResponse)
async def receive_prompt(payload: PromptRequest) -> PromptResponse:
    """
    Receive a raw prompt and produce a SearchPlan.

    Error strategy:
    - Invalid input -> HTTP 400
    - LLM/SearchPlan failures -> HTTP 500 (let n8n handle retries)
    """
    # ------------------------------------------------------------------------
    # Input validation
    # ------------------------------------------------------------------------
    prompt = norm(payload.prompt)
    if not prompt:
        raise HTTPException(status_code=400, detail="Prompt cannot be empty after trimming whitespace.")

    sheet_title = _build_sheet_title(prompt)
    provider = _get_llm_provider(payload.llm_provider)

    logger.info("PROMPT_RECEIVED | prompt=%r | provider=%s", prompt, provider)

    # ------------------------------------------------------------------------
    # Core operation (build SearchPlan)
    # ------------------------------------------------------------------------
    try:
        llm_client = get_llm_client(provider)
        search_plan = build_search_plan(
            prompt=prompt,
            request_id=None,  # not needed for n8n contract
            llm_client=llm_client,
            region=DEFAULT_REGION,
        )
    except Exception as exc:
        logger.exception("SEARCH_PLAN_ERROR | prompt=%r | provider=%s | error=%s", prompt, provider, exc)
        raise HTTPException(
            status_code=500,
            detail="Failed to generate search plan. Please try again."
        ) from exc

    # ------------------------------------------------------------------------
    # Response envelope (stable contract)
    # ------------------------------------------------------------------------
    return PromptResponse(
        sheet_title=sheet_title,
        search_plan=search_plan,
    )