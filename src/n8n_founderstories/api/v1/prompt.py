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
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ...core.config import settings
from ...core.utils.text import norm
from ...services.llm.base import LLMClient, get_llm_client
from ...services.search_plan import SearchPlan, build_search_plan

logger = logging.getLogger(__name__)
router = APIRouter()

DEFAULT_REGION = "DACH"
SHEET_TITLE_PREFIX = "FounderStories - "
SHEET_TITLE_MAX_LEN = 90  # keep conservative for spreadsheet systems


# ============================================================================
# Request/Response Models
# ============================================================================

class PromptRequest(BaseModel):
    """Primary entrypoint payload from n8n."""
    prompt: str = Field(..., description="User-entered search or discovery prompt.")
    source: str | None = Field(default="n8n", description="Caller/source identifier (e.g., n8n).")
    request_id: str | None = Field(default=None, description="Optional request ID for re-use.")
    new_excel: bool = Field(default=False, description="Force creation of a new Excel sheet.")
    llm_provider: str | None = Field(default=None, description="Override default LLM provider.")


class PromptResponse(BaseModel):
    """Unified response returned to n8n."""
    status: str
    source: str | None
    request_id: str
    received_prompt: str
    sheet_title: str
    timestamp: datetime
    llm_provider: str | None = None
    search_plan: SearchPlan | None = None
    warning: str | None = None


# ============================================================================
# Small helpers (endpoint-local)
# ============================================================================

def _utcnow() -> datetime:
    """Timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


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


def _init_llm_client(provider: str | None) -> tuple[LLMClient | None, str | None, str | None]:
    """
    Best-effort LLM initialization.

    Returns:
      (client, resolved_provider, warning_message)
    """
    requested = norm(provider) or norm(settings.llm_provider)
    if not requested:
        return None, None, "LLM provider is not configured."

    try:
        client = get_llm_client(requested)
        return client, requested, None
    except Exception as exc:
        return None, requested, f"LLM provider unavailable: {exc}"


# ============================================================================
# Endpoint
# ============================================================================

@router.post("/prompt", response_model=PromptResponse)
async def receive_prompt(payload: PromptRequest) -> PromptResponse:
    """
    Receive a raw prompt and (best-effort) produce a SearchPlan.

    Error strategy:
    - Invalid input -> HTTP 400
    - Provider/init errors -> status="error" with warnings (does not crash caller)
    - SearchPlan failures -> status="error" with warnings
    """
    # ------------------------------------------------------------------------
    # Input validation (transport-layer)
    # ------------------------------------------------------------------------
    prompt = norm(payload.prompt)
    if not prompt:
        raise HTTPException(status_code=400, detail="Prompt must not be empty.")

    request_id = norm(payload.request_id) or str(uuid4())
    sheet_title = _build_sheet_title(prompt)

    logger.info(
        "PROMPT_RECEIVED | id=%s | source=%s | new_excel=%s | prompt=%r",
        request_id,
        payload.source,
        payload.new_excel,
        prompt,
    )

    # ------------------------------------------------------------------------
    # Dependency initialization (best-effort)
    # ------------------------------------------------------------------------
    warnings: list[str] = []
    llm_client, resolved_provider, warn = _init_llm_client(payload.llm_provider)
    if warn:
        warnings.append(warn)
        logger.warning(
            "LLM_INIT_FAILED | id=%s | provider=%s | error=%s",
            request_id,
            resolved_provider,
            warn,
        )

    # ------------------------------------------------------------------------
    # Core operation (build SearchPlan)
    # ------------------------------------------------------------------------
    search_plan: SearchPlan | None = None
    if llm_client:
        try:
            search_plan = build_search_plan(
                prompt=prompt,
                request_id=request_id,
                llm_client=llm_client,
                region=DEFAULT_REGION,
            )
        except Exception as exc:
            warnings.append("Search plan generation failed.")
            logger.exception("SEARCH_PLAN_ERROR | id=%s | error=%s", request_id, exc)

    status = "ok" if search_plan else "error"

    # ------------------------------------------------------------------------
    # Response envelope (stable contract)
    # ------------------------------------------------------------------------
    return PromptResponse(
        status=status,
        request_id=request_id,
        sheet_title=sheet_title,
        received_prompt=prompt,
        source=payload.source,
        timestamp=_utcnow(),
        search_plan=search_plan,
        llm_provider=resolved_provider,
        warning=" ".join(warnings) if warnings else None,
    )