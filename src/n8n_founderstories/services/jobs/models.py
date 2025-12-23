from __future__ import annotations

# =============================================================================
# models.py
#
# Classification:
# - Role: canonical job and progress models shared across all background tools.
# - Consumers: API (status), runners (progress updates), n8n polling.
# =============================================================================

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class JobState(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"  # reserved for later


class JobProgress(BaseModel):
    """
    Lightweight progress payload designed for polling UI (n8n).

    Keep this stable and simple; avoid tool-specific deep structures.
    """
    phase: str | None = Field(default=None, description="High-level phase label.")
    current: int | None = Field(default=None, description="Current step index (1-based preferred).")
    total: int | None = Field(default=None, description="Total steps if known.")
    message: str | None = Field(default=None, description="Human-readable progress message.")
    metrics: Dict[str, Any] = Field(default_factory=dict, description="Tool-specific counters (safe subset).")


class JobRecord(BaseModel):
    """
    Canonical job record persisted to disk.

    Classification:
    - tool: identifies which subsystem owns the runner (hunter, google_maps, web_search, etc.)
    - request_id: correlation ID shared across the entire n8n workflow
    """
    job_id: str
    tool: str
    request_id: str

    state: JobState = Field(default=JobState.QUEUED)
    progress: JobProgress = Field(default_factory=JobProgress)

    created_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    updated_at: datetime = Field(default_factory=utc_now)
    finished_at: datetime | None = None

    # Optional: put spreadsheet_id, sheet_title, provider, etc. here
    meta: Dict[str, Any] = Field(default_factory=dict)

    # Failure details (kept short)
    error: str | None = None
