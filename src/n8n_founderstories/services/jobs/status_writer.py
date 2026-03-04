"""
Job status writer utilities.

Classification: Reliability / Defensive IO

Goal:
- One shared StatusWriter contract across all tools (hunter, google_maps, etc.)
- One safe writer helper that accepts either:
  - a callable(job_id, tool, request_id, state)  (positional OR keyword)
  - an object with .write(job_id=..., tool=..., request_id=..., state=...)
- Never raises; logs warnings only.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Protocol

logger = logging.getLogger(__name__)


# ============================================================================
# Public contract / typing
# ============================================================================

class StatusWriter(Protocol):
    """Writer object contract (ex: Sheets status writer)."""

    def write(self, *, job_id: str, tool: str, request_id: str, state: str) -> None:
        ...


# Callable signature: allow either positional or keyword usage in practice.
# Typing can't perfectly express "positional-or-keyword with exact names",
# so we keep it permissive and enforce behavior at runtime in safe_status_write.
StatusWriterFn = Callable[..., None]

StatusWriterLike = StatusWriter | StatusWriterFn | None


# ============================================================================
# Reliability / Defensive IO
# ============================================================================

def _call_best_effort(fn: Callable[..., Any], *, job_id: str, tool: str, request_id: str, state: str) -> bool:
    """
    Try calling `fn` with keywords first, then positional.
    Returns True if it worked, False otherwise.
    """
    try:
        fn(job_id=job_id, tool=tool, request_id=request_id, state=state)
        return True
    except TypeError:
        # likely positional-only or different kw names
        try:
            fn(job_id, tool, request_id, state)
            return True
        except TypeError:
            return False


def safe_status_write(
    status_writer: StatusWriterLike,
    *,
    job_id: str,
    tool: str,
    request_id: str,
    state: str,
) -> None:
    """
    Best-effort status write.

    Supports:
    - callable(job_id, tool, request_id, state) (positional or keyword)
    - object.write(job_id=..., tool=..., request_id=..., state=...)

    Never raises.
    """
    if not status_writer:
        return

    try:
        # Prefer object-with-write if present (reduces ambiguity with callable objects)
        write_fn = getattr(status_writer, "write", None)
        if callable(write_fn):
            ok = _call_best_effort(write_fn, job_id=job_id, tool=tool, request_id=request_id, state=state)
            if not ok:
                logger.warning(
                    "Status writer .write() has incompatible signature: %r",
                    type(status_writer),
                )
            return

        # Fall back to plain callable
        if callable(status_writer):
            ok = _call_best_effort(status_writer, job_id=job_id, tool=tool, request_id=request_id, state=state)
            if not ok:
                logger.warning(
                    "Status writer callable has incompatible signature: %r",
                    type(status_writer),
                )
            return

        logger.warning("Invalid status_writer: expected callable or .write(...), got %r", type(status_writer))

    except Exception:
        logger.warning("Status writer failed", exc_info=True)