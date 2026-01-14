"""
Shared job lifecycle management wrapper.

This module provides a standardized way to manage job state transitions,
progress updates, and Tool_Status writes across all tools (HunterIO, Google Maps, etc.).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

from .sheets_status import ToolStatusWriter
from .store import mark_failed, mark_running, mark_succeeded, update_progress

logger = logging.getLogger(__name__)


@contextmanager
def job_lifecycle(
    *,
    job_id: str,
    tool: str,
    request_id: str,
    phase: str,
    total: Optional[int] = None,
    status_writer: Optional[ToolStatusWriter] = None,
) -> Generator[None, None, None]:
    """
    Context manager for standardized job lifecycle management.
    
    This wrapper ensures consistent state transitions and guarantees that
    FAILED state is written if an exception escapes, while centralizing
    progress and Tool_Status logic.
    
    Args:
        job_id: Job identifier
        tool: Tool name (e.g., "hunter", "google_maps_discover")
        request_id: Request identifier
        phase: Current phase (e.g., "discover", "enrich")
        total: Total expected items for progress tracking
        status_writer: Optional ToolStatusWriter for live status updates
    
    Usage:
        with job_lifecycle(
            job_id=job_id,
            tool="google_maps_discover",
            request_id=rid,
            phase="discover",
            total=total_estimated,
            status_writer=status,
        ):
            # ... runner logic ...
            # Any unhandled exception will automatically mark job as FAILED
    """
    # Structured logging context
    log_context = {
        "job_id": job_id,
        "tool": tool,
        "request_id": request_id,
        "phase": phase,
    }
    
    logger.debug(
        "Starting job lifecycle: tool=%s, phase=%s (job_id=%s, request_id=%s)",
        tool,
        phase,
        job_id,
        request_id,
        extra=log_context
    )
    
    try:
        # Mark job as running
        mark_running(job_id)
        
        # Initial progress update
        update_progress(
            job_id,
            phase=phase,
            current=0,
            total=total,
            message=f"Starting {tool} {phase}.",
            metrics={"phase": phase, "tool": tool},
        )
        
        # Initial status write if writer provided
        if status_writer:
            status_writer.write(
                job_id=job_id,
                tool=tool,
                request_id=request_id,
                state="RUNNING",
                phase=phase,
                current=0,
                total=total or 0,
                message=f"Starting {tool} {phase}.",
                meta={"phase": phase, "tool": tool},
            )
        
        # Yield control to the runner logic
        yield
        
        # If we reach here, the job completed successfully
        logger.debug(
            "Job lifecycle completed successfully: tool=%s, phase=%s (job_id=%s, request_id=%s)",
            tool,
            phase,
            job_id,
            request_id,
            extra=log_context
        )
        
    except Exception as exc:
        # Ensure FAILED state is written for any unhandled exception
        error_msg = str(exc)
        log_context["error"] = error_msg
        
        logger.error(
            "Job lifecycle failed: tool=%s, phase=%s, error=%s (job_id=%s, request_id=%s)",
            tool,
            phase,
            error_msg,
            job_id,
            request_id,
            extra=log_context,
            exc_info=True
        )
        
        try:
            mark_failed(job_id, error=error_msg, message=f"{tool} {phase} failed.")
        except Exception as mark_exc:
            logger.error(
                "Failed to mark job as failed: %s (job_id=%s)",
                mark_exc,
                job_id,
                extra=log_context
            )
        
        try:
            if status_writer:
                status_writer.write(
                    job_id=job_id,
                    tool=tool,
                    request_id=request_id,
                    state="FAILED",
                    phase=phase,
                    current=0,
                    total=total or 0,
                    message=error_msg,
                    meta={"error": error_msg, "phase": phase, "tool": tool},
                    force=True,
                )
        except Exception as status_exc:
            logger.error(
                "Failed to write failed status: %s (job_id=%s)",
                status_exc,
                job_id,
                extra=log_context
            )
        
        # Re-raise the original exception
        raise


class JobProgressTracker:
    """
    Helper class for tracking and updating job progress within a lifecycle context.
    
    This provides a convenient way to update progress and status during job execution
    without repeating the same parameters.
    """
    
    def __init__(
        self,
        *,
        job_id: str,
        tool: str,
        request_id: str,
        phase: str,
        total: Optional[int] = None,
        status_writer: Optional[ToolStatusWriter] = None,
    ):
        self.job_id = job_id
        self.tool = tool
        self.request_id = request_id
        self.phase = phase
        self.total = total
        self.status_writer = status_writer
        
        # Structured logging context
        self.log_context = {
            "job_id": job_id,
            "tool": tool,
            "request_id": request_id,
            "phase": phase,
        }
    
    def update(
        self,
        *,
        current: Optional[int] = None,
        message: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None,
        force_status_write: bool = False,
    ) -> None:
        """
        Update job progress and optionally write status.
        
        Args:
            current: Current progress value
            message: Progress message
            metrics: Additional metrics to track
            force_status_write: Force writing to Tool_Status even if not needed
        """
        try:
            # Update progress in job store
            update_progress(
                self.job_id,
                phase=self.phase,
                current=current,
                total=self.total,
                message=message,
                metrics=metrics,
            )
            
            # Update status if writer provided
            if self.status_writer:
                self.status_writer.write(
                    job_id=self.job_id,
                    tool=self.tool,
                    request_id=self.request_id,
                    state="RUNNING",
                    phase=self.phase,
                    current=current,
                    total=self.total,
                    message=message,
                    meta=metrics,
                    force=force_status_write,
                )
                
        except Exception as exc:
            logger.warning(
                "Failed to update job progress: %s (job_id=%s)",
                exc,
                self.job_id,
                extra=self.log_context
            )
    
    def complete(
        self,
        *,
        message: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Mark the job as successfully completed.
        
        Args:
            message: Completion message
            metrics: Final metrics
        """
        try:
            completion_message = message or f"{self.tool} {self.phase} completed."
            
            mark_succeeded(
                self.job_id,
                message=completion_message,
                metrics=metrics,
            )
            
            if self.status_writer:
                self.status_writer.write(
                    job_id=self.job_id,
                    tool=self.tool,
                    request_id=self.request_id,
                    state="SUCCEEDED",
                    phase=self.phase,
                    current=self.total,
                    total=self.total,
                    message=completion_message,
                    meta=metrics,
                    force=True,
                )
                
            logger.debug(
                "Job completed successfully: %s (job_id=%s, request_id=%s)",
                completion_message,
                self.job_id,
                self.request_id,
                extra=self.log_context
            )
            
        except Exception as exc:
            logger.error(
                "Failed to mark job as completed: %s (job_id=%s)",
                exc,
                self.job_id,
                extra=self.log_context
            )