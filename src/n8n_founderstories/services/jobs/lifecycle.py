"""
Shared job lifecycle management wrapper.

Provides standardized job state transitions and Tool_Status writes.

Classification:
- Role: Infrastructure only
- No logging (services own their logs)
- No tool-specific logic
- No Google API logic (delegates to sheets_writer)
- Best-effort writes (never crash a job)
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

from .sheets_writer import JobsSheetWriter
from .store import mark_failed, mark_running, mark_succeeded, update_progress


@contextmanager
def job_lifecycle(
    *,
    job_id: str,
    tool: str,
    request_id: str,
    phase: str,
    total: Optional[int] = None,
    status_writer: Optional[JobsSheetWriter] = None,
) -> Generator[None, None, None]:
    """
    Context manager for standardized job lifecycle management.
    
    This wrapper ensures consistent state transitions and guarantees that
    FAILED state is written if an exception escapes, while centralizing
    progress and Tool_Status logic.
    
    Args:
        job_id: Job identifier
        tool: Tool name (e.g., "hunter", "google_maps")
        request_id: Request identifier
        phase: Current phase (e.g., "discover", "enrich")
        total: Total expected items for progress tracking
        status_writer: Optional JobsSheetWriter for live status updates
    
    Usage:
        with job_lifecycle(
            job_id=job_id,
            tool="google_maps",
            request_id=rid,
            phase="discover",
            total=total_estimated,
            status_writer=writer,
        ):
            # ... runner logic ...
            # Any unhandled exception will automatically mark job as FAILED
    """
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
            )
        
        # Yield control to the runner logic
        yield
        
    except Exception as exc:
        # Ensure FAILED state is written for any unhandled exception
        error_msg = str(exc)
        
        try:
            mark_failed(job_id, error=error_msg, message=f"{tool} {phase} failed.")
        except Exception:
            pass  # Best effort
        
        try:
            if status_writer:
                status_writer.write(
                    job_id=job_id,
                    tool=tool,
                    request_id=request_id,
                    state="FAILED",
                )
        except Exception:
            pass  # Best effort
        
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
        status_writer: Optional[JobsSheetWriter] = None,
    ):
        self.job_id = job_id
        self.tool = tool
        self.request_id = request_id
        self.phase = phase
        self.total = total
        self.status_writer = status_writer
    
    def update(
        self,
        *,
        current: Optional[int] = None,
        message: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Update job progress and optionally write status.
        
        Args:
            current: Current progress value
            message: Progress message
            metrics: Additional metrics to track
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
                )
                
        except Exception:
            pass  # Best effort - don't fail job on progress update errors
    
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
                )
                
        except Exception:
            pass  # Best effort