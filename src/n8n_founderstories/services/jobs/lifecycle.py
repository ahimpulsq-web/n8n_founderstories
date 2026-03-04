"""
Shared job lifecycle management wrapper.

Provides standardized job state transitions with optional Tool Status sheet updates.

Classification:
- Role: Infrastructure only
- No logging (services own their logs)
- No tool-specific logic
- No Google API logic (delegates to sheets exports)
- Best-effort writes (never crash a job)
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

from .status_writer import StatusWriterLike, safe_status_write
from .store import mark_failed, mark_running, mark_succeeded, update_progress


@contextmanager
def job_lifecycle(
    *,
    job_id: str,
    tool: str,
    request_id: str,
    phase: str,
    total: Optional[int] = None,
    status_writer: StatusWriterLike | None = None,
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
        status_writer: Optional callable(job_id, tool, request_id, state) for status updates
    
    Usage:
        with job_lifecycle(
            job_id=job_id,
            tool="google_maps",
            request_id=rid,
            phase="discover",
            total=total_estimated,
            status_writer=lambda jid, t, rid, s: write_single_job_status(
                sheet_id=sheet_id, job_id=jid, tool=t, request_id=rid, state=s
            ),
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
            current=1,
            total=total,
            message=f"Starting {tool} {phase}.",
            metrics={"phase": phase, "tool": tool},
        )
        
        # Do NOT write RUNNING state to Tool Status sheet
        # This avoids expensive full-export API calls on every job start
        # Tool Status is only updated at terminal states (SUCCEEDED/FAILED)
        # The job store already tracks RUNNING state for internal use
        
        # Yield control to the runner logic
        yield
        
    except Exception as exc:
        # Ensure FAILED state is written for any unhandled exception
        error_msg = str(exc)
        
        try:
            mark_failed(job_id, error=error_msg, message=f"{tool} {phase} failed.")
        except Exception:
            pass  # Best effort
        
        safe_status_write(
            status_writer,
            job_id=job_id,
            tool=tool,
            request_id=request_id,
            state="FAILED",
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
        status_writer: StatusWriterLike | None = None,
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
        Update job progress without writing to Tool Status sheet.
        
        Tool Status is only updated on state transitions (RUNNING, SUCCEEDED, FAILED)
        to avoid excessive API calls. Progress updates only write to the jobs store.
        
        Args:
            current: Current progress value
            message: Progress message
            metrics: Additional metrics to track
        """
        try:
            # Update progress in job store only
            # Do NOT write to Tool Status sheet here to avoid API quota hammering
            update_progress(
                self.job_id,
                phase=self.phase,
                current=current,
                total=self.total,
                message=message,
                metrics=metrics,
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
            
            safe_status_write(
                self.status_writer,
                job_id=self.job_id,
                tool=self.tool,
                request_id=self.request_id,
                state="SUCCEEDED",
            )
                
        except Exception:
            pass  # Best effort