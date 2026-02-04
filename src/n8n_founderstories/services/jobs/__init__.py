"""
Jobs service.

Classification:
- Role: universal background job tracking and progress reporting.
- Storage: file-based JSON under settings.data_dir/jobs.
- Used by: Hunter, Google Maps, web search, any long-running tools.
"""

from .lifecycle import JobProgressTracker, job_lifecycle
from .models import JobProgress, JobRecord, JobState
from .sheets_writer import JobsSheetWriter
from .store import (
    create_job,
    find_job_by_request_and_tool,
    load_job,
    load_latest_job,
    mark_failed,
    mark_running,
    mark_succeeded,
    update_progress,
)

__all__ = [
    # Lifecycle management
    "job_lifecycle",
    "JobProgressTracker",
    # Models
    "JobState",
    "JobProgress",
    "JobRecord",
    # Store operations
    "create_job",
    "load_job",
    "load_latest_job",
    "find_job_by_request_and_tool",
    "mark_running",
    "mark_succeeded",
    "mark_failed",
    "update_progress",
    # Sheets writer
    "JobsSheetWriter",
]
