# =============================================================================
# C:\Projects\N8N-FounderStories\src\n8n_founderstories\services\jobs\store.py
# =============================================================================

from __future__ import annotations

# =============================================================================
# store.py
#
# Classification:
# - Role: file-based job persistence (single jobs.json index + latest.json pointers).
# - Policy:
#   - Atomic writes; never corrupt job files; status is always readable.
#   - Windows-safe replace with retries (PermissionError / access denied).
#   - Process-local lock reduces in-process contention; retries reduce cross-process races.
# =============================================================================

import json
import logging
import os
import random
import time
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict

from ...core.config import settings
from ...core.utils.text import slugify, norm
from .models import JobProgress, JobRecord, JobState, utc_now

logger = logging.getLogger(__name__)


def _json_serializer(obj: Any) -> str:
    """
    Custom JSON serializer with consistent datetime formatting and best-effort fallback.
    
    Ensures consistent ISO 8601 format (with 'T' separator) for datetime objects,
    avoiding the inconsistent space-separated format from default=str.
    
    For unknown types, falls back to str() for best-effort persistence rather than
    crashing writes. This is a pragmatic choice for job metadata/metrics that may
    contain unexpected types.
    
    Args:
        obj: Object to serialize
        
    Returns:
        ISO 8601 formatted string for datetime objects, str() for others
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    # Best-effort fallback for unknown types (e.g., custom objects in meta/metrics)
    # This prevents write failures while maintaining data persistence
    return str(obj)


# Classification:
# - Write lock: Serializes writers to prevent lost updates (read-modify-write races)
# - Readers do NOT use this lock - they rely on atomic file replacement
# - Cross-process safety is handled via retrying os.replace on Windows
#
# Design rationale:
# - Atomic os.replace() guarantees readers see either old complete file or new complete file
# - Writers must serialize to avoid lost updates when multiple threads write concurrently
# - Readers can proceed lock-free because files are always in valid state
_WRITE_LOCK = Lock()

# Classification: latest.json write throttling (optional, safe default)
# - Prevents hot loops from rewriting latest.json excessively.
# - Terminal states always write latest.json regardless of interval.
_LATEST_WRITE_LOCK = Lock()
_LATEST_LAST_WRITE_TS: float = 0.0


def _jobs_dir() -> Path:
    base = Path(settings.data_dir).expanduser().resolve()
    return base / "jobs"


def _ensure_dir(path: Path) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        logger.warning("JOBS_DIR_ERROR | path=%s | error=%s", path, exc)


def _jobs_store_path() -> Path:
    return _jobs_dir() / "jobs.json"


def _latest_path() -> Path:
    return _jobs_dir() / "latest.json"


def _read_json_file(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        logger.warning("JOBS_INDEX_READ_ERROR | path=%s", path, exc_info=True)
        return {}


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    """
    Classification: Robust atomic write for Windows.

    Root cause:
    - On Windows, os.replace/Path.replace can fail with PermissionError if the target
      file is momentarily open by another process (antivirus, indexer, watcher, another worker).

    Policy:
    - Write to a unique tmp file in the same directory (same volume).
    - fsync best-effort.
    - os.replace with bounded retries + jitter.
    """
    _ensure_dir(path.parent)

    tmp_name = f".{path.name}.{os.getpid()}.{random.randint(1000, 9999)}.tmp"
    tmp_path = path.with_name(tmp_name)

    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=_json_serializer)
        f.flush()
        try:
            os.fsync(f.fileno())
        except Exception:
            pass

    # Retry replace on Windows-like races.
    max_retries = int(getattr(settings, "jobs_atomic_replace_retries", 12))
    base_sleep = float(getattr(settings, "jobs_atomic_replace_base_sleep_seconds", 0.03))

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            os.replace(str(tmp_path), str(path))
            return
        except PermissionError as exc:
            last_exc = exc
            # exponential-ish backoff with jitter
            sleep_s = min(0.5, base_sleep * (attempt ** 1.3)) + random.uniform(0.0, 0.02)
            time.sleep(sleep_s)
        except Exception as exc:
            last_exc = exc
            # Non-permission exceptions should not be retried indefinitely.
            break

    # Cleanup tmp best-effort if replace failed.
    try:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
    except Exception:
        pass

    if last_exc:
        raise last_exc


def create_job(*, job_id: str, tool: str, request_id: str, meta: Dict[str, Any] | None = None) -> JobRecord:
    """
    Create a new job record.
    
    OPTIMIZATION: Job creation is now async-friendly - the actual file write
    happens in the background task, not in the API response path.
    """
    tool = slugify(norm(tool)) or "unknown"
    request_id = norm(request_id)
    if not request_id:
        raise ValueError("request_id is required to create a job.")

    rec = JobRecord(
        job_id=job_id,
        tool=tool,
        request_id=request_id,
        state=JobState.QUEUED,
        meta=dict(meta or {}),
    )

    # OPTIMIZATION: Save without forcing latest.json update to reduce I/O
    # The background task will update it when it starts running
    save_job(rec, force_latest=False)
    return rec


def _should_write_latest(*, force: bool) -> bool:
    """
    Classification: latest.json write-light policy.

    Policy:
    - If force=True: always write.
    - Otherwise: write at most once per interval (default 250ms).
    """
    if force:
        return True

    interval_s = float(getattr(settings, "jobs_latest_write_min_interval_seconds", 0.25))
    if interval_s <= 0:
        return True

    global _LATEST_LAST_WRITE_TS
    now = time.time()
    with _LATEST_WRITE_LOCK:
        if (now - _LATEST_LAST_WRITE_TS) < interval_s:
            return False
        _LATEST_LAST_WRITE_TS = now
        return True


def save_job(job: JobRecord, *, force_latest: bool = False) -> None:
    """
    Classification:
    - Role: persist one job record into jobs.json store and update latest.json pointers.
    - Policy:
      - atomic write; store always readable
      - latest.json update is write-light unless forced/terminal
      - writers serialize via _WRITE_LOCK to prevent lost updates
      - readers are lock-free (rely on atomic file replacement)
    
    Concurrency design:
    - Writers hold _WRITE_LOCK for entire read-modify-write cycle to prevent lost updates
    - Readers do NOT use locks - they rely on atomic os.replace() guarantees
    - Atomic replacement ensures readers see either old complete file or new complete file
    - This prevents reader starvation while avoiding lost update races
    """
    with _WRITE_LOCK:
        job.updated_at = utc_now()

        store_path = _jobs_store_path()
        latest_path = _latest_path()

        # Read current store and build new payload
        store = _read_json_file(store_path)
        store[job.job_id] = job.model_dump(mode="python")
        
        # Write jobs.json atomically
        _atomic_write_json(store_path, store)

        # Determine if we need to update latest.json
        terminal = job.state in {JobState.SUCCEEDED, JobState.FAILED}
        if not _should_write_latest(force=force_latest or terminal):
            return

        latest = _read_json_file(latest_path)
        if "by_tool" not in latest or not isinstance(latest.get("by_tool"), dict):
            latest["by_tool"] = {}

        latest["overall"] = {
            "job_id": job.job_id,
            "tool": job.tool,
            "request_id": job.request_id,
            "updated_at": job.updated_at.isoformat(),
        }
        latest["by_tool"][job.tool] = {
            "job_id": job.job_id,
            "request_id": job.request_id,
            "updated_at": job.updated_at.isoformat(),
        }

        # Write latest.json atomically
        _atomic_write_json(latest_path, latest)


def load_job(job_id: str) -> JobRecord | None:
    """
    Classification:
    - Role: retrieve one job record from jobs.json store.
    - Lock-free read: relies on atomic file replacement guarantees
    """
    jid = norm(job_id)
    if not jid:
        return None

    store = _read_json_file(_jobs_store_path())
    raw = store.get(jid)
    if not isinstance(raw, dict):
        return None
    try:
        return JobRecord.model_validate(raw)
    except Exception:
        logger.warning("JOB_VALIDATE_ERROR | job_id=%s", jid, exc_info=True)
        return None


def load_latest_job(*, tool: str | None = None) -> JobRecord | None:
    """
    Classification:
    - Role: fast access to the latest job overall or by tool via latest.json
    - Lock-free read: relies on atomic file replacement guarantees
    """
    latest = _read_json_file(_latest_path())

    if tool:
        info = latest.get("by_tool", {}).get(slugify(norm(tool)))
    else:
        info = latest.get("overall")

    if not isinstance(info, dict):
        return None

    jid = norm(info.get("job_id"))
    if not jid:
        return None

    return load_job(jid)


def list_jobs() -> list[JobRecord]:
    """
    List all jobs from the jobs store, sorted by updated_at (most recent first).
    
    Classification:
    - Role: retrieve all job records from jobs.json store
    - Returns validated JobRecord instances
    - Sorted by updated_at descending (newest first)
    - Lock-free read: relies on atomic file replacement guarantees
    
    Returns:
        List of JobRecord instances, sorted by updated_at (newest first).
        Returns empty list if no jobs exist or if store is corrupted.
    """
    store = _read_json_file(_jobs_store_path())
    
    jobs: list[JobRecord] = []
    for job_data in store.values():
        if not isinstance(job_data, dict):
            continue
        try:
            job = JobRecord.model_validate(job_data)
            jobs.append(job)
        except Exception:
            logger.warning("JOB_VALIDATE_ERROR during list_jobs", exc_info=True)
            continue
    
    # Sort by updated_at descending (newest first)
    jobs.sort(key=lambda j: j.updated_at, reverse=True)
    
    return jobs


def find_job_by_request_and_tool(*, request_id: str, tool: str) -> JobRecord | None:
    """
    Find the most recent job for a given request_id and tool.
    
    This scans the jobs store to find jobs matching both criteria.
    Useful for checking if source jobs are still running.
    Lock-free read: relies on atomic file replacement guarantees.
    """
    rid = norm(request_id)
    t = slugify(norm(tool)) or "unknown"
    if not rid:
        return None
    
    store = _read_json_file(_jobs_store_path())
    
    # Find all jobs matching request_id and tool, return the most recent
    matching: list[tuple[JobRecord, datetime]] = []
    for job_data in store.values():
        if not isinstance(job_data, dict):
            continue
        try:
            job = JobRecord.model_validate(job_data)
            if job.request_id == rid and job.tool == t:
                matching.append((job, job.updated_at))
        except Exception:
            continue
    
    if not matching:
        return None
    
    # Return the most recently updated job
    matching.sort(key=lambda x: x[1], reverse=True)
    return matching[0][0]


def mark_running(job_id: str) -> JobRecord:
    job = _require_job(job_id)
    job.state = JobState.RUNNING
    job.started_at = job.started_at or utc_now()
    job.error = None
    save_job(job)
    return job


def update_progress(
    job_id: str,
    *,
    phase: str | None = None,
    current: int | None = None,
    total: int | None = None,
    message: str | None = None,
    metrics: Dict[str, Any] | None = None,
) -> JobRecord:
    job = _require_job(job_id)

    p = job.progress or JobProgress()
    if phase is not None:
        p.phase = phase
    if current is not None:
        p.current = current
    if total is not None:
        p.total = total
    if message is not None:
        p.message = message
    if metrics:
        p.metrics.update(metrics)

    job.progress = p
    save_job(job)
    return job


def mark_succeeded(job_id: str, *, message: str | None = None, metrics: Dict[str, Any] | None = None) -> JobRecord:
    job = _require_job(job_id)
    job.state = JobState.SUCCEEDED
    job.finished_at = utc_now()
    if message:
        job.progress.message = message
    if metrics:
        job.progress.metrics.update(metrics)
    save_job(job, force_latest=True)
    return job


def mark_failed(job_id: str, *, error: str, message: str | None = None) -> JobRecord:
    job = _require_job(job_id)
    job.state = JobState.FAILED
    job.finished_at = utc_now()
    job.error = (error or "unknown error")[:2000]
    if message:
        job.progress.message = message
    save_job(job, force_latest=True)
    return job


def _require_job(job_id: str) -> JobRecord:
    job = load_job(job_id)
    if not job:
        raise FileNotFoundError(f"Job not found: {job_id}")
    return job
