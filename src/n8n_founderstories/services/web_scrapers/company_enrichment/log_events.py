from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("n8n_founderstories.web_scrapers.events")

# ----------------------------
# Disk logging state
# ----------------------------

@dataclass(frozen=True)
class RunLogPaths:
    run_dir: Path
    events_jsonl: Path

_run_paths: Optional[RunLogPaths] = None


def _utc_iso() -> str:
    # compact and stable ISO-ish timestamp
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _write_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def init_run_logging(
    *,
    base_dir: str | Path = "logs/deterministic",
    run_id: Optional[str] = None,
) -> Path:
    """
    Initialize a per-run log folder and enable disk sinks:
      - <run_dir>/events.jsonl   (all log_event output)
      - additional artifacts via log_artifact_jsonl()

    Returns run_dir so callers can store meta files, etc.
    """
    global _run_paths

    rid = run_id or time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
    run_dir = Path(base_dir) / rid
    run_dir.mkdir(parents=True, exist_ok=True)

    _run_paths = RunLogPaths(
        run_dir=run_dir,
        events_jsonl=run_dir / "events.jsonl",
    )

    # Optional: expose for other parts of the system
    os.environ["N8N_WS_RUN_LOG_DIR"] = str(run_dir)

    return run_dir


def get_run_log_dir() -> Optional[Path]:
    return _run_paths.run_dir if _run_paths else None


def log_artifact_jsonl(relative_path: str, **fields: Any) -> None:
    """
    Write a JSONL line into <run_dir>/<relative_path>.
    Example:
      log_artifact_jsonl("discovery/selected.jsonl", domain=..., url=..., score=...)
    """
    if not _run_paths:
        return
    p = _run_paths.run_dir / relative_path
    payload = {"ts": _utc_iso(), **fields}
    _write_jsonl(p, payload)


def log_event(event: str, **fields: Any) -> None:
    """
    Existing event logger + disk sink.
    Writes:
      - python logger output (as before)
      - <run_dir>/events.jsonl if init_run_logging() was called
    """
    payload = {"ts": _utc_iso(), "event": event, **fields}

    # Keep your existing behavior (structured log to stdout)
    logger.info(json.dumps({"event": event, **fields}, ensure_ascii=False))

    # Disk sink
    if _run_paths:
        _write_jsonl(_run_paths.events_jsonl, payload)
