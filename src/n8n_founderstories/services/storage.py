# src/n8n_founderstories/services/storage.py

"""
Centralized persistence utilities (production-grade).

Design goals
- One universal artifact saver (avoid N functions for N tools).
- Consistent directory layout under a root-level /data folder.
- Consistent JSON serialization (Pydantic, datetime, URLs, nested models).
- Request-ID based filenames (<request_id>.json) for deterministic storage.
- latest.json is always a full copy of the most recently saved artifact per folder.
- runs/<request_id>/manifest.json tracks all artifacts produced for a request.

Directory layout (root-level, recommended):
data/
  runs/
    <request_id>/
      manifest.json

  search_plan/
    <provider>/
      <kind>/
        <request_id>.json
        latest.json

  web_search/
    <provider>/
      <kind>/
        <request_id>.json
        latest.json

  location/
    <provider>/
      <kind>/
        <request_id>.json
        latest.json

  enrichment/
    <provider>/
      <kind>/
        <request_id>.json
        latest.json
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from ..core.config import settings
from ..core.utils.text import slugify, norm

logger = logging.getLogger(__name__)

# =============================================================================
# Base paths
# =============================================================================

# storage.py path:
#   <root>/src/n8n_founderstories/services/storage.py
# parents[3] -> <root>
BASE_DATA_DIR = Path(settings.data_dir).expanduser().resolve()
RUNS_DIR = BASE_DATA_DIR / "runs"

# Canonical categories
CATEGORY_SEARCH_PLAN = "search_plan"
CATEGORY_WEB_SEARCH = "web_search"
CATEGORY_LOCATION = "location"
CATEGORY_ENRICHMENT = "enrichment"

# =============================================================================
# Small helpers
# =============================================================================

def _utc_now_iso() -> str:
    """UTC timestamp in ISO 8601, timezone-aware."""
    return datetime.now(timezone.utc).isoformat()


def _ensure_dir(path: Path) -> None:
    """
    Create directory (best-effort).

    Best-effort means: never raise an exception to the caller.
    Persistence must not crash the app due to filesystem edge cases.
    """
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        logger.warning("STORAGE_DIR_ERROR | path=%s | error=%s", path, exc)


def _normalize_provider(provider: str | None) -> str:
    """
    Normalize provider names into stable folder names.

    Examples:
      "groq/llama-3.1-8b-instant" -> "groq"
      "google_maps"              -> "google_maps"
      None                       -> "unknown"
    """
    raw = norm(provider)
    if not raw:
        return "unknown"
    # If caller passes a source-id style string, use the top-level provider segment.
    if "/" in raw:
        raw = raw.split("/", 1)[0]
    return slugify(raw) or "unknown"


def _normalize_kind(kind: str | None, default: str = "result") -> str:
    """Normalize artifact kind folder."""
    return slugify(kind or "") or default


def _relative_to_data(path: Path) -> str:
    """
    Prefer storing relative paths in manifests so data is portable across machines.

    If relative calculation fails, fall back to absolute.
    """
    try:
        return str(path.resolve().relative_to(BASE_DATA_DIR.resolve()))
    except Exception:
        return str(path)


def _atomic_write_json(path: Path, payload: Any) -> None:
    """
    Atomic-ish JSON write:
    - write to <file>.tmp
    - fsync best-effort
    - replace final file
    """
    _ensure_dir(path.parent)
    tmp_path = path.with_suffix(path.suffix + ".tmp")

    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
        try:
            os.fsync(f.fileno())
        except Exception:
            pass

    tmp_path.replace(path)


# =============================================================================
# Serialization (single source of truth)
# =============================================================================

def _to_serializable(data: Any) -> Any:
    """
    Convert common objects into JSON-serializable structures.

    Supported:
    - Pydantic models -> dict
    - Pydantic URL types -> str
    - datetime -> ISO string
    - dict/list -> recursive conversion
    - primitives -> unchanged
    - anything else -> str(data) fallback
    """
    if data is None:
        return None

    if isinstance(data, BaseModel):
        return _to_serializable(data.model_dump(mode="python"))

    # Pydantic URL types (compatible across v1/v2)
    try:
        from pydantic import AnyUrl  # type: ignore
        if isinstance(data, AnyUrl):
            return str(data)
    except Exception:
        pass

    if isinstance(data, datetime):
        if data.tzinfo is None:
            data = data.replace(tzinfo=timezone.utc)
        return data.isoformat()

    if isinstance(data, list):
        return [_to_serializable(x) for x in data]

    if isinstance(data, dict):
        return {str(k): _to_serializable(v) for k, v in data.items()}

    if isinstance(data, (str, int, float, bool)):
        return data

    return str(data)


# =============================================================================
# latest.json + manifest.json helpers
# =============================================================================

def _write_latest_copy(*, folder: Path, payload: dict) -> None:
    """
    Overwrite folder/latest.json with a full copy of the latest payload.

    Non-critical: failures must never break main persistence.
    """
    try:
        _atomic_write_json(folder / "latest.json", payload)
    except Exception as exc:
        logger.warning("LATEST_WRITE_ERROR | folder=%s | error=%s", folder, exc)


def _update_run_manifest(
    *,
    request_id: str,
    raw_prompt: str | None,
    artifact_key: str,
    artifact_path: Path,
    meta: dict[str, Any],
) -> None:
    """
    Update data/runs/<request_id>/manifest.json with the new artifact.

    Non-critical: failures must never break main persistence.
    """
    try:
        run_dir = RUNS_DIR / request_id
        _ensure_dir(run_dir)

        manifest_path = run_dir / "manifest.json"

        existing: dict[str, Any] = {}
        if manifest_path.exists():
            try:
                existing = json.loads(manifest_path.read_text(encoding="utf-8") or "{}")
            except Exception:
                existing = {}

        created_at = existing.get("created_at") or _utc_now_iso()

        artifacts = existing.get("artifacts")
        if not isinstance(artifacts, dict):
            artifacts = {}

        artifacts[str(artifact_key)] = {
            "path": _relative_to_data(artifact_path),
            **meta,
        }

        updated = {
            "request_id": request_id,
            "raw_prompt": raw_prompt or existing.get("raw_prompt"),
            "created_at": created_at,
            "updated_at": _utc_now_iso(),
            "artifacts": artifacts,
        }

        _atomic_write_json(manifest_path, updated)

    except Exception as exc:
        logger.warning("MANIFEST_UPDATE_ERROR | request_id=%s | error=%s", request_id, exc)


# =============================================================================
# Universal artifact saver (public API for persistence)
# =============================================================================

def save_artifact(
    *,
    category: str,
    provider: str,
    kind: str,
    request_id: str,
    payload: Any,
    raw_prompt: str | None = None,
    write_latest: bool = True,
    update_manifest: bool = True,
) -> Path | None:
    """
    Save an artifact with a consistent structure.

    Layout:
      data/<category>/<provider>/<kind>/<request_id>.json
      data/<category>/<provider>/<kind>/latest.json  (full copy of latest payload)

    Manifest:
      data/runs/<request_id>/manifest.json

    Notes:
    - request_id is the only filename (deterministic).
    - latest.json is a full payload copy (no pointer indirection).
    """
    request_id = str(request_id or "").strip()
    if not request_id:
        logger.warning("ARTIFACT_SKIP | reason=no_request_id | category=%s", category)
        return None

    category_slug = slugify(category or "artifact") or "artifact"
    provider_slug = _normalize_provider(provider)
    kind_slug = _normalize_kind(kind, default="result")

    target_dir = BASE_DATA_DIR / category_slug / provider_slug / kind_slug
    _ensure_dir(target_dir)

    file_path = target_dir / f"{request_id}.json"

    try:
        serializable = _to_serializable(payload)

        # Ensure dict payload so we can enrich consistently.
        if isinstance(serializable, dict):
            serializable.setdefault("request_id", request_id)
            if raw_prompt:
                serializable.setdefault("raw_prompt", raw_prompt)
            serializable.setdefault("saved_at", _utc_now_iso())
        else:
            serializable = {
                "request_id": request_id,
                "raw_prompt": raw_prompt,
                "saved_at": _utc_now_iso(),
                "payload": serializable,
            }

        # Primary artifact write (critical)
        _atomic_write_json(file_path, serializable)

        logger.info(
            "ARTIFACT_SAVED | category=%s | provider=%s | kind=%s | request_id=%s | path=%s",
            category_slug,
            provider_slug,
            kind_slug,
            request_id,
            file_path,
        )

        # latest.json (best-effort)
        if write_latest:
            _write_latest_copy(folder=target_dir, payload=serializable)

        # manifest.json (best-effort)
        if update_manifest:
            rp = None
            if isinstance(serializable, dict):
                rp = serializable.get("raw_prompt") or serializable.get("prompt") or raw_prompt

            artifact_key = f"{category_slug}.{provider_slug}.{kind_slug}"
            meta = {
                "category": category_slug,
                "provider": provider_slug,
                "kind": kind_slug,
                "saved_at": serializable.get("saved_at"),
            }
            _update_run_manifest(
                request_id=request_id,
                raw_prompt=str(rp) if rp else raw_prompt,
                artifact_key=artifact_key,
                artifact_path=file_path,
                meta=meta,
            )

        return file_path

    except Exception as exc:
        logger.warning(
            "ARTIFACT_SAVE_ERROR | category=%s | provider=%s | kind=%s | request_id=%s | path=%s | error=%s",
            category_slug,
            provider_slug,
            kind_slug,
            request_id,
            file_path,
            exc,
        )
        return None


# =============================================================================
# Convenience wrappers (thin, consistent defaults)
# =============================================================================

def save_search_plan_output(*, provider: str, request_id: str, payload: Any) -> Path | None:
    """
    Canonical SearchPlan persistence (single source of truth).

    data/search_plan/<provider>/plan/<request_id>.json
    data/search_plan/<provider>/plan/latest.json
    """
    raw_prompt = None
    try:
        raw_prompt = payload.get("raw_prompt") if isinstance(payload, dict) else getattr(payload, "raw_prompt", None)
    except Exception:
        raw_prompt = None

    return save_artifact(
        category=CATEGORY_SEARCH_PLAN,
        provider=provider,
        kind="plan",
        request_id=request_id,
        payload=payload,
        raw_prompt=raw_prompt,
        write_latest=True,
        update_manifest=True,
    )


def save_web_search_output(
    *,
    provider: str,
    kind: str,
    request_id: str,
    payload: Any,
    raw_prompt: str | None = None,
) -> Path | None:
    """data/web_search/<provider>/<kind>/<request_id>.json"""
    return save_artifact(
        category=CATEGORY_WEB_SEARCH,
        provider=provider,
        kind=kind,
        request_id=request_id,
        payload=payload,
        raw_prompt=raw_prompt,
        write_latest=True,
        update_manifest=True,
    )


def save_location_output(
    *,
    provider: str,
    kind: str,
    request_id: str,
    payload: Any,
    raw_prompt: str | None = None,
) -> Path | None:
    """data/location/<provider>/<kind>/<request_id>.json"""
    return save_artifact(
        category=CATEGORY_LOCATION,
        provider=provider,
        kind=kind,
        request_id=request_id,
        payload=payload,
        raw_prompt=raw_prompt,
        write_latest=True,
        update_manifest=True,
    )


def save_enrichment_output(
    *,
    provider: str,
    kind: str,
    request_id: str,
    payload: Any,
    raw_prompt: str | None = None,
) -> Path | None:
    """data/enrichment/<provider>/<kind>/<request_id>.json"""
    return save_artifact(
        category=CATEGORY_ENRICHMENT,
        provider=provider,
        kind=kind,
        request_id=request_id,
        payload=payload,
        raw_prompt=raw_prompt,
        write_latest=True,
        update_manifest=True,
    )


# =============================================================================
# Backward-compatible names (keep your current code working)
# =============================================================================

def save_google_maps_output(
    *,
    request_id: str,
    prompt: str,
    payload: Any,
    provider: str = "google_places",
    kind: str = "text_search",
) -> None:
    """
    Backward-compatible wrapper for existing google_maps saving calls.

    Stored as:
      data/location/<provider>/<kind>/<request_id>.json
    """
    save_location_output(
        provider=provider,
        kind=kind,
        request_id=request_id,
        payload=payload,
        raw_prompt=prompt,
    )


def save_google_search_output(
    *,
    request_id: str,
    prompt: str,
    payload: Any,
    provider: str = "serpapi",
    kind: str = "google",
) -> None:
    """
    Backward-compatible wrapper for google search saving calls.

    Stored as:
      data/web_search/<provider>/<kind>/<request_id>.json
    """
    save_web_search_output(
        provider=provider,
        kind=kind,
        request_id=request_id,
        payload=payload,
        raw_prompt=prompt,
    )

