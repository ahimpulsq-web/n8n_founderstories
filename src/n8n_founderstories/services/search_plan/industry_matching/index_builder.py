from __future__ import annotations

# ============================================================================
# index_builder.py
#
# Role:
# - OFFLINE/ADMIN script to build industry_embeddings.json from industries.json
# - Uses the SAME OpenRouterClient as runtime matcher.py (consistency)
# - Saves incrementally to avoid losing progress
# ============================================================================

import json
import logging
import time
from pathlib import Path

from n8n_founderstories.core.config import settings
from n8n_founderstories.services.openrouter import OpenRouterClient


# ─────────────────────────────────────────────────────────────
# FILES (same folder)
# ─────────────────────────────────────────────────────────────
HERE = Path(__file__).parent
INDUSTRIES_FILE = HERE / "industries.json"
OUT_FILE = HERE / "industry_embeddings.json"


# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
MAX_RETRIES = 5
BACKOFF_BASE_S = 0.8  # exponential backoff base


# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger("industry_index")


# ─────────────────────────────────────────────────────────────
# OpenRouter client (reused)
# ─────────────────────────────────────────────────────────────
_client = OpenRouterClient()


def embed_with_retries(text: str) -> list[float]:
    """
    Embed text using OpenRouterClient with exponential backoff retries.

    Classification:
    - Deterministic wrapper + retry policy
    - External dependency call (OpenRouter API)
    """
    for attempt in range(MAX_RETRIES + 1):
        try:
            return _client.embed(
                model=settings.embedding_model,
                input=text,
            )
        except Exception as e:
            if attempt >= MAX_RETRIES:
                raise
            sleep_s = BACKOFF_BASE_S * (2**attempt)
            log.warning(
                "Embed failed (attempt %d/%d): %s | retrying in %.1fs",
                attempt + 1,
                MAX_RETRIES + 1,
                e,
                sleep_s,
            )
            time.sleep(sleep_s)

    raise RuntimeError("Unreachable")


def load_existing_index() -> dict[str, list[float]]:
    """
    Load existing embeddings (if any) to avoid re-embedding.

    Classification:
    - Pure IO + parsing
    - No external network calls
    """
    if not OUT_FILE.exists():
        return {}

    try:
        raw = json.loads(OUT_FILE.read_text(encoding="utf-8"))
        # expected: [{"industry": "...", "embedding": [...]}, ...]
        out: dict[str, list[float]] = {}
        for row in raw:
            if isinstance(row, dict) and "industry" in row and "embedding" in row:
                out[str(row["industry"])] = row["embedding"]
        return out
    except Exception:
        return {}


def write_index(index_map: dict[str, list[float]]) -> None:
    """
    Write embeddings to disk.

    Classification:
    - Pure IO
    """
    rows = [{"industry": k, "embedding": v} for k, v in index_map.items()]
    OUT_FILE.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    """
    Build/update the industry embedding index.

    Classification:
    - Orchestration (offline script)
    """
    if not settings.openrouter_api_key:
        raise RuntimeError("OPENROUTER_API_KEY must be set in environment or .env file")

    industries = json.loads(INDUSTRIES_FILE.read_text(encoding="utf-8"))
    if not isinstance(industries, list) or not all(isinstance(x, str) for x in industries):
        raise ValueError("industries.json must be a JSON list of strings")

    existing = load_existing_index()
    log.info("Loaded %d existing embeddings from %s", len(existing), OUT_FILE)
    log.info("Total industries: %d", len(industries))
    log.info("Writing output to: %s", OUT_FILE)

    for i, name in enumerate(industries, 1):
        if name in existing:
            if i % 50 == 0:
                log.info("Progress %d/%d (skipping cached)", i, len(industries))
            continue

        log.info("Embedding %d/%d: %s", i, len(industries), name)
        vec = embed_with_retries(name)
        existing[name] = vec

        # save incrementally (so you never lose progress)
        if i % 10 == 0:
            write_index(existing)
            log.info("Checkpoint saved (%d embeddings)", len(existing))

    write_index(existing)
    log.info("DONE. Wrote %d embeddings to %s", len(existing), OUT_FILE)


if __name__ == "__main__":
    main()
