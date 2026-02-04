from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import requests


# ─────────────────────────────────────────────────────────────
# CONFIG (no settings/env dependency)
# ─────────────────────────────────────────────────────────────
OPENROUTER_API_KEY = "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"  # <-- set this
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
EMBEDDING_MODEL = "openai/text-embedding-3-large"

TIMEOUT_S = 120
MAX_RETRIES = 5
BACKOFF_BASE_S = 0.8  # exponential backoff base


# ─────────────────────────────────────────────────────────────
# FILES (same folder)
# ─────────────────────────────────────────────────────────────
HERE = Path(__file__).parent
INDUSTRIES_FILE = HERE / "industries.json"
OUT_FILE = HERE / "industry_embeddings.json"


# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger("industry_index")


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }


def embed(text: str) -> list[float]:
    url = f"{OPENROUTER_BASE_URL.rstrip('/')}/embeddings"
    payload: dict[str, Any] = {"model": EMBEDDING_MODEL, "input": text}

    for attempt in range(MAX_RETRIES + 1):
        try:
            r = requests.post(url, headers=_headers(), json=payload, timeout=TIMEOUT_S)
            r.raise_for_status()
            data = r.json()
            return data["data"][0]["embedding"]
        except Exception as e:
            if attempt >= MAX_RETRIES:
                raise
            sleep_s = BACKOFF_BASE_S * (2**attempt)
            log.warning("Embed failed (attempt %d/%d): %s | retrying in %.1fs", attempt + 1, MAX_RETRIES + 1, e, sleep_s)
            time.sleep(sleep_s)

    raise RuntimeError("Unreachable")


def load_existing_index() -> dict[str, list[float]]:
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
    rows = [{"industry": k, "embedding": v} for k, v in index_map.items()]
    OUT_FILE.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    if OPENROUTER_API_KEY.strip() == "PASTE_YOUR_OPENROUTER_KEY_HERE":
        raise RuntimeError("Set OPENROUTER_API_KEY at top of industry_index.py")

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
        vec = embed(name)
        existing[name] = vec

        # save incrementally (so you never lose progress)
        if i % 10 == 0:
            write_index(existing)
            log.info("Checkpoint saved (%d embeddings)", len(existing))

    write_index(existing)
    log.info("DONE. Wrote %d embeddings to %s", len(existing), OUT_FILE)


if __name__ == "__main__":
    main()
