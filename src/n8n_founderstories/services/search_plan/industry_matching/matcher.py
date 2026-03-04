from __future__ import annotations

import json
import math
from pathlib import Path

from n8n_founderstories.core.config import settings
from n8n_founderstories.services.openrouter import OpenRouterClient


INDEX_FILE = Path(__file__).parent / "industry_embeddings.json"

# Classification: Module-level singleton - Performance optimization
# Reuse OpenRouter client across calls to avoid repeated initialization
_client = OpenRouterClient()

# Classification: Module-level cache - Performance optimization
# Lazy-loaded index cache (list of dicts with keys: "industry", "embedding")
_INDEX_CACHE: list[dict] | None = None


def _load_index() -> list[dict]:
    """
    Load industry embedding index from disk once and cache it.

    Classification:
    - Pure IO + parsing
    - Deterministic (no network calls)
    
    Returns:
        List of dicts, each containing "industry" (str) and "embedding" (list[float])
    
    Raises:
        ValueError: If file format is invalid or contains no valid rows
    """
    global _INDEX_CACHE
    if _INDEX_CACHE is not None:
        return _INDEX_CACHE

    raw = json.loads(INDEX_FILE.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("industry_embeddings.json must be a JSON list")

    # Classification: Strict data validation - ensure correct structure
    cleaned: list[dict] = []

    for row in raw:
        if not isinstance(row, dict):
            continue

        industry = row.get("industry")
        embedding = row.get("embedding")

        # Validate industry
        if not isinstance(industry, str) or not industry.strip():
            continue

        # Validate embedding structure
        if not isinstance(embedding, list) or not embedding:
            continue

        # Validate embedding numeric content
        if not all(isinstance(x, (int, float)) for x in embedding):
            continue

        cleaned.append({
            "industry": industry,
            "embedding": embedding,
        })

    if not cleaned:
        raise ValueError(
            "industry_embeddings.json contains no valid rows "
            "(expected: {'industry': str, 'embedding': list[float]})."
        )

    _INDEX_CACHE = cleaned
    return _INDEX_CACHE


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


def match_industries(
    *,
    prompt_target: str,
    top_k: int = 8,
) -> list[str]:
    # Classification: Use module-level client singleton for performance
    query_vec = _client.embed(
        model=settings.embedding_model,
        input=prompt_target,
    )

    # Classification: Use cached index to avoid repeated file I/O
    index = _load_index()

    # Classification: Dimension validation - Prevent silent failures from model changes
    query_dim = len(query_vec)

    # Check first row dimension (guaranteed valid by _load_index)
    first_dim = len(index[0]["embedding"])

    if first_dim != query_dim:
        raise ValueError(
            f"Embedding dimension mismatch: query_dim={query_dim} "
            f"index_dim={first_dim}. Rebuild industry_embeddings.json using "
            f"index_builder.py with embedding_model='{settings.embedding_model}'."
        )

    # Classification: Data consistency validation - ensure ALL rows match same dim
    for row in index:
        emb = row["embedding"]
        if len(emb) != query_dim:
            raise ValueError(
                "industry_embeddings.json is inconsistent: not all embeddings "
                f"match query_dim={query_dim}. Rebuild the index."
            )

    # Classification: Similarity scoring
    scored: list[tuple[str, float]] = []
    for row in index:
        row_embedding = row["embedding"]
        score = _cosine(query_vec, row_embedding)
        scored.append((row["industry"], score))

    scored.sort(key=lambda x: x[1], reverse=True)
    return [name for name, _ in scored[:top_k]]
