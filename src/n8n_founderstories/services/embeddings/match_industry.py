from __future__ import annotations

import json
import math
from pathlib import Path

from n8n_founderstories.core.config import settings
from n8n_founderstories.services.openrouter.openrouter_client import get_client


INDEX_FILE = Path(__file__).parent / "industry_embeddings.json"


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
    client = get_client()

    query_vec = client.embed(
        model=settings.embedding_model,
        input=prompt_target,
    )

    index = json.loads(INDEX_FILE.read_text(encoding="utf-8"))

    scored = []
    for row in index:
        score = _cosine(query_vec, row["embedding"])
        scored.append((row["industry"], score))

    scored.sort(key=lambda x: x[1], reverse=True)
    return [name for name, _ in scored[:top_k]]
