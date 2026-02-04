from __future__ import annotations

import math
from typing import List

from .models import EmailOccurrence


CATEGORY_SCORE = {
    "impressum": 1.0,
    "home": 1.0,
    "contact": 1.0,
    "legal": 0.5,
    "unknown": 0.5,
}

SOURCE_SCORE = {
    "both": 1.0,
    "llm": 0.5,
    "deterministic": 0.5,
}


def frequency_score(n: int) -> float:
    return min(1.0, math.log2(n + 1) / 3)


def compute_confidence(
    *,
    occurrences: List[EmailOccurrence],
    source_agreement: str,
) -> float:

    domain_score = max(o.domain_score for o in occurrences) / 2.0
    page_category = max(CATEGORY_SCORE.get(o.page_category, 0.5) for o in occurrences)
    source_score = SOURCE_SCORE[source_agreement]
    freq = frequency_score(len(occurrences))

    confidence = (
        0.4 * domain_score
        + 0.25 * page_category
        + 0.2 * source_score
        + 0.15 * freq
    )
    confidence = max(confidence, 0.1)

    return round(confidence, 3)
