from __future__ import annotations

from typing import List

from ..models import LLMExtraction
from .models import CombinedDescription


def normalize_descriptions(llm: LLMExtraction) -> List[CombinedDescription]:
    out: List[CombinedDescription] = []

    about = llm.about
    if not about:
        return out

    # SHORT
    if about.short_description and about.short_evidence:
        out.append(
            CombinedDescription(
                kind="short",
                text=str(about.short_description).strip(),
                source_url=str(about.short_evidence.url),
            )
        )

    # LONG
    if about.long_description and about.long_evidence:
        out.append(
            CombinedDescription(
                kind="long",
                text=str(about.long_description).strip(),
                source_url=str(about.long_evidence.url),
            )
        )

    return out
