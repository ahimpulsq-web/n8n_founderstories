from __future__ import annotations
from collections import defaultdict
from typing import Dict, List

from ..models import LLMExtraction
from .models import CombinedPerson


def normalize_people(llm: LLMExtraction) -> List[CombinedPerson]:
    grouped: Dict[str, Dict] = defaultdict(lambda: {"role": None, "sources": set()})

    for c in llm.contacts or []:
        name = c.name.strip()
        if not name:
            continue

        if c.role:
            grouped[name]["role"] = c.role

        grouped[name]["sources"].add(str(c.evidence.url))

    out: List[CombinedPerson] = []
    for name, data in grouped.items():
        out.append(
            CombinedPerson(
                name=name,
                role=data["role"],
                sources=sorted(data["sources"]),
            )
        )

    return out
