from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

from .models import EmailOccurrence


def group_by_email(
    occurrences: List[EmailOccurrence],
) -> Dict[str, List[EmailOccurrence]]:
    grouped: Dict[str, List[EmailOccurrence]] = defaultdict(list)
    for o in occurrences:
        grouped[str(o.email)].append(o)
    return grouped


def source_agreement(occurrences: List[EmailOccurrence]) -> str:
    sources = {o.source for o in occurrences}
    if len(sources) == 2:
        return "both"
    return sources.pop()
