from __future__ import annotations

from typing import List
from urllib.parse import urlparse

from ..models import LLMExtraction
from .models import CompanyOccurrence


def normalize_company_occurrences(
    *,
    llm: LLMExtraction,
) -> List[CompanyOccurrence]:
    out: List[CompanyOccurrence] = []

    if not llm.company:
        return out

    name = (llm.company.name or "").strip()
    if not name:
        return out

    url = str(llm.company.evidence.url)
    out.append(
        CompanyOccurrence(
            name=name,
            source_url=url,
        )
    )

    return out
