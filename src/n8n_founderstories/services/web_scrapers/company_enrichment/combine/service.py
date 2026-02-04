from __future__ import annotations

from typing import List, Optional

from ..models import CrawlArtifacts, DeterministicExtraction, LLMExtraction
from .models import CombinedEmail, CombinedCompany
from .email_normaliser import normalize_email_occurrences
from .email_merger import group_by_email, source_agreement
from .email_scorer import compute_confidence
from .company_normaliser import normalize_company_occurrences
from .company_scorer import compute_company_confidence
from .description_normaliser import normalize_descriptions
from .people_normaliser import normalize_people
from .run_log import append_combine_result


def combine_enrichment(
    *,
    domain: str,
    crawl: CrawlArtifacts,
    deterministic: DeterministicExtraction,
    llm: LLMExtraction,
) -> tuple[List[CombinedEmail], Optional[CombinedCompany], list, list]:

    # =====================================================
    # EMAIL COMBINE
    # =====================================================
    occurrences = normalize_email_occurrences(
        domain=domain,
        crawl=crawl,
        deterministic=deterministic,
        llm=llm,
    )

    grouped = group_by_email(occurrences)

    email_results: List[CombinedEmail] = []

    for email, occs in grouped.items():
        agreement = source_agreement(occs)
        confidence = compute_confidence(
            occurrences=occs,
            source_agreement=agreement,
        )

        email_results.append(
            CombinedEmail(
                email=email,
                frequency=len(occs),
                source_agreement=agreement,
                confidence=confidence,
                sources=[{"source": o.source, "url": o.source_url} for o in occs],
            )
        )

    email_results.sort(key=lambda x: x.confidence, reverse=True)

    # =====================================================
    # COMPANY NAME COMBINE
    # =====================================================
    company_result: Optional[CombinedCompany] = None

    company_occs = normalize_company_occurrences(llm=llm)

    if company_occs:
        name = company_occs[0].name
        sources = [o.source_url for o in company_occs]

        confidence = compute_company_confidence(
            name=name,
            domain=domain,
            source_urls=sources,
        )

        company_result = CombinedCompany(
            name=name,
            frequency=len(company_occs),
            confidence=confidence,
            sources=sources,
        )

    descriptions = normalize_descriptions(llm)
    people = normalize_people(llm)

    # =====================================================
    # LOGGING
    # =====================================================
    det_lines = [
        f"{str(e.email).strip().lower()} | {str(e.source_url) if e.source_url else '-'}"
        for e in (deterministic.emails or [])
    ]

    llm_lines = [
        f"{str(e.email).strip().lower()} | {str(e.evidence.url) if getattr(e, 'evidence', None) else '-'}"
        for e in (llm.emails or [])
    ]

    append_combine_result(
        domain=domain,
        deterministic_emails=det_lines,
        llm_emails=llm_lines,
        combined=email_results,
        company=company_result,
        descriptions=descriptions,
        people=people,
    )


    return email_results, company_result, descriptions, people
