"""
=============================================================================
MODULE: LLM Extraction Core - Main Extractor
=============================================================================

CLASSIFICATION: Core Business Logic Module
LAYER: Business Logic
DEPENDENCIES:
    - models (data structures)
    - prompts (prompt builders)
    - adapters (OpenRouter)
    - utils (sanitization, helpers)

PURPOSE:
    Orchestrates the complete LLM extraction process for company data from
    crawled web pages. Handles case-based routing, prompt construction,
    LLM API calls, and result normalization.

EXTRACTION CASES:
    Case 1, 2, 5.1, 5.2 (Impressum Path):
        - Uses Impressum page for contact data
        - Uses Homepage for company description
        - Optimal for German/Austrian companies
    
    Case 3, 5.3 (No Impressum):
        - Uses Homepage for description
        - Uses Contact/Privacy pages for contact data
        - Fallback for companies without Impressum

EXPORTS:
    - extract: Main extraction function

USAGE:
    from n8n_founderstories.services.enrichment.extract.llm.core import extract
    
    extraction = await extract(
        domain="example.com",
        crawl_meta={"contact_case": "1", ...},
        pages=[page1, page2, ...],
        router=router,
    )

NOTES:
    - All LLM responses are sanitized before validation
    - Evidence quotes are truncated to prevent validation errors
    - Email validation prevents invalid formats
    - Results are normalized into consistent LLMExtraction format
=============================================================================
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ....models import (
    PageArtifact,
    LLMExtraction,
    LLMEmail,
    LLMContact,
    LLMCompany,
    LLMAbout,
    Evidence,
)
from ..prompts import (
    build_case1_prompt_bundle,
    build_case3_contact_prompt_page,
)
from ..adapters import OpenRouterLLMRouter
from ..utils import (
    normalize_url,
    index_pages,
    pick_first_page,
    parse_json_strict,
    extract_assistant_text,
    sanitize_extraction,
)

logger = logging.getLogger(__name__)


# =============================================================================
# RESULT NORMALIZATION HELPERS
# =============================================================================

def _emails_from_section(section: Optional[Dict[str, Any]]) -> List[LLMEmail]:
    if not isinstance(section, dict):
        return []

    source_url = section.get("source_url")
    extracted = section.get("extracted") or {}
    out: List[LLMEmail] = []

    for e in extracted.get("emails") or []:
        if not isinstance(e, dict):
            continue
        email = (e.get("email") or "").strip()
        ev = e.get("evidence")
        
        # Validate email format before creating LLMEmail object
        # Skip invalid emails to prevent pydantic ValidationError
        if email and isinstance(ev, dict) and source_url:
            # Simple email validation: must have @ and at least one dot after @
            import re
            if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
                logger.debug(
                    "LLM_EXTRACT | INVALID_EMAIL | email=%s | source=%s | reason=invalid_format",
                    email,
                    source_url
                )
                continue
            
            out.append(
                LLMEmail(
                    email=email,
                    evidence=Evidence(
                        url=source_url,
                        quote=ev.get("quote", ""),
                    ),
                )
            )
    return out


def _contacts_from_section(section: Optional[Dict[str, Any]]) -> List[LLMContact]:
    if not isinstance(section, dict):
        return []

    source_url = section.get("source_url")
    extracted = section.get("extracted") or {}
    out: List[LLMContact] = []

    for c in extracted.get("contacts") or []:
        if not isinstance(c, dict):
            continue
        name = (c.get("name") or "").strip()
        ev = c.get("evidence")
        if name and isinstance(ev, dict) and source_url:
            out.append(
                LLMContact(
                    name=name,
                    role=c.get("role"),
                    evidence=Evidence(
                        url=source_url,
                        quote=ev.get("quote", ""),
                    ),
                )
            )
    return out


# -------------------------------------------------------------------
# Internal result (kept, but NOT returned)
# -------------------------------------------------------------------

@dataclass(frozen=True)
class ExtractResult:
    contact_case: str
    about_case: Optional[str]
    sections: Dict[str, Dict[str, Any]]


# -------------------------------------------------------------------
# Main extract
# -------------------------------------------------------------------

async def extract(
    *,
    domain: str,
    crawl_meta: Dict[str, Any],
    pages: List[PageArtifact],
    router: OpenRouterLLMRouter,
) -> LLMExtraction:

    contact_case = str(crawl_meta.get("contact_case") or "")
    about_case = crawl_meta.get("about_case")
    idx = index_pages(pages)

    sections: Dict[str, Dict[str, Any]] = {}

    # ============================================================
    # CASES 1,2,5.1,5.2 (Impressum path)
    # ============================================================
    if contact_case in {"1", "2", "5.1", "5.2"}:
        contact_links = crawl_meta.get("contact_selected_links") or []
        about_links = crawl_meta.get("about_selected_links") or []

        impressum_url = contact_links[0] if len(contact_links) >= 1 else None
        homepage_url = contact_links[1] if len(contact_links) >= 2 else None
        about_url = about_links[0] if about_links else None

        # Safely get markdown from pages, handling None cases
        impressum_page = pick_first_page(idx, impressum_url) if impressum_url else None
        homepage_page = pick_first_page(idx, homepage_url) if homepage_url else None
        about_page = pick_first_page(idx, about_url) if about_url else None
        
        bundle = build_case1_prompt_bundle(
            impressum_markdown=impressum_page.markdown if impressum_page else "",
            homepage_markdown=homepage_page.markdown if homepage_page else "",
            about_markdown=about_page.markdown if about_page else None,
        )

        r1 = await router.complete(prompt=bundle.contact_prompt_impressum)
        r2 = await router.complete(prompt=bundle.short_about_prompt_homepage)

        # Parse and sanitize LLM responses BEFORE creating Evidence objects
        impressum_data = parse_json_strict(extract_assistant_text(r1))
        homepage_data = parse_json_strict(extract_assistant_text(r2))
        
        sections["impressum"] = {
            "source_url": impressum_url,
            "extracted": sanitize_extraction(impressum_data, logger, domain),
        }
        sections["homepage"] = {
            "source_url": homepage_url,
            "extracted": sanitize_extraction(homepage_data, logger, domain),
        }

        # REMOVED: long_about_prompt_about processing - no longer used

    # ============================================================
    # CASES 3,5.3 (No impressum)
    # ============================================================
    elif contact_case in {"3", "5.3"}:
        contact_links = crawl_meta.get("contact_selected_links") or []
        homepage_url = contact_links[0] if contact_links else None

        homepage_page = pick_first_page(idx, homepage_url)
        bundle = build_case1_prompt_bundle(
            impressum_markdown="",
            homepage_markdown=homepage_page.markdown if homepage_page else "",
            about_markdown=None,
        )

        r_home = await router.complete(prompt=bundle.short_about_prompt_homepage)
        homepage_data = parse_json_strict(extract_assistant_text(r_home))
        sections["homepage"] = {
            "source_url": homepage_url,
            "extracted": sanitize_extraction(homepage_data, logger, domain),
        }

        for url in contact_links[1:]:
            page = pick_first_page(idx, url)
            if not page:
                continue
            r = await router.complete(
                prompt=build_case3_contact_prompt_page(markdown=page.markdown)
            )
            contact_data = parse_json_strict(extract_assistant_text(r))
            sections.setdefault("contact", {
                "source_url": url,
                "extracted": sanitize_extraction(contact_data, logger, domain),
            })

# ============================================================
    # FINAL NORMALIZATION (PUBLIC CONTRACT)
    # ============================================================

    emails: List[LLMEmail] = []
    contacts: List[LLMContact] = []

    for sec in sections.values():
        emails.extend(_emails_from_section(sec))
        contacts.extend(_contacts_from_section(sec))

    company = None
    if "impressum" in sections:
        cn = (sections["impressum"]["extracted"] or {}).get("company_name")
        if (
            isinstance(cn, dict)
            and cn.get("value")
            and isinstance(cn.get("evidence"), dict)
        ):
            company = LLMCompany(
                name=cn["value"],
                evidence=Evidence(
                    url=sections["impressum"]["source_url"],
                    quote=cn["evidence"].get("quote", ""),
                ),
            )


    about = None
    # -------------------------
    # SHORT DESCRIPTION (from homepage section)
    # -------------------------
    if "homepage" in sections:
        ex = sections["homepage"]["extracted"] or {}
        sd = ex.get("short_description")
        if isinstance(sd, dict) and sd.get("value") and isinstance(sd.get("evidence"), dict):
            about = about or LLMAbout()
            about.short_description = sd["value"]
            about.short_evidence = Evidence(
                url=sections["homepage"]["source_url"],
                quote=sd["evidence"].get("quote", ""),
            )
# REMOVED: LONG DESCRIPTION processing - no longer used in combine outputs


    return LLMExtraction(
        company=company,
        emails=emails,
        contacts=contacts,
        about=about,
    )
