"""
=============================================================================
PACKAGE: LLM Extraction Prompts
=============================================================================

CLASSIFICATION: Prompt Management Package
LAYER: Business Logic

PURPOSE:
    Provides prompt templates and builders for LLM extraction operations.

MODULES:
    - builder: Prompt construction functions

EXPORTS:
    - PromptBundleCase1: Data class for Case 1 prompts
    - build_case1_prompt_bundle: Build prompts for Case 1 (Impressum path)
    - build_case3_contact_prompt_page: Build prompts for Case 3 (No impressum)

USAGE:
    from n8n_founderstories.services.enrichment.extract.llm.prompts import (
        build_case1_prompt_bundle,
        build_case3_contact_prompt_page,
    )
    
    # Build prompts for Case 1 (Impressum path)
    bundle = build_case1_prompt_bundle(
        impressum_markdown=impressum_md,
        homepage_markdown=homepage_md,
        about_markdown=about_md,
    )
    
    # Build prompts for Case 3 (No impressum)
    prompt = build_case3_contact_prompt_page(markdown=contact_md)

NOTES:
    - Prompts are case-specific based on crawl strategy
    - All prompts enforce strict JSON output format
    - Evidence quotes must be verbatim from markdown
=============================================================================
"""
from .builder import (
    PromptBundleCase1,
    build_case1_prompt_bundle,
    build_case1_contact_prompt_impressum,
    build_case1_short_about_prompt_homepage,
    build_case3_contact_prompt_page,
)

__all__ = [
    "PromptBundleCase1",
    "build_case1_prompt_bundle",
    "build_case1_contact_prompt_impressum",
    "build_case1_short_about_prompt_homepage",
    "build_case3_contact_prompt_page",
]