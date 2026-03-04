"""
=============================================================================
MODULE: LLM Extraction Prompts - Prompt Builder
=============================================================================

CLASSIFICATION: Prompt Management Module
LAYER: Business Logic
DEPENDENCIES: None (pure functions)

PURPOSE:
    Provides functions to construct LLM prompts for company data extraction.
    Handles case-specific prompt generation with proper schema definitions
    and system rules.

PROMPT STRUCTURE:
    Each prompt consists of:
    1. System Rules: Output format, evidence requirements, validation rules
    2. Task Description: What to extract and from where
    3. JSON Schema: Exact structure of expected output
    4. Extra Constraints: Additional requirements (optional)
    5. Markdown Content: The actual content to extract from

EXTRACTION CASES:
    Case 1 (Impressum Path):
        - contact_prompt_impressum: Extract from Impressum page
        - short_about_prompt_homepage: Extract from Homepage
    
    Case 3 (No Impressum):
        - contact_prompt_page: Extract from Contact/Privacy pages

EXPORTS:
    - PromptBundleCase1: Data class for Case 1 prompts
    - build_case1_prompt_bundle: Build prompts for Case 1
    - build_case3_contact_prompt_page: Build prompts for Case 3

USAGE:
    from n8n_founderstories.services.enrichment.extract.llm.prompts import (
        build_case1_prompt_bundle,
        build_case3_contact_prompt_page,
    )
    
    # Case 1: Impressum path
    bundle = build_case1_prompt_bundle(
        impressum_markdown=impressum_md,
        homepage_markdown=homepage_md,
        about_markdown=about_md,
    )
    
    # Case 3: No impressum
    prompt = build_case3_contact_prompt_page(markdown=contact_md)

SYSTEM RULES:
    - Output MUST be valid JSON only (no markdown, no commentary)
    - Do NOT guess (use null or empty list if unknown)
    - Evidence quotes must be verbatim from markdown
    - Emails must be real emails found in markdown
    - Names/roles must be explicitly present in markdown
    - Keep quotes short (<= 250 chars)

NOTES:
    - All functions are pure (no side effects)
    - Prompts enforce strict JSON output format
    - Evidence tracking is mandatory for all extractions
    - Temperature should be set to 0.0 for deterministic outputs
=============================================================================
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class PromptBundleCase1:
    contact_prompt_impressum: str
    short_about_prompt_homepage: str


def _system_rules() -> str:
    return (
        "You are an information extraction system.\n"
        "Rules:\n"
        "- Output MUST be valid JSON only. No markdown. No commentary.\n"
        "- Do NOT guess. If unknown, use null or empty list.\n"
        "- Evidence quotes must be copied verbatim from the provided MARKDOWN.\n"
        "- Emails must be real emails found in the MARKDOWN.\n"
        "- Names/roles must be explicitly present in the MARKDOWN.\n"
        "- Keep quotes short (<= 250 chars).\n"
    )


def _wrap(task: str, schema: str, markdown: str, extra: str = "") -> str:
    extra = (extra or "").strip()
    extra_block = f"\n{extra}\n" if extra else "\n"
    return (
        f"{_system_rules()}\n"
        f"{task}\n"
        "Return JSON with this exact schema:\n"
        f"{schema}\n"
        f"{extra_block}"
        "MARKDOWN:\n"
        "-----\n"
        f"{markdown}\n"
        "-----\n"
    )


def build_case1_contact_prompt_impressum(*, markdown: str) -> str:
    schema = (
        "{\n"
        '  "company_name": {"value": string|null, "evidence": {"quote": string}|null},\n'
        '  "emails": [ {"email": string, "evidence": {"quote": string}} ],\n'
        '  "contacts": [ {"name": string, "role": string|null, "evidence": {"quote": string}} ]\n'
        "}\n"
    )
    task = "Task: Extract CONTACT DATA from the MARKDOWN."
    return _wrap(task, schema, markdown)


def build_case1_short_about_prompt_homepage(*, markdown: str) -> str:
    schema = (
        "{\n"
        '  "short_description": {"value": string|null, "evidence": {"quote": string}|null},\n'
        '  "company_name": {"value": string|null, "evidence": {"quote": string}|null},\n'
        '  "emails": [ {"email": string, "evidence": {"quote": string}} ],\n'
        '  "contacts": [ {"name": string, "role": string|null, "evidence": {"quote": string}} ]\n'
        "}\n"
    )
    task = (
        "Task: From the MARKDOWN, produce a SHORT company description (2-3 sentences) "
        "and ALSO extract any contact signals present (company name, emails, people+roles)."
    )
    extra = (
        "Constraints for short_description:\n"
        "- 2 to 3 sentences.\n"
        "- Based only on text present in the MARKDOWN.\n"
    )
    return _wrap(task, schema, markdown, extra=extra)



def build_case3_contact_prompt_page(*, markdown: str) -> str:
    schema = (
        "{\n"
        '  "emails": [ {"email": string, "evidence": {"quote": string}} ],\n'
        '  "contacts": [ {"name": string, "role": string|null, "evidence": {"quote": string}} ]\n'
        "}\n"
    )
    task = "Task: Extract EMAILS and CONTACT PERSONS from the MARKDOWN."
    return _wrap(task, schema, markdown)


def build_case1_prompt_bundle(
    *,
    impressum_markdown: str,
    homepage_markdown: str,
    about_markdown: Optional[str],
) -> PromptBundleCase1:
    return PromptBundleCase1(
        contact_prompt_impressum=build_case1_contact_prompt_impressum(markdown=impressum_markdown),
        short_about_prompt_homepage=build_case1_short_about_prompt_homepage(markdown=homepage_markdown),
    )
