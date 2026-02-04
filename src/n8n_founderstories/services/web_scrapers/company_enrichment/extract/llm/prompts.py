# src/n8n_founderstories/services/web_scrapers/company_enrichment/llm/prompts.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class PromptBundleCase1:
    contact_prompt_impressum: str
    short_about_prompt_homepage: str
    long_about_prompt_about: Optional[str] = None


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


def build_case1_long_about_prompt_about_page(*, markdown: str) -> str:
    """
    About page extraction:
    - company_name ONLY if explicitly present; else null
    - long_description (5-6 sentences)
    - NO emails, NO contacts
    """
    schema = (
        "{\n"
        '  "company_name": {"value": string|null, "evidence": {"quote": string}|null},\n'
        '  "long_description": {"value": string|null, "evidence": {"quote": string}|null}\n'
        "}\n"
    )
    task = (
        "Task: From the MARKDOWN, extract the COMPANY NAME (only if explicitly stated) "
        "and produce a LONG company description (5-6 sentences)."
    )
    extra = (
        "Rules for company_name:\n"
        "- Extract ONLY if explicitly present in the MARKDOWN.\n"
        "- If not present, set value to null and evidence to null.\n"
        "\n"
        "Constraints for long_description:\n"
        "- 5 to 6 sentences.\n"
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
        long_about_prompt_about=build_case1_long_about_prompt_about_page(markdown=about_markdown)
        if about_markdown
        else None,
    )
