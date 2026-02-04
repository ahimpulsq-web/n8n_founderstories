# src/n8n_founderstories/services/mailer/base.py

from __future__ import annotations

import logging

from n8n_founderstories.core.utils.llm_selection import select_single_model
from n8n_founderstories.services.llm.operouter_client import OpenRouterLLMClient

logger = logging.getLogger(__name__)

MAIL_SUBJECT = "Invitation to share your founder story on AFS Akademie"

# Create the client once at import time using explicit construction
# Note: This uses "search_plan" module for now. If mailer needs its own tier,
# add a "mailer" case to llm_selection.py
api_key, model = select_single_model(module="search_plan")
llm = OpenRouterLLMClient(api_key=api_key, model_name=model)


def generate_mail_content(data: dict) -> tuple[str, str]:
    company = data.get("company_name")
    logger.info("CONTENT_WRITER | company=%s", company)

    prompt = f"""
You are writing a short, professional outreach email body.

Context:
We run AFS Akademie, an educational and business platform focused on entrepreneurship,
professional development, and founder knowledge. As part of this, we publish
"Gründerstories" — editorial founder interviews sharing real entrepreneurial journeys.

Examples:
https://www.afs-akademie.org/topic/gruenderstories/

Goal:
Invite the founder/company to be featured in a Gründerstories interview.

Company details:
- Company name: {data.get("company_name")}
- Domain: {data.get("domain")}
- Contact names: {data.get("contact_names")}
- Short description: {data.get("short_description")}
- Long description: {data.get("long_description")}

Rules:
- Do not use: “I hope this message finds you well”, because that has become a trademark of ChatGPT.
- If a field is missing, do not reference it
- Do not invent names, roles, or facts
- Use only provided data

Instructions:
- Professional, friendly, editorial tone
- No sales language
- Under 150 words
- Plain text only
- Do NOT include a subject
- Do NOT include signatures

Return only the email body text.
""".strip()

    # Preserve your previous behavior: just a user message, no system prompt.
    content = llm.generate_text(
        user_prompt=prompt,
        system_instructions=None,
    )

    return MAIL_SUBJECT, content.strip()
