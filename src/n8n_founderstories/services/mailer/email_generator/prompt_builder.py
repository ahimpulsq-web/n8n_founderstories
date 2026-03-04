"""
Email Prompt Builder Module.

This module provides pure functions for building LLM prompts for personalized
email content generation. The prompts are designed to produce professional,
B2B-appropriate email content that is personalized based on available data.

Key Features:
- Pure function design (no side effects)
- Personalization based on available contact data
- Professional B2B tone enforcement
- Clear length and format constraints
- No placeholders in output
"""

from __future__ import annotations


def build_email_prompt(
    contact_name: str | None,
    company: str | None,
    description: str | None,
    organisation: str,
    series_name: str,
) -> str:
    """
    Build a personalized email prompt for LLM content generation.
    
    This function creates a detailed prompt that instructs the LLM to generate
    a professional B2B email invitation. The prompt adapts based on available
    contact information to maximize personalization.
    
    Prompt Requirements:
    - Professional B2B tone
    - 150-220 words in length
    - Personalized greeting if contact_name is available
    - Company name mention when available
    - Reference 1-2 insights from description when available
    - Clear call-to-action at the end
    - No placeholders in output (e.g., no [Name], [Company])
    - No subject line generation (handled separately)
    - Only email body content
    
    Args:
        contact_name: Name of the contact person (optional)
            If None or empty, prompt will instruct generic greeting
        company: Company name (optional)
            If None or empty, prompt will adapt accordingly
        description: Company description or background info (optional)
            If None or empty, prompt will focus on general value proposition
        organisation: Name of the organization sending the invitation
            (e.g., "AFS Akademie")
        series_name: Name of the story series/campaign
            (e.g., "Gründerstories")
    
    Returns:
        Complete prompt string ready for LLM processing
        
    Example:
        >>> prompt = build_email_prompt(
        ...     contact_name="Max Müller",
        ...     company="TechStart GmbH",
        ...     description="Innovative AI startup in Munich",
        ...     organisation="AFS Akademie",
        ...     series_name="Gründerstories"
        ... )
        >>> # Returns detailed prompt for personalized email generation
    """
    # Build personalization context based on available data
    personalization_parts = []
    
    if contact_name and contact_name.strip():
        personalization_parts.append(
            f"- Address the recipient by name: {contact_name}"
        )
    else:
        personalization_parts.append(
            "- Use a professional generic greeting (e.g., 'Dear Founder' or 'Hello')"
        )
    
    if company and company.strip():
        personalization_parts.append(
            f"- Mention the company name: {company}"
        )
    else:
        personalization_parts.append(
            "- Focus on the recipient's role as a founder/entrepreneur"
        )
    
    if description and description.strip():
        personalization_parts.append(
            f"- Reference 1-2 specific insights from this company background: {description}"
        )
    else:
        personalization_parts.append(
            "- Focus on general value proposition for founders"
        )
    
    personalization_instructions = "\n".join(personalization_parts)
    
    # Build the complete prompt
    prompt = f"""You are writing a professional B2B email invitation for {organisation}'s {series_name} campaign.

TASK:
Write a personalized email body inviting a founder to share their entrepreneurial story.

PERSONALIZATION REQUIREMENTS:
{personalization_instructions}

CONTENT REQUIREMENTS:
- Length: 150-220 words
- Tone: Professional, warm, and respectful
- Structure:
  1. Personalized greeting
  2. Brief introduction of {series_name} and {organisation}
  3. Why their story matters (reference company/description if available)
  4. Clear call-to-action (invitation to participate)
  5. Professional closing

STRICT RULES:
- Return ONLY the email body content
- NO subject line
- NO placeholders like [Name], [Company], or [Insert X]
- NO explanations or meta-commentary
- Use actual names/companies provided or adapt naturally if not available
- Write in German language
- Be specific and genuine, not generic

OUTPUT FORMAT:
Return only the complete email body text, ready to send.
"""
    
    return prompt