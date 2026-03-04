"""
LLM Client Module for Email Content Generation.

This module provides a clean interface for generating email content using
the OpenRouter LLM service. It handles all LLM-specific logic including
model selection, temperature settings, and output post-processing.

Key Features:
- Uses existing OpenRouter client with fallback strategy
- Deterministic temperature for consistent output
- Automatic markdown stripping
- Whitespace normalization
- No database logic (pure LLM interaction)
"""

from __future__ import annotations

import logging
import re

from ...openrouter import OpenRouterClient, LLMRunSpec, set_run_context
from ....core.config import settings

logger = logging.getLogger(__name__)

# Reuse client instance across calls for efficiency
_client = OpenRouterClient()


def generate_email_content(prompt: str, request_id: str | None = None) -> str:
    """
    Generate email content using LLM based on the provided prompt.
    
    This function calls the OpenRouter API with a fallback strategy to ensure
    reliable content generation. It uses deterministic temperature settings
    for consistent output quality and applies post-processing to clean the
    generated text.
    
    LLM Configuration:
    - Strategy: Fallback (tries models sequentially until success)
    - Models: Uses premium models from settings
    - Temperature: 0.6 (balanced between creativity and consistency)
    - Max tokens: 1000 (sufficient for 150-220 word emails)
    
    Post-processing:
    - Strips markdown formatting (**, __, etc.)
    - Removes leading/trailing whitespace
    - Normalizes internal whitespace
    - Returns clean plain text
    
    Args:
        prompt: Complete prompt string for LLM (from prompt_builder)
        request_id: Optional request ID for tracing/logging
            If None, a generic context will be used
    
    Returns:
        Clean email content string ready for database storage
        
    Raises:
        OpenRouterAllModelsFailedError: If all models in fallback chain fail
        OpenRouterError: For other API-related errors
        
    Example:
        >>> from .prompt_builder import build_email_prompt
        >>> prompt = build_email_prompt(
        ...     contact_name="Max Müller",
        ...     company="TechStart GmbH",
        ...     description="AI startup",
        ...     organisation="AFS Akademie",
        ...     series_name="Gründerstories"
        ... )
        >>> content = generate_email_content(prompt, request_id="req-123")
        >>> # Returns: "Sehr geehrter Herr Müller,\n\n..."
    """
    # Set run context for tracing and logging
    request_id = request_id or "email-gen"
    set_run_context(module="email_generator", request_id=request_id)
    
    logger.debug(
        f"EMAIL_GEN | action=GENERATE_START | request_id={request_id}"
    )
    
    # Configure LLM run specification
    # Using fallback strategy for reliability
    # Temperature 0.6 balances creativity with consistency
    spec = LLMRunSpec(
        models=list(settings.llm_premium_models),
        strategy="fallback",
        temperature=0.6,
        max_tokens=1000,  # Sufficient for 150-220 word emails
    )
    
    try:
        # Call LLM with text completion
        raw_content = _client.complete_text(
            user_prompt=prompt,
            system_instructions=None,  # Instructions are in the prompt
            spec=spec,
        )
        
        # Post-process the generated content
        clean_content = _clean_llm_output(raw_content)
        
        logger.debug(
            f"EMAIL_GEN | action=GENERATE_SUCCESS | request_id={request_id} | "
            f"length={len(clean_content)}"
        )
        
        return clean_content
        
    except Exception as e:
        logger.error(
            f"EMAIL_GEN | action=GENERATE_FAILED | request_id={request_id} | "
            f"error={str(e)}"
        )
        raise


def _clean_llm_output(text: str) -> str:
    """
    Clean LLM output by removing markdown and normalizing whitespace.
    
    This function performs the following cleaning operations:
    1. Strips markdown bold (**text** or __text__)
    2. Strips markdown italic (*text* or _text_)
    3. Removes leading/trailing whitespace
    4. Normalizes internal whitespace (multiple spaces/newlines)
    
    Args:
        text: Raw text from LLM
        
    Returns:
        Cleaned plain text
        
    Example:
        >>> _clean_llm_output("**Hello**  world\\n\\n\\nTest")
        'Hello world\\n\\nTest'
    """
    if not text:
        return ""
    
    # Remove markdown bold: **text** or __text__
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
    text = re.sub(r'__(.+?)__', r'\1', text)
    
    # Remove markdown italic: *text* or _text_
    # Be careful not to remove underscores in email addresses or URLs
    text = re.sub(r'(?<!\w)\*(.+?)\*(?!\w)', r'\1', text)
    text = re.sub(r'(?<!\w)_(.+?)_(?!\w)', r'\1', text)
    
    # Strip leading/trailing whitespace
    text = text.strip()
    
    # Normalize multiple newlines to maximum of 2 (paragraph break)
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    # Normalize multiple spaces to single space
    text = re.sub(r' {2,}', ' ', text)
    
    return text