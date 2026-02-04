from __future__ import annotations

from ..llm.interface import LLMClient
from .models import ResolvedGeoLLM
from .prompts import _GEO_SYSTEM_INSTRUCTIONS


def resolve_geo_llm(*, prompt: str, default_region: str, llm_client: LLMClient) -> ResolvedGeoLLM:
    """
    Resolve geo intent from a user prompt using LLM-based extraction.
    
    This function uses the LLM to extract and structure geographic intent from
    the user's prompt, providing a more flexible and language-aware alternative
    to deterministic geo resolution.
    
    Args:
        prompt: The user's raw prompt text
        default_region: The default region to use if no location is detected (e.g., "DACH")
        llm_client: The LLM client instance to use for structured generation
        
    Returns:
        ResolvedGeoLLM: Structured geo resolution with resolved_geo, geo_mode, and geo_location_keywords
        
    Example:
        >>> resolved = resolve_geo_llm(
        ...     prompt="vegan protein brands in Germany",
        ...     default_region="DACH",
        ...     llm_client=client
        ... )
        >>> resolved.resolved_geo
        'Germany'
        >>> resolved.geo_mode
        'country'
        >>> resolved.geo_location_keywords
        {'DE': {'hl': 'en', 'locations': ['Germany']}}
    """
    user_prompt = (
        f"User prompt: {prompt}\n"
        f"default_region: {default_region}\n"
        "Extract geo intent per schema."
    )

    return llm_client.generate_structured(
        user_prompt=user_prompt,
        system_instructions=_GEO_SYSTEM_INSTRUCTIONS,
        schema_model=ResolvedGeoLLM,
        temperature=0.0,
    )