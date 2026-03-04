"""
=============================================================================
PACKAGE: LLM Extraction Adapters
=============================================================================

CLASSIFICATION: Adapter Package
LAYER: Integration/Adapters

PURPOSE:
    Provides adapters for external LLM services and APIs.

MODULES:
    - router: OpenRouter API adapter

EXPORTS:
    - OpenRouterLLMRouter: Adapter for OpenRouter API

USAGE:
    from n8n_founderstories.services.enrichment.extract.llm.adapters import (
        OpenRouterLLMRouter,
    )
    
    router = OpenRouterLLMRouter()
    response = await router.complete(prompt="Extract data from...")

NOTES:
    - Adapters provide a consistent interface to external services
    - They handle API-specific details (authentication, retries, etc.)
    - They translate between internal and external data formats
=============================================================================
"""
from .router import OpenRouterLLMRouter

__all__ = [
    "OpenRouterLLMRouter",
]