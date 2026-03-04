"""
=============================================================================
MODULE: LLM Extraction Adapters - OpenRouter Adapter
=============================================================================

CLASSIFICATION: Adapter Module
LAYER: Integration/Adapters
DEPENDENCIES:
    - services.openrouter (global OpenRouter client)
    - core.config (settings)

PURPOSE:
    Provides an adapter interface for the OpenRouter API that bridges the
    extraction logic with the global OpenRouter client. This adapter translates
    between the extraction module's interface and the OpenRouter API format.

ADAPTER PATTERN:
    This module implements the Adapter pattern to:
    - Provide a consistent interface for LLM completion
    - Hide OpenRouter API details from extraction logic
    - Enable easy swapping of LLM providers in the future
    - Handle API-specific concerns (retries, formatting, etc.)

CONCURRENCY CONTROL:
    - Global semaphore limits concurrent LLM calls
    - ThreadPoolExecutor prevents thread explosion
    - Configurable via settings.llm_concurrency (default: 8)

EXPORTS:
    - OpenRouterLLMRouter: Adapter class for OpenRouter API

USAGE:
    from n8n_founderstories.services.enrichment.extract.llm.adapters import (
        OpenRouterLLMRouter
    )
    
    # Initialize router
    router = OpenRouterLLMRouter()
    
    # Complete a prompt
    response = await router.complete(prompt="Extract data from...")
    
    # Response format:
    # {
    #     "choices": [{
    #         "message": {
    #             "content": "<json_string>"
    #         }
    #     }]
    # }

NOTES:
    - Uses global OpenRouter client for connection pooling
    - Defaults to premium models for better quality
    - Temperature set to 0.0 for deterministic outputs
    - Enforces JSON response format via complete_json
    - Handles retries automatically via OpenRouterClient
    - Bounded concurrency prevents resource exhaustion
=============================================================================
"""
from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

from n8n_founderstories.core.config import settings
from n8n_founderstories.services.openrouter import OpenRouterClient, LLMRunSpec


# =============================================================================
# GLOBAL CONCURRENCY CONTROL
# =============================================================================

# Global bounded concurrency (prevents thread explosion)
# Configurable via settings.llm_max_concurrency (default: 6)
_LLM_CONCURRENCY = int(getattr(settings, "llm_max_concurrency", 6) or 6)
_LLM_SEM = asyncio.Semaphore(_LLM_CONCURRENCY)
_LLM_EXEC = ThreadPoolExecutor(max_workers=_LLM_CONCURRENCY)


# =============================================================================
# OPENROUTER ADAPTER
# =============================================================================

class OpenRouterLLMRouter:
    """
    Async adapter for OpenRouter API with bounded concurrency.
    
    This adapter provides a simple async interface: complete(prompt) -> response dict
    Internally uses services.openrouter.OpenRouterClient with fallback strategy.
    
    Features:
        - Async interface with bounded concurrency
        - Automatic fallback between models
        - JSON-enforced responses
        - Thread-safe execution
        - Configurable model and temperature
    
    Example:
        router = OpenRouterLLMRouter()
        response = await router.complete("Extract company data from...")
        content = response["choices"][0]["message"]["content"]
        data = json.loads(content)
    """
    
    def __init__(self, *, model: str | None = None, temperature: float = 0.0):
        """
        Initialize OpenRouter adapter.
        
        Args:
            model: Specific model to use (default: first premium model from settings)
            temperature: Temperature for generation (default: 0.0 for deterministic)
        
        Notes:
            - Uses fallback strategy with single model
            - Temperature 0.0 ensures deterministic outputs
            - Model defaults to settings.llm_premium_models[0]
        """
        self._client = OpenRouterClient()
        self._model = model or settings.llm_premium_models[0]
        self._temperature = float(temperature)
        
        # Fallback spec for extraction (single model)
        # Using fallback strategy ensures retries on failures
        self._spec = LLMRunSpec(
            models=[self._model],
            strategy="fallback",
            temperature=self._temperature,
            max_tokens=None,  # No limit, let model decide
        )
    
    async def complete(self, prompt: str) -> Dict[str, Any]:
        """
        Complete a prompt and return OpenRouter-style response.
        
        This method:
        1. Acquires semaphore for bounded concurrency
        2. Executes LLM call in thread pool (blocking I/O)
        3. Converts response to OpenRouter format
        4. Returns structured response dict
        
        Args:
            prompt: The complete prompt (system rules + task + schema + markdown)
        
        Returns:
            Dict with OpenRouter response format:
            {
                "choices": [
                    {
                        "message": {
                            "content": "<json_string>"
                        }
                    }
                ]
            }
        
        Raises:
            Exception: If LLM call fails after retries
        
        Notes:
            - Bounded by global semaphore (_LLM_SEM)
            - Runs in thread pool to avoid blocking event loop
            - Response is JSON string (parsed by extractor)
            - Automatic retries handled by OpenRouterClient
        """
        # Acquire semaphore for bounded concurrency
        async with _LLM_SEM:
            loop = asyncio.get_running_loop()
            
            # Define blocking call
            def _call() -> dict:
                """Execute blocking LLM call in thread pool."""
                return self._client.complete_json(
                    user_prompt=prompt,
                    system_instructions=None,  # Prompt includes system rules
                    spec=self._spec,
                )
            
            # Execute in thread pool (blocking I/O)
            data = await loop.run_in_executor(_LLM_EXEC, _call)
        
        # Convert dict -> JSON string to match extractor contract
        # Extractor expects: response["choices"][0]["message"]["content"]
        # Where content is a JSON string that gets parsed
        return {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(data)
                    }
                }
            ]
        }