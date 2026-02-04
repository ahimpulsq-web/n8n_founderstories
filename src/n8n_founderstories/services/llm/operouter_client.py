"""OpenRouter LLM client wrapper for search_plan compatibility."""
from __future__ import annotations

from typing import Type, TypeVar
from pydantic import BaseModel

from ..openrouter.runner import OpenRouterClient, LLMRunSpec
from ...core.config import settings

T = TypeVar("T", bound=BaseModel)


class OpenRouterLLMClient:
    """
    Wrapper around OpenRouterClient that provides a simplified interface
    for the search_plan module.
    
    This client uses a single model and fallback strategy by default.
    """
    
    def __init__(self, api_key: str, model_name: str):
        """
        Initialize the OpenRouter LLM client.
        
        Args:
            api_key: OpenRouter API key
            model_name: Name of the model to use
        """
        self._client = OpenRouterClient()
        self.provider_name = "openrouter"
        self.source_id = model_name
        self._model_name = model_name
    
    def generate_structured(
        self,
        *,
        user_prompt: str,
        system_instructions: str,
        schema_model: Type[T],
        temperature: float = 0.1,
    ) -> T:
        """
        Generate structured output from the LLM.
        
        Args:
            user_prompt: The user's prompt text
            system_instructions: System-level instructions for the LLM
            schema_model: Pydantic model class defining the expected output schema
            temperature: Temperature parameter for generation (default: 0.1)
            
        Returns:
            Instance of schema_model with the LLM's structured response
        """
        spec = LLMRunSpec(
            models=[self._model_name],
            strategy="single",
            temperature=temperature,
            max_tokens=None,
        )
        
        return self._client.generate_structured(
            user_prompt=user_prompt,
            system_instructions=system_instructions,
            schema_model=schema_model,
            spec=spec,
        )