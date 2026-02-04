"""LLM Client interface for search_plan module."""
from __future__ import annotations

from typing import Type, TypeVar, Protocol
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class LLMClient(Protocol):
    """Protocol defining the LLM client interface required by search_plan."""
    
    provider_name: str
    
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
        ...