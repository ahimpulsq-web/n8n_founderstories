"""OpenRouter client for LLM completions."""
from __future__ import annotations

from typing import Type, TypeVar

from pydantic import BaseModel

from ...core.config import settings
from .errors import OpenRouterValidationError
from .http import extract_content, post_with_retry
from .parsing import safe_json_loads
from .strategies import execute_fallback, execute_vote
from .types import LLMRunSpec

T = TypeVar("T", bound=BaseModel)


class OpenRouterClient:
    """Client for OpenRouter API with support for fallback and vote strategies.
    
    This client provides a clean interface for LLM completions with:
    - Text generation (complete_text)
    - JSON generation (complete_json)
    - Structured output with Pydantic models (generate_structured)
    - Two strategies: fallback (sequential) and vote (parallel with merge)
    """
    
    provider_name: str = "openrouter"
    
    def __init__(self) -> None:
        """Initialize OpenRouter client with settings from config."""
        self._url = f"{settings.openrouter_base_url.rstrip('/')}/chat/completions"
    
    def complete_text(
        self,
        *,
        user_prompt: str,
        system_instructions: str | None = None,
        spec: LLMRunSpec,
    ) -> str:
        """Generate plain text completion.
        
        Args:
            user_prompt: User message content
            system_instructions: Optional system message
            spec: LLM run specification (only "fallback" strategy supported)
            
        Returns:
            Generated text string
            
        Raises:
            ValueError: If vote strategy is used (not supported for text)
        """
        if spec.strategy == "vote":
            raise ValueError(
                "Vote strategy is only supported for structured outputs. "
                "Use strategy='fallback' for text completions."
            )
        
        # Fallback: try models sequentially
        def call_model(m: str) -> str:
            return self._call_text(m, user_prompt, system_instructions, spec)
        
        return execute_fallback(spec.models, call_model)
    
    def complete_json(
        self,
        *,
        user_prompt: str,
        system_instructions: str | None = None,
        spec: LLMRunSpec,
    ) -> dict:
        """Generate JSON completion.
        
        Args:
            user_prompt: User message content
            system_instructions: Optional system message
            spec: LLM run specification (only "fallback" strategy supported)
            
        Returns:
            Parsed JSON as dictionary
            
        Raises:
            ValueError: If vote strategy is used (not supported for raw JSON)
        """
        if spec.strategy == "vote":
            raise ValueError(
                "Vote strategy is only supported for structured outputs. "
                "Use strategy='fallback' for JSON completions."
            )
        
        # Fallback: try models sequentially
        def call_model(m: str) -> dict:
            return self._call_json(m, user_prompt, system_instructions, spec)
        
        return execute_fallback(spec.models, call_model)
    
    def generate_structured(
        self,
        *,
        user_prompt: str,
        system_instructions: str,
        schema_model: Type[T],
        spec: LLMRunSpec,
    ) -> T:
        """Generate structured output validated against Pydantic model.
        
        This is the main method for structured LLM calls with voting support.
        
        Args:
            user_prompt: User message content
            system_instructions: System message with instructions
            schema_model: Pydantic model class for output validation
            spec: LLM run specification
            
        Returns:
            Instance of schema_model with validated LLM output
            
        Raises:
            ValueError: If vote parameters are invalid
        """
        # Validate vote parameters
        if spec.strategy == "vote" and spec.vote_k < spec.vote_min_wins:
            raise ValueError(
                f"vote_k ({spec.vote_k}) must be >= vote_min_wins ({spec.vote_min_wins})"
            )
        
        def call_model(m: str) -> T:
            return self._call_structured(m, user_prompt, system_instructions, schema_model, spec)
        
        if spec.strategy == "vote":
            result = execute_vote(spec.models, spec, schema_model, call_model)
        else:
            result = execute_fallback(spec.models, call_model)
        
        return result
    
    def embed(self, *, model: str, input: str) -> list[float]:
        """Generate embeddings for input text.
        
        Args:
            model: Embedding model name
            input: Text to embed
            
        Returns:
            List of embedding values
        """
        payload = {
            "model": model,
            "input": input,
        }
        
        response = post_with_retry(
            f"{settings.openrouter_base_url.rstrip('/')}/embeddings",
            payload,
        )
        return response["data"][0]["embedding"]
    
    # Private methods
    
    def _call_text(
        self,
        model: str,
        user_prompt: str,
        system_instructions: str | None,
        spec: LLMRunSpec,
    ) -> str:
        """Call single model for text completion."""
        messages = []
        if system_instructions:
            messages.append({"role": "system", "content": system_instructions})
        messages.append({"role": "user", "content": user_prompt})
        
        payload = {
            "model": model,
            "messages": messages,
            "temperature": spec.temperature,
        }
        if spec.max_tokens:
            payload["max_tokens"] = spec.max_tokens
        
        response = post_with_retry(self._url, payload)
        return extract_content(response)
    
    def _call_json(
        self,
        model: str,
        user_prompt: str,
        system_instructions: str | None,
        spec: LLMRunSpec,
    ) -> dict:
        """Call single model for JSON completion."""
        messages = []
        if system_instructions:
            messages.append({"role": "system", "content": system_instructions})
        messages.append({"role": "user", "content": user_prompt})
        
        payload = {
            "model": model,
            "messages": messages,
            "temperature": spec.temperature,
            "response_format": {"type": "json_object"},
        }
        if spec.max_tokens:
            payload["max_tokens"] = spec.max_tokens
        
        response = post_with_retry(self._url, payload)
        content = extract_content(response)
        return safe_json_loads(content)
    
    def _call_structured(
        self,
        model: str,
        user_prompt: str,
        system_instructions: str,
        schema_model: Type[T],
        spec: LLMRunSpec,
    ) -> T:
        """Call single model for structured completion."""
        system_text = (
            system_instructions.rstrip()
            + "\n\nReturn ONLY a SINGLE JSON object matching this schema exactly.\n"
            + f"{schema_model.model_json_schema()}"
        )
        
        messages = [
            {"role": "system", "content": system_text},
            {"role": "user", "content": user_prompt},
        ]
        
        payload = {
            "model": model,
            "messages": messages,
            "temperature": spec.temperature,
            "response_format": {"type": "json_object"},
        }
        if spec.max_tokens:
            payload["max_tokens"] = spec.max_tokens
        
        response = post_with_retry(self._url, payload)
        content = extract_content(response)
        
        try:
            data = safe_json_loads(content)
            return schema_model.model_validate(data)
        except Exception as e:
            raise OpenRouterValidationError(
                f"Failed to validate response against {schema_model.__name__}: {e}"
            ) from e