# src/n8n_founderstories/services/llm/base.py

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Type, TypeVar

from pydantic import BaseModel

from ...core.config import settings

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


# -------------------------------------------------------------------------
# LLM base interface
# -------------------------------------------------------------------------
class LLMClient(ABC):
    """Contract for all LLM provider clients."""

    provider_name: str

    @abstractmethod
    def generate_text(
        self,
        *,
        user_prompt: str,
        system_instructions: str | None = None,
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    def generate_structured(
        self,
        *,
        user_prompt: str,
        system_instructions: str,
        schema_model: Type[T],
    ) -> T:
        raise NotImplementedError


# -------------------------------------------------------------------------
# LLM factory
# -------------------------------------------------------------------------
def get_llm_client(provider: str | None = None) -> LLMClient:
    """
    Return an LLM client based on provider name or settings.

    Supported:
      - groq
      - gemini
    """
    effective = (provider or settings.llm_provider or "groq").strip().lower()

    if effective in {"groq", "groq-llama3", "llama3"}:
        logger.debug("LLM_FACTORY | provider=%s", effective)
        from .groq_client import GroqLLMClient
        return GroqLLMClient()

    if effective in {"gemini", "google"}:
        logger.debug("LLM_FACTORY | provider=%s", effective)
        from .gemini_client import GeminiLLMClient
        return GeminiLLMClient()

    raise RuntimeError(f"Unsupported LLM provider: {effective!r}")
