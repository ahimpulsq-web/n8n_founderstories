# src/n8n_founderstories/services/llm/gemini_client.py

from __future__ import annotations

import logging
from typing import Type, TypeVar

from google import genai
from google.genai import types
from pydantic import BaseModel

from ...core.config import settings
from .base import LLMClient

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


# -------------------------------------------------------------------------
# Gemini LLM client
# -------------------------------------------------------------------------
class GeminiLLMClient(LLMClient):
    """Gemini implementation of the LLMClient interface."""

    provider_name: str = "gemini"

    def __init__(self) -> None:
        if not settings.gemini_api_key:
            raise RuntimeError("GEMINI_API_KEY is not configured.")
        self._client = genai.Client(api_key=settings.gemini_api_key)
        self._model_name = settings.gemini_model_name

    def generate_text(
        self,
        *,
        user_prompt: str,
        system_instructions: str | None = None,
    ) -> str:
        parts = []
        if system_instructions:
            parts.append(system_instructions.rstrip())
            parts.append("\n\n")
        parts.append(user_prompt.strip())
        full_prompt = "".join(parts)

        logger.debug(
            "GEMINI_GENERATE_TEXT | model=%s | prompt_len=%d",
            self._model_name,
            len(full_prompt),
        )

        resp = self._client.models.generate_content(
            model=self._model_name,
            contents=full_prompt,
        )
        return getattr(resp, "text", "").strip()

    def generate_structured(
        self,
        *,
        user_prompt: str,
        system_instructions: str,
        schema_model: Type[T],
    ) -> T:
        cfg = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=schema_model,
        )

        full_prompt = system_instructions.rstrip() + "\n\n" + user_prompt.strip()

        logger.debug(
            "GEMINI_GENERATE_STRUCTURED | model=%s | schema=%s",
            self._model_name,
            schema_model.__name__,
        )

        response = self._client.models.generate_content(
            model=self._model_name,
            contents=full_prompt,
            config=cfg,
        )

        return response.parsed
