# src/n8n_founderstories/services/llm/groq_client.py

from __future__ import annotations

import json
import logging
from typing import Type, TypeVar

from groq import Groq
from pydantic import BaseModel

from ...core.config import settings
from .base import LLMClient

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


# -------------------------------------------------------------------------
# Groq LLM client
# -------------------------------------------------------------------------
class GroqLLMClient(LLMClient):
    """Groq implementation of the LLMClient interface."""

    provider_name: str = "groq"

    def __init__(self, model_name: str | None = None) -> None:
        if not settings.groq_api_key:
            raise RuntimeError("GROQ_API_KEY is not configured.")

        self._client = Groq(api_key=settings.groq_api_key)
        self._model_name = (
            model_name
            or settings.groq_model_name
            or "llama-3.1-70b-versatile"
        )

    # ---------------------------------------------------------------------
    # Metadata
    # ---------------------------------------------------------------------
    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def source_id(self) -> str:
        return f"{self.provider_name}/{self.model_name}"

    # ---------------------------------------------------------------------
    # Text generation
    # ---------------------------------------------------------------------
    def generate_text(
        self,
        *,
        user_prompt: str,
        system_instructions: str | None = None,
        temperature: float = 0.2,
        max_tokens: int | None = None,
    ) -> str:
        messages: list[dict[str, str]] = []

        if system_instructions:
            messages.append({"role": "system", "content": system_instructions.rstrip()})
        messages.append({"role": "user", "content": user_prompt.strip()})

        logger.debug(
            "GROQ_GENERATE_TEXT | model=%s | prompt_len=%d",
            self._model_name,
            sum(len(m["content"]) for m in messages),
        )

        resp = self._client.chat.completions.create(
            model=self._model_name,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        content = resp.choices[0].message.content if resp.choices else ""
        return (content or "").strip()

    # ---------------------------------------------------------------------
    # Structured generation
    # ---------------------------------------------------------------------
    def generate_structured(
        self,
        *,
        user_prompt: str,
        system_instructions: str,
        schema_model: Type[T],
        temperature: float = 0.1,
        max_tokens: int | None = None,
    ) -> T:
        system_text = (
            system_instructions.rstrip()
            + "\n\nYou MUST respond with a single JSON value ONLY.\n"
            + "The JSON MUST match the following Pydantic schema exactly, "
            + "including field names and types:\n"
            + f"{schema_model.model_json_schema()}\n\n"
            + "Do NOT add explanations. Do NOT wrap the JSON in markdown. "
            + "Return ONLY the JSON value."
        )

        messages = [
            {"role": "system", "content": system_text},
            {
                "role": "user",
                "content": "Return ONLY valid JSON that matches the schema exactly.\n\n"
                + user_prompt.strip(),
            },
        ]

        logger.debug(
            "GROQ_GENERATE_STRUCTURED | model=%s | schema=%s",
            self._model_name,
            schema_model.__name__,
        )

        resp = self._client.chat.completions.create(
            model=self._model_name,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
        )

        raw_content = resp.choices[0].message.content if resp.choices else ""
        raw_content = (raw_content or "").strip()
        if not raw_content:
            raise ValueError("Groq returned an empty response for structured output.")

        try:
            data = json.loads(raw_content)
        except json.JSONDecodeError as exc:
            logger.error("GROQ_STRUCTURED_INVALID_JSON | content=%s", raw_content)
            raise ValueError("Groq returned invalid JSON.") from exc

        logger.debug("GROQ_STRUCTURED_TOP_LEVEL | type=%s", type(data).__name__)

        if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict):
            logger.warning("GROQ_STRUCTURED_UNWRAP | unwrapping single-element list")
            data = data[0]

        if schema_model.__name__ == "MultiChannelQueries" and isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and (
                    "web_queries" in item or "maps_queries" in item or "company_prompt" in item
                ):
                    logger.warning("GROQ_STRUCTURED_PICK | MultiChannelQueries picked dict from list")
                    data = item
                    break

        if schema_model.__name__ == "MultiChannelQueries" and isinstance(data, dict):
            for key in ("web_queries", "maps_queries"):
                seq = data.get(key)
                if not isinstance(seq, list):
                    continue

                cleaned: list[str] = []
                for item in seq:
                    if isinstance(item, str):
                        cleaned.append(item)
                        continue
                    if isinstance(item, list):
                        first_str = next((x for x in item if isinstance(x, str)), None)
                        if first_str:
                            cleaned.append(first_str)
                        continue
                    if isinstance(item, dict):
                        q = item.get("query") or item.get("text")
                        if isinstance(q, str):
                            cleaned.append(q)
                        continue

                data[key] = cleaned

        if schema_model.__name__ == "SearchPlanPayload" and isinstance(data, dict):
            if "industry" not in data and "niche" in data:
                data["industry"] = data["niche"]

        if isinstance(data, list) and hasattr(schema_model, "model_fields"):
            fields = schema_model.model_fields  # type: ignore[attr-defined]
            if isinstance(fields, dict) and len(fields) == 1:
                only_field_name = next(iter(fields.keys()))
                data = {only_field_name: data}

        if schema_model.__name__ == "CompanySeedList" and isinstance(data, dict):
            companies = data.get("companies")
            if isinstance(companies, list):
                cleaned: list[dict] = []
                for item in companies:
                    if not isinstance(item, dict):
                        continue
                    if any(k in item for k in ("$defs", "$schema", "properties", "title", "type")) and "name" not in item:
                        continue
                    if "companies" in item and isinstance(item["companies"], list):
                        for sub in item["companies"]:
                            if isinstance(sub, dict):
                                cleaned.append(sub)
                        continue
                    cleaned.append(item)
                data["companies"] = cleaned

        try:
            return schema_model.model_validate(data)  # type: ignore[attr-defined]
        except AttributeError:
            return schema_model.parse_obj(data)
