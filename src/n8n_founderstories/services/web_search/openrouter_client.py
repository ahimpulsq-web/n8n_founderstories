from __future__ import annotations

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()


class OpenRouterError(RuntimeError):
    pass


def _parse_json_strict_or_extract(text: str) -> dict:
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty model response")

    try:
        return json.loads(text)
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return json.loads(text[start : end + 1])

    raise ValueError(f"Non-JSON response: {text[:200]}")


class OpenRouterClient:
    def __init__(self, api_key: str, model: str):
        if not api_key:
            raise ValueError("Missing LLM_API_KEYS")
        if not model:
            raise ValueError("Missing OpenRouter model")
        self.api_key = api_key
        self.model = model
        self._models: list[str] = [model]

    @classmethod
    def from_env(cls, *, tier_env: str, fallback_env: str | None = None) -> "OpenRouterClient":
        api_key = os.getenv("LLM_API_KEYS", "").strip()

        def load_models(env_name: str) -> list[str]:
            tier = os.getenv(env_name)
            models = os.getenv(tier, "") if tier else ""
            return [m.strip() for m in models.split(",") if m.strip()]

        models = load_models(tier_env)
        if fallback_env:
            models += load_models(fallback_env)

        if not models:
            raise ValueError("No models configured")

        inst = cls(api_key=api_key, model=models[0])
        inst._models = models
        return inst

    def complete_json(self, prompt: str) -> dict:
        last_err: str | None = None

        def _call(include_json_mode: bool):
            body = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": "Return ONLY valid JSON. No markdown, no prose."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0,
            }
            if include_json_mode:
                body["response_format"] = {"type": "json_object"}

            return requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": os.getenv("OPENROUTER_HTTP_REFERER", ""),
                    "X-Title": os.getenv("OPENROUTER_APP_TITLE", ""),
                },
                json=body,
                timeout=60,
            )

        for model in self._models:
            self.model = model

            r = _call(include_json_mode=True)
            if r.status_code == 400:
                r = _call(include_json_mode=False)

            try:
                payload = r.json()
            except Exception:
                payload = None

            if r.status_code == 200 and isinstance(payload, dict) and "choices" in payload:
                try:
                    content = payload["choices"][0]["message"]["content"]
                    return _parse_json_strict_or_extract(content)
                except Exception as e:
                    last_err = f"{model}: {e}"
                    continue

            err_msg = None
            if isinstance(payload, dict) and "error" in payload:
                err = payload.get("error") or {}
                err_msg = err.get("message") if isinstance(err, dict) else str(err)
            else:
                err_msg = (r.text or "")[:300]

            # skip privacy/data-policy blocked endpoints
            if r.status_code == 404 and err_msg and "data policy" in err_msg.lower():
                last_err = f"{model}: HTTP 404 - {err_msg}"
                continue

            transient = r.status_code in (429, 500, 502, 503, 504)
            if transient:
                last_err = f"{model}: HTTP {r.status_code} - {err_msg}"
                continue

            # provider generic 400 -> try next
            if r.status_code == 400 and err_msg and "provider returned error" in err_msg.lower():
                last_err = f"{model}: HTTP 400 - {err_msg}"
                continue

            raise OpenRouterError(f"{model}: HTTP {r.status_code} - {err_msg}")

        raise OpenRouterError(last_err or "All models failed")
