# src/n8n_founderstories/services/openrouter/runner.py
from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Literal, Type, TypeVar

import requests
from pydantic import BaseModel

from ...core.config import settings

logger = logging.getLogger(__name__)
T = TypeVar("T", bound=BaseModel)

Strategy = Literal["single", "fallback", "vote"]


@dataclass(frozen=True)
class LLMRunSpec:
    models: list[str]
    strategy: Strategy = "fallback"
    vote_k: int = 3
    vote_min_wins: int = 2
    temperature: float = 0.1
    max_tokens: int | None = None


def _canon_str(s: str) -> str:
    return " ".join((s or "").strip().casefold().split())


def _is_list_of_dicts(x: Any) -> bool:
    return isinstance(x, list) and (len(x) == 0 or isinstance(x[0], dict))


class OpenRouterClient:
    provider_name: str = "openrouter"

    def __init__(self) -> None:
        self._api_key = settings.openrouter_api_key
        self._url = f"{settings.openrouter_base_url.rstrip('/')}/chat/completions"
        self._timeout_s = float(settings.llm_timeout_s)
        self._max_retries = int(settings.llm_max_retries)

    def embed(self, *, model: str, input: str) -> list[float]:
        payload = {
            "model": model,
            "input": input,
        }

        r = requests.post(
            f"{settings.openrouter_base_url.rstrip('/')}/embeddings",
            headers=self._headers(),
            json=payload,
            timeout=self._timeout_s,
        )
        r.raise_for_status()
        return r.json()["data"][0]["embedding"]


    # ---------- HTTP ----------

    def _headers(self) -> dict[str, str]:
        h = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        if settings.openrouter_http_referer:
            h["HTTP-Referer"] = settings.openrouter_http_referer
        if settings.openrouter_app_title:
            h["X-Title"] = settings.openrouter_app_title
        return h

    def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        for attempt in range(self._max_retries + 1):
            try:
                r = requests.post(
                    self._url,
                    headers=self._headers(),
                    json=payload,
                    timeout=self._timeout_s,
                )
                r.raise_for_status()
                return r.json()
            except Exception:
                if attempt >= self._max_retries:
                    raise
                time.sleep(0.6 * (2**attempt))
        raise RuntimeError("OpenRouter request failed")

    @staticmethod
    def _extract_content(data: dict[str, Any]) -> str:
        return str(data["choices"][0]["message"]["content"] or "").strip()

    # ---------- Structured ----------

    def generate_structured(
        self,
        *,
        user_prompt: str,
        system_instructions: str,
        schema_model: Type[T],
        spec: LLMRunSpec,
    ) -> T:
        from .run_log import append_llm_result
        from .openrouter_client import _get_ctx

        module, request_id = _get_ctx()
        start = time.perf_counter()

        models = spec.models[: spec.vote_k]
        results: dict[str, dict[str, Any]] = {}

        def _task(m: str) -> tuple[str, dict[str, Any]]:
            obj = self._call_structured(
                m, user_prompt, system_instructions, schema_model, spec
            )
            return m, obj.model_dump()

        with ThreadPoolExecutor(max_workers=len(models)) as ex:
            futures = {ex.submit(_task, m): m for m in models}
            for fut in as_completed(futures):
                m = futures[fut]
                try:
                    _, out = fut.result()
                    results[m] = out
                except Exception as exc:
                    results[m] = {"__error__": str(exc)}

        final: dict[str, Any] = {}
        field_sources: dict[str, Any] = {}
        per_model: list[dict[str, Any]] = []

        default_model = models[0]

        # ---------- FIELD PROCESSING ----------

        for field in schema_model.model_fields.keys():
            values = {
                m: out.get(field)
                for m, out in results.items()
                if "__error__" not in out
            }

            field_info = schema_model.model_fields[field]
            vote_mode = (
                field_info.json_schema_extra.get("vote")
                if field_info.json_schema_extra
                else None
            )

            # ---------- LIST FIELD ----------
            if any(isinstance(v, list) for v in values.values()):
                # Detect "list of dicts" generically → index-based, field-level voting
                listish = [v for v in values.values() if isinstance(v, list)]
                is_list_dicts = any(_is_list_of_dicts(v) for v in listish) and all(
                    _is_list_of_dicts(v) for v in listish
                )

                # ---- LIST OF DICTS: INDEX-BASED VOTING ----
                if is_list_dicts:
                    # fallback length = default model length (deterministic)
                    default_list = results.get(default_model, {}).get(field) or []
                    if not isinstance(default_list, list):
                        default_list = []

                    max_len = len(default_list)
                    final_list: list[dict[str, Any]] = []
                    sources: list[dict[str, list[str]]] = []

                    # union of keys per index across models (but fallback to default on no majority)
                    for i in range(max_len):
                        # gather dicts at index i
                        dicts_i: dict[str, dict[str, Any]] = {}
                        for m, v in values.items():
                            if not isinstance(v, list) or i >= len(v):
                                continue
                            if isinstance(v[i], dict):
                                dicts_i[m] = v[i]

                        # default dict for index i
                        default_dict = {}
                        if i < len(default_list) and isinstance(default_list[i], dict):
                            default_dict = default_list[i]

                        keys = set(default_dict.keys())
                        for d in dicts_i.values():
                            keys.update(d.keys())

                        out_i: dict[str, Any] = {}
                        src_i: dict[str, list[str]] = {}

                        for k in keys:
                            canon_map: defaultdict[str, list[str]] = defaultdict(list)
                            for m, d in dicts_i.items():
                                canon_map[_canon_str(str(d.get(k)))].append(m)

                            winner_value = None
                            winner_models: list[str] = []

                            for canon, ms in canon_map.items():
                                if len(ms) >= spec.vote_min_wins:
                                    winner_models = ms
                                    winner_value = dicts_i[ms[0]].get(k)
                                    break

                            if winner_value is None:
                                winner_value = default_dict.get(k)
                                winner_models = [default_model]

                            out_i[k] = winner_value
                            src_i[k] = winner_models

                        final_list.append(out_i)
                        sources.append(src_i)

                    final[field] = final_list
                    field_sources[field] = sources
                    continue

                # ---- UNION MODE (simple lists) ----
                if vote_mode == "union":
                    seen: dict[str, str] = {}
                    final_list = []

                    for m, v in values.items():
                        if not isinstance(v, list):
                            continue
                        for item in v:
                            canon = _canon_str(str(item))
                            if canon not in seen:
                                seen[canon] = m
                                final_list.append(item)

                    final[field] = final_list
                    field_sources[field] = {
                        str(item): [seen[_canon_str(str(item))]]
                        for item in final_list
                    }
                    continue

                # ---- MAJORITY VOTE MODE (simple lists) ----
                item_votes: defaultdict[str, list[str]] = defaultdict(list)

                for m, v in values.items():
                    if not isinstance(v, list):
                        continue
                    for item in v:
                        item_votes[_canon_str(str(item))].append(m)

                winners = [
                    canon
                    for canon, ms in item_votes.items()
                    if len(ms) >= spec.vote_min_wins
                ]

                final_list = []
                sources = {}

                for canon in winners:
                    model = item_votes[canon][0]
                    orig_item = next(
                        x
                        for x in results[model][field]
                        if _canon_str(str(x)) == canon
                    )
                    final_list.append(orig_item)
                    sources[str(orig_item)] = item_votes[canon]

                final[field] = final_list
                field_sources[field] = sources
                continue

            # ---------- SCALAR FIELD ----------
            canon_map: defaultdict[str, list[str]] = defaultdict(list)

            for m, v in values.items():
                canon_map[_canon_str(str(v))].append(m)

            winner_value = None
            winner_models: list[str] = []

            for canon, ms in canon_map.items():
                if len(ms) >= spec.vote_min_wins:
                    winner_models = ms
                    winner_value = results[ms[0]][field]
                    break

            if winner_value is None:
                winner_value = results[default_model][field]
                winner_models = [default_model]

            final[field] = winner_value
            field_sources[field] = winner_models

        # ---------- PER MODEL VIEW ----------

        for m in models:
            fields = {}
            for f, val in results.get(m, {}).items():
                if f.startswith("__"):
                    continue

                src = field_sources.get(f)

                # list of dicts: show per-index, per-key tick marks
                if isinstance(val, list) and isinstance(src, list) and (
                    len(val) == 0 or isinstance(val[0], dict)
                ):
                    out_list = []
                    for i, d in enumerate(val):
                        if not isinstance(d, dict):
                            out_list.append({"value": d, "winner": False})
                            continue
                        src_i = src[i] if i < len(src) and isinstance(src[i], dict) else {}
                        out_d = {}
                        for k, v in d.items():
                            winners = src_i.get(k, [])
                            out_d[k] = {"value": v, "winner": m in winners}
                        out_list.append(out_d)
                    fields[f] = out_list
                    continue

                # simple list: per-element ticks
                if isinstance(val, list) and isinstance(src, dict):
                    fields[f] = [
                        {"value": x, "winner": str(x) in src}
                        for x in val
                    ]
                    continue

                # scalar
                fields[f] = {
                    "value": val,
                    "winner": m in field_sources.get(f, []),
                }

            per_model.append(
                {
                    "model": m,
                    "status": "success",
                    "fields": fields,
                }
            )

        dur_ms = int((time.perf_counter() - start) * 1000)

        append_llm_result(
            module=module,
            request_id=request_id,
            kind="structured",
            schema_model=schema_model.__name__,
            strategy="vote",
            vote_k=spec.vote_k,
            vote_min_wins=spec.vote_min_wins,
            prompt=user_prompt,
            duration_ms=dur_ms,
            models=per_model,
            winner={
                "final": final,
                "field_sources": field_sources,
            },
        )

        return schema_model.model_validate(final)

    # ---------- Low-level ----------

    def _call_structured(
        self,
        model: str,
        user_prompt: str,
        system_instructions: str,
        schema_model: Type[T],
        spec: LLMRunSpec,
    ) -> T:
        system_text = (
            system_instructions.rstrip()
            + "\n\nReturn ONLY a SINGLE JSON object matching this schema exactly.\n"
            + f"{schema_model.model_json_schema()}"
        )

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_text},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": spec.temperature,
            "response_format": {"type": "json_object"},
        }

        data = self._post(payload)
        raw = self._extract_content(data)
        return schema_model.model_validate(json.loads(raw))
