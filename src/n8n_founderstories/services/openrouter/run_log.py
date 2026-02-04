# src/n8n_founderstories/services/openrouter/run_log.py
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_lock = threading.Lock()
_jsonl: Path | None = None
_txt: Path | None = None
_module: str | None = None


def start_run(*, module: str) -> tuple[Path, Path]:
    global _jsonl, _txt, _module
    logs = Path(__file__).parent / "logs"
    logs.mkdir(exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    _module = module
    _jsonl = logs / f"{module}_run_{ts}.jsonl"
    _txt = logs / f"{module}_run_{ts}.txt"
    return _jsonl, _txt


def _mark(winner: bool) -> str:
    return "✅" if winner else "⛔"


def _is_kv_winner_dict(x: Any) -> bool:
    # {"value": ..., "winner": ...}
    return isinstance(x, dict) and "value" in x and "winner" in x


def _format_list_of_kv_dicts(meta: list[dict[str, Any]]) -> list[str]:
    """
    meta item shape:
      {"value": X, "winner": bool}
    """
    out: list[str] = []
    for item in meta:
        if _is_kv_winner_dict(item):
            out.append(f"  - {item['value']} {_mark(bool(item['winner']))}")
        else:
            out.append(f"  - {item}")
    return out


def _format_list_of_dict_fields(meta: list[dict[str, Any]]) -> list[str]:
    """
    meta item shape (per-model view for list[dict]):
      {
        "city": {"value": "...", "winner": bool},
        "state": {"value": "...", "winner": bool},
        ...
      }
    Output per element:
      - {city: Munich ✅, state: Bavaria ⛔, ...}
    """
    out: list[str] = []
    for elem in meta:
        if not isinstance(elem, dict):
            out.append(f"  - {elem}")
            continue

        parts: list[str] = []
        for k, vv in elem.items():
            if _is_kv_winner_dict(vv):
                parts.append(f"{k}: {vv['value']} {_mark(bool(vv['winner']))}")
            else:
                parts.append(f"{k}: {vv}")
        out.append("  - {" + ", ".join(parts) + "}")
    return out


def _format_result_list_of_scalars(v: list[Any], srcs: dict[str, list[str]]) -> list[str]:
    out: list[str] = []
    for item in v:
        src = ", ".join(srcs.get(str(item), []))
        out.append(f"  - {item}  ({src})")
    return out


def _format_result_list_of_dicts(v: list[Any], srcs: list[dict[str, list[str]]]) -> list[str]:
    """
    winner["final"][field] is list[dict]
    winner["field_sources"][field] is list[dict[key] -> list[models]]
    Output:
      - {city: Munich, state: Bavaria, ...}
        (m1, m2)
    Where (m1,m2) is the union of sources across keys for that element.
    """
    out: list[str] = []
    for i, elem in enumerate(v):
        if not isinstance(elem, dict):
            out.append(f"  - {elem}")
            continue

        parts = [f"{k}: {vv}" for k, vv in elem.items()]
        out.append("  - {" + ", ".join(parts) + "}")

        src_i = srcs[i] if i < len(srcs) and isinstance(srcs[i], dict) else {}
        models_union: list[str] = []
        seen: set[str] = set()
        for ms in src_i.values():
            for m in ms:
                if m not in seen:
                    seen.add(m)
                    models_union.append(m)

        out.append(f"    ({', '.join(models_union)})")
    return out


def append_llm_result(
    *,
    module: str | None,
    request_id: str | None,
    kind: str,
    schema_model: str | None,
    strategy: str,
    vote_k: int | None,
    vote_min_wins: int | None,
    prompt: str | None,
    duration_ms: int | None,
    models: list[dict[str, Any]],
    winner: dict[str, Any] | None,
    error: str | None = None,
) -> None:
    jsonl, txt = _jsonl, _txt

    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "module": module,
        "request_id": request_id,
        "kind": kind,
        "schema": schema_model,
        "strategy": strategy,
        "models": models,
        "winner": winner,
        "error": error,
    }

    with _lock:
        with open(jsonl, "a", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
            f.write("\n")

        lines: list[str] = []
        lines.append("[OPENROUTER RUN]")
        lines.append(f"MODULE: {module}")
        if request_id:
            lines.append(f"REQUEST_ID: {request_id}")
        lines.append(f"STRATEGY: vote (k={vote_k}, min_wins={vote_min_wins})")
        lines.append("")

        lines.append("-" * 60)
        lines.append("MODEL RESPONSES")
        lines.append("-" * 60)

        for idx, m in enumerate(models, 1):
            lines.append(f"[{idx}] MODEL: {m['model']}")

            for field, meta in m.get("fields", {}).items():
                # list[dict] with per-field ticks (compact dict line)
                if isinstance(meta, list) and meta and isinstance(meta[0], dict) and not _is_kv_winner_dict(meta[0]):
                    lines.append(f"{field}:")
                    lines.extend(_format_list_of_dict_fields(meta))
                    continue

                # list of {"value","winner"}
                if isinstance(meta, list):
                    lines.append(f"{field}:")
                    lines.extend(_format_list_of_kv_dicts(meta))
                    continue

                # scalar {"value","winner"}
                if isinstance(meta, dict) and "value" in meta and "winner" in meta:
                    lines.append(f"{field}: {meta['value']} {_mark(bool(meta['winner']))}")
                    continue

                # fallback
                lines.append(f"{field}: {meta}")

            lines.append("")

        if winner:
            lines.append("-" * 60)
            lines.append("RESULT")
            lines.append("-" * 60)

            for f, v in winner["final"].items():
                srcs = winner["field_sources"].get(f)

                # list[dict] result (index-based sources list)
                if isinstance(v, list) and isinstance(srcs, list) and (len(v) == 0 or isinstance(v[0], dict)):
                    lines.append(f"{f}:")
                    lines.extend(_format_result_list_of_dicts(v, srcs))
                    continue

                # list scalar result (element->models dict)
                if isinstance(v, list) and isinstance(srcs, dict):
                    lines.append(f"{f}:")
                    lines.extend(_format_result_list_of_scalars(v, srcs))
                    continue

                # scalar result
                src = ", ".join(winner["field_sources"].get(f, []))
                lines.append(f"{f}: {v}  ({src})")

        lines.append("")
        lines.append("-" * 60)
        lines.append("")

        with open(txt, "a", encoding="utf-8") as f:
            f.write("\n".join(lines))
