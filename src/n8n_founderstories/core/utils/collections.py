# src/n8n_founderstories/core/utils/collections.py

from __future__ import annotations

from .text import norm


def cap(items: list[str] | None, max_items: int) -> list[str]:
    """
    Cap list length without mutating the input.
    """
    return (items or [])[:max_items]


def dedupe_strings_keep_order_case_insensitive(items: list[str] | None) -> list[str]:
    """
    De-dupe strings while preserving order (case-insensitive).

    - Normalizes whitespace
    - Drops empty values
    - Keeps first occurrence casing
    """
    seen: set[str] = set()
    out: list[str] = []

    for item in items or []:
        value = norm(item)
        if not value:
            continue

        key = value.lower()
        if key in seen:
            continue

        seen.add(key)
        out.append(value)

    return out


def dedupe_strings_keep_order(items: list[str] | None) -> list[str]:
    """
    De-dupe strings while preserving order (case-sensitive).

    - Normalizes whitespace
    - Drops empty values
    """
    seen: set[str] = set()
    out: list[str] = []

    for item in items or []:
        value = norm(item)
        if not value:
            continue

        if value in seen:
            continue

        seen.add(value)
        out.append(value)

    return out


def dedupe_sources_keep_order(sources: list[str] | None) -> list[str]:
    """
    De-dupe source/channel identifiers while preserving order.

    Intended for fields like:
      ["llm", "search_engine", "google_maps"]

    Behavior:
    - Normalizes whitespace
    - Case-insensitive comparison
    - Keeps original casing of first occurrence
    """
    seen: set[str] = set()
    out: list[str] = []

    for src in sources or []:
        value = norm(src)
        if not value:
            continue

        key = value.lower()
        if key in seen:
            continue

        seen.add(key)
        out.append(value)

    return out
