from __future__ import annotations

import re

_WORD_PATTERN = re.compile(r"[A-Za-z0-9ÄÖÜäöüß]+(?:-[A-Za-z0-9ÄÖÜäöüß]+)*")


def norm(value: str | None) -> str:
    """
    Normalize whitespace in user-provided text.

    - Strips leading/trailing whitespace
    - Collapses internal whitespace to single spaces
    """
    return " ".join(str(value or "").strip().split())


def tokenize_words(value: str | None) -> list[str]:
    """
    Tokenize text into words while preserving hyphenated terms as single tokens.

    Example:
      "Plant-based Protein" -> ["Plant-based", "Protein"]
    """
    return _WORD_PATTERN.findall(norm(value))


def slugify(value: str | None, *, max_length: int = 60) -> str:
    """
    Convert arbitrary text into a filesystem- and URL-safe slug.

    Rules:
    - lowercase
    - allow a–z, 0–9, underscore only
    - collapse multiple separators
    - trim to max_length

    Example:
      "Groq/LLaMA 3.1 Model" -> "groq_llama_3_1_model"
    """
    s = norm(value).lower()
    if not s:
        return ""

    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")

    return s[:max_length]
