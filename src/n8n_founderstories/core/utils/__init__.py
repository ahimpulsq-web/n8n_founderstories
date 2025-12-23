"""
Shared utility helpers used across services.

This package groups small, deterministic helpers that:
- do not depend on infrastructure (DB, network, APIs)
- are safe to reuse across search_plan, storage, services, etc.
"""

from .text import norm, tokenize_words, slugify
from .collections import (
    cap,
    dedupe_strings_keep_order_case_insensitive,
    dedupe_strings_keep_order,
    dedupe_sources_keep_order,
)

__all__ = [
    "norm",
    "tokenize_words",
    "slugify",
    "cap",
    "dedupe_strings_keep_order_case_insensitive",
    "dedupe_strings_keep_order",
    "dedupe_sources_keep_order",
]
