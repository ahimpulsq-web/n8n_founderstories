"""JSON parsing utilities for OpenRouter responses."""
from __future__ import annotations

import json
from typing import Any

from .errors import OpenRouterValidationError


def safe_json_loads(text: str) -> dict[str, Any]:
    """Parse JSON string with error handling.
    
    Args:
        text: JSON string to parse
        
    Returns:
        Parsed JSON as dictionary
        
    Raises:
        OpenRouterValidationError: If JSON parsing fails
    """
    try:
        result = json.loads(text)
        if not isinstance(result, dict):
            raise OpenRouterValidationError(
                f"Expected JSON object, got {type(result).__name__}"
            )
        return result
    except json.JSONDecodeError as e:
        raise OpenRouterValidationError(f"Invalid JSON: {e}") from e


def canonicalize_string(s: str) -> str:
    """Normalize string for comparison (lowercase, single spaces).
    
    Args:
        s: String to normalize
        
    Returns:
        Normalized string
    """
    return " ".join((s or "").strip().casefold().split())


def is_list_of_dicts(value: Any) -> bool:
    """Check if value is a list of dictionaries.
    
    Args:
        value: Value to check
        
    Returns:
        True if value is a list containing at least one dict, or empty list
    """
    return isinstance(value, list) and (len(value) == 0 or isinstance(value[0], dict))