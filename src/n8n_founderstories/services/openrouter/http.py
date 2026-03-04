"""HTTP layer for OpenRouter API requests."""
from __future__ import annotations

import time
from typing import Any

import requests

from ...core.config import settings
from .context import _get_ctx
from .errors import OpenRouterAPIError, OpenRouterTimeoutError


def build_headers() -> dict[str, str]:
    """Build HTTP headers for OpenRouter API requests.
    
    Returns:
        Dictionary of HTTP headers including authorization and context info
    """
    import logging
    logger = logging.getLogger(__name__)
    
    module, request_id = _get_ctx()
    
    app = (settings.app_name or "app").strip()
    mod = (module or "").strip()
    
    x_title = f"{app} | {mod}" if mod else app
    
    headers: dict[str, str] = {
        "Authorization": f"Bearer {settings.openrouter_api_key}",
        "Content-Type": "application/json",
        "X-Title": x_title,
    }
    
    if settings.openrouter_http_referer:
        headers["HTTP-Referer"] = settings.openrouter_http_referer
    
    if request_id:
        headers["X-Request-Id"] = str(request_id)
    
    # Debug logging
    logger.debug(f"OpenRouter headers: X-Title='{x_title}', module='{module}', app='{app}'")
    
    return headers


def post_with_retry(
    url: str,
    payload: dict[str, Any],
    timeout: float | None = None,
    max_retries: int | None = None,
) -> dict[str, Any]:
    """POST request to OpenRouter with exponential backoff retry.
    
    Only retries on transient errors (429, 5xx) and timeouts.
    Does not retry on client errors (400, 401, 403, 404) as they won't succeed.
    
    Args:
        url: API endpoint URL
        payload: JSON payload
        timeout: Request timeout in seconds (uses settings default if None)
        max_retries: Maximum retry attempts (uses settings default if None)
        
    Returns:
        JSON response as dictionary
        
    Raises:
        OpenRouterAPIError: On HTTP error response
        OpenRouterTimeoutError: On timeout
    """
    timeout = timeout or float(settings.llm_timeout_s)
    max_retries = max_retries if max_retries is not None else int(settings.llm_max_retries)
    
    for attempt in range(max_retries + 1):
        try:
            response = requests.post(
                url,
                headers=build_headers(),
                json=payload,
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()
            
        except requests.Timeout as e:
            if attempt >= max_retries:
                raise OpenRouterTimeoutError(f"Request timed out after {timeout}s") from e
            time.sleep(0.6 * (2 ** attempt))
            
        except requests.HTTPError as e:
            status_code = e.response.status_code if e.response else None
            error_msg = str(e)
            if e.response:
                try:
                    error_data = e.response.json()
                    error_msg = error_data.get("error", {}).get("message", str(e))
                except Exception:
                    pass
            
            # Only retry on transient errors (429 rate limit, 5xx server errors)
            # Don't retry on client errors (400, 401, 403, 404, etc.)
            should_retry = status_code in (429,) or (status_code and status_code >= 500)
            
            if not should_retry or attempt >= max_retries:
                raise OpenRouterAPIError(error_msg, status_code) from e
            
            time.sleep(0.6 * (2 ** attempt))
            
        except Exception as e:
            if attempt >= max_retries:
                raise OpenRouterAPIError(f"Request failed: {e}") from e
            time.sleep(0.6 * (2 ** attempt))
    
    raise OpenRouterAPIError("Request failed after all retries")


def extract_content(response_data: dict[str, Any]) -> str:
    """Extract assistant message content from OpenRouter response.
    
    Args:
        response_data: JSON response from OpenRouter API
        
    Returns:
        Extracted content string (stripped)
    """
    return str(response_data["choices"][0]["message"]["content"] or "").strip()