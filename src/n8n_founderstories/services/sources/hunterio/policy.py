"""
Hunter.io API policy module.

Provides retry logic and rate limiting for Hunter.io API calls.
Wraps client calls with:
- Dual rate limiting (3 req/sec, 40 req/min)
- Exponential backoff with jitter
- Retry-After header handling
- Smart 403 error handling (distinguishes plan access from rate limits)

Architecture:
    Orchestrator
         ↓
    Policy (THIS MODULE) - retry + rate limit wrapper
         ↓
    Client - pure HTTP calls

This module ensures API calls respect Hunter.io's rate limits
and handle transient failures gracefully while failing fast on
non-retryable errors like plan access restrictions.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Callable, Dict, Optional

import httpx

logger = logging.getLogger(__name__)

# ============================================================================
# RATE LIMITING
# ============================================================================


class DualRateLimiter:
    """
    Sliding window rate limiter for Hunter.io API.
    
    Uses a sliding window approach to enforce BOTH rate limits:
    - 3 requests/second (conservative, actual limit is 5/sec)
    - 40 requests/minute (conservative, actual limit is 50/min)
    
    This implementation tracks actual request timestamps and waits when
    necessary to stay within limits. This is the same approach used in
    the test script that successfully made 72 calls.
    
    The actual Hunter.io limits are 5 req/sec and 50 req/min, but we use
    conservative values to provide a safety margin.
    """

    def __init__(self, requests_per_second: int = 3, requests_per_minute: int = 40) -> None:
        self.requests_per_second = requests_per_second
        self.requests_per_minute = requests_per_minute
        self.request_times: list[float] = []
        self._lock = Lock()

    def acquire(self) -> None:
        """
        Acquire permission to make a request.
        
        Blocks (sleeps) if necessary to stay within rate limits.
        This ensures both per-second and per-minute limits are respected.
        """
        with self._lock:
            now = time.time()
            
            # Remove requests older than 1 minute
            self.request_times = [t for t in self.request_times if now - t < 60]
            
            # Check per-minute limit
            if len(self.request_times) >= self.requests_per_minute:
                oldest = self.request_times[0]
                wait_time = 60 - (now - oldest) + 0.1  # Add small buffer
                if wait_time > 0:
                    time.sleep(wait_time)
                    now = time.time()
                    # Clean up again after sleeping
                    self.request_times = [t for t in self.request_times if now - t < 60]
            
            # Check per-second limit
            recent = [t for t in self.request_times if now - t < 1]
            if len(recent) >= self.requests_per_second:
                wait_time = 1 - (now - recent[0]) + 0.1  # Add small buffer
                if wait_time > 0:
                    time.sleep(wait_time)
                    now = time.time()
            
            # Record this request
            self.request_times.append(now)


def _parse_retry_after_seconds(headers: httpx.Headers) -> Optional[float]:
    """
    Parse Retry-After header from HTTP response.
    
    The Retry-After header can be either:
    - Numeric seconds (e.g., "60")
    - HTTP date (not currently supported)
    
    Args:
        headers: HTTP response headers
        
    Returns:
        Seconds to wait, or None if header not present or invalid
    """
    ra = headers.get("Retry-After")
    if not ra:
        return None
    ra = ra.strip()
    try:
        # numeric seconds
        return max(0.0, float(ra))
    except ValueError:
        return None


def _extract_hunter_error_id(resp: httpx.Response) -> str | None:
    """
    Try to extract Hunter error identifier from JSON response body.
    Hunter typically returns structured JSON errors with an error id/code.
    This lets us distinguish non-retryable 403 (no_discover_access) from others.
    
    Args:
        resp: HTTP response object
        
    Returns:
        Error identifier string if found, None otherwise
        
    Example response formats:
        {"errors":[{"id":"no_discover_access", ...}]}
        {"error":"no_discover_access"}
    """
    try:
        data = resp.json()
    except Exception:
        return None

    # Common patterns (defensive):
    # {"errors":[{"id":"no_discover_access", ...}]}
    # {"error":"no_discover_access"}  (less likely)
    if isinstance(data, dict):
        if isinstance(data.get("error"), str):
            return data["error"].strip() or None

        errs = data.get("errors")
        if isinstance(errs, list) and errs:
            first = errs[0]
            if isinstance(first, dict):
                for k in ("id", "code", "type", "slug"):
                    v = first.get(k)
                    if isinstance(v, str) and v.strip():
                        return v.strip()
    return None


# ============================================================================
# RETRY CONFIGURATION
# ============================================================================

@dataclass
class RetryConfig:
    """
    Configuration for retry behavior.
    
    Attributes:
        max_attempts: Maximum number of attempts (including initial)
        base_delay_s: Initial backoff delay in seconds
        max_delay_s: Maximum backoff delay in seconds
        jitter_ratio: Jitter as fraction of delay (+/- 20%)
    """
    max_attempts: int = 6
    base_delay_s: float = 0.5      # initial backoff
    max_delay_s: float = 20.0      # cap backoff
    jitter_ratio: float = 0.2      # +/- 20%

# ============================================================================
# API POLICY
# ============================================================================


class HunterAPIPolicy:
    """
    Hunter.io API policy wrapper.
    
    Wraps Hunter.io client calls with production-grade reliability:
    - Dual rate limiting (3 req/sec, 40 req/min)
    - Exponential backoff with jitter
    - Retry-After header handling
    - Smart 403 handling (distinguishes plan access vs rate limit)
    - Network error retry
    
    This policy ensures API calls are resilient to:
    - Rate limit errors (429, 403 too_many_requests)
    - Server errors (5xx)
    - Network failures (timeouts, connection errors)
    
    Non-retryable errors (fail fast):
    - 403 no_discover_access (plan doesn't include Discover endpoint)
    - 403 other errors (unknown permission issues)
    - 400 validation errors (invalid input parameters)
    
    Usage:
        policy = HunterAPIPolicy()
        data = policy.call_discover(
            fn=lambda: client.discover(...),
            request_id="req_123"
        )
    """

    def __init__(
        self,
        *,
        limiter: Optional[DualRateLimiter] = None,
        retry: Optional[RetryConfig] = None,
    ) -> None:
        self.limiter = limiter or DualRateLimiter()
        self.retry = retry or RetryConfig()

    def _sleep_backoff(self, attempt_idx: int) -> None:
        # attempt_idx starts at 1 for the first retry sleep
        delay = min(self.retry.max_delay_s, self.retry.base_delay_s * (2 ** (attempt_idx - 1)))
        jitter = delay * self.retry.jitter_ratio
        delay = delay + random.uniform(-jitter, jitter)
        time.sleep(max(0.0, delay))

    def call_discover(self, *, fn: Callable[[], Dict[str, Any]], request_id: str | None = None) -> Dict[str, Any]:
        """
        Execute Hunter.io discover call with retry and rate limiting.
        
        This method wraps the actual HTTP call with:
        1. Rate limiting (acquire tokens before each attempt)
        2. Retry logic (exponential backoff with jitter)
        3. Smart error handling (429, 5xx, 403 too_many_requests retryable; 403 no_discover_access non-retryable)
        
        Retryable errors (with backoff):
        - 429 (Too Many Requests)
        - 403 too_many_requests (rate limit/quota exceeded)
        - 5xx (Server errors)
        - Network errors (timeouts, connection failures)
        
        Non-retryable errors (fail immediately):
        - 403 no_discover_access (plan doesn't include Discover endpoint)
        - 403 other errors (unknown permission issues)
        - 400 validation errors (invalid input parameters)
        
        Args:
            fn: Zero-arg callable that performs the HTTP request
                (e.g., lambda: client.discover(...))
            request_id: Optional request ID for logging
            
        Returns:
            Hunter.io API response dictionary
            
        Raises:
            httpx.HTTPStatusError: If non-retryable HTTP error occurs (403, 400)
                or if all retry attempts are exhausted for retryable errors
            httpx.TimeoutException: If all retry attempts are exhausted
            httpx.TransportError: If all retry attempts are exhausted
            
        Note:
            Rate limit tokens are acquired before EVERY attempt,
            including retries. This ensures we never exceed limits
            even during retry scenarios.
        """
        last_exc: Exception | None = None

        for attempt in range(1, self.retry.max_attempts + 1):
            self.limiter.acquire()

            try:
                return fn()

            except httpx.HTTPStatusError as e:
                status = e.response.status_code

                # -------------------------
                # Retryable HTTP statuses
                # -------------------------
                if status == 429 or 500 <= status <= 599:
                    last_exc = e

                    retry_after = _parse_retry_after_seconds(e.response.headers)
                    if retry_after is not None:
                        logger.warning(
                            "HUNTERIOV2 | RETRY_AFTER | request_id=%s | status=%s | retry_after_s=%.2f | attempt=%d/%d",
                            request_id, status, retry_after, attempt, self.retry.max_attempts
                        )
                        time.sleep(retry_after)
                    else:
                        logger.warning(
                            "HUNTERIOV2 | RETRY | request_id=%s | status=%s | attempt=%d/%d",
                            request_id, status, attempt, self.retry.max_attempts
                        )
                        if attempt < self.retry.max_attempts:
                            self._sleep_backoff(attempt_idx=attempt)
                    continue

                # -------------------------
                # 403: distinguish retryable vs non-retryable
                # -------------------------
                if status == 403:
                    err_id = _extract_hunter_error_id(e.response)

                    # Non-retryable: plan does not include Discover (documented error)
                    if err_id == "no_discover_access":
                        logger.error(
                            "HUNTERIOV2 | NO_DISCOVER_ACCESS | request_id=%s | "
                            "Your plan does not include access to the Discover endpoint. "
                            "This is a non-retryable error. Upgrade your Hunter.io plan to access this feature.",
                            request_id
                        )
                        raise

                    # Retryable: too_many_requests (rate limit/quota response)
                    if err_id == "too_many_requests":
                        # =========================================================================
                        # Classification: Rate-limit handling / Retryable condition
                        # =========================================================================
                        last_exc = e

                        retry_after = _parse_retry_after_seconds(e.response.headers)
                        if retry_after is not None:
                            logger.warning(
                                "HUNTERIOV2 | RETRY_AFTER_403 | request_id=%s | err_id=%s | retry_after_s=%.2f | attempt=%d/%d",
                                request_id, err_id, retry_after, attempt, self.retry.max_attempts
                            )
                            time.sleep(retry_after)
                        else:
                            logger.warning(
                                "HUNTERIOV2 | RETRY_403_RATE_LIMIT | request_id=%s | err_id=%s | attempt=%d/%d",
                                request_id, err_id, attempt, self.retry.max_attempts
                            )
                            if attempt < self.retry.max_attempts:
                                self._sleep_backoff(attempt_idx=attempt)

                        continue

                    # Otherwise: unknown 403 error (fail fast)
                    logger.error(
                        "HUNTERIOV2 | FORBIDDEN | request_id=%s | err_id=%s | "
                        "Received 403 Forbidden error. Not retrying unknown 403 errors. "
                        "Common causes: (1) Invalid/expired API key, (2) Account suspended, "
                        "(3) IP blocked, (4) Permission denied. "
                        "Check your Hunter.io account status and API key at https://hunter.io/api",
                        request_id, err_id
                    )
                    raise

                # Non-retryable HTTP error
                raise

            except (httpx.TimeoutException, httpx.TransportError) as e:
                # Retry on network problems
                last_exc = e
                logger.warning(
                    "HUNTERIOV2 | RETRY_NET | request_id=%s | err=%s | attempt=%d/%d",
                    request_id, type(e).__name__, attempt, self.retry.max_attempts
                )
                if attempt < self.retry.max_attempts:
                    self._sleep_backoff(attempt_idx=attempt)
                continue

        # If we exhaust attempts, raise the last error
        if last_exc:
            raise last_exc
        raise RuntimeError("HunterAPIPolicy.call_discover exhausted attempts without exception (unexpected)")
