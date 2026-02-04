from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Callable, Dict, Optional
from ....core.logging import get_live_status_logger


import httpx

logger = logging.getLogger(__name__)


class TokenBucket:
    """
    Simple token-bucket rate limiter.

    capacity: max tokens in the bucket
    refill_rate: tokens per second
    """

    def __init__(self, *, capacity: float, refill_rate: float) -> None:
        self.capacity = float(capacity)
        self.refill_rate = float(refill_rate)
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed <= 0:
            return
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last = now

    def time_until_available(self, tokens: float = 1.0) -> float:
        """
        Returns seconds until `tokens` can be consumed, 0 if available now.
        """
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                return 0.0
            missing = tokens - self._tokens
            return max(0.0, missing / self.refill_rate)

    def consume(self, tokens: float = 1.0) -> None:
        with self._lock:
            self._refill()
            self._tokens = max(0.0, self._tokens - tokens)


class DualRateLimiter:
    """
    Enforces BOTH:
      - 3 requests / second (conservative to avoid 403)
      - 40 requests / minute (conservative to avoid 403)
    """

    def __init__(self) -> None:
        self._per_second = TokenBucket(capacity=3, refill_rate=3.0)          # 3/sec
        self._per_minute = TokenBucket(capacity=40, refill_rate=40.0 / 60.0) # 40/min

    def acquire(self) -> None:
        """
        Blocks (sleeps) until both buckets can provide a token.
        """
        while True:
            t1 = self._per_second.time_until_available(1.0)
            t2 = self._per_minute.time_until_available(1.0)
            wait = max(t1, t2)
            if wait <= 0:
                self._per_second.consume(1.0)
                self._per_minute.consume(1.0)
                return
            time.sleep(wait)


def _parse_retry_after_seconds(headers: httpx.Headers) -> Optional[float]:
    """
    Retry-After can be seconds or HTTP date.
    We'll support numeric seconds (most common).
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


@dataclass
class RetryConfig:
    max_attempts: int = 6
    base_delay_s: float = 0.5      # initial backoff
    max_delay_s: float = 20.0      # cap backoff
    jitter_ratio: float = 0.2      # +/- 20%


class HunterAPIPolicy:
    """
    Wrap Hunter client calls with:
      - dual rate limiting (3/sec, 40/min)
      - retries with exponential backoff (+ jitter)
      - Retry-After handling
      - 403 handling with 60s wait (rolling window)
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
        `fn` must be a zero-arg callable that performs the actual http request
        (e.g. lambda: client.discover(...)).

        We acquire rate-limit tokens before EVERY attempt (including retries).
        """
        last_exc: Exception | None = None

        for attempt in range(1, self.retry.max_attempts + 1):
            self.limiter.acquire()

            try:
                return fn()

            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                # Retry on 403 (rate limit), 429 (too many requests), and 5xx (server errors)
                if status == 403 or status == 429 or 500 <= status <= 599:
                    last_exc = e
                    retry_after = _parse_retry_after_seconds(e.response.headers)
                    if retry_after is not None:
                        logger.warning(
                            "HUNTERIOV2 | RETRY_AFTER | request_id=%s | status=%s | retry_after_s=%.2f | attempt=%d/%d",
                            request_id, status, retry_after, attempt, self.retry.max_attempts
                        )
                        time.sleep(retry_after)
                    else:
                        # For 403, wait 60s for rolling window to reset
                        # For other errors, use exponential backoff
                        if status == 403:
                            wait_time = 60.0

                            live = get_live_status_logger()
                            live.update(
                                service="HUNTERIOV2",
                                state="SEARCHING",
                                level="WARNING",
                                request_id=request_id,
                                status="QUOTA_WAIT",
                                http_status=status,
                                wait_s=f"{wait_time:.2f}",
                                attempt=f"{attempt}/{self.retry.max_attempts}",
                            )

                            time.sleep(wait_time)

                        else:
                            backoff_time = self.retry.base_delay_s * (2 ** (attempt - 1))
                            logger.warning(
                                "HUNTERIOV2 | RETRY | request_id=%s | status=%s | backoff_s=%.2f | attempt=%d/%d",
                                request_id, status, backoff_time, attempt, self.retry.max_attempts
                            )
                            if attempt < self.retry.max_attempts:
                                time.sleep(min(backoff_time, self.retry.max_delay_s))
                    continue

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
