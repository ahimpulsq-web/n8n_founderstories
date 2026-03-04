"""
Google Sheets API Rate Limiter.

Implements token bucket algorithm with exponential backoff to respect Google Sheets API quotas:
- Read requests: 60 per minute per user
- Write requests: 60 per minute per user

This prevents quota exceeded errors and implements intelligent retry logic.
"""

import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Lock
from typing import Callable, TypeVar, Any

from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# Google Sheets API quota limits (per minute per user)
READ_QUOTA_PER_MINUTE = 60
WRITE_QUOTA_PER_MINUTE = 60

# Safety margin - use only 80% of quota to leave buffer
SAFETY_MARGIN = 0.8
EFFECTIVE_READ_QUOTA = int(READ_QUOTA_PER_MINUTE * SAFETY_MARGIN)
EFFECTIVE_WRITE_QUOTA = int(WRITE_QUOTA_PER_MINUTE * SAFETY_MARGIN)

# Exponential backoff configuration
INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 60.0
BACKOFF_MULTIPLIER = 2.0
MAX_RETRIES = 5

T = TypeVar('T')


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    max_requests_per_minute: int
    safety_margin: float = 0.8
    
    @property
    def effective_quota(self) -> int:
        """Calculate effective quota with safety margin."""
        return int(self.max_requests_per_minute * self.safety_margin)


class TokenBucket:
    """
    Token bucket rate limiter implementation.
    
    Allows bursts up to bucket capacity while maintaining average rate.
    Tokens refill at a constant rate (tokens_per_second).
    """
    
    def __init__(self, capacity: int, refill_rate: float):
        """
        Initialize token bucket.
        
        Args:
            capacity: Maximum number of tokens (burst capacity)
            refill_rate: Tokens added per second
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = float(capacity)
        self.last_refill = time.time()
        self.lock = Lock()
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        
        # Add tokens based on elapsed time
        tokens_to_add = elapsed * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill = now
    
    def acquire(self, tokens: int = 1, timeout: float | None = None) -> bool:
        """
        Acquire tokens from bucket, blocking if necessary.
        
        Args:
            tokens: Number of tokens to acquire
            timeout: Maximum time to wait in seconds (None = wait forever)
            
        Returns:
            True if tokens acquired, False if timeout
        """
        start_time = time.time()
        
        with self.lock:
            while True:
                self._refill()
                
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True
                
                # Check timeout
                if timeout is not None:
                    elapsed = time.time() - start_time
                    if elapsed >= timeout:
                        return False
                
                # Calculate wait time for next token
                tokens_needed = tokens - self.tokens
                wait_time = tokens_needed / self.refill_rate
                
                # Cap wait time to avoid excessive blocking
                wait_time = min(wait_time, 1.0)
        
        # Release lock and sleep
        time.sleep(wait_time)
        
        # Try again after sleep
        return self.acquire(tokens, timeout)
    
    def try_acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens without blocking.
        
        Args:
            tokens: Number of tokens to acquire
            
        Returns:
            True if tokens acquired, False otherwise
        """
        with self.lock:
            self._refill()
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            
            return False


class SheetsRateLimiter:
    """
    Rate limiter for Google Sheets API with separate buckets for reads and writes.
    
    Features:
    - Token bucket algorithm for smooth rate limiting
    - Separate quotas for read and write operations
    - Exponential backoff for 429 errors
    - Request tracking and metrics
    """
    
    def __init__(
        self,
        read_quota_per_minute: int = EFFECTIVE_READ_QUOTA,
        write_quota_per_minute: int = EFFECTIVE_WRITE_QUOTA
    ):
        """
        Initialize rate limiter.
        
        Args:
            read_quota_per_minute: Maximum read requests per minute
            write_quota_per_minute: Maximum write requests per minute
        """
        # Create token buckets for reads and writes
        # Refill rate = quota per minute / 60 seconds
        self.read_bucket = TokenBucket(
            capacity=read_quota_per_minute,
            refill_rate=read_quota_per_minute / 60.0
        )
        self.write_bucket = TokenBucket(
            capacity=write_quota_per_minute,
            refill_rate=write_quota_per_minute / 60.0
        )
        
        # Track request history for metrics
        self.read_history: deque[datetime] = deque(maxlen=1000)
        self.write_history: deque[datetime] = deque(maxlen=1000)
        self.quota_exceeded_count = 0
        self.lock = Lock()
        
        logger.info(
            "RATE_LIMITER | Initialized | read_quota=%d/min | write_quota=%d/min",
            read_quota_per_minute,
            write_quota_per_minute
        )
    
    def _is_read_operation(self, operation: str) -> bool:
        """Determine if operation is a read."""
        read_ops = ['get', 'batchGet', 'getByDataFilter']
        return any(op in operation for op in read_ops)
    
    def _is_write_operation(self, operation: str) -> bool:
        """Determine if operation is a write."""
        write_ops = ['update', 'batchUpdate', 'append', 'clear']
        return any(op in operation for op in write_ops)
    
    def _is_quota_error(self, error: Exception) -> bool:
        """Check if error is a quota exceeded error."""
        if not isinstance(error, HttpError):
            return False
        
        error_str = str(error).lower()
        return (
            'quota exceeded' in error_str or
            'rate limit' in error_str or
            error.resp.status == 429
        )
    
    def execute_with_retry(
        self,
        func: Callable[[], T],
        operation: str = "unknown",
        max_retries: int = MAX_RETRIES
    ) -> T:
        """
        Execute a Google Sheets API call with rate limiting and retry logic.
        
        Args:
            func: Function to execute (should return API response)
            operation: Operation name for logging (e.g., "spreadsheets.get")
            max_retries: Maximum number of retry attempts
            
        Returns:
            API response from func
            
        Raises:
            Exception: If all retries exhausted or non-retryable error
        """
        # Determine operation type and acquire tokens
        is_read = self._is_read_operation(operation)
        is_write = self._is_write_operation(operation)
        
        if is_read:
            bucket = self.read_bucket
            history = self.read_history
            op_type = "READ"
        elif is_write:
            bucket = self.write_bucket
            history = self.write_history
            op_type = "WRITE"
        else:
            # Unknown operation - treat as write (more conservative)
            bucket = self.write_bucket
            history = self.write_history
            op_type = "UNKNOWN"
        
        # Acquire token before making request
        acquired = bucket.acquire(tokens=1, timeout=120.0)
        if not acquired:
            raise TimeoutError(f"Failed to acquire rate limit token for {operation} within 120s")
        
        # Track request
        with self.lock:
            history.append(datetime.now())
        
        # Execute with exponential backoff
        backoff = INITIAL_BACKOFF_SECONDS
        last_error = None
        
        for attempt in range(max_retries):
            try:
                result = func()
                
                # Success - log if this was a retry
                if attempt > 0:
                    logger.info(
                        "RATE_LIMITER | RETRY_SUCCESS | op=%s | type=%s | attempt=%d",
                        operation,
                        op_type,
                        attempt + 1
                    )
                
                return result
            
            except Exception as e:
                last_error = e
                
                # Check if this is a quota error
                if self._is_quota_error(e):
                    with self.lock:
                        self.quota_exceeded_count += 1
                    
                    logger.warning(
                        "RATE_LIMITER | QUOTA_EXCEEDED | op=%s | type=%s | attempt=%d/%d | backoff=%.1fs",
                        operation,
                        op_type,
                        attempt + 1,
                        max_retries,
                        backoff
                    )
                    
                    # Wait before retry
                    if attempt < max_retries - 1:
                        time.sleep(backoff)
                        backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF_SECONDS)
                    
                    continue
                
                # Non-retryable error - raise immediately
                logger.error(
                    "RATE_LIMITER | NON_RETRYABLE_ERROR | op=%s | type=%s | error=%s",
                    operation,
                    op_type,
                    str(e)
                )
                raise
        
        # All retries exhausted
        logger.error(
            "RATE_LIMITER | MAX_RETRIES_EXCEEDED | op=%s | type=%s | retries=%d",
            operation,
            op_type,
            max_retries
        )
        raise last_error
    
    def get_metrics(self) -> dict[str, Any]:
        """
        Get rate limiter metrics.
        
        Returns:
            Dictionary with metrics:
            - read_requests_last_minute: Number of read requests in last minute
            - write_requests_last_minute: Number of write requests in last minute
            - quota_exceeded_count: Total quota exceeded errors
            - read_tokens_available: Current read tokens available
            - write_tokens_available: Current write tokens available
        """
        now = datetime.now()
        one_minute_ago = now - timedelta(minutes=1)
        
        with self.lock:
            read_last_minute = sum(1 for ts in self.read_history if ts > one_minute_ago)
            write_last_minute = sum(1 for ts in self.write_history if ts > one_minute_ago)
            
            # Refill buckets to get current token count
            self.read_bucket._refill()
            self.write_bucket._refill()
            
            return {
                'read_requests_last_minute': read_last_minute,
                'write_requests_last_minute': write_last_minute,
                'quota_exceeded_count': self.quota_exceeded_count,
                'read_tokens_available': int(self.read_bucket.tokens),
                'write_tokens_available': int(self.write_bucket.tokens),
                'read_quota_per_minute': EFFECTIVE_READ_QUOTA,
                'write_quota_per_minute': EFFECTIVE_WRITE_QUOTA
            }


# Global rate limiter instance
_rate_limiter: SheetsRateLimiter | None = None
_rate_limiter_lock = Lock()


def get_rate_limiter() -> SheetsRateLimiter:
    """
    Get or create the global rate limiter instance.
    
    Returns:
        Global SheetsRateLimiter instance
    """
    global _rate_limiter
    
    if _rate_limiter is None:
        with _rate_limiter_lock:
            if _rate_limiter is None:
                _rate_limiter = SheetsRateLimiter()
    
    return _rate_limiter


def reset_rate_limiter() -> None:
    """Reset the global rate limiter (useful for testing)."""
    global _rate_limiter
    with _rate_limiter_lock:
        _rate_limiter = None