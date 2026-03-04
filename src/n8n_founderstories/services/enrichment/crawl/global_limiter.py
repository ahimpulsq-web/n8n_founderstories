"""
═══════════════════════════════════════════════════════════════════════════════
GLOBAL LIMITER - Concurrency Control for Browser Automation
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [INFRASTRUCTURE] - Global concurrency management

PURPOSE:
    Provides a singleton threading.Semaphore to limit total concurrent crawl
    operations across all Crawl4AIClient instances, preventing system overload.

ARCHITECTURE:
    - Process-wide cap on total concurrent page fetches
    - Thread-safe using threading.Semaphore
    - Works across multiple asyncio event loops
    - Async-friendly interface using asyncio.to_thread()

CONFIGURATION:
    Environment Variable: CRAWL_GLOBAL_MAX_CONCURRENCY
    - Default: 5
    - Min: 3 (values below 3 are clamped to 3)
    - Max: 20 (values above 20 are clamped to 20)
    - Concurrency is always enabled (cannot be disabled)

SINGLETON PATTERN:
    - Single semaphore shared across all Crawl4AIClient instances
    - Thread-safe initialization
    - Async context manager interface

USAGE:
    # In crawl4ai_engine.py:
    limiter = get_global_limiter()
    async with limiter:
        result = await crawler.arun(url)
    
    # Limits total concurrent requests across all crawlers

EXAMPLE:
    CRAWL_DOMAIN_WORKERS=15
    CRAWL_GLOBAL_MAX_CONCURRENCY=10
    
    Job A starts → uses 10 slots (clamped from 15)
    Job B starts → waits until slots become available

BENEFITS:
    - Prevents system resource exhaustion
    - Controls total browser instances
    - Protects against memory/CPU overload
    - Works alongside per-domain limits

DEPENDENCIES:
    - threading: For Semaphore and thread-safe coordination
    - asyncio: For async context manager
    - os: For environment variable access

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import os
import threading
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional


class GlobalConcurrencyLimiter:
    """
    Process-wide concurrency limiter using threading.Semaphore.
    
    Thread-safe and works across multiple asyncio event loops.
    """
    
    def __init__(self, max_concurrency: int):
        """
        Initialize global limiter.
        
        Args:
            max_concurrency: Maximum concurrent operations (clamped between 3-20)
        """
        self._max_concurrency = max_concurrency
        self._enabled = max_concurrency > 0
        self._semaphore: Optional[threading.Semaphore] = None
        
        if self._enabled:
            self._semaphore = threading.Semaphore(max_concurrency)
    
    @property
    def enabled(self) -> bool:
        """Check if global limiting is enabled."""
        return self._enabled
    
    @property
    def max_concurrency(self) -> int:
        """Get maximum concurrency limit."""
        return self._max_concurrency
    
    async def acquire(self) -> None:
        """
        Acquire a slot from the global limiter.
        
        Blocks until a slot is available if at capacity.
        Uses asyncio.to_thread to avoid blocking the event loop.
        """
        if not self._enabled or self._semaphore is None:
            return
        
        # Run blocking semaphore.acquire in thread pool
        await asyncio.to_thread(self._semaphore.acquire)
    
    def release(self) -> None:
        """
        Release a slot back to the global limiter.
        
        Synchronous operation - safe to call from any context.
        """
        if not self._enabled or self._semaphore is None:
            return
        
        self._semaphore.release()
    
    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[None]:
        """
        Async context manager for automatic acquire/release.
        
        Usage:
            async with global_limiter():
                # Your code here
                pass
        """
        if not self._enabled:
            yield
            return
        
        await self.acquire()
        try:
            yield
        finally:
            self.release()


# Singleton instance - shared across all jobs and event loops
_global_limiter: Optional[GlobalConcurrencyLimiter] = None


def _initialize_limiter() -> GlobalConcurrencyLimiter:
    """
    Initialize the global limiter singleton.
    
    Reads CRAWL_GLOBAL_MAX_CONCURRENCY from environment.
    - Default: 5
    - Min: 3 (values below 3 are clamped to 3)
    - Max: 20 (values above 20 are clamped to 20)
    - Concurrency is always enabled (cannot be disabled)
    
    Returns:
        GlobalConcurrencyLimiter instance
    """

    # =========================================================================
    # [INFRASTRUCTURE] Global concurrency configuration
    # - Default: 5
    # - Min: 3 (values below 3 are clamped to 3)
    # - Max: 20 (values above 20 are clamped to 20)
    # - Concurrency is always enabled (cannot be disabled)
    # =========================================================================
    raw_value = int(os.getenv("CRAWL_GLOBAL_MAX_CONCURRENCY", "5"))

    # Clamp to safe bounds (min: 3, max: 20)
    max_concurrency = max(3, min(raw_value, 20))

    return GlobalConcurrencyLimiter(max_concurrency)


def get_global_limiter() -> GlobalConcurrencyLimiter:
    """
    Get the global limiter singleton.
    
    Lazy initialization on first access.
    Thread-safe.
    
    Returns:
        GlobalConcurrencyLimiter instance
    """
    global _global_limiter
    
    if _global_limiter is None:
        _global_limiter = _initialize_limiter()
    
    return _global_limiter


# Convenience exports
global_limiter = get_global_limiter()

def is_enabled() -> bool:
    """[INFRASTRUCTURE] True if global limiter is active."""
    return global_limiter.enabled