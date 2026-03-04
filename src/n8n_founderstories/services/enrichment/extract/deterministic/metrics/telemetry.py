"""
Telemetry and Performance Tracking Module

This module provides comprehensive metrics collection and performance tracking
for the deterministic email extraction system. It tracks:
- Extraction performance (timing, throughput)
- Success/failure rates
- Email quality metrics
- Resource usage statistics

Author: N8N FounderStories Team
Last Modified: 2026-02-18
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


# ============================================================================
# STATISTICS MODELS
# ============================================================================

@dataclass
class ExtractionStats:
    """
    Statistics for email extraction operations.
    
    Attributes:
        total_extractions: Total number of extraction operations
        successful_extractions: Number of successful extractions
        failed_extractions: Number of failed extractions
        total_pages_processed: Total pages processed across all extractions
        total_emails_extracted: Total emails extracted
        total_time_ms: Total time spent in extraction (milliseconds)
        avg_time_ms: Average extraction time (milliseconds)
        avg_emails_per_extraction: Average emails per extraction
        avg_pages_per_extraction: Average pages per extraction
    """
    total_extractions: int = 0
    successful_extractions: int = 0
    failed_extractions: int = 0
    total_pages_processed: int = 0
    total_emails_extracted: int = 0
    total_time_ms: int = 0
    
    @property
    def avg_time_ms(self) -> float:
        """Calculate average extraction time."""
        if self.total_extractions == 0:
            return 0.0
        return self.total_time_ms / self.total_extractions
    
    @property
    def avg_emails_per_extraction(self) -> float:
        """Calculate average emails per extraction."""
        if self.successful_extractions == 0:
            return 0.0
        return self.total_emails_extracted / self.successful_extractions
    
    @property
    def avg_pages_per_extraction(self) -> float:
        """Calculate average pages per extraction."""
        if self.total_extractions == 0:
            return 0.0
        return self.total_pages_processed / self.total_extractions
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate (0.0 to 1.0)."""
        if self.total_extractions == 0:
            return 0.0
        return self.successful_extractions / self.total_extractions
    
    def to_dict(self) -> dict:
        """Convert stats to dictionary."""
        return {
            "total_extractions": self.total_extractions,
            "successful_extractions": self.successful_extractions,
            "failed_extractions": self.failed_extractions,
            "total_pages_processed": self.total_pages_processed,
            "total_emails_extracted": self.total_emails_extracted,
            "total_time_ms": self.total_time_ms,
            "avg_time_ms": self.avg_time_ms,
            "avg_emails_per_extraction": self.avg_emails_per_extraction,
            "avg_pages_per_extraction": self.avg_pages_per_extraction,
            "success_rate": self.success_rate,
        }
    
    def __repr__(self) -> str:
        return (
            f"ExtractionStats("
            f"extractions={self.total_extractions}, "
            f"success_rate={self.success_rate:.2%}, "
            f"avg_time={self.avg_time_ms:.1f}ms, "
            f"avg_emails={self.avg_emails_per_extraction:.1f})"
        )


@dataclass
class ExtractionEvent:
    """
    Single extraction event for detailed tracking.
    
    Attributes:
        timestamp: When extraction occurred
        domain: Domain being extracted
        pages_processed: Number of pages processed
        emails_extracted: Number of emails extracted
        time_ms: Time taken (milliseconds)
        success: Whether extraction succeeded
        error: Error message if failed
    """
    timestamp: datetime
    domain: str
    pages_processed: int
    emails_extracted: int
    time_ms: int
    success: bool
    error: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert event to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "domain": self.domain,
            "pages_processed": self.pages_processed,
            "emails_extracted": self.emails_extracted,
            "time_ms": self.time_ms,
            "success": self.success,
            "error": self.error,
        }


# ============================================================================
# GLOBAL STATISTICS TRACKER
# ============================================================================

class GlobalStatsTracker:
    """
    Global statistics tracker for all extraction operations.
    
    This maintains running statistics across all extractions in the process.
    """
    
    def __init__(self):
        self.stats = ExtractionStats()
        self.recent_events: list[ExtractionEvent] = []
        self.max_recent_events = 100
    
    def record_extraction(
        self,
        domain: str,
        pages_processed: int,
        emails_extracted: int,
        time_ms: int,
        success: bool,
        error: Optional[str] = None,
    ):
        """
        Record an extraction event.
        
        Args:
            domain: Domain that was extracted
            pages_processed: Number of pages processed
            emails_extracted: Number of emails extracted
            time_ms: Time taken in milliseconds
            success: Whether extraction succeeded
            error: Error message if failed
        """
        # Update stats
        self.stats.total_extractions += 1
        if success:
            self.stats.successful_extractions += 1
        else:
            self.stats.failed_extractions += 1
        
        self.stats.total_pages_processed += pages_processed
        self.stats.total_emails_extracted += emails_extracted
        self.stats.total_time_ms += time_ms
        
        # Record event
        event = ExtractionEvent(
            timestamp=datetime.utcnow(),
            domain=domain,
            pages_processed=pages_processed,
            emails_extracted=emails_extracted,
            time_ms=time_ms,
            success=success,
            error=error,
        )
        
        self.recent_events.append(event)
        
        # Trim old events
        if len(self.recent_events) > self.max_recent_events:
            self.recent_events = self.recent_events[-self.max_recent_events:]
    
    def get_stats(self) -> ExtractionStats:
        """Get current statistics."""
        return self.stats
    
    def get_recent_events(self, count: Optional[int] = None) -> list[ExtractionEvent]:
        """
        Get recent extraction events.
        
        Args:
            count: Number of events to return (None for all)
            
        Returns:
            List of recent events (newest first)
        """
        events = list(reversed(self.recent_events))
        if count is not None:
            events = events[:count]
        return events
    
    def reset(self):
        """Reset all statistics."""
        self.stats = ExtractionStats()
        self.recent_events = []


# Global tracker instance
_global_tracker = GlobalStatsTracker()


# ============================================================================
# PUBLIC API
# ============================================================================

def track_extraction(
    domain: str,
    pages_processed: int,
    emails_extracted: int,
    time_ms: int,
    success: bool = True,
    error: Optional[str] = None,
):
    """
    Track an extraction operation.
    
    This should be called after each extraction to record metrics.
    
    Args:
        domain: Domain that was extracted
        pages_processed: Number of pages processed
        emails_extracted: Number of emails extracted
        time_ms: Time taken in milliseconds
        success: Whether extraction succeeded
        error: Error message if failed
        
    Examples:
        >>> track_extraction("example.com", pages_processed=5, emails_extracted=3, time_ms=150)
    """
    _global_tracker.record_extraction(
        domain=domain,
        pages_processed=pages_processed,
        emails_extracted=emails_extracted,
        time_ms=time_ms,
        success=success,
        error=error,
    )


def get_global_stats() -> ExtractionStats:
    """
    Get global extraction statistics.
    
    Returns:
        ExtractionStats with cumulative statistics
        
    Examples:
        >>> stats = get_global_stats()
        >>> print(f"Success rate: {stats.success_rate:.2%}")
    """
    return _global_tracker.get_stats()


def get_recent_events(count: Optional[int] = 10) -> list[ExtractionEvent]:
    """
    Get recent extraction events.
    
    Args:
        count: Number of events to return
        
    Returns:
        List of recent events (newest first)
        
    Examples:
        >>> events = get_recent_events(5)
        >>> for event in events:
        ...     print(f"{event.domain}: {event.emails_extracted} emails")
    """
    return _global_tracker.get_recent_events(count)


def reset_global_stats():
    """
    Reset global statistics.
    
    This is useful for testing or when starting a new session.
    
    Examples:
        >>> reset_global_stats()
    """
    _global_tracker.reset()


# ============================================================================
# CONTEXT MANAGERS
# ============================================================================

@contextmanager
def timed_extraction(domain: str, pages_count: int):
    """
    Context manager for timing extraction operations.
    
    This automatically tracks timing and records the extraction event.
    
    Args:
        domain: Domain being extracted
        pages_count: Number of pages being processed
        
    Yields:
        Dictionary to store results (emails_extracted, success, error)
        
    Examples:
        >>> with timed_extraction("example.com", pages_count=5) as result:
        ...     # Perform extraction
        ...     result["emails_extracted"] = 3
        ...     result["success"] = True
    """
    start_time = time.perf_counter()
    result = {
        "emails_extracted": 0,
        "success": True,
        "error": None,
    }
    
    try:
        yield result
    except Exception as e:
        result["success"] = False
        result["error"] = str(e)
        raise
    finally:
        time_ms = int((time.perf_counter() - start_time) * 1000)
        track_extraction(
            domain=domain,
            pages_processed=pages_count,
            emails_extracted=result["emails_extracted"],
            time_ms=time_ms,
            success=result["success"],
            error=result["error"],
        )


# ============================================================================
# PERFORMANCE MONITORING
# ============================================================================

@dataclass
class PerformanceMetrics:
    """
    Detailed performance metrics for monitoring.
    
    Attributes:
        p50_time_ms: 50th percentile extraction time
        p95_time_ms: 95th percentile extraction time
        p99_time_ms: 99th percentile extraction time
        min_time_ms: Minimum extraction time
        max_time_ms: Maximum extraction time
    """
    p50_time_ms: float = 0.0
    p95_time_ms: float = 0.0
    p99_time_ms: float = 0.0
    min_time_ms: int = 0
    max_time_ms: int = 0


def get_performance_metrics() -> PerformanceMetrics:
    """
    Calculate performance percentiles from recent events.
    
    Returns:
        PerformanceMetrics with percentile data
        
    Examples:
        >>> metrics = get_performance_metrics()
        >>> print(f"P95 latency: {metrics.p95_time_ms}ms")
    """
    events = _global_tracker.get_recent_events()
    
    if not events:
        return PerformanceMetrics()
    
    times = sorted([e.time_ms for e in events])
    
    def percentile(data: list[int], p: float) -> float:
        """Calculate percentile."""
        if not data:
            return 0.0
        k = (len(data) - 1) * p
        f = int(k)
        c = f + 1
        if c >= len(data):
            return float(data[-1])
        return data[f] + (k - f) * (data[c] - data[f])
    
    return PerformanceMetrics(
        p50_time_ms=percentile(times, 0.50),
        p95_time_ms=percentile(times, 0.95),
        p99_time_ms=percentile(times, 0.99),
        min_time_ms=min(times),
        max_time_ms=max(times),
    )


def print_stats_summary():
    """
    Print a formatted summary of extraction statistics.
    
    This is useful for debugging and monitoring.
    
    Examples:
        >>> print_stats_summary()
        Extraction Statistics:
        =====================
        Total Extractions: 100
        Success Rate: 95.00%
        ...
    """
    stats = get_global_stats()
    perf = get_performance_metrics()
    
    print("Extraction Statistics:")
    print("=" * 50)
    print(f"Total Extractions: {stats.total_extractions}")
    print(f"Success Rate: {stats.success_rate:.2%}")
    print(f"Total Emails Extracted: {stats.total_emails_extracted}")
    print(f"Avg Emails/Extraction: {stats.avg_emails_per_extraction:.1f}")
    print(f"Avg Pages/Extraction: {stats.avg_pages_per_extraction:.1f}")
    print(f"\nPerformance:")
    print(f"  Average Time: {stats.avg_time_ms:.1f}ms")
    print(f"  P50 Time: {perf.p50_time_ms:.1f}ms")
    print(f"  P95 Time: {perf.p95_time_ms:.1f}ms")
    print(f"  P99 Time: {perf.p99_time_ms:.1f}ms")
    print(f"  Min Time: {perf.min_time_ms}ms")
    print(f"  Max Time: {perf.max_time_ms}ms")
    print("=" * 50)