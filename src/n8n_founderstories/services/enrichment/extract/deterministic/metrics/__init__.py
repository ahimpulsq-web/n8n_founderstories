"""
Metrics and telemetry for deterministic email extraction.

This package provides performance tracking, statistics collection,
and monitoring capabilities for the extraction system.

Modules:
    telemetry: Performance tracking and metrics collection
"""

from .telemetry import (
    ExtractionStats,
    track_extraction,
    get_global_stats,
    reset_global_stats,
)

__all__ = [
    "ExtractionStats",
    "track_extraction",
    "get_global_stats",
    "reset_global_stats",
]