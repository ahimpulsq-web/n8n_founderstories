"""
Master - Simplified master consolidation module.

Maintains a single master table of deduped leads across multiple sources.
Uses global sheets infrastructure for export.
"""

from .service import sync_from_source, export_to_sheets

__all__ = ["sync_from_source", "export_to_sheets"]