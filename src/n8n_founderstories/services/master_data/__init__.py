"""
Master data service for tool-agnostic result aggregation.

This module provides DB-first Master ingestion that:
- Reads from tool DB tables (Hunter, Google Maps, etc.)
- Aggregates results into master_results with idempotent upserts
- Tracks watermarks for incremental ingestion
- Exports to Sheets at job end (optional)
"""

from .runner import run_master_job_db_first
from .models import MasterRow, MasterWatermark, MasterSource
from .repos import MasterResultsRepository, MasterWatermarkRepository, MasterSourceRepository
from .adapters import (
    BaseSourceAdapter,
    HunterIOAdapter,
    GoogleMapsAdapter,
    GoogleSearchAdapter,
    get_available_adapters,
    get_adapter_by_name,
)
# Import from centralized exports module
from ..exports.sheets_exporter import export_master_results

__all__ = [
    # Main runner
    "run_master_job_db_first",
    
    # Models
    "MasterRow",
    "MasterWatermark",
    "MasterSource",
    
    # Repositories
    "MasterResultsRepository",
    "MasterWatermarkRepository",
    "MasterSourceRepository",
    
    # Adapters
    "BaseSourceAdapter",
    "HunterIOAdapter",
    "GoogleMapsAdapter",
    "GoogleSearchAdapter",
    "get_available_adapters",
    "get_adapter_by_name",
    
    # Exporter (from centralized exports module)
    "export_master_results",
]