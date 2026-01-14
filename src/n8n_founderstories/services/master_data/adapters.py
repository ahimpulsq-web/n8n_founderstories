"""
Tool adapters for Master data ingestion.

This module provides a plugin architecture for adding new tools to Master.
Each adapter knows how to read from a specific tool's DB table and normalize
the data to the Master schema.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ...core.utils.text import norm
from ..database.connection import get_connection_context, DatabaseConnectionError
from ..database.config import db_config

from .models import MasterRow

logger = logging.getLogger(__name__)


class BaseSourceAdapter(ABC):
    """
    Base adapter interface for source tools.
    
    To add a new tool (e.g., Google Search):
    1. Create a new adapter class inheriting from BaseSourceAdapter
    2. Implement source_tool_name, source_table_name, and normalize_to_master
    3. Optionally override fetch_rows_after_watermark for custom queries
    4. Register the adapter in get_available_adapters()
    """
    
    def __init__(self, dsn: Optional[str] = None):
        """
        Initialize adapter with optional DSN override.
        
        Args:
            dsn: PostgreSQL connection string. If None, uses global config.
        """
        self.dsn = dsn or db_config.postgres_dsn
    
    @property
    @abstractmethod
    def source_tool_name(self) -> str:
        """Return the source tool identifier (e.g., 'HunterIO', 'GoogleMaps')."""
        pass
    
    @property
    @abstractmethod
    def source_table_name(self) -> str:
        """Return the source table name (e.g., 'hunterio_results')."""
        pass
    
    @abstractmethod
    def normalize_to_master(self, source_row: Dict[str, Any]) -> Optional[MasterRow]:
        """
        Normalize a source row to Master schema.
        
        Args:
            source_row: Dictionary with source table columns
            
        Returns:
            MasterRow instance or None if row should be skipped
        """
        pass
    
    def fetch_rows_after_watermark(
        self,
        request_id: str,
        watermark: Optional[datetime],
        limit: int = 500
    ) -> Tuple[bool, Optional[str], List[Dict[str, Any]], Optional[datetime]]:
        """
        Fetch rows from source table after the watermark timestamp.
        
        This base implementation uses created_at for watermarking.
        Subclasses can override to use different watermark columns (e.g., updated_at).
        
        Args:
            request_id: Request identifier
            watermark: Last processed timestamp (None for first run)
            limit: Maximum number of rows to fetch
            
        Returns:
            Tuple of (success, error_message, rows, new_watermark)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", [], None
        
        # Determine watermark column (can be overridden by subclasses)
        watermark_column = getattr(self, 'watermark_column', 'created_at')
        
        # Build query based on watermark
        if watermark:
            select_sql = f"""
            SELECT *
            FROM {self.source_table_name}
            WHERE request_id = %s
              AND {watermark_column} > %s
            ORDER BY {watermark_column} ASC
            LIMIT %s
            """
            params = (request_id, watermark, limit)
        else:
            select_sql = f"""
            SELECT *
            FROM {self.source_table_name}
            WHERE request_id = %s
            ORDER BY {watermark_column} ASC
            LIMIT %s
            """
            params = (request_id, limit)
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, params)
                    
                    # Get column names
                    columns = [desc[0] for desc in cur.description]
                    
                    # Fetch rows and convert to dicts
                    rows = []
                    new_watermark = watermark
                    
                    for row_tuple in cur.fetchall():
                        row_dict = dict(zip(columns, row_tuple))
                        rows.append(row_dict)
                        
                        # Track the latest watermark value
                        if watermark_column in row_dict and row_dict[watermark_column]:
                            if new_watermark is None or row_dict[watermark_column] > new_watermark:
                                new_watermark = row_dict[watermark_column]
                    
                    logger.debug(
                        "Fetched %d rows from %s (request_id=%s, watermark_col=%s, watermark=%s)",
                        len(rows),
                        self.source_table_name,
                        request_id,
                        watermark_column,
                        watermark.isoformat() if watermark else "None"
                    )
                    
                    return True, None, rows, new_watermark
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, [], None
        except Exception as e:
            error_msg = f"Failed to fetch rows from {self.source_table_name}: {e}"
            logger.error(error_msg)
            return False, error_msg, [], None
    
    def table_exists(self) -> bool:
        """
        Check if the source table exists in the database.
        
        Returns:
            True if table exists, False otherwise
        """
        if not self.dsn:
            return False
        
        check_sql = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = %s
        )
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(check_sql, (self.source_table_name,))
                    exists = cur.fetchone()[0]
                    return bool(exists)
        except Exception as e:
            logger.warning(
                "Failed to check if table %s exists: %s",
                self.source_table_name,
                e
            )
            return False


class HunterIOAdapter(BaseSourceAdapter):
    """
    Adapter for Hunter.io results.
    
    Reads from hunterio_results table and normalizes to Master schema.
    """
    
    @property
    def source_tool_name(self) -> str:
        return "HunterIO"
    
    @property
    def source_table_name(self) -> str:
        return "hunterio_results"
    
    def normalize_to_master(self, source_row: Dict[str, Any]) -> Optional[MasterRow]:
        """
        Normalize Hunter.io row to Master schema.
        
        Hunter.io schema:
        - organisation: company name
        - domain: primary domain
        - location: location string
        - search_query: search query used
        """
        domain = norm(source_row.get("domain"))
        if not domain:
            return None  # Skip rows without domain
        
        return MasterRow(
            job_id=norm(source_row.get("job_id", "")),
            request_id=norm(source_row.get("request_id", "")),
            domain=domain,
            company=norm(source_row.get("organisation")) or None,
            website=domain,  # Hunter uses domain as website
            source_tool=self.source_tool_name,
            location=norm(source_row.get("location")) or None,
            lead_query=norm(source_row.get("search_query")) or None,
            dup_in_run=None,  # Will be computed during ingestion
            source_ref=source_row.get("id"),  # Store source ID for traceability
        )


class GoogleMapsAdapter(BaseSourceAdapter):
    """
    Adapter for Google Maps results.
    
    Reads from google_maps_results table and normalizes to Master schema.
    Uses updated_at for watermarking to detect enrichment changes.
    """
    
    # Use updated_at for watermarking to capture enrichment updates
    watermark_column = 'updated_at'
    
    @property
    def source_tool_name(self) -> str:
        return "GoogleMaps"
    
    @property
    def source_table_name(self) -> str:
        return "google_maps_results"
    
    def normalize_to_master(self, source_row: Dict[str, Any]) -> Optional[MasterRow]:
        """
        Normalize Google Maps row to Master schema.
        
        Google Maps schema:
        - organisation: place name
        - domain: extracted domain (may be None initially, filled during enrich)
        - website: website URL (may be None initially, filled during enrich)
        - location_label: user-friendly location
        - search_query: search query used
        - place_id: unique Google Maps identifier
        
        IMPORTANT: This adapter now includes rows WITHOUT domains.
        For no-domain rows, we use place_id as the entity key.
        Domain can be empty/None - this is valid for discover-stage rows.
        """
        # Get organisation and place_id
        organisation = norm(source_row.get("organisation"))
        place_id = norm(source_row.get("place_id"))
        
        # Skip only if BOTH organisation and place_id are missing
        if not organisation and not place_id:
            return None
        
        # Get domain/website (can be empty for discover-stage rows)
        domain = norm(source_row.get("domain"))
        website = norm(source_row.get("website"))
        
        # Use domain if available, otherwise use empty string
        # Empty domain is valid - it means row hasn't been enriched yet
        final_domain = domain or ""
        
        return MasterRow(
            job_id=norm(source_row.get("job_id", "")),
            request_id=norm(source_row.get("request_id", "")),
            domain=final_domain,
            company=organisation or None,
            website=website or None,
            source_tool=self.source_tool_name,
            location=norm(source_row.get("location_label")) or None,
            lead_query=norm(source_row.get("search_query")) or None,
            dup_in_run=None,  # Will be computed during ingestion
            source_ref=place_id,  # Store place_id for traceability and uniqueness
        )


class GoogleSearchAdapter(BaseSourceAdapter):
    """
    Adapter for Google Search results (future implementation).
    
    Placeholder for when Google Search tool is added.
    """
    
    @property
    def source_tool_name(self) -> str:
        return "GoogleSearch"
    
    @property
    def source_table_name(self) -> str:
        return "google_search_results"
    
    def normalize_to_master(self, source_row: Dict[str, Any]) -> Optional[MasterRow]:
        """
        Normalize Google Search row to Master schema.
        
        TODO: Implement when Google Search table schema is defined.
        """
        domain = norm(source_row.get("domain"))
        if not domain:
            return None
        
        return MasterRow(
            job_id=norm(source_row.get("job_id", "")),
            request_id=norm(source_row.get("request_id", "")),
            domain=domain,
            company=norm(source_row.get("organisation")) or None,
            website=norm(source_row.get("website")) or None,
            source_tool=self.source_tool_name,
            location=None,  # Google Search doesn't have location
            lead_query=norm(source_row.get("search_query")) or None,
            dup_in_run=None,
            source_ref=source_row.get("id"),
        )


def get_available_adapters(dsn: Optional[str] = None) -> List[BaseSourceAdapter]:
    """
    Get list of all available source adapters.
    
    To add a new tool:
    1. Create a new adapter class above
    2. Add it to this list
    
    Args:
        dsn: Optional PostgreSQL connection string
        
    Returns:
        List of adapter instances
    """
    return [
        HunterIOAdapter(dsn=dsn),
        GoogleMapsAdapter(dsn=dsn),
        # GoogleSearchAdapter(dsn=dsn),  # Uncomment when ready
    ]


def get_adapter_by_name(
    source_tool: str,
    dsn: Optional[str] = None
) -> Optional[BaseSourceAdapter]:
    """
    Get adapter by source tool name.
    
    Args:
        source_tool: Source tool identifier (e.g., 'HunterIO')
        dsn: Optional PostgreSQL connection string
        
    Returns:
        Adapter instance or None if not found
    """
    for adapter in get_available_adapters(dsn=dsn):
        if adapter.source_tool_name == source_tool:
            return adapter
    return None