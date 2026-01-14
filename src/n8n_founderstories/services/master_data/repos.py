"""
Master data PostgreSQL repositories.

This module provides safe database operations for Master results aggregation
with proper error handling and transaction management.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import psycopg

from ...core.utils.text import norm
from ...core.utils.domain import normalize_domain
from ..database.connection import get_connection_context, DatabaseConnectionError
from ..database.config import db_config

from .models import MasterRow, MasterWatermark, MasterSource

logger = logging.getLogger(__name__)


class PermanentError(Exception):
    """Permanent error that should not be retried (schema, constraint, config errors)."""
    pass


class MasterResultsRepository:
    """
    Repository for master_results database operations.
    
    Provides idempotent upsert operations for unified results from all tools.
    """
    
    def __init__(self, dsn: Optional[str] = None):
        """
        Initialize repository with optional DSN override.
        
        Args:
            dsn: PostgreSQL connection string. If None, uses global config.
        """
        self.dsn = dsn or db_config.postgres_dsn
        self._schema_validated = False
    
    def _validate_schema(self) -> None:
        """
        Validate that required schema elements exist.
        
        Raises:
            PermanentError: If schema is missing required elements
        """
        if self._schema_validated:
            return
        
        if not self.dsn:
            raise PermanentError("No PostgreSQL DSN configured")
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    # Check entity_key column exists (new schema)
                    cur.execute("""
                        SELECT COUNT(*) FROM information_schema.columns
                        WHERE table_name = 'master_results'
                          AND column_name = 'entity_key'
                    """)
                    has_entity_key = cur.fetchone()[0] > 0
                    
                    if has_entity_key:
                        # New schema: check entity_key constraint (with source_tool)
                        cur.execute("""
                            SELECT COUNT(*) FROM pg_constraint
                            WHERE conname = 'uq_master_results_request_tool_entity'
                              AND conrelid = 'master_results'::regclass
                        """)
                        if cur.fetchone()[0] == 0:
                            # Check for old constraint name
                            cur.execute("""
                                SELECT COUNT(*) FROM pg_constraint
                                WHERE conname = 'uq_master_results_request_entity_key'
                                  AND conrelid = 'master_results'::regclass
                            """)
                            if cur.fetchone()[0] > 0:
                                raise PermanentError(
                                    "Schema error: old constraint 'uq_master_results_request_entity_key' exists. "
                                    "Please run migration: 0003_fix_upsert_with_source_tool.sql to upgrade to new constraint"
                                )
                            else:
                                raise PermanentError(
                                    "Schema error: constraint 'uq_master_results_request_tool_entity' does not exist. "
                                    "Please run migrations: 0002_support_no_domain_rows.sql and 0003_fix_upsert_with_source_tool.sql"
                                )
                    else:
                        # Old schema: check domain_norm constraint
                        cur.execute("""
                            SELECT COUNT(*) FROM information_schema.columns
                            WHERE table_name = 'master_results'
                              AND column_name = 'domain_norm'
                        """)
                        if cur.fetchone()[0] == 0:
                            raise PermanentError(
                                "Schema error: column 'domain_norm' does not exist in master_results. "
                                "Please run migration: 0001_master_schema.sql"
                            )
                        
                        cur.execute("""
                            SELECT COUNT(*) FROM pg_constraint
                            WHERE conname = 'uq_master_results_request_domain_norm'
                              AND conrelid = 'master_results'::regclass
                        """)
                        if cur.fetchone()[0] == 0:
                            raise PermanentError(
                                "Schema error: constraint 'uq_master_results_request_domain_norm' does not exist. "
                                "Please run migration: 0001_master_schema.sql"
                            )
            
            self._schema_validated = True
            logger.debug("MASTER_SCHEMA_VALIDATED | domain_norm_exists=true | constraint_exists=true")
            
        except PermanentError:
            raise
        except Exception as e:
            raise PermanentError(f"Schema validation failed: {e}") from e
    
    def upsert_many(self, rows: List[MasterRow]) -> Tuple[bool, Optional[str], int]:
        """
        Upsert multiple master result rows with domain normalization.
        
        Uses ON CONFLICT to update existing rows or insert new ones.
        Normalizes domains before insert for consistent deduplication.
        
        Args:
            rows: List of MasterRow instances to upsert
            
        Returns:
            Tuple of (success, error_message, affected_count)
            
        Raises:
            PermanentError: For schema/constraint errors that should not be retried
        """
        if not rows:
            return True, None, 0
        
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", 0
        
        # Validate schema before attempting upsert
        self._validate_schema()
        
        # Normalize domains (allow empty domains for Google Maps discover stage)
        valid_rows = []
        for row in rows:
            # Normalize domain if present, otherwise use empty string
            if row.domain and row.domain.strip():
                domain_norm = normalize_domain(row.domain)
                if not domain_norm:
                    logger.warning(
                        "Skipping row with invalid domain: %s",
                        row.domain
                    )
                    continue
                row.domain_norm = domain_norm
            else:
                # Empty domain is valid (for Google Maps discover stage)
                row.domain_norm = ""
            
            valid_rows.append(row)
        
        if not valid_rows:
            logger.warning("All rows filtered out")
            return True, None, 0
        
        # Insert both raw domain and normalized domain
        # Note: entity_key is auto-computed by trigger, so we don't insert it
        upsert_sql = """
        INSERT INTO master_results (
            id, job_id, request_id, domain, domain_norm, company, website,
            source_tool, source_ref, location, lead_query, dup_in_run
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ON CONSTRAINT uq_master_results_request_tool_entity
        DO UPDATE SET
            domain = COALESCE(EXCLUDED.domain, master_results.domain),
            domain_norm = COALESCE(EXCLUDED.domain_norm, master_results.domain_norm),
            company = COALESCE(EXCLUDED.company, master_results.company),
            website = COALESCE(EXCLUDED.website, master_results.website),
            source_ref = COALESCE(EXCLUDED.source_ref, master_results.source_ref),
            location = COALESCE(EXCLUDED.location, master_results.location),
            lead_query = COALESCE(EXCLUDED.lead_query, master_results.lead_query),
            dup_in_run = EXCLUDED.dup_in_run,
            updated_at = NOW()
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    # Prepare batch data
                    batch_data = []
                    for row in valid_rows:
                        batch_data.append((
                            str(uuid.uuid4()),
                            norm(row.job_id),
                            norm(row.request_id),
                            norm(row.domain),      # Raw domain
                            row.domain_norm,        # Normalized domain
                            norm(row.company) or None,
                            norm(row.website) or None,
                            norm(row.source_tool),
                            norm(row.source_ref) or None,
                            norm(row.location) or None,
                            norm(row.lead_query) or None,
                            norm(row.dup_in_run) or None,
                        ))
                    
                    # Execute batch upsert
                    cur.executemany(upsert_sql, batch_data)
                    
                    # Get count of rows affected (inserts + updates)
                    affected_count = cur.rowcount
                    
                    conn.commit()
                    
                    # Log with details about inserts vs updates
                    # Note: PostgreSQL doesn't distinguish between INSERT and UPDATE in rowcount
                    # but we can infer: if rowcount < batch_size, some were updates
                    inserted_count = affected_count  # Approximate
                    updated_count = 0  # Would need separate query to determine exactly
                    
                    logger.debug(
                        "MASTER_UPSERT_SUCCESS | rows=%d | affected=%d | tool_breakdown=%s",
                        len(valid_rows),
                        affected_count,
                        {row.source_tool: 1 for row in valid_rows}  # Count by tool
                    )
                    
                    return True, None, affected_count
                    
        except psycopg.errors.UndefinedObject as e:
            # Constraint or column doesn't exist - permanent error
            error_msg = f"Schema error - constraint/column missing: {e}"
            logger.error("MASTER_UPSERT_SCHEMA_ERROR | error=%s", error_msg)
            raise PermanentError(error_msg) from e
        except psycopg.errors.InvalidColumnReference as e:
            # Column doesn't exist - permanent error
            error_msg = f"Schema error - invalid column: {e}"
            logger.error("MASTER_UPSERT_SCHEMA_ERROR | error=%s", error_msg)
            raise PermanentError(error_msg) from e
        except psycopg.errors.NotNullViolation as e:
            # NOT NULL constraint - permanent error
            error_msg = f"Schema error - NOT NULL violation: {e}"
            logger.error("MASTER_UPSERT_SCHEMA_ERROR | error=%s", error_msg)
            raise PermanentError(error_msg) from e
        except DatabaseConnectionError as e:
            # Transient network error
            error_msg = f"Database connection failed: {e}"
            logger.warning("MASTER_UPSERT_CONNECTION_ERROR | error=%s", error_msg)
            return False, error_msg, 0
        except Exception as e:
            error_msg = f"Failed to upsert master results: {e}"
            logger.error("MASTER_UPSERT_ERROR | error=%s", error_msg, exc_info=True)
            # Check if it's a schema error by message content
            if any(keyword in str(e).lower() for keyword in ['constraint', 'column', 'does not exist', 'not null']):
                raise PermanentError(error_msg) from e
            return False, error_msg, 0
    
    def get_results_by_request(
        self,
        request_id: str,
        source_tool: Optional[str] = None
    ) -> Tuple[bool, Optional[str], List[Dict[str, Any]]]:
        """
        Retrieve master results for a specific request ID.
        
        Args:
            request_id: Request identifier
            source_tool: Optional filter by source tool
            
        Returns:
            Tuple of (success, error_message, results_list)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", []
        
        if source_tool:
            select_sql = """
            SELECT id, created_at, updated_at, job_id, request_id, domain,
                   company, website, source_tool, source_ref, location,
                   lead_query, dup_in_run
            FROM master_results
            WHERE request_id = %s AND source_tool = %s
            ORDER BY updated_at DESC, domain
            """
            params = (request_id, source_tool)
        else:
            select_sql = """
            SELECT id, created_at, updated_at, job_id, request_id, domain,
                   company, website, source_tool, source_ref, location,
                   lead_query, dup_in_run
            FROM master_results
            WHERE request_id = %s
            ORDER BY updated_at DESC, domain
            """
            params = (request_id,)
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, params)
                    rows = cur.fetchall()
                    
                    results = []
                    for row in rows:
                        results.append({
                            'id': str(row[0]),
                            'created_at': row[1].isoformat() if row[1] else None,
                            'updated_at': row[2].isoformat() if row[2] else None,
                            'job_id': row[3],
                            'request_id': row[4],
                            'domain': row[5],
                            'company': row[6],
                            'website': row[7],
                            'source_tool': row[8],
                            'source_ref': row[9],
                            'location': row[10],
                            'lead_query': row[11],
                            'dup_in_run': row[12],
                        })
                    
                    return True, None, results
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, []
        except Exception as e:
            error_msg = f"Failed to retrieve master results: {e}"
            logger.error(error_msg)
            return False, error_msg, []


class MasterWatermarkRepository:
    """
    Repository for master_watermarks database operations.
    
    Tracks last processed timestamp for each source tool to enable
    incremental ingestion.
    """
    
    def __init__(self, dsn: Optional[str] = None):
        """
        Initialize repository with optional DSN override.
        
        Args:
            dsn: PostgreSQL connection string. If None, uses global config.
        """
        self.dsn = dsn or db_config.postgres_dsn
    
    def get_watermark(
        self,
        request_id: str,
        source_tool: str
    ) -> Tuple[bool, Optional[str], Optional[MasterWatermark]]:
        """
        Get watermark for a specific request and source tool.
        
        Args:
            request_id: Request identifier
            source_tool: Source tool identifier
            
        Returns:
            Tuple of (success, error_message, watermark)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", None
        
        select_sql = """
        SELECT request_id, source_tool, last_seen_created_at,
               last_processed_count, total_processed
        FROM master_watermarks
        WHERE request_id = %s AND source_tool = %s
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, (request_id, source_tool))
                    row = cur.fetchone()
                    
                    if not row:
                        return True, None, None
                    
                    watermark = MasterWatermark(
                        request_id=row[0],
                        source_tool=row[1],
                        last_seen_created_at=row[2],
                        last_processed_count=row[3] or 0,
                        total_processed=row[4] or 0,
                    )
                    
                    return True, None, watermark
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, None
        except Exception as e:
            error_msg = f"Failed to get watermark: {e}"
            logger.error(error_msg)
            return False, error_msg, None
    
    def get_watermarks_for_request(
        self,
        request_id: str
    ) -> Tuple[bool, Optional[str], Dict[str, Optional[datetime]]]:
        """
        Get all watermarks for a specific request ID.
        
        This is used for delta checking before reruns to determine if any
        adapter has new data beyond its watermark.
        
        Args:
            request_id: Request identifier
            
        Returns:
            Tuple of (success, error_message, watermarks_dict)
            where watermarks_dict maps tool_name -> last_seen_created_at
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", {}
        
        select_sql = """
        SELECT source_tool, last_seen_created_at
        FROM master_watermarks
        WHERE request_id = %s
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, (request_id,))
                    rows = cur.fetchall()
                    
                    watermarks = {}
                    for row in rows:
                        tool_name = row[0]
                        watermark = row[1]  # Can be None
                        watermarks[tool_name] = watermark
                    
                    logger.debug(
                        "Retrieved %d watermarks for request_id=%s",
                        len(watermarks),
                        request_id
                    )
                    
                    return True, None, watermarks
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, {}
        except Exception as e:
            error_msg = f"Failed to get watermarks for request: {e}"
            logger.error(error_msg)
            return False, error_msg, {}
    
    def set_watermark(
        self,
        request_id: str,
        source_tool: str,
        last_seen_created_at: Optional[datetime],
        last_processed_count: int = 0
    ) -> Tuple[bool, Optional[str]]:
        """
        Set or update watermark for a specific request and source tool.
        
        Args:
            request_id: Request identifier
            source_tool: Source tool identifier
            last_seen_created_at: Timestamp of last processed record
            last_processed_count: Number of records processed in this batch
            
        Returns:
            Tuple of (success, error_message)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured"
        
        upsert_sql = """
        INSERT INTO master_watermarks (
            id, request_id, source_tool, last_seen_created_at,
            last_processed_count, total_processed
        ) VALUES (
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ON CONSTRAINT uq_master_watermarks_request_tool
        DO UPDATE SET
            last_seen_created_at = EXCLUDED.last_seen_created_at,
            last_processed_count = EXCLUDED.last_processed_count,
            total_processed = master_watermarks.total_processed + EXCLUDED.last_processed_count,
            updated_at = NOW()
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        upsert_sql,
                        (
                            str(uuid.uuid4()),
                            norm(request_id),
                            norm(source_tool),
                            last_seen_created_at,
                            last_processed_count,
                            last_processed_count,  # Initial total_processed
                        )
                    )
                    conn.commit()
                    
                    logger.debug(
                        "Updated watermark for request_id=%s source_tool=%s",
                        request_id,
                        source_tool
                    )
                    return True, None
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Failed to set watermark: {e}"
            logger.error(error_msg)
            return False, error_msg


class MasterSourceRepository:
    """
    Repository for master_sources database operations.
    
    Manages registry of known source tools and their configurations.
    """
    
    def __init__(self, dsn: Optional[str] = None):
        """
        Initialize repository with optional DSN override.
        
        Args:
            dsn: PostgreSQL connection string. If None, uses global config.
        """
        self.dsn = dsn or db_config.postgres_dsn
    
    def get_active_sources(self) -> Tuple[bool, Optional[str], List[MasterSource]]:
        """
        Get all active source tool configurations.
        
        Returns:
            Tuple of (success, error_message, sources_list)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", []
        
        select_sql = """
        SELECT source_tool, display_name, source_table, is_active,
               column_mapping, description
        FROM master_sources
        WHERE is_active = true
        ORDER BY source_tool
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql)
                    rows = cur.fetchall()
                    
                    sources = []
                    for row in rows:
                        sources.append(MasterSource(
                            source_tool=row[0],
                            display_name=row[1],
                            source_table=row[2],
                            is_active=row[3],
                            column_mapping=row[4],  # Already parsed as dict by psycopg
                            description=row[5],
                        ))
                    
                    return True, None, sources
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, []
        except Exception as e:
            error_msg = f"Failed to get active sources: {e}"
            logger.error(error_msg)
            return False, error_msg, []
    
    def get_source(self, source_tool: str) -> Tuple[bool, Optional[str], Optional[MasterSource]]:
        """
        Get configuration for a specific source tool.
        
        Args:
            source_tool: Source tool identifier
            
        Returns:
            Tuple of (success, error_message, source)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", None
        
        select_sql = """
        SELECT source_tool, display_name, source_table, is_active,
               column_mapping, description
        FROM master_sources
        WHERE source_tool = %s
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, (source_tool,))
                    row = cur.fetchone()
                    
                    if not row:
                        return True, None, None
                    
                    source = MasterSource(
                        source_tool=row[0],
                        display_name=row[1],
                        source_table=row[2],
                        is_active=row[3],
                        column_mapping=row[4],  # Already parsed as dict by psycopg
                        description=row[5],
                    )
                    
                    return True, None, source
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, None
        except Exception as e:
            error_msg = f"Failed to get source: {e}"
            logger.error(error_msg)
            return False, error_msg, None