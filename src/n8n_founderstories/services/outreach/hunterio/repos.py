"""
Hunter PostgreSQL repositories.

This module provides safe database operations for Hunter.io company and audit data
with proper error handling and transaction management.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ....core.utils.text import norm
from ...database.connection import get_connection_context, DatabaseConnectionError
from ...database.config import db_config
from ....core.config import settings

logger = logging.getLogger(__name__)


@dataclass
class HunterIOResultRow:
    """
    Data structure for Hunter.io results database rows.
    
    This matches the structure expected by the PostgreSQL hunterio_results table.
    """
    job_id: str
    request_id: str
    organisation: Optional[str] = None
    domain: Optional[str] = None
    location: Optional[str] = None
    headcount: Optional[str] = None
    search_query: Optional[str] = None
    debug_filters: Optional[str] = None
    
    @classmethod
    def from_runner_data(
        cls,
        job_id: str,
        request_id: str,
        organisation: Optional[str] = None,
        domain: Optional[str] = None,
        location: Optional[str] = None,
        headcount: Optional[str] = None,
        search_query: Optional[str] = None,
        debug_filters: Optional[str] = None,
    ) -> "HunterIOResultRow":
        """
        Create HunterIOResultRow from runner data.
        
        Args:
            job_id: Job identifier
            request_id: Request identifier
            organisation: Company organization name
            domain: Company domain
            location: Applied location label
            headcount: Applied headcount bucket
            search_query: Search query in format "<keyword> | <query_type>"
            debug_filters: Debug filters in format "<intended_location> | <intended_headcount>"
            
        Returns:
            HunterIOResultRow instance with normalized data
        """
        return cls(
            job_id=norm(job_id),
            request_id=norm(request_id),
            organisation=norm(organisation) or None,
            domain=norm(domain) or None,
            location=norm(location) or None,
            headcount=norm(headcount) or None,
            search_query=norm(search_query) or None,
            debug_filters=norm(debug_filters) or None,
        )


@dataclass
class HunterAuditRow:
    """
    Data structure for Hunter audit database rows.
    
    This matches the structure expected by the PostgreSQL hunter_audit table
    and the Google Sheets audit columns.
    """
    job_id: str
    request_id: str
    query_type: Optional[str] = None
    intended_location: Optional[str] = None
    intended_headcount: Optional[str] = None
    applied_location: Optional[str] = None
    applied_headcount: Optional[str] = None
    query_text: Optional[str] = None
    keywords: Optional[str] = None
    keyword_match: Optional[str] = None
    total_results: Optional[int] = None
    returned_count: Optional[int] = None
    appended_rows: Optional[int] = None
    applied_filters: Optional[Dict[str, Any]] = None
    
    @classmethod
    def from_sheets_row(
        cls,
        job_id: str,
        request_id: str,
        sheets_row: List[str]
    ) -> "HunterAuditRow":
        """
        Create HunterAuditRow from Google Sheets audit row data.
        
        Expected sheets_row format (matching HUNTER_HEADERS_AUDIT):
        [Job ID, Request ID, Query Type, Intended Location, Intended Headcount,
         Applied Location, Applied Headcount, Query Text, Keywords, Keyword Match,
         Total Results, Returned Count, Appended Rows, Applied Filters (JSON)]
        
        Args:
            job_id: Job identifier
            request_id: Request identifier
            sheets_row: List of strings from Google Sheets audit tab
            
        Returns:
            HunterAuditRow instance with normalized data
        """
        # Pad row to ensure we have enough elements
        padded_row = sheets_row + [""] * (14 - len(sheets_row))
        
        # Parse numeric fields safely
        def safe_int(value: str) -> Optional[int]:
            try:
                return int(value) if value and value.strip() else None
            except (ValueError, TypeError):
                return None
        
        # Parse JSON field safely
        def safe_json(value: str) -> Optional[Dict[str, Any]]:
            try:
                return json.loads(value) if value and value.strip() else None
            except (json.JSONDecodeError, TypeError):
                return None
        
        return cls(
            job_id=norm(job_id),
            request_id=norm(request_id),
            query_type=norm(padded_row[2]) or None,
            intended_location=norm(padded_row[3]) or None,
            intended_headcount=norm(padded_row[4]) or None,
            applied_location=norm(padded_row[5]) or None,
            applied_headcount=norm(padded_row[6]) or None,
            query_text=norm(padded_row[7]) or None,
            keywords=norm(padded_row[8]) or None,
            keyword_match=norm(padded_row[9]) or None,
            total_results=safe_int(padded_row[10]),
            returned_count=safe_int(padded_row[11]),
            appended_rows=safe_int(padded_row[12]),
            applied_filters=safe_json(padded_row[13]),
        )


class HunterIOResultsRepository:
    """
    Repository for Hunter.io results database operations.
    
    Provides safe database operations with proper error handling
    and transaction management using psycopg v3.
    """
    
    def __init__(self, dsn: Optional[str] = None):
        """
        Initialize repository with optional DSN override.
        
        Args:
            dsn: PostgreSQL connection string. If None, uses global config.
        """
        self.dsn = dsn or db_config.postgres_dsn
    
    def insert_many(self, rows: List[HunterIOResultRow]) -> tuple[bool, Optional[str], int]:
        """
        Insert multiple Hunter.io result rows in a single transaction.
        
        Args:
            rows: List of HunterIOResultRow instances to insert
            
        Returns:
            Tuple of (success, error_message, inserted_count)
        """
        if not rows:
            return True, None, 0
        
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", 0
        
        insert_sql = """
        INSERT INTO hunterio_results (
            id, job_id, request_id, organisation, domain,
            location, headcount, search_query, debug_filters
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (LOWER(domain), job_id, request_id) DO NOTHING
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    # Prepare batch data
                    batch_data = []
                    for row in rows:
                        batch_data.append((
                            str(uuid.uuid4()),  # Generate UUID for each row
                            row.job_id,
                            row.request_id,
                            row.organisation,
                            row.domain,
                            row.location,
                            row.headcount,
                            row.search_query,
                            row.debug_filters,
                        ))
                    
                    # Execute batch insert
                    cur.executemany(insert_sql, batch_data)
                    conn.commit()
                    
                    inserted_count = len(batch_data)
                    logger.info(
                        "Successfully inserted %d Hunter.io results to PostgreSQL",
                        inserted_count
                    )
                    return True, None, inserted_count
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, 0
        except Exception as e:
            error_msg = f"Failed to insert Hunter.io results: {e}"
            logger.error(error_msg)
            return False, error_msg, 0
    
    def get_companies_by_job(self, job_id: str) -> tuple[bool, Optional[str], List[dict]]:
        """
        Retrieve Hunter.io results for a specific job ID.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Tuple of (success, error_message, results_list)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", []
        
        select_sql = """
        SELECT id, created_at, job_id, request_id, organisation, domain,
               location, headcount, search_query, debug_filters
        FROM hunterio_results
        WHERE job_id = %s
        ORDER BY created_at
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, (job_id,))
                    rows = cur.fetchall()
                    
                    results = []
                    for row in rows:
                        results.append({
                            'id': str(row[0]),
                            'created_at': row[1].isoformat() if row[1] else None,
                            'job_id': row[2],
                            'request_id': row[3],
                            'organisation': row[4],
                            'domain': row[5],
                            'location': row[6],
                            'headcount': row[7],
                            'search_query': row[8],
                            'debug_filters': row[9],
                        })
                    
                    return True, None, results
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, []
        except Exception as e:
            error_msg = f"Failed to retrieve Hunter.io results: {e}"
            logger.error(error_msg)
            return False, error_msg, []


class HunterAuditRepository:
    """
    Repository for Hunter audit database operations.
    
    Provides safe database operations with proper error handling
    and transaction management using psycopg v3.
    """
    
    def __init__(self, dsn: Optional[str] = None):
        """
        Initialize repository with optional DSN override.
        
        Args:
            dsn: PostgreSQL connection string. If None, uses global config.
        """
        self.dsn = dsn or db_config.postgres_dsn
    
    def insert_many(self, rows: List[HunterAuditRow]) -> tuple[bool, Optional[str], int]:
        """
        Insert multiple Hunter audit rows in a single transaction.
        
        Args:
            rows: List of HunterAuditRow instances to insert
            
        Returns:
            Tuple of (success, error_message, inserted_count)
        """
        if not rows:
            return True, None, 0
        
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", 0
        
        insert_sql = """
        INSERT INTO hunter_audit (
            id, job_id, request_id, query_type, intended_location, intended_headcount,
            applied_location, applied_headcount, query_text, keywords, keyword_match,
            total_results, returned_count, appended_rows, applied_filters
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (job_id, request_id, query_type, intended_location, intended_headcount, query_text, keywords, keyword_match) DO NOTHING
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    # Prepare batch data
                    batch_data = []
                    for row in rows:
                        batch_data.append((
                            str(uuid.uuid4()),  # Generate UUID for each row
                            row.job_id,
                            row.request_id,
                            row.query_type,
                            row.intended_location,
                            row.intended_headcount,
                            row.applied_location,
                            row.applied_headcount,
                            row.query_text,
                            row.keywords,
                            row.keyword_match,
                            row.total_results,
                            row.returned_count,
                            row.appended_rows,
                            json.dumps(row.applied_filters) if row.applied_filters else None,
                        ))
                    
                    # Execute batch insert
                    cur.executemany(insert_sql, batch_data)
                    conn.commit()
                    
                    inserted_count = len(batch_data)
                    logger.info(
                        "Successfully inserted %d Hunter audit records to PostgreSQL",
                        inserted_count
                    )
                    return True, None, inserted_count
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, 0
        except Exception as e:
            error_msg = f"Failed to insert Hunter audit records: {e}"
            logger.error(error_msg)
            return False, error_msg, 0
    
    def get_audit_by_job(self, job_id: str) -> tuple[bool, Optional[str], List[dict]]:
        """
        Retrieve audit records for a specific job ID.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Tuple of (success, error_message, audit_list)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", []
        
        select_sql = """
        SELECT id, created_at, job_id, request_id, query_type, intended_location, intended_headcount,
               applied_location, applied_headcount, query_text, keywords, keyword_match,
               total_results, returned_count, appended_rows, applied_filters
        FROM hunter_audit
        WHERE job_id = %s
        ORDER BY created_at
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, (job_id,))
                    rows = cur.fetchall()
                    
                    audit_records = []
                    for row in rows:
                        audit_records.append({
                            'id': str(row[0]),
                            'created_at': row[1].isoformat() if row[1] else None,
                            'job_id': row[2],
                            'request_id': row[3],
                            'query_type': row[4],
                            'intended_location': row[5],
                            'intended_headcount': row[6],
                            'applied_location': row[7],
                            'applied_headcount': row[8],
                            'query_text': row[9],
                            'keywords': row[10],
                            'keyword_match': row[11],
                            'total_results': row[12],
                            'returned_count': row[13],
                            'appended_rows': row[14],
                            'applied_filters': row[15],  # Already parsed as dict by psycopg
                        })
                    
                    return True, None, audit_records
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, []
        except Exception as e:
            error_msg = f"Failed to retrieve Hunter audit records: {e}"
            logger.error(error_msg)
            return False, error_msg, []


def safe_insert_hunterio_results(
    job_id: str,
    request_id: str,
    result_rows: List[HunterIOResultRow]
) -> None:
    """
    Safe wrapper for inserting Hunter.io results to PostgreSQL.
    
    DEPRECATED: Use the shared bulk_writer.safe_bulk_insert instead.
    This function is kept for backward compatibility.
    """
    from ...database.utils.bulk_writer import safe_bulk_insert
    
    repo = HunterIOResultsRepository()
    safe_bulk_insert(
        repo=repo,
        rows=result_rows,
        job_id=job_id,
        request_id=request_id,
        feature_enabled=db_config.is_hunter_companies_enabled,
        log_prefix="Hunter.io results",
    )


def safe_insert_hunter_audit(
    job_id: str,
    request_id: str,
    sheets_rows: List[List[str]]
) -> None:
    """
    Safe wrapper for inserting Hunter audit records to PostgreSQL.
    
    DEPRECATED: Use the shared bulk_writer.safe_bulk_insert_with_converter instead.
    This function is kept for backward compatibility.
    """
    from ...database.utils.bulk_writer import safe_bulk_insert_with_converter
    
    def converter(sheets_row: List[str]) -> HunterAuditRow:
        return HunterAuditRow.from_sheets_row(job_id, request_id, sheets_row)
    
    repo = HunterAuditRepository()
    safe_bulk_insert_with_converter(
        repo=repo,
        raw_data=sheets_rows,
        converter=converter,
        job_id=job_id,
        request_id=request_id,
        feature_enabled=db_config.is_hunter_audit_enabled,
        log_prefix="Hunter audit",
    )


class HunterIOBatchProcessor:
    """
    Batch processor for Hunter.io results with configurable buffer sizes.
    
    This class buffers results and audit records in memory and flushes them
    to the database when buffer limits are reached or when explicitly flushed.
    """
    
    def __init__(self, job_id: str, request_id: str):
        """
        Initialize batch processor.
        
        Args:
            job_id: Job identifier
            request_id: Request identifier
        """
        self.job_id = job_id
        self.request_id = request_id
        
        # Buffers
        self._results_buffer: List[HunterIOResultRow] = []
        self._audit_buffer: List[HunterAuditRow] = []
        
        # Repositories
        self._results_repo = HunterIOResultsRepository()
        self._audit_repo = HunterAuditRepository()
        
        # Configuration
        self._results_batch_size = settings.hunter_db_batch_size_results
        self._audit_batch_size = settings.hunter_db_batch_size_audit
        
        logger.info(
            "Initialized Hunter.io batch processor (job_id=%s, request_id=%s, "
            "results_batch_size=%d, audit_batch_size=%d)",
            job_id, request_id, self._results_batch_size, self._audit_batch_size
        )
    
    def add_results(self, results: List[HunterIOResultRow]) -> None:
        """
        Add results to the buffer and flush if batch size is reached.
        
        Args:
            results: List of HunterIOResultRow instances to add
        """
        if not results:
            return
        
        self._results_buffer.extend(results)
        
        if len(self._results_buffer) >= self._results_batch_size:
            self._flush_results()
    
    def add_audit_records(self, audit_records: List[List[str]]) -> None:
        """
        Add audit records to the buffer and flush if batch size is reached.
        
        Args:
            audit_records: List of Google Sheets audit row data
        """
        if not audit_records:
            return
        
        # Convert sheets rows to database rows
        db_rows = []
        for sheets_row in audit_records:
            db_row = HunterAuditRow.from_sheets_row(self.job_id, self.request_id, sheets_row)
            db_rows.append(db_row)
        
        self._audit_buffer.extend(db_rows)
        
        if len(self._audit_buffer) >= self._audit_batch_size:
            self._flush_audit()
    
    def _flush_results(self) -> None:
        """Flush results buffer to database."""
        if not self._results_buffer:
            return
        
        try:
            success, error, count = self._results_repo.insert_many(self._results_buffer)
            
            if success:
                logger.info(
                    "Batch flushed %d Hunter.io results to PostgreSQL "
                    "(job_id=%s, request_id=%s)",
                    count, self.job_id, self.request_id
                )
            else:
                logger.warning(
                    "Batch flush failed for Hunter.io results: %s "
                    "(job_id=%s, request_id=%s)",
                    error, self.job_id, self.request_id
                )
            
            # Clear buffer regardless of success/failure to prevent memory buildup
            self._results_buffer.clear()
            
        except Exception as e:
            logger.error(
                "Unexpected error during Hunter.io results batch flush: %s "
                "(job_id=%s, request_id=%s)",
                e, self.job_id, self.request_id
            )
            # Clear buffer to prevent memory buildup
            self._results_buffer.clear()
    
    def _flush_audit(self) -> None:
        """Flush audit buffer to database."""
        if not self._audit_buffer:
            return
        
        try:
            success, error, count = self._audit_repo.insert_many(self._audit_buffer)
            
            if success:
                logger.info(
                    "Batch flushed %d Hunter audit records to PostgreSQL "
                    "(job_id=%s, request_id=%s)",
                    count, self.job_id, self.request_id
                )
            else:
                logger.warning(
                    "Batch flush failed for Hunter audit records: %s "
                    "(job_id=%s, request_id=%s)",
                    error, self.job_id, self.request_id
                )
            
            # Clear buffer regardless of success/failure to prevent memory buildup
            self._audit_buffer.clear()
            
        except Exception as e:
            logger.error(
                "Unexpected error during Hunter audit batch flush: %s "
                "(job_id=%s, request_id=%s)",
                e, self.job_id, self.request_id
            )
            # Clear buffer to prevent memory buildup
            self._audit_buffer.clear()
    
    def flush_all(self) -> None:
        """Flush all buffers to database."""
        self._flush_results()
        self._flush_audit()
    
    def get_buffer_sizes(self) -> tuple[int, int]:
        """
        Get current buffer sizes.
        
        Returns:
            Tuple of (results_buffer_size, audit_buffer_size)
        """
        return len(self._results_buffer), len(self._audit_buffer)


def convert_db_results_to_sheets_format(results: List[dict]) -> List[List[str]]:
    """
    Convert database results to Google Sheets format.
    
    DEPRECATED: Use the shared sheets_parity.create_hunter_results_converter instead.
    This function is kept for backward compatibility.
    """
    from ...database.sheets_parity import create_hunter_results_converter
    
    converter = create_hunter_results_converter()
    return converter(results)


def convert_db_audit_to_sheets_format(audit_records: List[dict]) -> List[List[str]]:
    """
    Convert database audit records to Google Sheets format.
    
    DEPRECATED: Use the shared sheets_parity.create_hunter_audit_converter instead.
    This function is kept for backward compatibility.
    """
    from ...database.sheets_parity import create_hunter_audit_converter
    
    converter = create_hunter_audit_converter()
    return converter(audit_records)