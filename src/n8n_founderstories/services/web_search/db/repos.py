"""
Web Search PostgreSQL repositories.

This module provides safe database operations for web search results
with proper error handling and transaction management.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import List, Optional

from ....core.utils.text import norm
from ...database.connection import get_connection_context, DatabaseConnectionError
from ...database.config import db_config

from .models import WebSearchResultRow

logger = logging.getLogger(__name__)


class WebSearchResultsRepository:
    """
    Repository for web search results database operations.
    
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
    
    def insert_many(self, rows: List[WebSearchResultRow]) -> tuple[bool, Optional[str], int]:
        """
        Insert multiple web search result rows in a single transaction.
        
        Args:
            rows: List of WebSearchResultRow instances to insert
            
        Returns:
            Tuple of (success, error_message, inserted_count)
        """
        if not rows:
            return True, None, 0
        
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", 0
        
        insert_sql = """
        INSERT INTO web_search_results (
            id, job_id, request_id, source_type, organisation, website,
            query, country, location, language, domain, source_url,
            confidence, reason, evidence, snippet, raw_json, dedupe_key
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ON CONSTRAINT uq_web_search_results_request_dedupe DO NOTHING
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
                            row.source_type,
                            row.organisation,
                            row.website,
                            row.query,
                            row.country,
                            row.location,
                            row.language,
                            row.domain,
                            row.source_url,
                            row.confidence,
                            row.reason,
                            row.evidence,
                            row.snippet,
                            json.dumps(row.raw_json) if row.raw_json else None,
                            row.dedupe_key,
                        ))
                    
                    # Execute batch insert
                    cur.executemany(insert_sql, batch_data)
                    conn.commit()
                    
                    inserted_count = len(batch_data)
                    logger.debug(
                        "Successfully inserted %d web search results to PostgreSQL",
                        inserted_count
                    )
                    return True, None, inserted_count
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, 0
        except Exception as e:
            error_msg = f"Failed to insert web search results: {e}"
            logger.error(error_msg)
            return False, error_msg, 0
    
    def get_results_by_request(self, request_id: str) -> tuple[bool, Optional[str], List[dict]]:
        """
        Retrieve web search results for a specific request ID.
        
        Args:
            request_id: Request identifier
            
        Returns:
            Tuple of (success, error_message, results_list)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", []
        
        select_sql = """
        SELECT id, created_at, job_id, request_id, source_type, organisation, website,
               query, country, location, language, domain, source_url,
               confidence, reason, evidence, snippet, raw_json, dedupe_key
        FROM web_search_results
        WHERE request_id = %s
        ORDER BY created_at, source_type, organisation
        """
        
        try:
            with get_connection_context(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(select_sql, (request_id,))
                    rows = cur.fetchall()
                    
                    results = []
                    for row in rows:
                        results.append({
                            'id': str(row[0]),
                            'created_at': row[1].isoformat() if row[1] else None,
                            'job_id': row[2],
                            'request_id': row[3],
                            'source_type': row[4],
                            'organisation': row[5],
                            'website': row[6],
                            'query': row[7],
                            'country': row[8],
                            'location': row[9],
                            'language': row[10],
                            'domain': row[11],
                            'source_url': row[12],
                            'confidence': row[13],
                            'reason': row[14],
                            'evidence': row[15],
                            'snippet': row[16],
                            'raw_json': row[17],  # Already parsed as dict by psycopg
                            'dedupe_key': row[18],
                        })
                    
                    return True, None, results
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, []
        except Exception as e:
            error_msg = f"Failed to retrieve web search results: {e}"
            logger.error(error_msg)
            return False, error_msg, []
    
    def get_results_by_job(self, job_id: str) -> tuple[bool, Optional[str], List[dict]]:
        """
        Retrieve web search results for a specific job ID.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Tuple of (success, error_message, results_list)
        """
        if not self.dsn:
            return False, "No PostgreSQL DSN configured", []
        
        select_sql = """
        SELECT id, created_at, job_id, request_id, source_type, organisation, website,
               query, country, location, language, domain, source_url,
               confidence, reason, evidence, snippet, raw_json, dedupe_key
        FROM web_search_results
        WHERE job_id = %s
        ORDER BY created_at, source_type, organisation
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
                            'source_type': row[4],
                            'organisation': row[5],
                            'website': row[6],
                            'query': row[7],
                            'country': row[8],
                            'location': row[9],
                            'language': row[10],
                            'domain': row[11],
                            'source_url': row[12],
                            'confidence': row[13],
                            'reason': row[14],
                            'evidence': row[15],
                            'snippet': row[16],
                            'raw_json': row[17],  # Already parsed as dict by psycopg
                            'dedupe_key': row[18],
                        })
                    
                    return True, None, results
                    
        except DatabaseConnectionError as e:
            error_msg = f"Database connection failed: {e}"
            logger.warning(error_msg)
            return False, error_msg, []
        except Exception as e:
            error_msg = f"Failed to retrieve web search results: {e}"
            logger.error(error_msg)
            return False, error_msg, []


def convert_db_results_to_sheets_format(results: List[dict]) -> List[List[str]]:
    """
    Convert database results to Google Sheets format.
    
    Format: ["Organisation", "Website", "Source Type", "Query", "Location", "Country", "Evidence/Reason", "Source URL"]
    
    Args:
        results: List of database result dictionaries
        
    Returns:
        List of rows in sheets format
    """
    sheets_rows = []
    
    for result in results:
        # Use evidence for blog_extracted, reason for company_hit
        evidence_or_reason = norm(result.get('evidence', '')) or norm(result.get('reason', ''))
        
        row = [
            norm(result.get('organisation', '')),
            norm(result.get('website', '')),
            norm(result.get('source_type', '')),
            norm(result.get('query', '')),
            norm(result.get('location', '')),
            norm(result.get('country', '')),
            evidence_or_reason,
            norm(result.get('source_url', '')),
        ]
        sheets_rows.append(row)
    
    return sheets_rows