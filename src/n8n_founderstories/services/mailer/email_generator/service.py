"""
Email Content Generation Service Module.

This module orchestrates the email content generation process by coordinating
between the database, prompt builder, and LLM client. It handles batch processing,
error handling, and status management.

Architecture:
    Database (mail_content table)
         ↓
    Service (THIS MODULE) - orchestration
         ↓
    Prompt Builder → LLM Client → Database Update

Key Responsibilities:
- Fetch pending rows from database
- Build personalized prompts for each row
- Generate content via LLM (concurrent processing)
- Update database with results
- Handle errors gracefully
- Track processing metrics
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any

import psycopg

from .config import DEFAULT_SUBJECT, SERIES_NAME, ORGANISATION, BATCH_SIZE
from ....core.config import settings

# Number of concurrent LLM API calls (from settings or default to 6)
CONCURRENT_WORKERS = getattr(settings, "llm_max_concurrency", 6)
from .prompt_builder import build_email_prompt
from .llm_client import generate_email_content
from . import repo
from ....services.master import repo as master_repo

logger = logging.getLogger(__name__)


async def process_pending_emails(conn: psycopg.Connection[Any], job_id: str) -> int:
    """
    Process pending emails by generating content for rows without content.
    
    This is the main orchestration function that:
    1. Fetches rows where content IS NULL and send_status = 'OK'
    2. For each row, builds a personalized prompt
    3. Calls LLM to generate email content
    4. Updates the database with generated content
    5. Handles errors by marking rows as FAILED
    
    Processing Flow:
        SELECT rows WHERE content IS NULL AND send_status = 'OK'
             ↓
        For each row:
             ↓
        Build personalized prompt (prompt_builder.py)
             ↓
        Generate content via LLM (llm_client.py)
             ↓
        Prepare update dict with subject + content
             ↓
        Call repo.upsert_batch_rows() to save
             ↓
        Commit transaction
    
    Error Handling:
    - LLM failures: Row marked with send_status = "FAILED"
    - Database errors: Logged and re-raised
    - Partial batch success: Successful rows are saved
    
    Args:
        conn: Active psycopg connection (caller manages lifecycle)
            Must have autocommit=False for transaction control
        job_id: Service-level run identifier for tracking
    
    Returns:
        Number of rows successfully processed in this batch
        
    Raises:
        psycopg.Error: If database operations fail
        
    Example:
        >>> from n8n_founderstories.core.db import get_conn
        >>> conn = get_conn()
        >>> processed = process_pending_emails(conn, "job-123")
        >>> print(f"Processed {processed} emails")
    """
    # ========================================================================
    # STEP 1: Fetch pending rows from database
    # ========================================================================
    try:
        with conn.cursor() as cur:
            # Get pending rows (case-insensitive status check)
            # Handles both NULL and empty string content
            cur.execute(
                """
                SELECT request_id, domain, company, contacts, description, spreadsheet_id
                FROM mail_content
                WHERE (content IS NULL OR content = '')
                  AND UPPER(send_status) = 'OK'
                LIMIT %s
                """,
                (BATCH_SIZE,)
            )
            rows = cur.fetchall()
            
            # Get total remaining count (case-insensitive status check)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM mail_content
                WHERE (content IS NULL OR content = '')
                  AND UPPER(send_status) = 'OK'
                """
            )
            total_remaining = cur.fetchone()[0]
            
    except Exception as e:
        logger.error(
            f"EMAIL_GEN | action=FETCH_FAILED | job_id={job_id} | error={str(e)}"
        )
        raise
    
    if not rows:
        logger.debug(
            f"EMAIL_GEN | action=NO_PENDING | job_id={job_id}"
        )
        return 0
    
    logger.info(
        f"EMAIL_GEN | action=FETCH_SUCCESS | job_id={job_id} | "
        f"fetched={len(rows)} | total_remaining={total_remaining}"
    )
    
    # ========================================================================
    # STEP 2: Process rows concurrently with asyncio.gather
    # ========================================================================
    # Process multiple rows simultaneously for faster throughput
    # Uses same pattern as LLM extractor worker
    updates_with_metadata = []
    failed_count = 0
    
    async def process_single_row(row):
        """Process a single row (generate content and update DB)."""
        nonlocal failed_count, updates_with_metadata
        request_id, domain, company, contacts, description, spreadsheet_id = row
        
        try:
            # Build personalized prompt
            prompt = build_email_prompt(
                contact_name=contacts,
                company=company,
                description=description,
                organisation=ORGANISATION,
                series_name=SERIES_NAME,
            )
            
            # Generate content via LLM (run in executor for true concurrency)
            loop = asyncio.get_event_loop()
            content = await loop.run_in_executor(
                None,  # Use default executor
                generate_email_content,
                prompt,
                request_id,
            )
            
            # Update database immediately for this row
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE mail_content
                        SET
                            subject = %s,
                            content = %s,
                            send_status = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE request_id = %s
                          AND domain = %s
                        """,
                        (
                            DEFAULT_SUBJECT,
                            content,
                            "READY",
                            request_id,
                            domain,
                        )
                    )
                conn.commit()  # Commit immediately after each row
                
                # Update mail_write_status in mstr_results to SUCCEEDED
                try:
                    master_repo.update_mail_write_status(conn, request_id, domain, "succeeded")
                    conn.commit()
                except Exception as master_error:
                    logger.warning(
                        f"MAIL CONTENT | request_id={request_id} | domain={domain} | "
                        f"job_id={job_id} | status=MASTER_UPDATE_FAILED | error={str(master_error)}"
                    )
                    # Don't fail the whole operation if master update fails
                
                logger.info(
                    f"MAIL CONTENT | request_id={request_id} | domain={domain} | "
                    f"job_id={job_id} | sheet_id={spreadsheet_id or 'N/A'} | "
                    f"status=GENERATED | content_length={len(content)}"
                )
                
                # Track for final summary
                updates_with_metadata.append({
                    "request_id": request_id,
                    "domain": domain,
                    "status": "READY",
                })
                
            except Exception as db_error:
                logger.error(
                    f"MAIL CONTENT | request_id={request_id} | domain={domain} | "
                    f"job_id={job_id} | status=DB_UPDATE_FAILED | error={str(db_error)}"
                )
                failed_count += 1
            
        except Exception as e:
            # LLM generation failed - mark row as FAILED
            failed_count += 1
            
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE mail_content
                        SET
                            send_status = %s,
                            comments = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE request_id = %s
                          AND domain = %s
                        """,
                        (
                            "FAILED",
                            f"LLM generation failed: {str(e)[:200]}",
                            request_id,
                            domain,
                        )
                    )
                conn.commit()  # Commit immediately
                
                logger.error(
                    f"MAIL CONTENT | request_id={request_id} | domain={domain} | "
                    f"job_id={job_id} | sheet_id={spreadsheet_id or 'N/A'} | "
                    f"status=FAILED | error={str(e)}"
                )
                
                # Track for final summary
                updates_with_metadata.append({
                    "request_id": request_id,
                    "domain": domain,
                    "status": "FAILED",
                })
                
            except Exception as db_error:
                logger.error(
                    f"MAIL CONTENT | request_id={request_id} | domain={domain} | "
                    f"job_id={job_id} | status=DB_UPDATE_FAILED | error={str(db_error)}"
                )
        
    # Process rows concurrently using asyncio.gather (same as LLM extractor)
    tasks = [process_single_row(row) for row in rows]
    await asyncio.gather(*tasks, return_exceptions=True)
    
    # ========================================================================
    # STEP 3: Batch update database with all results
    # ========================================================================
    if not updates_with_metadata:
        logger.warning(
            f"EMAIL_GEN | action=NO_UPDATES | job_id={job_id}"
        )
        return 0
    
    # ========================================================================
    # STEP 4: Final summary (updates already committed per-row above)
    # ========================================================================
    try:
        total_upserted = len(updates_with_metadata)
        
        # Group by request_id for logging
        grouped_updates: dict[str, int] = defaultdict(int)
        for update in updates_with_metadata:
            grouped_updates[update["request_id"]] += 1
        
        for rid, count in grouped_updates.items():
            logger.debug(
                f"EMAIL_GEN | action=UPDATE_GROUP | job_id={job_id} | "
                f"request_id={rid} | count={count}"
            )
        
        success_count = len(updates_with_metadata) - failed_count
        
        # Get updated remaining count
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM mail_content
                    WHERE content IS NULL
                      AND send_status = 'OK'
                    """
                )
                remaining_after = cur.fetchone()[0]
        except Exception:
            remaining_after = "unknown"
        
        logger.info(
            f"EMAIL_GEN | action=PROCESS_COMPLETE | job_id={job_id} | "
            f"processed={len(rows)} | success={success_count} | failed={failed_count} | "
            f"request_groups={len(grouped_updates)} | total_upserted={total_upserted} | "
            f"remaining={remaining_after}"
        )
        
        return success_count
        
    except Exception as e:
        logger.error(
            f"EMAIL_GEN | action=UPDATE_FAILED | job_id={job_id} | error={str(e)}"
        )
        raise