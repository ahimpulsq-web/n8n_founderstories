"""
Email Content Generation Background Worker.

This module implements a continuous background worker that polls the database
for pending emails and generates content using LLM. It runs independently of
the API and ensures emails are processed reliably.

Worker Behavior:
- Runs in infinite loop
- Polls database every POLL_INTERVAL_SECONDS
- Processes BATCH_SIZE rows per iteration
- Commits after each batch
- Handles errors gracefully
- Logs all operations

Architecture:
    Worker Loop (THIS MODULE)
         ↓
    Service Layer (service.py)
         ↓
    Database + LLM

Usage:
    # Run as standalone script
    python -m n8n_founderstories.services.mailer.email_generator.worker
    
    # Or import and run
    from n8n_founderstories.services.mailer.email_generator.worker import run_worker
    run_worker()
"""

from __future__ import annotations

import asyncio
import logging
import time
from uuid import uuid4

from .config import POLL_INTERVAL_SECONDS
from .service import process_pending_emails
from ....core.db import get_conn

logger = logging.getLogger(__name__)


def run_worker() -> None:
    """
    Run the continuous email generation worker.
    
    This function implements the main worker loop that:
    1. Opens a database connection
    2. Processes pending emails in batches
    3. Commits the transaction
    4. Sleeps for POLL_INTERVAL_SECONDS
    5. Repeats indefinitely
    
    Worker Loop:
        while True:
            ↓
        Open DB connection
            ↓
        Process batch (service.process_pending_emails)
            ↓
        Commit transaction
            ↓
        Sleep POLL_INTERVAL_SECONDS
            ↓
        Repeat
    
    Error Handling:
    - Database connection errors: Logged and retried after sleep
    - Processing errors: Logged and retried after sleep
    - Keyboard interrupt: Graceful shutdown
    
    Connection Management:
    - New connection per iteration (prevents stale connections)
    - Autocommit disabled for transaction control
    - Connection closed after each iteration
    
    Logging:
    - INFO: Worker start, batch processing results
    - DEBUG: Detailed operation logs
    - ERROR: All failures with context
    
    Example:
        >>> run_worker()
        # Runs indefinitely, processing emails continuously
        # Press Ctrl+C to stop
    """
    iteration = 0
    
    # Create event loop for async operations (same as LLM extractor)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        while True:
            iteration += 1
            job_id = f"worker-{uuid4().hex[:8]}"
            
            logger.debug(
                f"EMAIL_WORKER | action=ITERATION_START | iteration={iteration} | job_id={job_id}"
            )
            
            try:
                # ============================================================
                # STEP 1: Open database connection
                # ============================================================
                # Note: We get a fresh connection each iteration to avoid
                # stale connection issues in long-running workers
                conn = get_conn()
                
                # Disable autocommit for transaction control
                conn.autocommit = False
                
                logger.debug(
                    f"EMAIL_WORKER | action=DB_CONNECTED | job_id={job_id}"
                )
                
                try:
                    # ========================================================
                    # STEP 2: Process pending emails (async)
                    # ========================================================
                    processed = loop.run_until_complete(process_pending_emails(conn, job_id))
                    
                    # ========================================================
                    # STEP 3: Commit transaction
                    # ========================================================
                    conn.commit()
                    
                    if processed > 0:
                        logger.info(
                            f"EMAIL_WORKER | action=BATCH_COMPLETE | iteration={iteration} | "
                            f"job_id={job_id} | processed={processed}"
                        )
                    else:
                        logger.debug(
                            f"EMAIL_WORKER | action=NO_WORK | iteration={iteration} | job_id={job_id}"
                        )
                
                except Exception as e:
                    # Rollback on error
                    conn.rollback()
                    logger.error(
                        f"EMAIL_WORKER | action=PROCESS_ERROR | iteration={iteration} | "
                        f"job_id={job_id} | error={str(e)}",
                        exc_info=True
                    )
                
                finally:
                    # Always close connection
                    conn.close()
                    logger.debug(
                        f"EMAIL_WORKER | action=DB_CLOSED | job_id={job_id}"
                    )
            
            except Exception as e:
                # Connection or other critical error
                logger.error(
                    f"EMAIL_WORKER | action=ITERATION_ERROR | iteration={iteration} | "
                    f"job_id={job_id} | error={str(e)}",
                    exc_info=True
                )
            
            # ================================================================
            # STEP 4: Sleep before next iteration
            # ================================================================
            logger.debug(
                f"EMAIL_WORKER | action=SLEEP | iteration={iteration} | "
                f"duration={POLL_INTERVAL_SECONDS}s"
            )
            time.sleep(POLL_INTERVAL_SECONDS)
    
    except KeyboardInterrupt:
        logger.info(
            "EMAIL_WORKER | action=SHUTDOWN | reason=keyboard_interrupt | "
            f"iterations={iteration}"
        )
    
    except Exception as e:
        logger.critical(
            f"EMAIL_WORKER | action=FATAL_ERROR | iterations={iteration} | error={str(e)}",
            exc_info=True
        )
        raise
    
    finally:
        # Clean up event loop
        loop.close()
        logger.info("EMAIL_WORKER | action=LOOP_CLOSED")


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    # Run the worker
    run_worker()
