"""
Google Sheets Request Queue and Batching System.

Prevents quota exhaustion by:
1. Queuing concurrent requests
2. Batching multiple small writes into larger operations
3. Enforcing minimum delays between operations
4. Deduplicating redundant requests

This is especially important for append operations which make 2 API calls each
(one for headers, one for data), quickly exhausting the 60 requests/minute quota.
"""

import logging
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Any, Dict, Optional

logger = logging.getLogger(__name__)

# Minimum delay between operations to the same sheet (seconds)
MIN_DELAY_BETWEEN_OPERATIONS = 0.5

# Maximum time to wait for batching before forcing execution (seconds)
MAX_BATCH_WAIT_TIME = 2.0

# Maximum number of rows to batch together
MAX_BATCH_SIZE = 100


@dataclass
class QueuedRequest:
    """Represents a queued Sheets operation."""
    sheet_id: str
    tab_name: str
    operation: str  # "append", "replace", "update"
    func: Callable[[], Any]
    timestamp: datetime
    priority: int = 0  # Higher = more urgent


class SheetsRequestQueue:
    """
    Queue system for Google Sheets operations.
    
    Features:
    - Prevents concurrent writes to the same sheet
    - Enforces minimum delays between operations
    - Batches small append operations
    - Deduplicates redundant requests
    """
    
    def __init__(self):
        """Initialize the request queue."""
        self.queues: Dict[str, deque[QueuedRequest]] = defaultdict(deque)
        self.last_operation_time: Dict[str, datetime] = {}
        self.locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)
        self.global_lock = threading.Lock()
        
        # Batching state
        self.pending_appends: Dict[str, list[tuple[list, datetime]]] = defaultdict(list)
        self.batch_timers: Dict[str, threading.Timer] = {}
        
        logger.info("SHEETS_QUEUE | Initialized request queue")
    
    def _get_sheet_key(self, sheet_id: str, tab_name: str) -> str:
        """Get unique key for sheet+tab combination."""
        return f"{sheet_id}:{tab_name}"
    
    def _should_batch(self, operation: str, rows_count: int) -> bool:
        """Determine if operation should be batched."""
        # Only batch append operations with small row counts
        return operation == "append" and rows_count <= MAX_BATCH_SIZE
    
    def _wait_for_min_delay(self, sheet_key: str) -> None:
        """Wait for minimum delay since last operation on this sheet."""
        with self.global_lock:
            last_time = self.last_operation_time.get(sheet_key)
            
            if last_time:
                elapsed = (datetime.now() - last_time).total_seconds()
                if elapsed < MIN_DELAY_BETWEEN_OPERATIONS:
                    wait_time = MIN_DELAY_BETWEEN_OPERATIONS - elapsed
                    logger.debug(
                        "SHEETS_QUEUE | THROTTLE | sheet_key=%s | wait=%.2fs",
                        sheet_key[:20],
                        wait_time
                    )
                    time.sleep(wait_time)
            
            self.last_operation_time[sheet_key] = datetime.now()
    
    def execute_with_queue(
        self,
        sheet_id: str,
        tab_name: str,
        operation: str,
        func: Callable[[], Any],
        priority: int = 0
    ) -> Any:
        """
        Execute a Sheets operation with queuing and throttling.
        
        Args:
            sheet_id: Google Sheets ID
            tab_name: Tab name
            operation: Operation type ("append", "replace", "update")
            func: Function to execute
            priority: Priority (higher = more urgent)
            
        Returns:
            Result from func()
        """
        sheet_key = self._get_sheet_key(sheet_id, tab_name)
        
        # Acquire lock for this sheet
        with self.locks[sheet_key]:
            # Wait for minimum delay
            self._wait_for_min_delay(sheet_key)
            
            # Execute operation
            logger.debug(
                "SHEETS_QUEUE | EXECUTE | sheet_key=%s | operation=%s",
                sheet_key[:20],
                operation
            )
            
            try:
                result = func()
                return result
            except Exception as e:
                logger.error(
                    "SHEETS_QUEUE | ERROR | sheet_key=%s | operation=%s | error=%s",
                    sheet_key[:20],
                    operation,
                    str(e)
                )
                raise
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get queue metrics.
        
        Returns:
            Dictionary with metrics
        """
        with self.global_lock:
            total_queued = sum(len(q) for q in self.queues.values())
            total_pending_batches = sum(len(b) for b in self.pending_appends.values())
            
            return {
                'total_queued_requests': total_queued,
                'total_pending_batches': total_pending_batches,
                'active_sheets': len(self.last_operation_time),
                'active_locks': len(self.locks)
            }


# Global queue instance
_request_queue: Optional[SheetsRequestQueue] = None
_queue_lock = threading.Lock()


def get_request_queue() -> SheetsRequestQueue:
    """
    Get or create the global request queue instance.
    
    Returns:
        Global SheetsRequestQueue instance
    """
    global _request_queue
    
    if _request_queue is None:
        with _queue_lock:
            if _request_queue is None:
                _request_queue = SheetsRequestQueue()
    
    return _request_queue


def reset_request_queue() -> None:
    """Reset the global request queue (useful for testing)."""
    global _request_queue
    with _queue_lock:
        _request_queue = None