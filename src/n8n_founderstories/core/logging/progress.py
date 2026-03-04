"""
Progress logging throttler for production environments.

Prevents log spam by throttling progress updates to a configurable interval.
"""

import time


class ProgressThrottler:
    """
    Time-based throttler for progress logging.
    
    Ensures progress logs are emitted at most once per interval,
    preventing log spam while maintaining visibility into long-running operations.
    
    Example:
        throttler = ProgressThrottler(5.0)  # Log at most every 5 seconds
        
        for i in range(1000):
            # Do work...
            if throttler.should_log():
                logger.info("Progress: %d/1000", i)
    """
    
    def __init__(self, interval_s: float = 5.0):
        """
        Initialize throttler.
        
        Args:
            interval_s: Minimum seconds between log emissions (default: 5.0)
        """
        self.interval_s = interval_s
        self._last = 0.0
    
    def should_log(self) -> bool:
        """
        Check if enough time has passed to emit another log.
        
        Returns:
            True if logging should occur, False otherwise
        """
        now = time.monotonic()
        if now - self._last >= self.interval_s:
            self._last = now
            return True
        return False