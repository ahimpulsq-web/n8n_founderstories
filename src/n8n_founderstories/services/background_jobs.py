# src/n8n_founderstories/services/background_jobs.py

import logging
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# Single worker so you don’t run multiple long Hunter jobs concurrently by accident.
# Increase later if you really want concurrency.
_EXECUTOR = ThreadPoolExecutor(max_workers=1)


def submit_job(fn, *args, **kwargs) -> None:
    """
    Fire-and-forget execution inside the API process.
    Note: if the server restarts, the job is lost. (Good enough for now.)
    """
    _EXECUTOR.submit(_run_job, fn, *args, **kwargs)


def _run_job(fn, *args, **kwargs) -> None:
    try:
        fn(*args, **kwargs)
    except Exception:
        logger.exception("BACKGROUND_JOB_FAILED")
