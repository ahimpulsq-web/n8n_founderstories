from __future__ import annotations

# =============================================================================
# logging.py
#
# Classification:
# - Role: creates a consistent logger adapter for job-scoped log lines.
# - Policy: every job log line includes tool + request_id + job_id.
# =============================================================================

import logging


class JobLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        prefix = f"tool={self.extra.get('tool')} | request_id={self.extra.get('request_id')} | job_id={self.extra.get('job_id')}"
        return f"{prefix} | {msg}", kwargs


def job_logger(name: str, *, tool: str, request_id: str, job_id: str) -> JobLogger:
    base = logging.getLogger(name)
    return JobLogger(base, {"tool": tool, "request_id": request_id, "job_id": job_id})
