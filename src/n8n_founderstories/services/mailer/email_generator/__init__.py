"""
Email Content Generator Module.

This module provides a complete background worker system for generating
personalized email content using LLM for the mail_content table.

Main Components:
- config: Configuration constants (subject, batch size, etc.)
- prompt_builder: Pure functions for building LLM prompts
- llm_client: LLM interaction and content generation
- service: Orchestration layer for batch processing
- worker: Continuous background worker
- repo: Database persistence layer

Public API:
    # Run the worker
    from n8n_founderstories.services.mailer.email_generator import run_worker
    run_worker()
    
    # Or process a single batch
    from n8n_founderstories.services.mailer.email_generator import process_pending_emails
    from n8n_founderstories.core.db import get_conn
    
    conn = get_conn()
    processed = process_pending_emails(conn, "job-123")

Architecture:
    API inserts rows (send_status='OK', content=NULL)
         ↓
    Worker polls database
         ↓
    Service orchestrates generation
         ↓
    Prompt Builder creates personalized prompts
         ↓
    LLM Client generates content
         ↓
    Repo updates database (send_status='READY', content=<generated>)
         ↓
    Next service can send emails
"""

from __future__ import annotations

from .config import (
    DEFAULT_SUBJECT,
    SERIES_NAME,
    ORGANISATION,
    BATCH_SIZE,
    POLL_INTERVAL_SECONDS,
)
from .prompt_builder import build_email_prompt
from .llm_client import generate_email_content
from .service import process_pending_emails
from .worker import run_worker

__all__ = [
    # Configuration
    "DEFAULT_SUBJECT",
    "SERIES_NAME",
    "ORGANISATION",
    "BATCH_SIZE",
    "POLL_INTERVAL_SECONDS",
    # Functions
    "build_email_prompt",
    "generate_email_content",
    "process_pending_emails",
    "run_worker",
]