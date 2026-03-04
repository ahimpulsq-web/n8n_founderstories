from __future__ import annotations

import logging
from uuid import uuid4
from typing import Any, List

from fastapi import APIRouter, BackgroundTasks, HTTPException, status
from pydantic import BaseModel, Field

from n8n_founderstories.core.db import get_conn
from n8n_founderstories.core.config import settings
from n8n_founderstories.services.mailer.email_generator import repo as email_repo
from n8n_founderstories.services.mailer.mail_tracker import repo as mail_tracker_repo
from n8n_founderstories.services.mailer.mail_tracker.auth import verify_credentials
from n8n_founderstories.services.master import repo as master_repo
from n8n_founderstories.services.sheets.exports import global_mail_tracking

logger = logging.getLogger(__name__)
router = APIRouter()

class EmailContentItem(BaseModel):
    """Single email content item matching the data structure from the user."""
    spreadsheet_id: str | None = Field(None, description="Google Sheet ID", alias="spreadhseet_id")
    request_id: str = Field(..., description="Request identifier")
    job_id: str = Field(..., description="Job identifier")
    
    model_config = {"populate_by_name": True}
    organisation: str | None = Field(None, description="Organization name")
    domain: str = Field(..., description="Company domain")
    company: str | None = Field(None, description="Company name")
    email: str | None = Field(None, description="Contact email")
    contacts: str | None = Field(None, description="Contact names/roles")
    description: str | None = Field(None, description="Company description")
    verification_link: str | None = Field(None, description="Verification link")
    subject: str | None = Field(None, description="Email subject")
    content: str | None = Field(None, description="Email content")
    send_status: str | None = Field("VERIFY", description="Send status")
    comments: str | None = Field(None, description="Comments")

class MailerV1ContentWriterResponse(BaseModel):
    """Response from the mailerv1 contentwriter endpoint."""
    status: str
    job_id: str
    request_id: str
    rows_inserted: int



def _process_email_content_batch(*, job_id: str, request_id: str, data: List[EmailContentItem]) -> None:
    """
    Background task that processes and stores email content batch.
    Follows the same pattern as hunter and google_maps services.
    
    Implements proper transaction handling with automatic rollback on errors.
    """
    conn = None
    try:
        conn = get_conn()
        
        # Ensure table exists (DDL operation, auto-commits)
        email_repo.ensure_table(conn, job_id)
        
        # Convert Pydantic models to dicts for repo
        rows = [item.model_dump() for item in data]
        
        # Upsert batch (with automatic retry on deadlock)
        rows_inserted = email_repo.upsert_batch_rows(
            conn=conn,
            request_id=request_id,
            job_id=job_id,
            rows=rows
        )
        
        # Finalize job_id for all rows (with automatic retry on deadlock)
        total_rows = email_repo.finalize_request_job_id(
            conn=conn,
            request_id=request_id,
            job_id=job_id
        )
        
        # Commit transaction
        conn.commit()
        
        logger.info(
            f"MAILERV1 | STATE=SUCCESS | request_id={request_id} | job_id={job_id} | "
            f"rows_inserted={rows_inserted} | total_rows={total_rows}"
        )
            
    except Exception as e:
        # Rollback on any error
        if conn is not None:
            try:
                conn.rollback()
                logger.debug(f"MAILERV1 | action=ROLLBACK | request_id={request_id} | job_id={job_id}")
            except Exception as rollback_error:
                logger.error(
                    f"MAILERV1 | action=ROLLBACK_ERROR | request_id={request_id} | job_id={job_id} | error={rollback_error}"
                )
        
        logger.error(
            f"MAILERV1 | STATE=FAILED | request_id={request_id} | job_id={job_id} | error={e}"
        )
        raise
    
    finally:
        # Always close connection
        if conn is not None:
            try:
                conn.close()
            except Exception as close_error:
                logger.error(
                    f"MAILERV1 | action=CLOSE_ERROR | request_id={request_id} | job_id={job_id} | error={close_error}"
                )


@router.post("/mailerv1/contentwriter", response_model=MailerV1ContentWriterResponse, tags=["mailerv1"])
async def mailerv1_contentwriter(
    payload: EmailContentItem,
    background_tasks: BackgroundTasks
) -> MailerV1ContentWriterResponse:
    """
    Store email content data in the mail_content table.
    
    This endpoint accepts a single email content item and stores it in the database.
    It follows the same pattern as hunter and google_maps endpoints with background processing.
    
    The data structure matches the enrichment output with additional email-specific fields.
    """
    # Extract request_id from payload
    request_id = payload.request_id
    spreadsheet_id = payload.spreadsheet_id or "N/A"
    
    # Generate job_id
    job_id = f"mailcnt__{uuid4().hex}"
    
    # Log START
    logger.info(
        f"MAILERV1 | START | request_id={request_id} | job_id={job_id} | sheet_id={spreadsheet_id}"
    )
    
    # Process in background (wrap single item in list for batch processing)
    background_tasks.add_task(
        _process_email_content_batch,
        job_id=job_id,
        request_id=request_id,
        data=[payload]
    )
    
    return MailerV1ContentWriterResponse(
        status="accepted",
        job_id=job_id,
        request_id=request_id,
        rows_inserted=1  # Single item per request
    )

class MailTrackerItem(BaseModel):
    """Mail tracker item for tracking sent emails."""
    request_id: str = Field(..., description="Request identifier")
    thread_id: str | None = Field(None, description="Unique email send ID from email service (None for failed sends)")
    company: str = Field(..., description="Company name")
    domain: str = Field(..., description="Company domain")
    contacts: str | None = Field(None, description="Contact names")
    email: str = Field(..., description="Email address")
    subject: str | None = Field(None, description="Email subject")
    content: str | None = Field(None, description="Email content")
    send_status: str | None = Field(None, description="Send status (e.g., sent, pending, failed)")
    sent_at: str | None = Field(None, description="Timestamp when email was sent (ISO format)")
    reply_status: str | None = Field(None, description="Reply status (e.g., replied, no_reply, bounced)")
    received_at: str | None = Field(None, description="Timestamp when reply was received (ISO format)")
    comments: str | None = Field(None, description="Additional comments")


class MailTrackerResponse(BaseModel):
    """Response from the mail tracker endpoint."""
    status: str
    request_id: str
    rows_inserted: int


def _process_mail_tracker_batch(*, request_id: str, data: List[MailTrackerItem]) -> None:
    """
    Background task that processes and stores mail tracker batch.
    Follows the same pattern as email content writer.
    
    Implements proper transaction handling with automatic rollback on errors.
    Additionally updates the mail_content table to mark emails as SENT.
    """
    conn = None
    try:
        conn = get_conn()
        
        # Ensure table exists (DDL operation, auto-commits)
        mail_tracker_repo.ensure_table(conn)
        
        # Convert Pydantic models to dicts for repo
        rows = [item.model_dump() for item in data]
        
        # Upsert batch
        rows_inserted = mail_tracker_repo.upsert_batch_rows(
            conn=conn,
            request_id=request_id,
            rows=rows
        )
        
        # Update mail_content send_status and mstr_results mail_send_status for each tracked email
        # SENT: update mail_content to CONTACTED and mstr_results to contacted
        # FAILED: do NOT update mail_content, but DO update mstr_results to failed
        for item in data:
            # Determine the status based on send_status from tracker
            send_status_lower = (item.send_status or "").lower()
            
            if send_status_lower == "sent":
                mail_content_status = "CONTACTED"
                master_send_status = "contacted"
                update_mail_content = True
            elif send_status_lower == "failed":
                mail_content_status = None  # Don't update mail_content for FAILED
                master_send_status = "failed"
                update_mail_content = False  # Keep mail_content unchanged
            else:
                # For other statuses, skip all updates
                logger.debug(
                    f"MAIL_TRACKER | action=SKIP_UPDATE | request_id={item.request_id} | "
                    f"domain={item.domain} | send_status={item.send_status} | "
                    f"reason=Only SENT and FAILED statuses trigger updates"
                )
                continue
            
            # Update mail_content send_status (only for SENT status)
            if update_mail_content:
                try:
                    email_repo.update_mail_content_send_status(
                        conn=conn,
                        request_id=item.request_id,
                        domain=item.domain,
                        send_status=mail_content_status
                    )
                except Exception as update_error:
                    logger.warning(
                        f"MAIL_TRACKER | action=UPDATE_MAIL_CONTENT_FAILED | "
                        f"request_id={item.request_id} | domain={item.domain} | error={update_error}"
                    )
                    # Don't fail the whole operation if mail_content update fails
            
            # Update mstr_results mail_send_status (for both SENT and FAILED)
            try:
                master_repo.update_mail_send_status(
                    conn=conn,
                    request_id=item.request_id,
                    domain=item.domain,
                    status=master_send_status
                )
            except Exception as master_update_error:
                logger.warning(
                    f"MAIL_TRACKER | action=UPDATE_MASTER_SEND_STATUS_FAILED | "
                    f"request_id={item.request_id} | domain={item.domain} | error={master_update_error}"
                )
                # Don't fail the whole operation if master update fails
            
            # Update cross-request comments for mail_content (only for SENT status)
            # This updates ALL other requests that have the same email
            if update_mail_content:
                try:
                    email_repo.update_cross_request_comments_mail_content(
                        conn=conn,
                        email=item.email,
                        source_request_id=item.request_id,
                        send_status=mail_content_status
                    )
                except Exception as comment_error:
                    logger.warning(
                        f"MAIL_TRACKER | action=UPDATE_MAIL_CONTENT_COMMENTS_FAILED | "
                        f"request_id={item.request_id} | email={item.email} | error={comment_error}"
                    )
                    # Don't fail the whole operation if comment update fails
            
            # Update cross-request comments for mail_tracker (only for SENT status)
            # This updates ALL other requests that have the same email
            if update_mail_content:
                try:
                    mail_tracker_repo.update_cross_request_comments(
                        conn=conn,
                        email=item.email,
                        source_request_id=item.request_id,
                        send_status=mail_content_status
                    )
                except Exception as tracker_comment_error:
                    logger.warning(
                        f"MAIL_TRACKER | action=UPDATE_MAIL_TRACKER_COMMENTS_FAILED | "
                    f"request_id={item.request_id} | email={item.email} | error={tracker_comment_error}"
                )
                # Don't fail the whole operation if tracker comment update fails
        
        # Commit transaction
        conn.commit()
        
        # Update global mail tracking sheet if configured
        # This happens AFTER commit to ensure data is persisted
        if settings.global_mail_tracking_sheet_id:
            try:
                global_mail_tracking.export_to_sheet(
                    sheet_id=settings.global_mail_tracking_sheet_id,
                    request_id=request_id,
                    mode="replace",
                    suppress_log=False
                )
                logger.info(
                    f"MAIL_TRACKER | GLOBAL_SHEET_UPDATED | request_id={request_id} | "
                    f"sheet_id={settings.global_mail_tracking_sheet_id[:8]}..."
                )
            except Exception as sheet_error:
                # Log error but don't fail the whole operation
                logger.error(
                    f"MAIL_TRACKER | GLOBAL_SHEET_ERROR | request_id={request_id} | "
                    f"error={sheet_error}"
                )
        
        logger.info(
            f"MAIL_TRACKER | STATE=SUCCESS | request_id={request_id} | "
            f"rows_inserted={rows_inserted}"
        )
            
    except Exception as e:
        # Rollback on any error
        if conn is not None:
            try:
                conn.rollback()
                logger.debug(f"MAIL_TRACKER | action=ROLLBACK | request_id={request_id}")
            except Exception as rollback_error:
                logger.error(
                    f"MAIL_TRACKER | action=ROLLBACK_ERROR | request_id={request_id} | error={rollback_error}"
                )
        
        logger.error(
            f"MAIL_TRACKER | STATE=FAILED | request_id={request_id} | error={e}"
        )
        raise
    
    finally:
        # Always close connection
        if conn is not None:
            try:
                conn.close()
            except Exception as close_error:
                logger.error(
                    f"MAIL_TRACKER | action=CLOSE_ERROR | request_id={request_id} | error={close_error}"
                )


@router.post("/mail_tracker/track", response_model=MailTrackerResponse, tags=["mail_tracker"])
async def track_mail(
    payload: MailTrackerItem,
    background_tasks: BackgroundTasks
) -> MailTrackerResponse:
    """
    Track sent email data in the mail_tracker table.
    
    This endpoint accepts a single mail tracking item and stores it in the database.
    It follows the same pattern as other mailer endpoints with background processing.
    
    The endpoint tracks:
    - Email send status and timestamps
    - Reply status and timestamps
    - Company and contact information
    - Email content and metadata
    
    Args:
        payload: Mail tracker item with email tracking data
        background_tasks: FastAPI background tasks for async processing
        
    Returns:
        MailTrackerResponse with status and request information
    """
    # Extract request_id from payload
    request_id = payload.request_id
    
    # Log START
    logger.info(
        f"MAIL_TRACKER | START | request_id={request_id} | domain={payload.domain} | email={payload.email}"
    )
    
    # Process in background (wrap single item in list for batch processing)
    background_tasks.add_task(
        _process_mail_tracker_batch,
        request_id=request_id,
        data=[payload]
    )
    
    return MailTrackerResponse(
        status="accepted",
        request_id=request_id,
        rows_inserted=1  # Single item per request
    )

class AuthRequest(BaseModel):
    """Authentication request model."""
    username: str = Field(..., description="Username for authentication")
    password: str = Field(..., description="Password for authentication")


class AuthResponse(BaseModel):
    """Authentication response model."""
    authenticated: bool = Field(..., description="Whether authentication was successful")
    message: str = Field(..., description="Authentication result message")
    username: str | None = Field(None, description="Authenticated username")


@router.post("/auth", response_model=AuthResponse, tags=["mailer"])
async def authenticate(auth_request: AuthRequest) -> AuthResponse:
    """
    Authenticate a user with username and password.
    
    This endpoint verifies credentials against the stored credentials file.
    Credentials are stored in: src/n8n_founderstories/services/mailer/mail_tracker/credentials.json
    
    To add new users, you can:
    1. Manually edit the credentials.json file with hashed passwords (SHA-256)
    2. Use the auth module's add_user() function programmatically
    
    Default credentials:
    - Username: admin
    - Password: admin
    
    Args:
        auth_request: Authentication request containing username and password
        
    Returns:
        AuthResponse with authentication result
    """
    try:
        # Verify credentials
        is_valid = verify_credentials(auth_request.username, auth_request.password)
        
        if is_valid:
            logger.info(f"AUTH | SUCCESS | username={auth_request.username}")
            return AuthResponse(
                authenticated=True,
                message="Authentication successful",
                username=auth_request.username
            )
        else:
            logger.warning(f"AUTH | FAILED | username={auth_request.username}")
            return AuthResponse(
                authenticated=False,
                message="Invalid username or password",
                username=None
            )
    
    except Exception as e:
        logger.error(f"AUTH | ERROR | username={auth_request.username} | error={e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication service error"
        )


class ReplyTrackerRequest(BaseModel):
    """Request model for reply tracker endpoint."""
    thread_id: str = Field(..., description="Thread ID of the email to mark as replied")
    time: str | None = Field(None, description="Timestamp when reply was received (ISO format)", alias="replied_at")
    replied_at: str | None = Field(None, description="Timestamp when reply was received (ISO format)")
    
    model_config = {"populate_by_name": True}
    
    def get_timestamp(self) -> str:
        """Get the timestamp from either time or replied_at field."""
        return self.time or self.replied_at or ""


class ReplyTrackerResponse(BaseModel):
    """Response model for reply tracker endpoint."""
    status: str = Field(..., description="Status of the operation (success/not_found/error)")
    message: str = Field(..., description="Human-readable message")
    thread_id: str = Field(..., description="Thread ID that was processed")
    updated: bool = Field(..., description="Whether a record was updated")


@router.post("/mail_tracker/reply", response_model=ReplyTrackerResponse, tags=["mail_tracker"])
async def track_reply(
    payload: ReplyTrackerRequest,
    background_tasks: BackgroundTasks
) -> ReplyTrackerResponse:
    """
    Track email reply by thread_id.
    
    This endpoint marks an email as replied when a reply is received.
    It searches for the email by thread_id and updates the reply_status to "received"
    with the provided timestamp.
    
    The endpoint:
    1. Checks if thread_id exists in the database
    2. If yes, marks reply_status as "received" and notes the time
    3. Updates the global mail tracking sheet
    
    Args:
        payload: Reply tracker request with thread_id and timestamp
        background_tasks: FastAPI background tasks for async sheet updates
        
    Returns:
        ReplyTrackerResponse with operation status
    """
    from datetime import datetime
    
    thread_id = payload.thread_id
    
    # Log START
    logger.info(f"REPLY_TRACKER | START | thread_id={thread_id}")
    
    conn = None
    try:
        # Parse timestamp
        timestamp_str = payload.get_timestamp()
        if not timestamp_str:
            logger.error(f"REPLY_TRACKER | MISSING_TIME | thread_id={thread_id}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Either 'time' or 'replied_at' field is required"
            )
        
        try:
            received_at = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except ValueError as e:
            logger.error(f"REPLY_TRACKER | INVALID_TIME | thread_id={thread_id} | error={e}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid timestamp format: {timestamp_str}. Expected ISO format."
            )
        
        # Get database connection
        conn = get_conn()
        
        # Update reply status by thread_id
        updated = mail_tracker_repo.update_reply_status_by_thread_id(
            conn=conn,
            thread_id=thread_id,
            received_at=received_at
        )
        
        # Commit transaction
        conn.commit()
        
        if updated:
            # Schedule background task to update global mail tracking sheet
            if settings.global_mail_tracking_sheet_id:
                def update_sheet():
                    try:
                        # Get request_id for this thread_id to update the sheet
                        with get_conn() as sheet_conn:
                            with sheet_conn.cursor() as cur:
                                cur.execute(
                                    "SELECT request_id FROM mail_tracker WHERE thread_id = %s LIMIT 1",
                                    (thread_id,)
                                )
                                result = cur.fetchone()
                                if result:
                                    request_id = result[0]
                                    global_mail_tracking.export_to_sheet(
                                        sheet_id=settings.global_mail_tracking_sheet_id,
                                        request_id=request_id,
                                        mode="replace",
                                        suppress_log=False
                                    )
                                    logger.info(
                                        f"REPLY_TRACKER | GLOBAL_SHEET_UPDATED | thread_id={thread_id} | "
                                        f"request_id={request_id}"
                                    )
                    except Exception as sheet_error:
                        logger.error(
                            f"REPLY_TRACKER | GLOBAL_SHEET_ERROR | thread_id={thread_id} | "
                            f"error={sheet_error}"
                        )
                
                background_tasks.add_task(update_sheet)
            
            logger.info(f"REPLY_TRACKER | SUCCESS | thread_id={thread_id}")
            return ReplyTrackerResponse(
                status="success",
                message=f"Reply tracked successfully for thread_id: {thread_id}",
                thread_id=thread_id,
                updated=True
            )
        else:
            logger.warning(f"REPLY_TRACKER | NOT_FOUND | thread_id={thread_id}")
            return ReplyTrackerResponse(
                status="not_found",
                message=f"No email found with thread_id: {thread_id}",
                thread_id=thread_id,
                updated=False
            )
    
    except HTTPException:
        raise
    except Exception as e:
        # Rollback on any error
        if conn is not None:
            try:
                conn.rollback()
                logger.debug(f"REPLY_TRACKER | action=ROLLBACK | thread_id={thread_id}")
            except Exception as rollback_error:
                logger.error(
                    f"REPLY_TRACKER | action=ROLLBACK_ERROR | thread_id={thread_id} | error={rollback_error}"
                )
        
        logger.error(f"REPLY_TRACKER | ERROR | thread_id={thread_id} | error={e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to track reply: {str(e)}"
        )
    
    finally:
        # Always close connection
        if conn is not None:
            try:
                conn.close()
            except Exception as close_error:
                logger.error(
                    f"REPLY_TRACKER | action=CLOSE_ERROR | thread_id={thread_id} | error={close_error}"
                )
