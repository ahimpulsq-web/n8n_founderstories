"""
Centralized error handling for N8N-FounderStories.

This module provides:
- Base exception classes with structured error information
- HTTP error mapping for API responses
- Standardized error codes and messages
- Error context tracking for debugging
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


class ErrorCode(str, Enum):
    """Standardized error codes for consistent error handling."""
    
    # Configuration Errors (5xx)
    CONFIG_MISSING_API_KEY = "CONFIG_MISSING_API_KEY"
    CONFIG_INVALID_SETTINGS = "CONFIG_INVALID_SETTINGS"
    CONFIG_SERVICE_UNAVAILABLE = "CONFIG_SERVICE_UNAVAILABLE"
    
    # Validation Errors (4xx)
    VALIDATION_REQUIRED_FIELD = "VALIDATION_REQUIRED_FIELD"
    VALIDATION_INVALID_FORMAT = "VALIDATION_INVALID_FORMAT"
    VALIDATION_OUT_OF_RANGE = "VALIDATION_OUT_OF_RANGE"
    
    # Provider Errors (5xx)
    PROVIDER_API_ERROR = "PROVIDER_API_ERROR"
    PROVIDER_RATE_LIMITED = "PROVIDER_RATE_LIMITED"
    PROVIDER_QUOTA_EXCEEDED = "PROVIDER_QUOTA_EXCEEDED"
    PROVIDER_TIMEOUT = "PROVIDER_TIMEOUT"
    
    # Job Errors (4xx/5xx)
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    JOB_ALREADY_RUNNING = "JOB_ALREADY_RUNNING"
    JOB_FAILED = "JOB_FAILED"
    
    # Data Errors (4xx)
    DATA_NOT_FOUND = "DATA_NOT_FOUND"
    DATA_PROCESSING_ERROR = "DATA_PROCESSING_ERROR"
    DATA_EXPORT_ERROR = "DATA_EXPORT_ERROR"
    
    # Generic Errors
    INTERNAL_ERROR = "INTERNAL_ERROR"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


class BaseN8NError(Exception):
    """
    Base exception class for all N8N-FounderStories errors.
    
    Provides structured error information including:
    - Error code for programmatic handling
    - User-friendly message
    - Technical details for debugging
    - Context information
    """
    
    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.UNKNOWN_ERROR,
        details: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.context = context or {}
        self.cause = cause
        self.timestamp = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for JSON serialization."""
        return {
            "error_code": self.error_code.value,
            "message": self.message,
            "details": self.details,
            "context": self.context,
            "timestamp": self.timestamp.isoformat(),
            "cause": str(self.cause) if self.cause else None
        }
    
    def to_http_exception(self) -> HTTPException:
        """Convert to FastAPI HTTPException with appropriate status code."""
        status_code = self._get_http_status_code()
        return HTTPException(
            status_code=status_code,
            detail={
                "error_code": self.error_code.value,
                "message": self.message,
                "details": self.details,
                "timestamp": self.timestamp.isoformat()
            }
        )
    
    def _get_http_status_code(self) -> int:
        """Map error code to HTTP status code."""
        if self.error_code in [
            ErrorCode.CONFIG_MISSING_API_KEY,
            ErrorCode.CONFIG_INVALID_SETTINGS,
            ErrorCode.CONFIG_SERVICE_UNAVAILABLE,
            ErrorCode.PROVIDER_API_ERROR,
            ErrorCode.PROVIDER_TIMEOUT,
            ErrorCode.JOB_FAILED,
            ErrorCode.DATA_EXPORT_ERROR,
            ErrorCode.INTERNAL_ERROR,
            ErrorCode.UNKNOWN_ERROR
        ]:
            return 500
        elif self.error_code in [
            ErrorCode.PROVIDER_RATE_LIMITED,
            ErrorCode.PROVIDER_QUOTA_EXCEEDED
        ]:
            return 429
        elif self.error_code in [
            ErrorCode.JOB_NOT_FOUND,
            ErrorCode.DATA_NOT_FOUND
        ]:
            return 404
        elif self.error_code in [
            ErrorCode.JOB_ALREADY_RUNNING
        ]:
            return 409
        else:  # Validation errors
            return 400


# Configuration Errors
class ConfigError(BaseN8NError):
    """Configuration-related errors."""
    pass


class MissingAPIKeyError(ConfigError):
    """Missing required API key."""
    
    def __init__(self, service: str, key_name: str):
        super().__init__(
            message=f"{service} API key is not configured. Please set {key_name} environment variable.",
            error_code=ErrorCode.CONFIG_MISSING_API_KEY,
            details={"service": service, "key_name": key_name}
        )


# Validation Errors
class ValidationError(BaseN8NError):
    """Input validation errors."""
    pass


class RequiredFieldError(ValidationError):
    """Required field is missing or empty."""
    
    def __init__(self, field_name: str, field_path: str = None):
        path = f" at {field_path}" if field_path else ""
        super().__init__(
            message=f"Required field '{field_name}'{path} is missing or empty.",
            error_code=ErrorCode.VALIDATION_REQUIRED_FIELD,
            details={"field_name": field_name, "field_path": field_path}
        )


class InvalidFormatError(ValidationError):
    """Invalid field format."""
    
    def __init__(self, field_name: str, expected_format: str, actual_value: str = None):
        super().__init__(
            message=f"Field '{field_name}' has invalid format. Expected: {expected_format}.",
            error_code=ErrorCode.VALIDATION_INVALID_FORMAT,
            details={
                "field_name": field_name,
                "expected_format": expected_format,
                "actual_value": actual_value
            }
        )


# Provider Errors
class ProviderError(BaseN8NError):
    """External provider/API errors."""
    pass


class ProviderAPIError(ProviderError):
    """External API returned an error."""
    
    def __init__(self, provider: str, status_code: int = None, response: str = None):
        super().__init__(
            message=f"{provider} API error. Please try again later.",
            error_code=ErrorCode.PROVIDER_API_ERROR,
            details={
                "provider": provider,
                "status_code": status_code,
                "response": response
            }
        )


class ProviderRateLimitError(ProviderError):
    """Provider rate limit exceeded."""
    
    def __init__(self, provider: str, retry_after: int = None):
        super().__init__(
            message=f"{provider} rate limit exceeded. Please try again later.",
            error_code=ErrorCode.PROVIDER_RATE_LIMITED,
            details={"provider": provider, "retry_after": retry_after}
        )


# Job Errors
class JobError(BaseN8NError):
    """Job processing errors."""
    pass


class JobNotFoundError(JobError):
    """Job not found."""
    
    def __init__(self, job_id: str):
        super().__init__(
            message=f"Job '{job_id}' not found.",
            error_code=ErrorCode.JOB_NOT_FOUND,
            details={"job_id": job_id}
        )


class JobAlreadyRunningError(JobError):
    """Job is already running."""
    
    def __init__(self, job_id: str):
        super().__init__(
            message=f"Job '{job_id}' is already running.",
            error_code=ErrorCode.JOB_ALREADY_RUNNING,
            details={"job_id": job_id}
        )


# Data Errors
class DataError(BaseN8NError):
    """Data processing errors."""
    pass


class DataNotFoundError(DataError):
    """Requested data not found."""
    
    def __init__(self, resource: str, identifier: str = None):
        message = f"{resource} not found"
        if identifier:
            message += f": {identifier}"
        super().__init__(
            message=message,
            error_code=ErrorCode.DATA_NOT_FOUND,
            details={"resource": resource, "identifier": identifier}
        )


# Error Handler for FastAPI
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Global exception handler for FastAPI application.
    
    Converts all exceptions to standardized JSON responses.
    """
    # Log the error
    logger.exception(
        "UNHANDLED_EXCEPTION | path=%s | method=%s",
        request.url.path,
        request.method
    )
    
    # Handle our custom exceptions
    if isinstance(exc, BaseN8NError):
        return JSONResponse(
            status_code=exc._get_http_status_code(),
            content={
                "detail": exc.message,
                "error_code": exc.error_code.value,
                "timestamp": exc.timestamp.isoformat(),
                "details": exc.details
            }
        )
    
    # Handle FastAPI HTTPExceptions
    if isinstance(exc, HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "detail": exc.detail,
                "error_code": "HTTP_EXCEPTION",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )
    
    # Handle all other exceptions
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error. Please try again later.",
            "error_code": ErrorCode.INTERNAL_ERROR.value,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    )


# Utility functions for common error scenarios
def require_api_key(service: str, key_name: str, key_value: str = None) -> None:
    """Validate that an API key is configured."""
    if not key_value:
        raise MissingAPIKeyError(service, key_name)


def require_field(field_name: str, field_value: Any, field_path: str = None) -> None:
    """Validate that a required field is present and not empty."""
    if field_value is None or (isinstance(field_value, str) and not field_value.strip()):
        raise RequiredFieldError(field_name, field_path)


def validate_job_exists(job_id: str, job: Any) -> None:
    """Validate that a job exists."""
    if not job:
        raise JobNotFoundError(job_id)