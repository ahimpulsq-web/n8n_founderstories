"""
Validation modules for deterministic email extraction.

This package contains validators and filters for email addresses,
ensuring only valid and relevant emails are extracted.

Modules:
    email_validator: Email validation and quality checks
    filters: Filtering logic for deny lists and patterns
"""

from .email_validator import (
    is_valid_email,
    is_plausible_email,
    validate_email_structure,
    EmailValidationResult,
)
from .filters import (
    should_filter_email,
    is_system_email,
    is_asset_email,
    filter_email_list,
)

__all__ = [
    "is_valid_email",
    "is_plausible_email",
    "validate_email_structure",
    "EmailValidationResult",
    "should_filter_email",
    "is_system_email",
    "is_asset_email",
    "filter_email_list",
]