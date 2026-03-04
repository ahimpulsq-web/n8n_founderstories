"""
Configuration and Constants for Deterministic Email Extraction

This module centralizes all configuration parameters, constants, and tunable settings
for the deterministic email extraction system. This approach provides:
- Single source of truth for all configuration
- Easy tuning without code changes
- Clear documentation of all parameters
- Type-safe configuration access

Author: N8N FounderStories Team
Last Modified: 2026-02-18
"""

from __future__ import annotations

from typing import Dict, FrozenSet, Set
from dataclasses import dataclass, field


# ============================================================================
# EXTRACTION LIMITS
# ============================================================================

@dataclass(frozen=True)
class ExtractionLimits:
    """
    Limits for email extraction to prevent resource exhaustion and ensure quality.
    
    Attributes:
        max_emails: Maximum number of emails to extract per domain
        max_text_length: Maximum text length to process (characters)
        max_candidates_per_page: Maximum email candidates to process per page
    """
    max_emails: int = 5
    max_text_length: int = 1_000_000  # 1MB of text
    max_candidates_per_page: int = 100


# ============================================================================
# EMAIL VALIDATION RULES
# ============================================================================

@dataclass(frozen=True)
class EmailValidationRules:
    """
    Rules for validating and filtering email addresses.
    
    These rules help filter out false positives, spam traps, and invalid emails
    that commonly appear in web scraping scenarios.
    """
    
    # Local-part prefixes that indicate non-human/system emails
    deny_localpart_prefixes: FrozenSet[str] = field(default_factory=lambda: frozenset({
        "noreply",
        "no-reply",
        "donotreply",
        "do-not-reply",
        "mailer-daemon",
        "postmaster",
        "bounce",
        "bounces",
        "daemon",
        "abuse",
        "spam",
        "webmaster",
    }))
    
    # Domain suffixes that indicate file assets, not emails
    deny_domain_suffixes: FrozenSet[str] = field(default_factory=lambda: frozenset({
        ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico",
        ".css", ".js", ".map", ".json",
        ".pdf", ".zip", ".rar", ".7z", ".gz", ".tar",
        ".woff", ".woff2", ".ttf", ".eot", ".otf",
        ".mp3", ".mp4", ".mov", ".avi", ".mkv", ".webm",
        ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    }))
    
    # File extensions that should never be treated as email TLDs
    asset_tlds: FrozenSet[str] = field(default_factory=lambda: frozenset({
        "png", "jpg", "jpeg", "gif", "svg", "webp", "ico",
        "css", "js", "map", "json",
        "pdf", "zip", "rar", "7z", "gz", "tar",
        "woff", "woff2", "ttf", "eot", "otf",
        "mp3", "mp4", "mov", "avi", "mkv", "webm",
        "doc", "docx", "xls", "xlsx", "ppt", "pptx",
    }))
    
    # Minimum valid email length (e.g., "a@b.co" = 6 chars)
    min_email_length: int = 6
    
    # Maximum valid email length (RFC 5321 limit is 254)
    max_email_length: int = 254
    
    # Minimum domain parts (e.g., "example.com" = 2 parts)
    min_domain_parts: int = 2


# ============================================================================
# EMAIL PRIORITIZATION
# ============================================================================

@dataclass(frozen=True)
class EmailPrioritization:
    """
    Configuration for email ranking and prioritization.
    
    This determines which emails are considered more valuable based on:
    - Local-part patterns (info@, contact@, etc.)
    - Page type where email was found
    - Domain matching with company domain
    """
    
    # Local-part priority mapping (lower number = higher priority)
    # These are common business contact email patterns
    localpart_priority: Dict[str, int] = field(default_factory=lambda: {
        "info": 0,           # Most common business contact
        "hallo": 1,          # German greeting (common in DACH region)
        "kontakt": 2,        # German for "contact"
        "contact": 2,        # English contact
        "hello": 3,          # English greeting
        "mail": 4,           # Generic mail
        "office": 5,         # Office contact
        "service": 6,        # Service contact
        "support": 7,        # Support contact
        "sales": 8,          # Sales contact
        "inquiry": 9,        # Inquiry contact
        "anfrage": 9,        # German for "inquiry"
    })
    
    # Page type priority scores (higher = more likely to contain valid emails)
    page_type_priority: Dict[str, int] = field(default_factory=lambda: {
        "impressum": 100,    # German legal notice (highest priority)
        "imprint": 100,      # English equivalent
        "contact": 90,       # Contact page
        "kontakt": 90,       # German contact page
        "privacy": 60,       # Privacy policy
        "datenschutz": 60,   # German privacy policy
        "home": 40,          # Homepage
        "about": 20,         # About page
        "ueber-uns": 20,     # German about page
        "team": 20,          # Team page
        "default": 10,       # Default for unknown pages
    })
    
    # Domain match bonus (added to priority score)
    domain_match_bonus: int = 1000


# ============================================================================
# TEXT NORMALIZATION
# ============================================================================

@dataclass(frozen=True)
class TextNormalization:
    """
    Configuration for text cleaning and normalization.
    
    This handles various obfuscation techniques used to hide emails from bots.
    """
    
    # Common obfuscation patterns and their replacements
    # Format: (pattern, replacement)
    obfuscation_patterns: tuple = field(default_factory=lambda: (
        # [at] and (at) patterns
        (r"\s*\[\s*at\s*\]\s*", "@"),
        (r"\s*\(\s*at\s*\)\s*", "@"),
        (r"\s+at\s+", "@"),
        (r"\s*\{\s*at\s*\}\s*", "@"),
        
        # [dot] and (dot) patterns
        (r"\s*\[\s*dot\s*\]\s*", "."),
        (r"\s*\(\s*dot\s*\)\s*", "."),
        (r"\s+dot\s+", "."),
        (r"\s*\{\s*dot\s*\}\s*", "."),
        
        # [punkt] and (punkt) patterns (German)
        (r"\s*\[\s*punkt\s*\]\s*", "."),
        (r"\s*\(\s*punkt\s*\)\s*", "."),
        (r"\s+punkt\s+", "."),
    ))
    
    # Additional text replacements (applied after regex patterns)
    text_replacements: Dict[str, str] = field(default_factory=lambda: {
        "{at}": "@",
        "{dot}": ".",
        "[at]": "@",
        "[dot]": ".",
        "(at)": "@",
        "(dot)": ".",
    })
    
    # Characters to strip from email boundaries
    boundary_chars: str = " \t\r\n<>\"'()[]{}.,;:!?"


# ============================================================================
# PERFORMANCE TUNING
# ============================================================================

@dataclass(frozen=True)
class PerformanceConfig:
    """
    Performance-related configuration for optimization.
    
    These settings help balance extraction quality with processing speed.
    """
    
    # Enable/disable various optimization features
    enable_caching: bool = True
    enable_metrics: bool = True
    enable_detailed_logging: bool = False
    
    # Timeout settings (in seconds)
    extraction_timeout: float = 30.0
    page_processing_timeout: float = 5.0
    
    # Batch processing settings
    batch_size: int = 10
    max_concurrent_pages: int = 5


# ============================================================================
# GLOBAL CONFIGURATION INSTANCE
# ============================================================================

class DeterministicConfig:
    """
    Global configuration container for deterministic email extraction.
    
    This class provides a single access point for all configuration settings.
    All settings are immutable (frozen dataclasses) to prevent accidental modification.
    
    Usage:
        from .config import config
        
        max_emails = config.limits.max_emails
        deny_prefixes = config.validation.deny_localpart_prefixes
    """
    
    def __init__(self):
        self.limits = ExtractionLimits()
        self.validation = EmailValidationRules()
        self.prioritization = EmailPrioritization()
        self.normalization = TextNormalization()
        self.performance = PerformanceConfig()
    
    def __repr__(self) -> str:
        return (
            f"DeterministicConfig(\n"
            f"  limits={self.limits},\n"
            f"  validation={self.validation},\n"
            f"  prioritization={self.prioritization},\n"
            f"  normalization={self.normalization},\n"
            f"  performance={self.performance}\n"
            f")"
        )


# Global configuration instance
# Import this in other modules: from .config import config
config = DeterministicConfig()


# ============================================================================
# CONFIGURATION VALIDATION
# ============================================================================

def validate_config() -> bool:
    """
    Validate the global configuration for consistency and correctness.
    
    Returns:
        bool: True if configuration is valid, raises ValueError otherwise
        
    Raises:
        ValueError: If configuration contains invalid values
    """
    # Validate limits
    if config.limits.max_emails < 1:
        raise ValueError("max_emails must be at least 1")
    
    if config.limits.max_text_length < 1000:
        raise ValueError("max_text_length must be at least 1000")
    
    # Validate validation rules
    if config.validation.min_email_length < 3:
        raise ValueError("min_email_length must be at least 3")
    
    if config.validation.max_email_length > 254:
        raise ValueError("max_email_length cannot exceed 254 (RFC 5321 limit)")
    
    # Validate performance settings
    if config.performance.extraction_timeout < 1.0:
        raise ValueError("extraction_timeout must be at least 1.0 second")
    
    return True


# Validate configuration on module import
validate_config()