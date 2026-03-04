"""Authentication module for mail tracker."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict
import hashlib

logger = logging.getLogger(__name__)

# Default credentials file path (relative to project root)
DEFAULT_CREDENTIALS_FILE = Path("src/n8n_founderstories/services/mailer/mail_tracker/credentials.json")


def _hash_password(password: str) -> str:
    """
    Hash a password using SHA-256.
    
    Args:
        password: Plain text password
        
    Returns:
        Hashed password as hex string
    """
    return hashlib.sha256(password.encode()).hexdigest()


def load_credentials(credentials_file: Path | None = None) -> Dict[str, str]:
    """
    Load credentials from JSON file.
    
    Args:
        credentials_file: Path to credentials file. If None, uses default path.
        
    Returns:
        Dictionary mapping usernames to hashed passwords
        
    Raises:
        FileNotFoundError: If credentials file doesn't exist
        json.JSONDecodeError: If credentials file is invalid JSON
    """
    if credentials_file is None:
        credentials_file = DEFAULT_CREDENTIALS_FILE
    
    if not credentials_file.exists():
        logger.warning(f"Credentials file not found: {credentials_file}")
        raise FileNotFoundError(f"Credentials file not found: {credentials_file}")
    
    try:
        with open(credentials_file, 'r') as f:
            credentials = json.load(f)
        
        logger.info(f"Loaded {len(credentials)} credentials from {credentials_file}")
        return credentials
    
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in credentials file: {e}")
        raise


def save_credentials(credentials: Dict[str, str], credentials_file: Path | None = None) -> None:
    """
    Save credentials to JSON file.
    
    Args:
        credentials: Dictionary mapping usernames to hashed passwords
        credentials_file: Path to credentials file. If None, uses default path.
    """
    if credentials_file is None:
        credentials_file = DEFAULT_CREDENTIALS_FILE
    
    # Ensure directory exists
    credentials_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(credentials_file, 'w') as f:
            json.dump(credentials, f, indent=2)
        
        logger.info(f"Saved {len(credentials)} credentials to {credentials_file}")
    
    except Exception as e:
        logger.error(f"Failed to save credentials: {e}")
        raise


def verify_credentials(username: str, password: str, credentials_file: Path | None = None) -> bool:
    """
    Verify username and password against stored credentials.
    
    Args:
        username: Username to verify
        password: Plain text password to verify
        credentials_file: Path to credentials file. If None, uses default path.
        
    Returns:
        True if credentials are valid, False otherwise
    """
    try:
        credentials = load_credentials(credentials_file)
        
        # Check if username exists
        if username not in credentials:
            logger.warning(f"Authentication failed: username '{username}' not found")
            return False
        
        # Hash the provided password and compare
        hashed_password = _hash_password(password)
        
        if credentials[username] == hashed_password:
            logger.info(f"Authentication successful for user: {username}")
            return True
        else:
            logger.warning(f"Authentication failed: invalid password for user '{username}'")
            return False
    
    except FileNotFoundError:
        logger.error("Credentials file not found")
        return False
    
    except Exception as e:
        logger.error(f"Error during authentication: {e}")
        return False


def add_user(username: str, password: str, credentials_file: Path | None = None) -> bool:
    """
    Add a new user to the credentials file.
    
    Args:
        username: Username to add
        password: Plain text password (will be hashed)
        credentials_file: Path to credentials file. If None, uses default path.
        
    Returns:
        True if user was added successfully, False if user already exists
    """
    try:
        # Load existing credentials or create empty dict
        try:
            credentials = load_credentials(credentials_file)
        except FileNotFoundError:
            credentials = {}
        
        # Check if user already exists
        if username in credentials:
            logger.warning(f"User '{username}' already exists")
            return False
        
        # Add new user with hashed password
        credentials[username] = _hash_password(password)
        save_credentials(credentials, credentials_file)
        
        logger.info(f"Added new user: {username}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to add user: {e}")
        return False


def update_password(username: str, new_password: str, credentials_file: Path | None = None) -> bool:
    """
    Update password for an existing user.
    
    Args:
        username: Username to update
        new_password: New plain text password (will be hashed)
        credentials_file: Path to credentials file. If None, uses default path.
        
    Returns:
        True if password was updated successfully, False if user doesn't exist
    """
    try:
        credentials = load_credentials(credentials_file)
        
        # Check if user exists
        if username not in credentials:
            logger.warning(f"User '{username}' not found")
            return False
        
        # Update password
        credentials[username] = _hash_password(new_password)
        save_credentials(credentials, credentials_file)
        
        logger.info(f"Updated password for user: {username}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to update password: {e}")
        return False


def remove_user(username: str, credentials_file: Path | None = None) -> bool:
    """
    Remove a user from the credentials file.
    
    Args:
        username: Username to remove
        credentials_file: Path to credentials file. If None, uses default path.
        
    Returns:
        True if user was removed successfully, False if user doesn't exist
    """
    try:
        credentials = load_credentials(credentials_file)
        
        # Check if user exists
        if username not in credentials:
            logger.warning(f"User '{username}' not found")
            return False
        
        # Remove user
        del credentials[username]
        save_credentials(credentials, credentials_file)
        
        logger.info(f"Removed user: {username}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to remove user: {e}")
        return False