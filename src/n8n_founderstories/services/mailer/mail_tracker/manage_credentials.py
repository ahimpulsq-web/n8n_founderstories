#!/usr/bin/env python3
"""
Utility script to manage mail tracker credentials.

Usage:
    python manage_credentials.py add <username> <password>
    python manage_credentials.py update <username> <new_password>
    python manage_credentials.py remove <username>
    python manage_credentials.py list
"""

import sys
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from n8n_founderstories.services.mailer.mail_tracker.auth import (
    add_user,
    update_password,
    remove_user,
    load_credentials
)


def list_users():
    """List all users in the credentials file."""
    try:
        credentials = load_credentials()
        print(f"\nTotal users: {len(credentials)}")
        print("\nUsernames:")
        for username in credentials.keys():
            print(f"  - {username}")
        print()
    except FileNotFoundError:
        print("Error: Credentials file not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def add_user_cmd(username: str, password: str):
    """Add a new user."""
    if add_user(username, password):
        print(f"✓ User '{username}' added successfully")
    else:
        print(f"✗ User '{username}' already exists")
        sys.exit(1)


def update_password_cmd(username: str, new_password: str):
    """Update user password."""
    if update_password(username, new_password):
        print(f"✓ Password updated for user '{username}'")
    else:
        print(f"✗ User '{username}' not found")
        sys.exit(1)


def remove_user_cmd(username: str):
    """Remove a user."""
    if remove_user(username):
        print(f"✓ User '{username}' removed successfully")
    else:
        print(f"✗ User '{username}' not found")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Manage mail tracker credentials",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python manage_credentials.py list
  python manage_credentials.py add john password123
  python manage_credentials.py update john newpassword456
  python manage_credentials.py remove john
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # List command
    subparsers.add_parser('list', help='List all users')
    
    # Add command
    add_parser = subparsers.add_parser('add', help='Add a new user')
    add_parser.add_argument('username', help='Username to add')
    add_parser.add_argument('password', help='Password for the user')
    
    # Update command
    update_parser = subparsers.add_parser('update', help='Update user password')
    update_parser.add_argument('username', help='Username to update')
    update_parser.add_argument('password', help='New password')
    
    # Remove command
    remove_parser = subparsers.add_parser('remove', help='Remove a user')
    remove_parser.add_argument('username', help='Username to remove')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'list':
        list_users()
    elif args.command == 'add':
        add_user_cmd(args.username, args.password)
    elif args.command == 'update':
        update_password_cmd(args.username, args.password)
    elif args.command == 'remove':
        remove_user_cmd(args.username)


if __name__ == '__main__':
    main()