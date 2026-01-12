#!/usr/bin/env python3
"""
Migration runner entry point.

This script can be run directly to apply database migrations:
    python -m src.n8n_founderstories.services.database.migrations

Or from the project root:
    python -m src.n8n_founderstories.services.database.migrations
"""

from .apply_migrations import main

if __name__ == "__main__":
    main()