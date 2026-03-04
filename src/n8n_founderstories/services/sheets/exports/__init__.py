"""
Export orchestration package for Google Sheets.

Contains glue modules that coordinate data fetching, formatting,
and writing to Google Sheets. Each export module combines:
- Data fetcher (database queries + sorting)
- Sheet spec (layout + formatting)
- Writer (Google Sheets API calls)
"""