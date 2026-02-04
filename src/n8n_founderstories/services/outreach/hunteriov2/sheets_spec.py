"""
Google Sheets export specification for HunterIOV2 results.

Defines sheet layout and headers for exporting HunterIO results.
Data fetching logic has been moved to data_fetcher.py for separation of concerns.
"""

# Sheet configuration
TAB_NAME = "hunterio_results"
HEADERS = ["organization", "domain", "location", "headcount", "term"]