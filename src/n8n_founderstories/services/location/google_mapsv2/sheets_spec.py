"""
Google Sheets export specification for Google Maps Places V2 results.

Defines sheet layout and headers for exporting Google Maps Places results.
Data fetching logic has been moved to data_fetcher.py for separation of concerns.
"""

# Sheet configuration
TAB_NAME = "googlemaps_places"
HEADERS = ["organization", "website", "location", "description", "text_query"]