"""
Google Maps Places discovery service package.

Provides complete Google Maps Places API integration for B2B lead discovery.
Handles API calls, rate limiting, retry logic, data persistence,
and Google Sheets export.

Package structure:
    ├── runner.py           # High-level entrypoint
    ├── orchestrator.py     # Workflow coordination
    ├── parser.py           # Search plan → GooglePlacesInput
    ├── models.py           # Data models
    ├── client.py           # HTTP client
    ├── policy.py           # Retry + rate limiting
    └── repo.py             # Database persistence

Architecture:
    API Layer
         ↓
    Runner (runner.py)
         ↓
    Parser (parser.py) → GooglePlacesInput
         ↓
    Orchestrator (orchestrator.py)
         ↓
    ├─> Policy (policy.py) → Client (client.py) → Google Maps API
    ├─> Repo (repo.py) → PostgreSQL
    └─> sheets.exports.google_maps → Google Sheets

Separation of concerns:
- Google Maps package: API calls, DB persistence, job lifecycle
- Sheets package: Export coordination, data fetching, sheet layout
- No direct sheet writing in Google Maps package
- No Google Maps API calls in Sheets package

Usage:
    from services.sources.google_maps.runner import run_google_places_from_search_plan
    
    results = run_google_places_from_search_plan(
        search_plan={"request_id": "req_123", ...},
        job_id="gmap__abc123",
    )
"""