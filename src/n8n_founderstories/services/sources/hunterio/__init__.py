"""
Hunter.io lead discovery service package.

Provides complete Hunter.io API integration for B2B lead discovery.
Handles API calls, rate limiting, retry logic, data persistence,
and Google Sheets export.

Package structure:
    ├── api.py              # FastAPI router (thin API layer)
    ├── runner.py           # High-level entrypoint
    ├── orchestrator.py     # Workflow coordination
    ├── parser.py           # Search plan → HunterInput
    ├── models.py           # Data models
    ├── client.py           # HTTP client
    ├── policy.py           # Retry + rate limiting
    └── repo.py             # Database persistence

Architecture:
    API Layer (api.py)
         ↓
    Runner (runner.py)
         ↓
    Parser (parser.py) → HunterInput
         ↓
    Orchestrator (orchestrator.py)
         ↓
    ├─> Policy (policy.py) → Client (client.py) → Hunter.io API
    ├─> Repo (repo.py) → PostgreSQL
    └─> sheets.exports.hunterio → Google Sheets

Separation of concerns:
- Hunter package: API calls, DB persistence, job lifecycle
- Sheets package: Export coordination, data fetching, sheet layout
- No direct sheet writing in Hunter package
- No Hunter API calls in Sheets package

Usage:
    from services.sources.hunterio.runner import run_hunter_from_search_plan
    
    results = run_hunter_from_search_plan(
        search_plan={"request_id": "req_123", ...},
        job_id="htrio__abc123",
    )
"""