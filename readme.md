# FounderStories Backend

A Python backend that powers the FounderStories lead discovery and enrichment pipeline.  
Designed to be orchestrated by **n8n**, and structured as modular services for:
- lead discovery (e.g., Google Maps / web search),
- enrichment (e.g., Hunter.io),
- website-based contact extraction (ethical crawling posture),
- job tracking and exports (e.g., Google Sheets or other sinks).

> This repository is under active development. Interfaces and modules may evolve as the pipeline hardens.

---

## Table of Contents
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Local Development](#local-development)
- [Configuration](#configuration)
- [Running the Service](#running-the-service)
- [n8n Integration](#n8n-integration)
- [Operational Concerns](#operational-concerns)
- [Testing](#testing)
- [Deployment](#deployment)
- [Security](#security)
- [Ethics and Compliance](#ethics-and-compliance)
- [Roadmap](#roadmap)

---

## Architecture

**High-level flow**

1. n8n triggers the backend (HTTP calls).
2. The backend runs one pipeline action (discovery / enrichment / extraction).
3. Results are returned to n8n (and optionally persisted by the backend depending on configuration).
4. n8n writes results to Google Sheets / DB / CRM and continues the workflow.

**Design goals**
- Modular domain services (clean boundaries between discovery, enrichment, scraping).
- Strong observability (structured logs; job/run tracking).
- Safe-by-default crawling posture (rate limits, robots awareness where applicable).
- Configuration-driven execution for multiple environments (dev/staging/prod).

---

## Repository Structure
src/
n8n_founderstories/
api/ # API layer (routing, request/response)
core/ # config, logging, shared utilities
services/ # pipeline services (maps, hunter, scraping, jobs, exports)
requirements.txt
pyproject.toml

## Local Development

### Prerequisites
- Python 3.10+ (recommended)
- Git
- A virtual environment tool (`venv` / `virtualenv`)

### Setup (Windows PowerShell / CMD)

Create and activate venv (example):

```bat
python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt
```

### Running the Service

```bat
# Development mode with auto-reload
python -m uvicorn src.n8n_founderstories.main:app --reload --host 0.0.0.0 --port 8000

# Production mode
python -m uvicorn src.n8n_founderstories.main:app --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`
- Interactive API docs: `http://localhost:8000/docs`
- OpenAPI schema: `http://localhost:8000/openapi.json`

---

## Configuration

### Environment Variables

Create a `.env` file in the project root with the following configuration:

```env
# Application Settings
N8N_APP_NAME=N8N-FounderStories
N8N_ENVIRONMENT=development
N8N_LOG_LEVEL=INFO
N8N_HOST=0.0.0.0
N8N_PORT=8000
N8N_RELOAD=true

# Data Directory
N8N_DATA_DIR=./data

# LLM Configuration
N8N_LLM_PROVIDER=groq
GEMINI_API_KEY=your_gemini_api_key_here
GEMINI_MODEL_NAME=gemini-2.0-flash
GROQ_API_KEY=your_groq_api_key_here
GROQ_MODEL_NAME=llama-3.1-70b-versatile

# External API Keys
HUNTERIO_API_KEY=your_hunter_api_key_here
GOOGLE_MAPS_API_KEY=your_google_maps_api_key_here
SERPAPI_API_KEY=your_serpapi_api_key_here

# Google Sheets Integration
GOOGLE_SERVICE_ACCOUNT_FILE=path/to/your/service-account.json

# Google Sheets Formatting (Optional)
GOOGLE_SHEETS_HEADER_ROW_HEIGHT=30
GOOGLE_SHEETS_BODY_ROW_HEIGHT=21
GOOGLE_SHEETS_WRAP_STRATEGY=CLIP

# PostgreSQL Integration (Optional)
POSTGRES_DSN=postgresql://user:password@localhost:5432/n8n_founderstories
HUNTER_COMPANIES_DB_ENABLED=true
HUNTER_AUDIT_DB_ENABLED=true
```

### Google Service Account Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Enable Google Sheets API and Google Drive API
4. Create a Service Account and download the JSON key file
5. Share your Google Sheets with the service account email
6. Set the path to the JSON file in `GOOGLE_SERVICE_ACCOUNT_FILE`

### Google Sheets Formatting Options

The system provides configurable formatting options to optimize sheet readability and prevent excessive row heights:

```env
# Row Heights (pixels)
GOOGLE_SHEETS_HEADER_ROW_HEIGHT=30    # Header row height (15-100)
GOOGLE_SHEETS_BODY_ROW_HEIGHT=21      # Body row height (15-50)

# Text Wrapping Strategy
GOOGLE_SHEETS_WRAP_STRATEGY=CLIP      # CLIP, OVERFLOW_CELL, or WRAP
```

**Wrap Strategy Options:**
- `CLIP` - Truncate text that doesn't fit (prevents tall rows) ✅ **Recommended**
- `OVERFLOW_CELL` - Text overflows into adjacent empty cells
- `WRAP` - Wrap text within cell (may increase row height)

**Benefits of Optimized Formatting:**
- **Compact Layout**: Fixed row heights prevent excessive scrolling
- **Better Readability**: Consistent spacing and alignment
- **Faster Navigation**: Standard row heights make data scanning easier
- **Professional Appearance**: Clean, uniform table formatting

### PostgreSQL Integration Setup (Optional)

The system supports optional PostgreSQL integration for Hunter.io data, providing "Sheets parity" where the same data appears in both Google Sheets and PostgreSQL.

1. **Install PostgreSQL** (if not already installed)
2. **Create Database:**
   ```sql
   CREATE DATABASE n8n_founderstories;
   ```
3. **Set Environment Variables:**
   ```env
   POSTGRES_DSN=postgresql://user:password@localhost:5432/n8n_founderstories
   HUNTER_COMPANIES_DB_ENABLED=true
   HUNTER_AUDIT_DB_ENABLED=true
   ```
4. **Run Database Migrations:**
   ```bash
   python -m src.n8n_founderstories.services.database.migrations
   ```
5. **Verify Setup:**
   ```sql
   -- Check tables exist
   \dt
   
   -- Check migrations applied
   SELECT * FROM schema_migrations ORDER BY applied_at;
   ```

**Benefits:**
- **API Access**: Query Hunter data directly via REST endpoints
- **Safe-by-Default**: PostgreSQL failures never break Hunter jobs
- **Idempotent**: Duplicate data is handled gracefully
- **Sheets Parity**: Database mirrors exact Google Sheets content

For detailed setup and usage instructions, see [`docs/postgresql-integration.md`](docs/postgresql-integration.md).

---

## API Endpoints

### Base URL: `/api/v1`

#### 1. Prompt Processing
**POST** `/prompt`

Convert natural language prompts into structured search plans.

```json
{
  "prompt": "Find tech startups in Berlin with 10-50 employees",
  "source": "n8n",
  "request_id": "optional-unique-id",
  "new_excel": false,
  "llm_provider": "groq"
}
```

**Response:**
```json
{
  "status": "success",
  "source": "n8n",
  "request_id": "generated-uuid",
  "search_plan": {
    "industries": ["Technology"],
    "locations": ["Berlin, Germany"],
    "company_size": "10-50",
    "keywords": ["startup", "tech"]
  },
  "spreadsheet_id": "google-sheets-id",
  "spreadsheet_url": "https://docs.google.com/spreadsheets/d/..."
}
```

#### 2. Google Maps Discovery
**POST** `/google_maps/jobs`

Discover companies using Google Maps/Places API.

```json
{
  "search_plan": {
    "industries": ["Technology"],
    "locations": ["Berlin, Germany"],
    "keywords": ["software", "startup"]
  },
  "spreadsheet_id": "your-sheet-id",
  "mode": "discover",
  "max_queries": 5,
  "max_locations": 3,
  "max_results": 250,
  "dedupe_places": true
}
```

**Response:**
```json
{
  "status": "accepted",
  "job_id": "maps_abc123",
  "request_id": "your-request-id"
}
```

#### 3. Hunter.io Enrichment
**POST** `/hunter/jobs`

Enrich company data with contact information via Hunter.io.

```json
{
  "search_plan": {
    "industries": ["Technology"],
    "locations": ["Berlin, Germany"]
  },
  "spreadsheet_id": "your-sheet-id",
  "max_web_queries": 100,
  "target_unique_domains": 250,
  "max_cities_per_country": 4
}
```

#### 4. Google Search
**POST** `/google_search/jobs`

Discover companies through web search results.

```json
{
  "search_plan": {
    "industries": ["Technology"],
    "locations": ["Berlin, Germany"],
    "keywords": ["startup", "software"]
  },
  "spreadsheet_id": "your-sheet-id",
  "max_queries": 10,
  "max_results_per_query": 10,
  "max_total_results": 250,
  "dedupe_in_run": true,
  "use_cache": true
}
```

#### 5. Web Scraping
**POST** `/company_data_extractor/jobs`

Extract contact information from company websites.

```json
{
  "search_plan": {
    "request_id": "your-request-id"
  },
  "spreadsheet_id": "your-sheet-id",
  "sheet_title": "Master",
  "linger_seconds": 3.0,
  "max_empty_passes": 10,
  "window_rows": 500,
  "max_total_updates": 5000
}
```

#### 6. Master Data Aggregation
**POST** `/master/jobs`

Combine and deduplicate data from multiple sources.

```json
{
  "search_plan": {
    "request_id": "your-request-id"
  },
  "spreadsheet_id": "your-sheet-id",
  "source_tabs": ["HunterIO", "GoogleMaps", "GoogleSearch"],
  "apply_formatting": true,
  "hide_state_tab": true,
  "hide_audit_tabs": true,
  "reorder_tabs": true
}
```

#### 7. Job Status
**GET** `/jobs/{job_id}`

Check the status of background jobs.

**Response:**
```json
{
  "job_id": "maps_abc123",
  "tool": "google_maps",
  "request_id": "your-request-id",
  "state": "running",
  "progress": {
    "current": 50,
    "total": 100,
    "message": "Processing locations..."
  },
  "created_at": "2026-01-06T00:00:00Z",
  "started_at": "2026-01-06T00:01:00Z",
  "updated_at": "2026-01-06T00:05:00Z",
  "finished_at": null,
  "meta": {},
  "error": null
}
```

---

## n8n Integration

### Workflow Example

1. **Trigger**: HTTP request or schedule
2. **Prompt Processing**: POST to `/api/v1/prompt`
3. **Data Discovery**: POST to discovery endpoints (Maps, Search, Hunter)
4. **Job Monitoring**: Poll `/api/v1/jobs/{job_id}` until complete
5. **Data Extraction**: POST to `/api/v1/company_data_extractor/jobs`
6. **Master Aggregation**: POST to `/api/v1/master/jobs`
7. **Export**: Data automatically written to Google Sheets

### Error Handling

All endpoints return standardized error responses:

```json
{
  "detail": "Error description",
  "error_code": "VALIDATION_ERROR",
  "timestamp": "2026-01-06T00:00:00Z"
}
```

Common HTTP status codes:
- `200`: Success
- `202`: Accepted (background job started)
- `400`: Bad Request (validation error)
- `401`: Unauthorized (missing/invalid API key)
- `404`: Not Found (job/resource not found)
- `422`: Unprocessable Entity (invalid request format)
- `500`: Internal Server Error

---

## Running the Service

### Development Mode

```bat
python -m uvicorn src.n8n_founderstories.main:app --reload --host 0.0.0.0 --port 8000
```

### Production Mode

```bat
python -m uvicorn src.n8n_founderstories.main:app --host 0.0.0.0 --port 8000
```

---

## Testing

### Running Tests

```bat
# Install test dependencies
pip install pytest pytest-asyncio httpx

# Run all tests
pytest

# Run with coverage
pytest --cov=src/n8n_founderstories

# Run specific test file
pytest tests/test_sheets_preview.py
```

### API Testing

Use the interactive docs at `http://localhost:8000/docs` to test endpoints, or use curl:

```bash
# Test prompt endpoint
curl -X POST "http://localhost:8000/api/v1/prompt" \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "Find tech startups in Berlin",
    "source": "test"
  }'

# Check job status
curl "http://localhost:8000/api/v1/jobs/your-job-id"
```

