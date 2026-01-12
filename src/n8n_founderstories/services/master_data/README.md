# Master Data Service - DB-First Architecture

## Overview

The Master data service provides tool-agnostic result aggregation with a DB-first architecture. It replaces the previous Sheets-driven approach with a PostgreSQL-based system that:

- **Reads from tool DB tables** (Hunter, Google Maps, etc.) using adapters
- **Aggregates results** into `master_results` table with idempotent upserts
- **Tracks watermarks** per tool for incremental ingestion
- **Exports to Sheets** only at job end (optional)
- **Safe under parallel execution** - tools can run concurrently
- **Rerunnable** - can be called multiple times without duplicates

## Architecture

### Database Tables

#### `master_results`
Unified results from all tools with idempotent upserts.

**Key columns:**
- `id` (UUID, PK)
- `request_id` (TEXT, required) - Groups results by request
- `domain` (TEXT, required) - Primary deduplication key
- `company`, `website`, `location`, `lead_query` - Normalized fields
- `source_tool` (TEXT) - 'HunterIO', 'GoogleMaps', 'GoogleSearch'
- `source_ref` (TEXT) - Reference to source record (place_id, etc.)
- `dup_in_run` (TEXT) - 'YES' or 'NO' for duplicate tracking
- `created_at`, `updated_at` (TIMESTAMPTZ)

**Unique constraint:** `UNIQUE(request_id, LOWER(domain))`

#### `master_watermarks`
Per-tool watermarks for incremental ingestion.

**Key columns:**
- `request_id`, `source_tool` (composite key)
- `last_seen_created_at` (TIMESTAMPTZ) - Last processed timestamp
- `last_processed_count`, `total_processed` (INTEGER)

#### `master_sources`
Registry of known source tools and their configurations.

**Key columns:**
- `source_tool` (TEXT, unique) - Tool identifier
- `source_table` (TEXT) - DB table name
- `column_mapping` (JSONB) - Maps source columns to master fields
- `is_active` (BOOLEAN)

### Plugin Architecture

The adapter pattern makes adding new tools trivial:

```python
class NewToolAdapter(BaseSourceAdapter):
    @property
    def source_tool_name(self) -> str:
        return "NewTool"
    
    @property
    def source_table_name(self) -> str:
        return "new_tool_results"
    
    def normalize_to_master(self, source_row: Dict[str, Any]) -> Optional[MasterRow]:
        # Map source columns to Master schema
        return MasterRow(
            job_id=source_row["job_id"],
            request_id=source_row["request_id"],
            domain=source_row["domain"],
            company=source_row["company_name"],
            # ... etc
        )
```

Then register in `adapters.py`:

```python
def get_available_adapters(dsn: Optional[str] = None) -> List[BaseSourceAdapter]:
    return [
        HunterIOAdapter(dsn=dsn),
        GoogleMapsAdapter(dsn=dsn),
        NewToolAdapter(dsn=dsn),  # Add here
    ]
```

## Usage

### Running Master Job

```python
from n8n_founderstories.services.master_data import run_master_job_db_first

run_master_job_db_first(
    job_id="master_job_123",
    request_id="req_456",
    spreadsheet_id="1abc...",
    source_tools=["HunterIO", "GoogleMaps"],  # Optional, auto-detects if None
    window_size=500,  # Rows per adapter per pass
    max_empty_passes=10,  # Stop after N passes with no data
    export_to_sheets=True,  # Export at end
)
```

### Auto-Detection

If `source_tools=None`, Master auto-detects which tools have data:

```python
run_master_job_db_first(
    job_id="master_job_123",
    request_id="req_456",
    spreadsheet_id="1abc...",
    # Auto-detects Hunter, Maps, etc.
)
```

### Incremental Ingestion

Master uses watermarks for incremental reads:

1. First run: reads all rows from source tables
2. Subsequent runs: reads only rows with `created_at > last_watermark`
3. Upserts into `master_results` (updates existing, inserts new)
4. Updates watermark to latest `created_at`

### Sheets Export

When `export_to_sheets=True` and `settings.master_sheets_export_enabled=True`:

- Creates `Master_v2` tab with all results
- Creates `Master_Audit_v2` tab with summary statistics
- Export happens **only at job end** (not during ingestion)
- DB is system of record; Sheets is export/view layer

## Migration

### Running Migrations

```bash
# Set environment variable
export POSTGRES_DSN="postgresql://user:pass@localhost:5432/dbname"

# Run migrations
python -m n8n_founderstories.services.database.migrations.apply_migrations
```

Or programmatically:

```python
from n8n_founderstories.services.database.migrations.apply_migrations import run_migrations

run_migrations(dsn="postgresql://...")
```

### Migration Files

Located in `services/database/migrations/master/`:

- `0001_create_master_tables.sql` - Creates all Master tables

## Configuration

Add to `.env`:

```bash
# Enable Master Sheets export (default: true)
MASTER_SHEETS_EXPORT_ENABLED=true

# PostgreSQL connection (required)
POSTGRES_DSN=postgresql://user:pass@localhost:5432/dbname
```

## Concurrency Safety

Master is safe under parallel tool execution:

- **Idempotent upserts**: `ON CONFLICT DO UPDATE` prevents duplicates
- **Per-tool watermarks**: Each tool tracks its own progress
- **Eventually consistent**: Master reads tool results as they arrive
- **Rerunnable**: Can be called multiple times safely

## Stop Conditions

Master stops when:

1. **Max empty passes reached**: No new data for N consecutive passes
2. **Source jobs complete**: All source tools are no longer RUNNING/QUEUED
3. **Max passes reached**: Optional hard limit on total passes

## Adding a New Tool (Google Search Example)

### 1. Ensure DB table exists

```sql
CREATE TABLE google_search_results (
    id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    job_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    domain TEXT,
    organisation TEXT,
    website TEXT,
    search_query TEXT,
    -- ... other fields
);
```

### 2. Create adapter

Already exists as `GoogleSearchAdapter` in `adapters.py` - just uncomment in `get_available_adapters()`.

### 3. Register in master_sources

```sql
INSERT INTO master_sources (source_tool, display_name, source_table, is_active, column_mapping)
VALUES (
    'GoogleSearch',
    'Google Search',
    'google_search_results',
    true,
    '{"company": "organisation", "domain": "domain", "website": "website", "lead_query": "search_query"}'::jsonb
);
```

### 4. Done!

Master will automatically detect and process Google Search results.

## Troubleshooting

### No data ingested

Check:
1. Source tables have data for the request_id
2. Adapters are registered in `get_available_adapters()`
3. Source tables exist in DB
4. Watermarks aren't blocking (check `master_watermarks` table)

### Duplicates appearing

Check:
1. `UNIQUE(request_id, LOWER(domain))` constraint exists
2. Domain normalization is consistent
3. Upsert logic uses correct conflict target

### Watermarks not updating

Check:
1. `created_at` column exists in source tables
2. Watermark repository has DB access
3. No errors in logs during watermark update

## Developer Notes

### How to add a new tool adapter

See "Adding a New Tool" section above. The key steps are:

1. Create adapter class inheriting from `BaseSourceAdapter`
2. Implement `source_tool_name`, `source_table_name`, `normalize_to_master`
3. Register in `get_available_adapters()`
4. Optionally add to `master_sources` table

### Testing adapters

```python
from n8n_founderstories.services.master_data.adapters import HunterIOAdapter

adapter = HunterIOAdapter()

# Check table exists
assert adapter.table_exists()

# Fetch rows
success, error, rows, watermark = adapter.fetch_rows_after_watermark(
    request_id="test_req",
    watermark=None,
    limit=10
)

# Normalize
for row in rows:
    master_row = adapter.normalize_to_master(row)
    print(master_row)
```

### Sheets export format

**Master_v2 columns:**
1. Company Name
2. Primary Domain
3. Website URL
4. Source Tool
5. Location
6. Lead Source Query
7. Duplicate (This Run)

**Master_Audit_v2 columns:**
1. Request ID
2. Source Tool
3. Total Rows
4. Unique Domains
5. Duplicates
6. Last Updated

## Files

```
services/master_data/
├── __init__.py              # Module exports
├── README.md                # This file
├── models.py                # Data models (MasterRow, etc.)
├── repos.py                 # DB repositories
├── adapters.py              # Tool adapters (plugin architecture)
├── runner_db_first.py       # Main ingestion runner
├── sheets_exporter.py       # Sheets export logic
└── runner.py                # Legacy Sheets-driven runner (deprecated)

services/database/migrations/master/
└── 0001_create_master_tables.sql  # DB schema
```

## Future Enhancements

- [ ] Add Google Search adapter when table is ready
- [ ] Add metrics/observability (Prometheus, etc.)
- [ ] Add retry logic for transient DB errors
- [ ] Add batch size tuning based on performance
- [ ] Add parallel adapter execution (currently sequential)
- [ ] Add incremental Sheets updates (currently full export)