-- Migration: 0006_db_first_refactor.sql
-- Purpose: Complete DB-first refactoring for Google Maps
--
-- Changes:
-- 1. Ensure google_maps_results has all required columns
-- 2. Add unique constraint for upsert support
-- 3. Drop google_maps_enriched table (dev-stage)
-- 4. Update export views to use location_label instead of country
-- 5. Simplify export columns (remove place_id, business_status)
-- 6. Ensure google_maps_audit table exists for DB-first audit

BEGIN;

-- =============================================================================
-- 1. Ensure google_maps_results table has all required columns
-- =============================================================================

-- Add missing columns if they don't exist (idempotent)
DO $$
BEGIN
    -- Add updated_at if missing
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'google_maps_results' AND column_name = 'updated_at'
    ) THEN
        ALTER TABLE google_maps_results 
        ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
    END IF;

    -- Ensure organisation column exists (rename from name if needed)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'google_maps_results' AND column_name = 'organisation'
    ) THEN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'google_maps_results' AND column_name = 'name'
        ) THEN
            ALTER TABLE google_maps_results RENAME COLUMN name TO organisation;
        ELSE
            ALTER TABLE google_maps_results ADD COLUMN organisation TEXT;
        END IF;
    END IF;

    -- Ensure type column exists (rename from category if needed)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'google_maps_results' AND column_name = 'type'
    ) THEN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'google_maps_results' AND column_name = 'category'
        ) THEN
            ALTER TABLE google_maps_results RENAME COLUMN type TO type;
        ELSE
            ALTER TABLE google_maps_results ADD COLUMN type TEXT;
        END IF;
    END IF;

    -- Ensure search_query column exists (rename from query_text if needed)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'google_maps_results' AND column_name = 'search_query'
    ) THEN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'google_maps_results' AND column_name = 'query_text'
        ) THEN
            ALTER TABLE google_maps_results RENAME COLUMN query_text TO search_query;
        ELSE
            ALTER TABLE google_maps_results ADD COLUMN search_query TEXT;
        END IF;
    END IF;

    -- Ensure country column exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'google_maps_results' AND column_name = 'country'
    ) THEN
        ALTER TABLE google_maps_results ADD COLUMN country TEXT;
    END IF;

    -- Ensure enrichment fields exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'google_maps_results' AND column_name = 'website'
    ) THEN
        ALTER TABLE google_maps_results ADD COLUMN website TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'google_maps_results' AND column_name = 'domain'
    ) THEN
        ALTER TABLE google_maps_results ADD COLUMN domain TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'google_maps_results' AND column_name = 'phone'
    ) THEN
        ALTER TABLE google_maps_results ADD COLUMN phone TEXT;
    END IF;
END $$;

-- Ensure place_id is NOT NULL (required for uniqueness)
DO $$
BEGIN
    -- First, delete any rows with NULL place_id (shouldn't exist in production)
    DELETE FROM google_maps_results WHERE place_id IS NULL OR place_id = '';
    
    -- Then set NOT NULL constraint
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'google_maps_results' 
        AND column_name = 'place_id' 
        AND is_nullable = 'YES'
    ) THEN
        ALTER TABLE google_maps_results ALTER COLUMN place_id SET NOT NULL;
    END IF;
END $$;

-- =============================================================================
-- 2. Add unique constraint for upsert support
-- =============================================================================

-- Drop old unique index if it exists (partial index with WHERE clause)
DROP INDEX IF EXISTS uq_gmaps_results_place_job_request;
DROP INDEX IF EXISTS uq_google_maps_results_place_job_request_idx;

-- Add named unique constraint (not partial, since place_id is now NOT NULL)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'uq_google_maps_results_place_job_request'
    ) THEN
        ALTER TABLE google_maps_results
        ADD CONSTRAINT uq_google_maps_results_place_job_request 
        UNIQUE (place_id, job_id, request_id);
    END IF;
END $$;

-- =============================================================================
-- 3. Create/update trigger for updated_at
-- =============================================================================

CREATE OR REPLACE FUNCTION update_google_maps_results_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_google_maps_results_updated_at ON google_maps_results;
CREATE TRIGGER trg_google_maps_results_updated_at
    BEFORE UPDATE ON google_maps_results
    FOR EACH ROW
    EXECUTE FUNCTION update_google_maps_results_updated_at();

-- =============================================================================
-- 4. Drop google_maps_enriched table (dev-stage)
-- =============================================================================

-- Drop the enriched table as it's no longer needed
-- Enrichment data now lives directly in google_maps_results
DROP TABLE IF EXISTS google_maps_enriched CASCADE;

-- =============================================================================
-- 5. Ensure google_maps_audit table exists
-- =============================================================================

CREATE TABLE IF NOT EXISTS google_maps_audit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Job context
    job_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    
    -- Phase tracking
    phase TEXT NOT NULL,  -- 'discover' or 'enrich'
    
    -- Location/language parameters
    country_iso2 TEXT,
    hl_plan TEXT,
    language_used TEXT,
    location_label TEXT,
    
    -- Query details
    base_query TEXT,
    final_query TEXT,
    region_param TEXT,
    
    -- Results metrics
    take_n INTEGER,
    returned_count INTEGER,
    eligible_after_dedupe INTEGER,
    appended_rows INTEGER,
    unique_places_job INTEGER,
    
    -- Completion status
    stop_reason TEXT,
    error TEXT,
    
    -- Timestamp
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Raw metadata (for debugging)
    raw_meta_json JSONB
);

-- Indexes for google_maps_audit
CREATE INDEX IF NOT EXISTS idx_google_maps_audit_job_id
    ON google_maps_audit(job_id);

CREATE INDEX IF NOT EXISTS idx_google_maps_audit_request_id
    ON google_maps_audit(request_id);

CREATE INDEX IF NOT EXISTS idx_google_maps_audit_job_request
    ON google_maps_audit(job_id, request_id);

CREATE INDEX IF NOT EXISTS idx_google_maps_audit_phase
    ON google_maps_audit(phase);

CREATE INDEX IF NOT EXISTS idx_google_maps_audit_timestamp
    ON google_maps_audit(timestamp);

-- =============================================================================
-- 6. Update export views for operational results
-- =============================================================================

-- Drop existing view first to allow column changes
DROP VIEW IF EXISTS v_google_maps_operational_results CASCADE;

-- View: Operational results ready for Sheets export
-- Uses location_label instead of country, excludes place_id and business_status
CREATE VIEW v_google_maps_operational_results AS
SELECT 
    job_id,
    request_id,
    organisation,
    domain,
    phone,
    location_label,  -- Changed from country to location_label
    address,
    type,
    google_maps_url,
    search_query,
    created_at,
    updated_at
FROM google_maps_results
WHERE business_status = 'OPERATIONAL'
ORDER BY created_at, place_id;

COMMENT ON VIEW v_google_maps_operational_results IS 
'Filtered view of operational results ready for Sheets export. Uses location_label instead of country.';

-- =============================================================================
-- 7. Create audit export view
-- =============================================================================

-- Drop existing view first to allow any schema changes
DROP VIEW IF EXISTS v_google_maps_audit_export CASCADE;

-- View: Audit records ready for Sheets export
CREATE VIEW v_google_maps_audit_export AS
SELECT 
    job_id,
    request_id,
    phase,
    country_iso2,
    hl_plan,
    language_used,
    location_label,
    base_query,
    final_query,
    region_param,
    take_n,
    returned_count,
    eligible_after_dedupe,
    appended_rows,
    unique_places_job,
    stop_reason,
    error,
    timestamp,
    raw_meta_json
FROM google_maps_audit
ORDER BY timestamp;

COMMENT ON VIEW v_google_maps_audit_export IS 
'Audit records formatted for Sheets export (excludes internal id column)';

-- =============================================================================
-- 8. Drop backward compatibility view (gmaps_results)
-- =============================================================================

-- Drop the old compatibility view since we're fully migrating to google_maps_results
DROP VIEW IF EXISTS gmaps_results CASCADE;

-- =============================================================================
-- 9. Add helpful comments
-- =============================================================================

COMMENT ON TABLE google_maps_results IS 
'Single source of truth for Google Maps discover and enrich results. DB-first architecture.';

COMMENT ON TABLE google_maps_audit IS 
'Audit log for Google Maps discover and enrich operations. DB-first architecture.';

COMMENT ON COLUMN google_maps_results.organisation IS 
'Place name (formerly "name" column)';

COMMENT ON COLUMN google_maps_results.type IS 
'Place type/category (formerly "category" column)';

COMMENT ON COLUMN google_maps_results.search_query IS 
'The actual query used (formerly "query_text" column)';

COMMENT ON COLUMN google_maps_results.location_label IS 
'User-friendly location label (exported as "Location" in Sheets)';

COMMENT ON COLUMN google_maps_results.country IS 
'ISO2 country code (internal use only, not exported to Sheets)';

COMMENT ON COLUMN google_maps_results.website IS 
'Website URL (filled during enrich phase)';

COMMENT ON COLUMN google_maps_results.domain IS 
'Extracted domain from website (filled during enrich phase)';

COMMENT ON COLUMN google_maps_results.phone IS 
'Phone number (filled during enrich phase)';

COMMIT;