-- Migration: 0005_unify_schema_db_first.sql
-- Purpose: Unify Google Maps schema for DB-first workflow
--
-- Changes:
-- 1. Rename gmaps_results to google_maps_results (single source of truth)
-- 2. Add country column (ISO2) to results table
-- 3. Rename columns to match task requirements
-- 4. Create google_maps_audit table matching Sheets audit schema
-- 5. Ensure all tables support both discover and enrich phases

BEGIN;

-- =============================================================================
-- 1. Create new unified google_maps_results table
-- =============================================================================

CREATE TABLE IF NOT EXISTS google_maps_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Job context (indexed, required)
    job_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    
    -- Core identification (required for discover, enriched during enrich)
    organisation TEXT,  -- Place name (from 'name' field)
    domain TEXT,        -- Extracted domain (filled during enrich)
    website TEXT,       -- Full website URL (filled during enrich)
    phone TEXT,         -- Phone number (filled during enrich)
    
    -- Location data (required)
    address TEXT,
    country TEXT,       -- ISO2 country code
    
    -- Classification
    type TEXT,          -- Category/type of place
    business_status TEXT,
    
    -- URLs
    google_maps_url TEXT,
    
    -- Search context
    search_query TEXT,  -- The actual query used
    
    -- Internal fields (not exported to Sheets)
    place_id TEXT NOT NULL,
    location_label TEXT,
    intended_location TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    source TEXT DEFAULT 'google_maps',
    raw_json JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Unique constraint for upsert
    CONSTRAINT uq_google_maps_results_place_job_request
        UNIQUE (place_id, job_id, request_id)
);

-- Indexes for google_maps_results
CREATE INDEX IF NOT EXISTS idx_google_maps_results_job_id
    ON google_maps_results(job_id);

CREATE INDEX IF NOT EXISTS idx_google_maps_results_request_id
    ON google_maps_results(request_id);

CREATE INDEX IF NOT EXISTS idx_google_maps_results_job_request
    ON google_maps_results(job_id, request_id);

CREATE INDEX IF NOT EXISTS idx_google_maps_results_created_at
    ON google_maps_results(created_at);

CREATE INDEX IF NOT EXISTS idx_google_maps_results_place_id
    ON google_maps_results(place_id);

CREATE INDEX IF NOT EXISTS idx_google_maps_results_business_status
    ON google_maps_results(business_status);

CREATE INDEX IF NOT EXISTS idx_google_maps_results_country
    ON google_maps_results(country);

-- Trigger for updated_at
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
-- 2. Create google_maps_audit table (matches Sheets audit schema exactly)
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
-- 3. Migrate data from gmaps_results to google_maps_results
-- =============================================================================

-- Only migrate if gmaps_results exists and google_maps_results is empty
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'gmaps_results'
    ) AND NOT EXISTS (
        SELECT 1 FROM google_maps_results LIMIT 1
    ) THEN
        INSERT INTO google_maps_results (
            id, created_at, job_id, request_id,
            organisation, domain, website, phone,
            address, country, type, business_status,
            google_maps_url, search_query,
            place_id, location_label, intended_location,
            lat, lng, source, raw_json, updated_at
        )
        SELECT 
            id, created_at, job_id, request_id,
            name as organisation,  -- Rename name -> organisation
            domain, website, phone,
            address,
            COALESCE(
                -- Try to extract country from intended_location (e.g., "Berlin, DE" -> "DE")
                CASE 
                    WHEN intended_location ~ ', [A-Z]{2}$' 
                    THEN SUBSTRING(intended_location FROM ', ([A-Z]{2})$')
                    ELSE NULL
                END,
                ''
            ) as country,
            category as type,  -- Rename category -> type
            business_status,
            google_maps_url,
            query_text as search_query,  -- Rename query_text -> search_query
            place_id, location_label, intended_location,
            lat, lng, source, raw_json,
            COALESCE(updated_at, created_at) as updated_at
        FROM gmaps_results;
        
        RAISE NOTICE 'Migrated % rows from gmaps_results to google_maps_results', 
            (SELECT COUNT(*) FROM google_maps_results);
    END IF;
END $$;

-- =============================================================================
-- 4. Drop old gmaps_results table (after successful migration)
-- =============================================================================

-- Commented out for safety - uncomment after verifying migration
-- DROP TABLE IF EXISTS gmaps_results CASCADE;

-- =============================================================================
-- 5. Update google_maps_enriched to reference new table
-- =============================================================================

-- The google_maps_enriched table remains unchanged as it's already correct
-- It stores additional enrichment data separately

-- =============================================================================
-- 6. Create views for backward compatibility
-- =============================================================================

-- View to maintain backward compatibility with old gmaps_results name
CREATE OR REPLACE VIEW gmaps_results AS
SELECT 
    id, created_at, job_id, request_id,
    organisation as name,  -- Map back to old name
    domain, website, phone,
    address,
    type as category,  -- Map back to old name
    lat, lng,
    search_query as query_text,  -- Map back to old name
    intended_location,
    source, raw_json,
    location_label,
    business_status,
    google_maps_url,
    updated_at
FROM google_maps_results;

COMMENT ON VIEW gmaps_results IS 
'Backward compatibility view - maps google_maps_results to old gmaps_results schema';

-- =============================================================================
-- 7. Create helper views for export
-- =============================================================================

-- View: Operational results ready for Sheets export
CREATE OR REPLACE VIEW v_google_maps_operational_results AS
SELECT 
    job_id,
    request_id,
    organisation,
    domain,
    phone,
    country,
    address,
    type,
    google_maps_url,
    search_query
FROM google_maps_results
WHERE business_status = 'OPERATIONAL'
ORDER BY created_at, place_id;

COMMENT ON VIEW v_google_maps_operational_results IS 
'Filtered view of operational results ready for Sheets export';

-- View: Audit records ready for Sheets export
CREATE OR REPLACE VIEW v_google_maps_audit_export AS
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

COMMIT;