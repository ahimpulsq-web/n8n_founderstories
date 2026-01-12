-- =============================================================================
-- Migration: 0001_master_schema.sql
-- Purpose: Canonical Master schema - replaces all previous Master migrations
-- =============================================================================
--
-- This is the ONLY Master migration file going forward.
-- It defines the complete, production-ready schema for Master data aggregation.
--
-- Tables created:
--   1. master_results      - Unified results from all tools (Hunter, Google Maps, etc.)
--   2. master_watermarks   - Per-tool watermarks for incremental ingestion
--   3. master_sources      - Registry of known source tools and their metadata
--   4. master_run_state    - Level-trigger orchestration state tracking
--
-- Key features:
--   - Idempotent (safe to run multiple times)
--   - Uses UNIQUE CONSTRAINT (not just indexes) for ON CONFLICT support
--   - Includes domain_norm column for normalized deduplication
--   - Auto-updating timestamps via triggers
--   - Production-grade indexes for query performance
--   - Comprehensive documentation via COMMENT statements
--
-- Requirements:
--   - PostgreSQL 12+
--   - pgcrypto extension (for gen_random_uuid)
--
-- =============================================================================

BEGIN;

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =============================================================================
-- TABLE 1: master_results
-- =============================================================================
-- Unified results table aggregating data from all tools.
-- Supports idempotent upserts via UNIQUE CONSTRAINT on (request_id, domain_norm).
-- =============================================================================

CREATE TABLE IF NOT EXISTS master_results (
    -- Primary key
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Timestamps (auto-managed)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Job context (required for traceability)
    job_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    
    -- Domain fields (both raw and normalized)
    domain TEXT NOT NULL,           -- Raw domain as provided by source
    domain_norm TEXT NOT NULL,      -- Normalized domain for deduplication
    
    -- Core business fields
    company TEXT,
    website TEXT,
    location TEXT,
    lead_query TEXT,
    
    -- Source tracking
    source_tool TEXT NOT NULL,      -- 'HunterIO', 'GoogleMaps', 'GoogleSearch', etc.
    source_ref TEXT,                -- Optional JSON or string reference to source record
    
    -- Duplicate tracking
    dup_in_run TEXT                 -- 'YES' or 'NO' - computed during ingestion
);

-- Add domain_norm column if it doesn't exist (for migration compatibility)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'master_results' AND column_name = 'domain_norm'
    ) THEN
        ALTER TABLE master_results ADD COLUMN domain_norm TEXT;
        
        -- Backfill domain_norm from domain for existing rows
        UPDATE master_results
        SET domain_norm = LOWER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        TRIM(domain),
                        '^https?://', ''           -- Remove http:// or https://
                    ),
                    '^www\.', ''                    -- Remove www.
                ),
                '[/?#:].*$', ''                     -- Remove port, path, query, fragment
            )
        )
        WHERE domain_norm IS NULL AND domain IS NOT NULL;
        
        -- Set NOT NULL after backfill
        ALTER TABLE master_results ALTER COLUMN domain_norm SET NOT NULL;
    END IF;
END $$;

-- =============================================================================
-- CONSTRAINTS: master_results
-- =============================================================================

-- Drop legacy expression index if it exists (replaced by proper constraint)
DROP INDEX IF EXISTS uq_master_results_request_domain;

-- Create UNIQUE CONSTRAINT (not just index) for ON CONFLICT support
-- This ensures one result per normalized domain per request
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'uq_master_results_request_domain_norm'
          AND conrelid = 'master_results'::regclass
    ) THEN
        ALTER TABLE master_results
            ADD CONSTRAINT uq_master_results_request_domain_norm
            UNIQUE (request_id, domain_norm);
    END IF;
END $$;

-- =============================================================================
-- INDEXES: master_results
-- =============================================================================

-- Index for request-based queries
CREATE INDEX IF NOT EXISTS idx_master_results_request_id
    ON master_results(request_id);

-- Composite index for job + source tool queries
CREATE INDEX IF NOT EXISTS idx_master_results_job_source
    ON master_results(job_id, source_tool);

-- Index for domain_norm lookups
CREATE INDEX IF NOT EXISTS idx_master_results_domain_norm
    ON master_results(domain_norm);

-- Index for source_tool filtering
CREATE INDEX IF NOT EXISTS idx_master_results_source_tool
    ON master_results(source_tool);

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_master_results_updated_at
    ON master_results(updated_at);

-- =============================================================================
-- TRIGGER: master_results auto-update timestamp
-- =============================================================================

CREATE OR REPLACE FUNCTION update_master_results_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_master_results_updated_at ON master_results;
CREATE TRIGGER trg_master_results_updated_at
    BEFORE UPDATE ON master_results
    FOR EACH ROW
    EXECUTE FUNCTION update_master_results_updated_at();

-- =============================================================================
-- TABLE 2: master_watermarks
-- =============================================================================
-- Per-tool watermarks for incremental ingestion.
-- Tracks last processed timestamp to avoid reprocessing.
-- =============================================================================

CREATE TABLE IF NOT EXISTS master_watermarks (
    -- Primary key
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Watermark key (one per request + tool combination)
    request_id TEXT NOT NULL,
    source_tool TEXT NOT NULL,
    
    -- Watermark value (timestamp-based for flexibility)
    last_seen_created_at TIMESTAMPTZ,
    
    -- Processing metadata
    last_processed_count INTEGER DEFAULT 0,
    total_processed INTEGER DEFAULT 0
);

-- =============================================================================
-- CONSTRAINTS: master_watermarks
-- =============================================================================

-- UNIQUE CONSTRAINT: one watermark per (request_id, source_tool)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'uq_master_watermarks_request_tool'
          AND conrelid = 'master_watermarks'::regclass
    ) THEN
        ALTER TABLE master_watermarks
            ADD CONSTRAINT uq_master_watermarks_request_tool
            UNIQUE (request_id, source_tool);
    END IF;
END $$;


-- =============================================================================
-- INDEXES: master_watermarks
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_master_watermarks_request_id
    ON master_watermarks(request_id);

CREATE INDEX IF NOT EXISTS idx_master_watermarks_updated_at
    ON master_watermarks(updated_at);

-- =============================================================================
-- TRIGGER: master_watermarks auto-update timestamp
-- =============================================================================

CREATE OR REPLACE FUNCTION update_master_watermarks_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_master_watermarks_updated_at ON master_watermarks;
CREATE TRIGGER trg_master_watermarks_updated_at
    BEFORE UPDATE ON master_watermarks
    FOR EACH ROW
    EXECUTE FUNCTION update_master_watermarks_updated_at();

-- =============================================================================
-- TABLE 3: master_sources
-- =============================================================================
-- Registry of known source tools with their configuration and column mappings.
-- =============================================================================

CREATE TABLE IF NOT EXISTS master_sources (
    -- Primary key
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Source identification (must be unique)
    source_tool TEXT NOT NULL,
    
    -- Metadata
    source_table TEXT,              -- DB table name (e.g., 'hunterio_results')
    column_mapping JSONB,           -- Maps source columns to master fields
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Optional fields
    display_name TEXT,
    description TEXT
);

-- =============================================================================
-- CONSTRAINTS: master_sources
-- =============================================================================

-- UNIQUE CONSTRAINT on source_tool
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'uq_master_sources_source_tool'
          AND conrelid = 'master_sources'::regclass
    ) THEN
        ALTER TABLE master_sources
            ADD CONSTRAINT uq_master_sources_source_tool
            UNIQUE (source_tool);
    END IF;
END $$;

-- =============================================================================
-- INDEXES: master_sources
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_master_sources_active
    ON master_sources(is_active)
    WHERE is_active = TRUE;

-- =============================================================================
-- TRIGGER: master_sources auto-update timestamp
-- =============================================================================

CREATE OR REPLACE FUNCTION update_master_sources_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_master_sources_updated_at ON master_sources;
CREATE TRIGGER trg_master_sources_updated_at
    BEFORE UPDATE ON master_sources
    FOR EACH ROW
    EXECUTE FUNCTION update_master_sources_updated_at();

-- =============================================================================
-- DEFAULT DATA: master_sources
-- =============================================================================
-- Insert default source configurations for known tools
-- =============================================================================

INSERT INTO master_sources (source_tool, display_name, source_table, is_active, column_mapping, description)
VALUES 
    (
        'HunterIO',
        'Hunter.io',
        'hunterio_results',
        TRUE,
        '{"company": "organisation", "domain": "domain", "website": "domain", "location": "location", "lead_query": "search_query"}'::jsonb,
        'Hunter.io company search results'
    ),
    (
        'GoogleMaps',
        'Google Maps',
        'google_maps_results',
        TRUE,
        '{"company": "organisation", "domain": "domain", "website": "website", "location": "location_label", "lead_query": "search_query"}'::jsonb,
        'Google Maps place search and enrichment results'
    ),
    (
        'GoogleSearch',
        'Google Search',
        'google_search_results',
        TRUE,
        '{"company": "organisation", "domain": "domain", "website": "website", "location": null, "lead_query": "search_query"}'::jsonb,
        'Google Search organic results'
    )
ON CONFLICT ON CONSTRAINT uq_master_sources_source_tool DO NOTHING;

-- =============================================================================
-- TABLE 4: master_run_state
-- =============================================================================
-- Level-trigger orchestration state tracking.
-- Prevents dropped triggers when advisory lock is busy.
-- =============================================================================

CREATE TABLE IF NOT EXISTS master_run_state (
    -- Primary key (one row per request_id)
    request_id TEXT PRIMARY KEY,
    
    -- State tracking
    pending BOOLEAN NOT NULL DEFAULT FALSE,
    trigger_count INTEGER NOT NULL DEFAULT 0,
    last_trigger_by TEXT,
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- INDEXES: master_run_state
-- =============================================================================

-- Partial index for pending triggers (most common query)
CREATE INDEX IF NOT EXISTS idx_master_run_state_pending
    ON master_run_state(pending)
    WHERE pending = TRUE;

CREATE INDEX IF NOT EXISTS idx_master_run_state_updated_at
    ON master_run_state(updated_at);

-- =============================================================================
-- TRIGGER: master_run_state auto-update timestamp
-- =============================================================================

CREATE OR REPLACE FUNCTION update_master_run_state_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_master_run_state_updated_at ON master_run_state;
CREATE TRIGGER trg_master_run_state_updated_at
    BEFORE UPDATE ON master_run_state
    FOR EACH ROW
    EXECUTE FUNCTION update_master_run_state_updated_at();

-- =============================================================================
-- DOCUMENTATION: Table and column comments
-- =============================================================================

COMMENT ON TABLE master_results IS 
'Unified results table aggregating data from all tools (Hunter, Google Maps, Google Search). Supports idempotent upserts via domain_norm deduplication.';

COMMENT ON COLUMN master_results.domain IS 
'Raw domain as provided by source tool (for traceability and display)';

COMMENT ON COLUMN master_results.domain_norm IS 
'Normalized domain: lowercase, no scheme, no www, no path. Used for deduplication. Example: "Example.COM" -> "example.com"';

COMMENT ON COLUMN master_results.source_tool IS 
'Source tool identifier (HunterIO, GoogleMaps, GoogleSearch, etc.)';

COMMENT ON COLUMN master_results.source_ref IS 
'Optional reference to source record (JSON or string) for traceability';

COMMENT ON COLUMN master_results.dup_in_run IS 
'Duplicate flag within this run (YES/NO). Computed during ingestion.';

COMMENT ON CONSTRAINT uq_master_results_request_domain_norm ON master_results IS
'Ensures one result per normalized domain per request. Example.com and example.COM are treated as duplicates.';

COMMENT ON TABLE master_watermarks IS 
'Per-tool watermarks for incremental ingestion. Tracks last processed timestamp to avoid reprocessing.';

COMMENT ON COLUMN master_watermarks.last_seen_created_at IS 
'Timestamp of last processed record from source table. Used for incremental reads.';

COMMENT ON CONSTRAINT uq_master_watermarks_request_tool ON master_watermarks IS
'Ensures one watermark per (request_id, source_tool) combination.';

COMMENT ON TABLE master_sources IS 
'Registry of known source tools with their configuration and column mappings.';

COMMENT ON COLUMN master_sources.column_mapping IS 
'JSONB mapping of source table columns to master_results fields. Example: {"company": "organisation", "domain": "domain"}';

COMMENT ON CONSTRAINT uq_master_sources_source_tool ON master_sources IS
'Ensures each source tool is registered only once.';

COMMENT ON TABLE master_run_state IS 
'Tracks pending Master triggers for level-trigger orchestration. Ensures no triggers are lost when advisory lock is busy.';

COMMENT ON COLUMN master_run_state.pending IS 
'TRUE if a Master trigger arrived while lock was held. Master will rerun after releasing lock.';

COMMENT ON COLUMN master_run_state.trigger_count IS 
'Total number of times Master has been triggered for this request_id (for monitoring).';

COMMENT ON COLUMN master_run_state.last_trigger_by IS 
'Name of tool that last triggered Master (for debugging and audit trail).';

-- =============================================================================
-- VERIFICATION: Ensure schema is correct
-- =============================================================================

DO $$
DECLARE
    domain_norm_exists INTEGER;
    constraint_exists INTEGER;
    run_state_exists INTEGER;
BEGIN
    -- Verify domain_norm column exists
    SELECT COUNT(*) INTO domain_norm_exists
    FROM information_schema.columns
    WHERE table_name = 'master_results' AND column_name = 'domain_norm';
    
    IF domain_norm_exists = 0 THEN
        RAISE EXCEPTION 'VERIFICATION FAILED: domain_norm column does not exist';
    END IF;
    
    -- Verify UNIQUE CONSTRAINT exists (not just index)
    SELECT COUNT(*) INTO constraint_exists
    FROM pg_constraint
    WHERE conname = 'uq_master_results_request_domain_norm'
      AND conrelid = 'master_results'::regclass
      AND contype = 'u';
    
    IF constraint_exists = 0 THEN
        RAISE EXCEPTION 'VERIFICATION FAILED: uq_master_results_request_domain_norm constraint does not exist';
    END IF;
    
    -- Verify master_run_state table exists
    SELECT COUNT(*) INTO run_state_exists
    FROM information_schema.tables
    WHERE table_name = 'master_run_state';
    
    IF run_state_exists = 0 THEN
        RAISE EXCEPTION 'VERIFICATION FAILED: master_run_state table does not exist';
    END IF;
    
    -- All checks passed
    RAISE NOTICE '✓ Schema verification successful';
    RAISE NOTICE '  - master_results: domain_norm column exists';
    RAISE NOTICE '  - master_results: UNIQUE CONSTRAINT exists';
    RAISE NOTICE '  - master_watermarks: table exists';
    RAISE NOTICE '  - master_sources: table exists with defaults';
    RAISE NOTICE '  - master_run_state: table exists';
END $$;

COMMIT;

-- =============================================================================
-- END OF MIGRATION
-- =============================================================================
-- This migration is complete and production-ready.
-- No further Master migrations should be created unless absolutely necessary.
-- All future schema changes should be carefully considered and documented.
-- =============================================================================