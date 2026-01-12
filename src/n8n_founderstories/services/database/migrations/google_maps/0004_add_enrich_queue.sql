-- Migration: 0004_add_enrich_queue.sql
-- Purpose: Create DB-based enrichment queue to replace Sheets-based queue
--
-- This migration creates a PostgreSQL-based queue for Google Maps enrichment,
-- eliminating the need for Sheets-based orchestration and enabling:
-- - Concurrency-safe worker processing (FOR UPDATE SKIP LOCKED)
-- - Atomic state transitions (PENDING → PROCESSING → DONE/FAILED)
-- - Retry logic with attempt tracking
-- - Full audit trail in database

BEGIN;

-- =============================================================================
-- 1. Create enrichment queue table
-- =============================================================================

CREATE TABLE IF NOT EXISTS gmaps_enrich_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Job context
    job_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    place_id TEXT NOT NULL,
    
    -- Enrichment parameters (from discovery phase)
    iso2 TEXT NULL,  -- Country code for region parameter
    hl TEXT NULL,    -- Language code for language parameter
    
    -- Queue state management
    state TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING, PROCESSING, DONE, FAILED
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    
    -- Constraints
    CONSTRAINT chk_gmaps_enrich_queue_state 
        CHECK (state IN ('PENDING', 'PROCESSING', 'DONE', 'FAILED'))
);

-- Unique constraint: one queue entry per (job_id, request_id, place_id)
-- This prevents duplicate enrichment work
CREATE UNIQUE INDEX IF NOT EXISTS uq_gmaps_enrich_queue_job_request_place
    ON gmaps_enrich_queue (job_id, request_id, place_id);

-- Worker pickup index: optimized for SELECT ... FOR UPDATE SKIP LOCKED
-- Workers query: WHERE state='PENDING' AND job_id=? ORDER BY created_at
CREATE INDEX IF NOT EXISTS idx_gmaps_enrich_queue_worker_pickup
    ON gmaps_enrich_queue (job_id, state, created_at)
    WHERE state IN ('PENDING', 'PROCESSING');

-- Job monitoring index: track progress per job
CREATE INDEX IF NOT EXISTS idx_gmaps_enrich_queue_job_state
    ON gmaps_enrich_queue (job_id, state);

-- Cleanup/archival index: find old completed items
CREATE INDEX IF NOT EXISTS idx_gmaps_enrich_queue_completed
    ON gmaps_enrich_queue (state, updated_at)
    WHERE state IN ('DONE', 'FAILED');

-- =============================================================================
-- 2. Create trigger for automatic updated_at maintenance
-- =============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_gmaps_enrich_queue_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger on UPDATE
DROP TRIGGER IF EXISTS trg_gmaps_enrich_queue_updated_at ON gmaps_enrich_queue;
CREATE TRIGGER trg_gmaps_enrich_queue_updated_at
    BEFORE UPDATE ON gmaps_enrich_queue
    FOR EACH ROW
    EXECUTE FUNCTION update_gmaps_enrich_queue_updated_at();

-- =============================================================================
-- 3. Add updated_at to gmaps_results for enrichment tracking
-- =============================================================================

-- Add updated_at column if it doesn't exist (for tracking enrichment updates)
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'gmaps_results' 
        AND column_name = 'updated_at'
    ) THEN
        ALTER TABLE gmaps_results 
            ADD COLUMN updated_at TIMESTAMPTZ NULL DEFAULT NOW();
        
        -- Backfill existing rows
        UPDATE gmaps_results SET updated_at = created_at WHERE updated_at IS NULL;
        
        -- Make NOT NULL after backfill
        ALTER TABLE gmaps_results 
            ALTER COLUMN updated_at SET NOT NULL;
    END IF;
END $$;

-- Create trigger for gmaps_results updated_at
CREATE OR REPLACE FUNCTION update_gmaps_results_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_gmaps_results_updated_at ON gmaps_results;
CREATE TRIGGER trg_gmaps_results_updated_at
    BEFORE UPDATE ON gmaps_results
    FOR EACH ROW
    EXECUTE FUNCTION update_gmaps_results_updated_at();

-- =============================================================================
-- 4. Create helper views for monitoring
-- =============================================================================

-- View: Queue status summary per job
CREATE OR REPLACE VIEW v_gmaps_enrich_queue_summary AS
SELECT 
    job_id,
    request_id,
    COUNT(*) as total_items,
    COUNT(*) FILTER (WHERE state = 'PENDING') as pending,
    COUNT(*) FILTER (WHERE state = 'PROCESSING') as processing,
    COUNT(*) FILTER (WHERE state = 'DONE') as done,
    COUNT(*) FILTER (WHERE state = 'FAILED') as failed,
    MIN(created_at) as first_created,
    MAX(updated_at) as last_updated,
    AVG(attempts) FILTER (WHERE state IN ('DONE', 'FAILED')) as avg_attempts
FROM gmaps_enrich_queue
GROUP BY job_id, request_id;

COMMENT ON VIEW v_gmaps_enrich_queue_summary IS 
'Summary view of enrichment queue status per job for monitoring and debugging';

COMMIT;