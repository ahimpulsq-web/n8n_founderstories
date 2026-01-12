-- Migration: 0003_fix_unique_constraints.sql
-- Purpose: Fix ON CONFLICT compatibility by adding proper named unique constraints
-- 
-- Background:
-- - The partial unique index (WHERE place_id IS NOT NULL AND place_id <> '') 
--   cannot be used with ON CONFLICT (place_id, job_id, request_id)
-- - We need a proper UNIQUE CONSTRAINT for ON CONFLICT to work
-- - Strategy: Make place_id NOT NULL and add a proper unique constraint

BEGIN;

-- Step 1: For gmaps_results - Add NOT NULL constraint to place_id
-- This is safe because we filter out null/empty place_ids before insert
ALTER TABLE gmaps_results 
    ALTER COLUMN place_id SET NOT NULL;

-- Step 2: Drop constraint if it already exists (safe)
ALTER TABLE gmaps_results
    DROP CONSTRAINT IF EXISTS uq_gmaps_results_place_job_request;

-- Step 3: Drop the index if it exists (safe)
DROP INDEX IF EXISTS uq_gmaps_results_place_job_request;

-- Step 4: Re-add proper UNIQUE CONSTRAINT (only if missing)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_gmaps_results_place_job_request'
    ) THEN
        ALTER TABLE gmaps_results
            ADD CONSTRAINT uq_gmaps_results_place_job_request
            UNIQUE (place_id, job_id, request_id);
    END IF;
END $$;

-- Step 5: Improve query performance with better composite index
-- This supports deterministic ordering: ORDER BY created_at, place_id
CREATE INDEX IF NOT EXISTS idx_gmaps_results_job_created_place
    ON gmaps_results(job_id, created_at, place_id);

CREATE INDEX IF NOT EXISTS idx_google_maps_enriched_job_created_place
    ON google_maps_enriched(job_id, created_at, place_id);

COMMIT;