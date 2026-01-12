-- =============================================================================
-- Migration: 0003_fix_upsert_with_source_tool.sql
-- Purpose: Fix Master upsert to prevent duplicates during enrichment
-- =============================================================================
--
-- Fixes:
-- - Reorders operations to avoid constraint violations.
-- - Backfills GoogleMaps entity_key BEFORE creating the new unique constraint.
-- - Cleans duplicates AFTER backfill using the final uniqueness definition.
--
-- Target uniqueness:
--   UNIQUE (request_id, source_tool, entity_key)
--
-- GoogleMaps stable key:
--   entity_key = 'gmaps:' || source_ref  (place_id)
-- =============================================================================

BEGIN;

-- =============================================================================
-- STEP 0: Update entity_key computation function (stable GoogleMaps key)
-- =============================================================================

CREATE OR REPLACE FUNCTION compute_master_entity_key()
RETURNS TRIGGER AS $$
BEGIN
    -- GoogleMaps: stable key from place_id (source_ref)
    IF NEW.source_tool = 'GoogleMaps'
       AND NEW.source_ref IS NOT NULL
       AND NEW.source_ref <> '' THEN
        NEW.entity_key = 'gmaps:' || NEW.source_ref;

    -- Other tools: prefer domain_norm
    ELSIF NEW.domain_norm IS NOT NULL
          AND NEW.domain_norm <> '' THEN
        NEW.entity_key = NEW.domain_norm;

    -- Fallback: source_ref if present
    ELSIF NEW.source_ref IS NOT NULL
          AND NEW.source_ref <> '' THEN
        NEW.entity_key = 'ref:' || NEW.source_ref;

    -- Final fallback: stable unknown key
    ELSE
        -- If id exists, use it; otherwise keep a stable placeholder
        NEW.entity_key = 'unknown:' || COALESCE(NEW.id::text, '');
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    RAISE NOTICE 'Updated compute_master_entity_key() to prioritize source_ref for GoogleMaps';
END $$;

-- =============================================================================
-- STEP 1: Backfill entity_key for existing GoogleMaps rows FIRST
-- =============================================================================
-- IMPORTANT: This must happen BEFORE creating the new unique constraint,
-- because the backfill may create duplicates that we will then clean up.

WITH upd AS (
    UPDATE master_results
    SET entity_key = 'gmaps:' || source_ref
    WHERE source_tool = 'GoogleMaps'
      AND source_ref IS NOT NULL
      AND source_ref <> ''
      AND entity_key IS DISTINCT FROM ('gmaps:' || source_ref)
    RETURNING 1
)
SELECT 1;

DO $$
DECLARE
    updated_count INTEGER;
BEGIN
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RAISE NOTICE 'Backfilled entity_key for % GoogleMaps rows', updated_count;
END $$;

-- =============================================================================
-- STEP 2: Clean duplicates under the FINAL uniqueness definition
-- =============================================================================
-- Keep the most recently updated row per (request_id, source_tool, entity_key)
-- and delete the rest.

DO $$
DECLARE
    duplicate_groups INTEGER;
    deleted_rows INTEGER;
BEGIN
    SELECT COUNT(*) INTO duplicate_groups
    FROM (
        SELECT request_id, source_tool, entity_key, COUNT(*) AS cnt
        FROM master_results
        GROUP BY request_id, source_tool, entity_key
        HAVING COUNT(*) > 1
    ) d;

    IF duplicate_groups > 0 THEN
        RAISE NOTICE 'Found % duplicate groups (post-backfill). Cleaning now...', duplicate_groups;

        WITH ranked AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY request_id, source_tool, entity_key
                    ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
                ) AS rn
            FROM master_results
        ),
        to_delete AS (
            SELECT id FROM ranked WHERE rn > 1
        )
        DELETE FROM master_results m
        USING to_delete d
        WHERE m.id = d.id;

        GET DIAGNOSTICS deleted_rows = ROW_COUNT;
        RAISE NOTICE 'Deleted % duplicate rows (kept newest per group)', deleted_rows;
    ELSE
        RAISE NOTICE 'No duplicate groups found under (request_id, source_tool, entity_key)';
    END IF;
END $$;

-- =============================================================================
-- STEP 3: Drop old constraint (if it exists)
-- =============================================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_master_results_request_entity_key'
          AND conrelid = 'master_results'::regclass
    ) THEN
        ALTER TABLE master_results
            DROP CONSTRAINT uq_master_results_request_entity_key;
        RAISE NOTICE 'Dropped old constraint uq_master_results_request_entity_key';
    ELSE
        RAISE NOTICE 'Old constraint uq_master_results_request_entity_key not found (skipping)';
    END IF;
END $$;

-- =============================================================================
-- STEP 4: Create new constraint with source_tool
-- =============================================================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_master_results_request_tool_entity'
          AND conrelid = 'master_results'::regclass
    ) THEN
        ALTER TABLE master_results
            ADD CONSTRAINT uq_master_results_request_tool_entity
            UNIQUE (request_id, source_tool, entity_key);

        RAISE NOTICE 'Created new constraint uq_master_results_request_tool_entity';
    ELSE
        RAISE NOTICE 'Constraint uq_master_results_request_tool_entity already exists (skipping)';
    END IF;
END $$;

-- =============================================================================
-- STEP 5: Add composite index for query performance
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_master_results_request_tool
    ON master_results(request_id, source_tool);

DO $$
BEGIN
    RAISE NOTICE 'Ensured index idx_master_results_request_tool exists';
END $$;

-- =============================================================================
-- STEP 6: Documentation
-- =============================================================================

COMMENT ON CONSTRAINT uq_master_results_request_tool_entity ON master_results IS
'Ensures one result per (request_id, source_tool, entity_key). Prevents duplicates when enrichment updates existing rows. GoogleMaps uses gmaps:{place_id} as stable entity_key.';

-- =============================================================================
-- STEP 7: Verification
-- =============================================================================

DO $$
DECLARE
    constraint_exists INTEGER;
    index_exists INTEGER;
    remaining_duplicates INTEGER;
BEGIN
    -- Verify new constraint exists
    SELECT COUNT(*) INTO constraint_exists
    FROM pg_constraint
    WHERE conname = 'uq_master_results_request_tool_entity'
      AND conrelid = 'master_results'::regclass
      AND contype = 'u';

    IF constraint_exists = 0 THEN
        RAISE EXCEPTION 'VERIFICATION FAILED: uq_master_results_request_tool_entity constraint does not exist';
    END IF;

    -- Verify index exists
    SELECT COUNT(*) INTO index_exists
    FROM pg_indexes
    WHERE tablename = 'master_results'
      AND indexname = 'idx_master_results_request_tool';

    IF index_exists = 0 THEN
        RAISE EXCEPTION 'VERIFICATION FAILED: idx_master_results_request_tool index does not exist';
    END IF;

    -- Verify no duplicates remain under final definition
    SELECT COUNT(*) INTO remaining_duplicates
    FROM (
        SELECT request_id, source_tool, entity_key, COUNT(*) AS cnt
        FROM master_results
        GROUP BY request_id, source_tool, entity_key
        HAVING COUNT(*) > 1
    ) d;

    IF remaining_duplicates > 0 THEN
        RAISE EXCEPTION 'VERIFICATION FAILED: % duplicate groups still exist', remaining_duplicates;
    END IF;

    RAISE NOTICE '✓ Migration verification successful';
    RAISE NOTICE '  - UNIQUE (request_id, source_tool, entity_key) exists';
    RAISE NOTICE '  - Index (request_id, source_tool) exists';
    RAISE NOTICE '  - No duplicates remain under new uniqueness rule';
END $$;

COMMIT;

-- =============================================================================
-- END OF MIGRATION
-- =============================================================================
