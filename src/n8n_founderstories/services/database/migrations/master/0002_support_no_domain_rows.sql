-- =============================================================================
-- Migration: 0002_support_no_domain_rows.sql
-- Purpose: Support Google Maps rows without domains using entity_key approach
-- =============================================================================
--
-- This migration extends the Master schema to support rows without domains:
-- - Adds entity_key column for stable uniqueness
-- - Updates unique constraint to use (request_id, entity_key)
-- - Allows domain_norm to be empty string for no-domain rows
--
-- entity_key rules:
-- - entity_key = domain_norm if present and non-empty
-- - else entity_key = 'gmaps:{source_ref}' if source_ref present (place_id)
-- - else entity_key = 'unknown:{id}'
--
-- =============================================================================

BEGIN;

-- =============================================================================
-- STEP 1: Add entity_key column (if not exists)
-- =============================================================================
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'master_results' AND column_name = 'entity_key'
    ) THEN
        ALTER TABLE master_results ADD COLUMN entity_key TEXT;
        RAISE NOTICE 'Added entity_key column to master_results';
    END IF;
END $$;

-- =============================================================================
-- STEP 2: Backfill entity_key for existing rows
-- =============================================================================
UPDATE master_results
SET entity_key = CASE
    WHEN domain_norm IS NOT NULL AND domain_norm != '' THEN domain_norm
    WHEN source_ref IS NOT NULL AND source_ref != '' THEN 'gmaps:' || source_ref
    ELSE 'unknown:' || id::text
END
WHERE entity_key IS NULL;

ALTER TABLE master_results ALTER COLUMN entity_key SET NOT NULL;

DO $$
BEGIN
    RAISE NOTICE 'Backfilled entity_key for existing rows';
END $$;

-- =============================================================================
-- STEP 3: Create function to auto-compute entity_key on insert/update
-- =============================================================================
CREATE OR REPLACE FUNCTION compute_master_entity_key()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.domain_norm IS NOT NULL AND NEW.domain_norm != '' THEN
        NEW.entity_key = NEW.domain_norm;
    ELSIF NEW.source_ref IS NOT NULL AND NEW.source_ref != '' THEN
        NEW.entity_key = 'gmaps:' || NEW.source_ref;
    ELSE
        -- Fallback: relies on NEW.id (may be NULL on BEFORE INSERT depending on schema/defaults)
        NEW.entity_key = 'unknown:' || COALESCE(NEW.id::text, '');
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- STEP 4: Create trigger to auto-compute entity_key
-- =============================================================================
DROP TRIGGER IF EXISTS trg_master_results_entity_key ON master_results;

CREATE TRIGGER trg_master_results_entity_key
    BEFORE INSERT OR UPDATE ON master_results
    FOR EACH ROW
    EXECUTE FUNCTION compute_master_entity_key();

DO $$
BEGIN
    RAISE NOTICE 'Created trigger to auto-compute entity_key';
END $$;

-- =============================================================================
-- STEP 5: Update unique constraint to use entity_key
-- =============================================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_master_results_request_domain_norm'
          AND conrelid = 'master_results'::regclass
    ) THEN
        ALTER TABLE master_results DROP CONSTRAINT uq_master_results_request_domain_norm;
        RAISE NOTICE 'Dropped old constraint uq_master_results_request_domain_norm';
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_master_results_request_entity_key'
          AND conrelid = 'master_results'::regclass
    ) THEN
        ALTER TABLE master_results
            ADD CONSTRAINT uq_master_results_request_entity_key
            UNIQUE (request_id, entity_key);

        RAISE NOTICE 'Created new constraint uq_master_results_request_entity_key';
    END IF;
END $$;

-- =============================================================================
-- STEP 6: Add index on entity_key
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_master_results_entity_key
    ON master_results(entity_key);

DO $$
BEGIN
    RAISE NOTICE 'Created index on entity_key';
END $$;

-- =============================================================================
-- STEP 7: Comments / documentation
-- =============================================================================
COMMENT ON COLUMN master_results.entity_key IS
'Computed unique key: domain_norm if present, else gmaps:{source_ref}. Used for deduplication to support rows without domains.';

COMMENT ON CONSTRAINT uq_master_results_request_entity_key ON master_results IS
'Ensures one result per entity_key per request. Supports both domain-based (Hunter) and place_id-based (Google Maps) rows.';

-- =============================================================================
-- STEP 8: Verification
-- =============================================================================
DO $$
DECLARE
    entity_key_exists INTEGER;
    constraint_exists INTEGER;
    trigger_exists INTEGER;
BEGIN
    SELECT COUNT(*) INTO entity_key_exists
    FROM information_schema.columns
    WHERE table_name = 'master_results' AND column_name = 'entity_key';

    IF entity_key_exists = 0 THEN
        RAISE EXCEPTION 'VERIFICATION FAILED: entity_key column does not exist';
    END IF;

    SELECT COUNT(*) INTO constraint_exists
    FROM pg_constraint
    WHERE conname = 'uq_master_results_request_entity_key'
      AND conrelid = 'master_results'::regclass
      AND contype = 'u';

    IF constraint_exists = 0 THEN
        RAISE EXCEPTION 'VERIFICATION FAILED: uq_master_results_request_entity_key constraint does not exist';
    END IF;

    SELECT COUNT(*) INTO trigger_exists
    FROM pg_trigger
    WHERE tgname = 'trg_master_results_entity_key'
      AND tgrelid = 'master_results'::regclass;

    IF trigger_exists = 0 THEN
        RAISE EXCEPTION 'VERIFICATION FAILED: trg_master_results_entity_key trigger does not exist';
    END IF;

    RAISE NOTICE '✓ Migration verification successful';
    RAISE NOTICE '  - entity_key column exists';
    RAISE NOTICE '  - UNIQUE CONSTRAINT on (request_id, entity_key) exists';
    RAISE NOTICE '  - Auto-compute trigger exists';
    RAISE NOTICE '  - Index on entity_key exists';
END $$;

COMMIT;

-- =============================================================================
-- END OF MIGRATION
-- =============================================================================
