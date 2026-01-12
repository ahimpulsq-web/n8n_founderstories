-- Migration: 0005_fix_enrich_queue_state_machine.sql
-- Purpose: Fix queue state machine to prevent infinite reprocessing loops
--
-- Root Cause: Queue items marked FAILED immediately become eligible again
-- without any retry gating, causing the same items to be re-fetched repeatedly.
--
-- This migration adds:
-- - next_retry_at: Timestamp-based retry gating with exponential backoff
-- - locked_at: Track when item was claimed by worker
-- - locked_by: Optional worker identifier for debugging
-- - FAILED_FINAL: Terminal state for items that exceeded max_attempts

BEGIN;

-- =============================================================================
-- 1. Add new columns for retry gating and locking
-- =============================================================================

-- Add next_retry_at for exponential backoff
ALTER TABLE gmaps_enrich_queue 
    ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ NULL;

-- Add locked_at to track when item was claimed
ALTER TABLE gmaps_enrich_queue 
    ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ NULL;

-- Add locked_by for worker identification (optional, for debugging)
ALTER TABLE gmaps_enrich_queue 
    ADD COLUMN IF NOT EXISTS locked_by TEXT NULL;

COMMENT ON COLUMN gmaps_enrich_queue.next_retry_at IS 
'Timestamp when failed item becomes eligible for retry (exponential backoff)';

COMMENT ON COLUMN gmaps_enrich_queue.locked_at IS 
'Timestamp when item was claimed by worker (for monitoring stuck items)';

COMMENT ON COLUMN gmaps_enrich_queue.locked_by IS 
'Worker identifier (optional, for debugging concurrent processing)';

-- =============================================================================
-- 2. Update state constraint to include FAILED_FINAL
-- =============================================================================

-- Drop old constraint
ALTER TABLE gmaps_enrich_queue 
    DROP CONSTRAINT IF EXISTS chk_gmaps_enrich_queue_state;

-- Add new constraint with FAILED_FINAL state
ALTER TABLE gmaps_enrich_queue 
    ADD CONSTRAINT chk_gmaps_enrich_queue_state 
        CHECK (state IN ('PENDING', 'PROCESSING', 'IN_PROGRESS', 'DONE', 'FAILED', 'FAILED_FINAL'));

COMMENT ON CONSTRAINT chk_gmaps_enrich_queue_state ON gmaps_enrich_queue IS 
'Valid states: PENDING (new/retry), PROCESSING/IN_PROGRESS (claimed), DONE (success), FAILED (retry eligible), FAILED_FINAL (terminal)';

-- =============================================================================
-- 3. Update indexes for efficient retry-gated queries
-- =============================================================================

-- Drop old worker pickup index
DROP INDEX IF EXISTS idx_gmaps_enrich_queue_worker_pickup;

-- New worker pickup index: includes next_retry_at for retry gating
-- Workers query: WHERE request_id=? AND state IN ('PENDING','FAILED') 
--                AND (next_retry_at IS NULL OR next_retry_at <= NOW())
CREATE INDEX IF NOT EXISTS idx_gmaps_enrich_queue_worker_pickup_v2
    ON gmaps_enrich_queue (request_id, state, next_retry_at, created_at)
    WHERE state IN ('PENDING', 'FAILED');

COMMENT ON INDEX idx_gmaps_enrich_queue_worker_pickup_v2 IS 
'Optimized for worker queries with retry gating: filters by request_id, state, and next_retry_at';

-- Index for monitoring stuck IN_PROGRESS items
CREATE INDEX IF NOT EXISTS idx_gmaps_enrich_queue_stuck_items
    ON gmaps_enrich_queue (state, locked_at)
    WHERE state IN ('PROCESSING', 'IN_PROGRESS');

COMMENT ON INDEX idx_gmaps_enrich_queue_stuck_items IS 
'Find items stuck in PROCESSING/IN_PROGRESS state for too long (worker crash recovery)';

-- =============================================================================
-- 4. Backfill existing data
-- =============================================================================

-- Set next_retry_at = NULL for PENDING items (immediately eligible)
UPDATE gmaps_enrich_queue 
SET next_retry_at = NULL 
WHERE state = 'PENDING' AND next_retry_at IS NOT NULL;

-- Set next_retry_at = NOW() for FAILED items (immediately eligible for first retry)
-- This gives existing failed items a chance to retry immediately
UPDATE gmaps_enrich_queue 
SET next_retry_at = NOW() 
WHERE state = 'FAILED' AND next_retry_at IS NULL;

-- Normalize state: PROCESSING -> IN_PROGRESS for consistency
UPDATE gmaps_enrich_queue 
SET state = 'IN_PROGRESS' 
WHERE state = 'PROCESSING';

-- =============================================================================
-- 5. Update monitoring view
-- =============================================================================

-- Drop old view
DROP VIEW IF EXISTS v_gmaps_enrich_queue_summary;

-- Recreate with new states and retry info
CREATE OR REPLACE VIEW v_gmaps_enrich_queue_summary AS
SELECT 
    job_id,
    request_id,
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE state = 'PENDING') as pending,
    COUNT(*) FILTER (WHERE state IN ('PROCESSING', 'IN_PROGRESS')) as processing,
    COUNT(*) FILTER (WHERE state = 'DONE') as done,
    COUNT(*) FILTER (WHERE state = 'FAILED') as failed,
    COUNT(*) FILTER (WHERE state = 'FAILED_FINAL') as failed_final,
    COUNT(*) FILTER (WHERE state = 'FAILED' AND (next_retry_at IS NULL OR next_retry_at <= NOW())) as retry_eligible,
    MIN(created_at) as first_created,
    MAX(updated_at) as last_updated,
    AVG(attempts) FILTER (WHERE state IN ('DONE', 'FAILED', 'FAILED_FINAL')) as avg_attempts,
    MAX(attempts) as max_attempts
FROM gmaps_enrich_queue
GROUP BY job_id, request_id;

COMMENT ON VIEW v_gmaps_enrich_queue_summary IS 
'Enhanced queue summary with retry eligibility and terminal failure tracking';

-- =============================================================================
-- 6. Create helper function for exponential backoff calculation
-- =============================================================================

CREATE OR REPLACE FUNCTION calculate_next_retry_at(
    current_attempts INT,
    base_delay_seconds INT DEFAULT 2,
    max_delay_seconds INT DEFAULT 300
) RETURNS TIMESTAMPTZ AS $$
DECLARE
    delay_seconds INT;
BEGIN
    -- Exponential backoff: min(max_delay, base_delay * 2^attempts)
    delay_seconds := LEAST(
        max_delay_seconds,
        base_delay_seconds * POWER(2, current_attempts)::INT
    );
    
    RETURN NOW() + (delay_seconds || ' seconds')::INTERVAL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION calculate_next_retry_at IS 
'Calculate next retry timestamp using exponential backoff: min(300s, 2s * 2^attempts)';

COMMIT;