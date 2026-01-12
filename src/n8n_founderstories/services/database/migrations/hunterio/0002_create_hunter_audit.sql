-- Migration: 0002_create_hunter_audit.sql
-- Purpose: Store Hunter.io audit rows mirrored from Google Sheets "HunterIO_Audit"

BEGIN;

CREATE TABLE IF NOT EXISTS hunter_audit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    job_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    query_type TEXT,
    intended_location TEXT,
    intended_headcount TEXT,
    applied_location TEXT,
    applied_headcount TEXT,
    query_text TEXT,
    keywords TEXT,
    keyword_match TEXT,
    total_results INTEGER,
    returned_count INTEGER,
    appended_rows INTEGER,
    applied_filters JSONB,
    
    -- Unique constraint for idempotency: prevent duplicate audit entries
    CONSTRAINT uq_hunter_audit_unique_run 
        UNIQUE (job_id, request_id, query_type, intended_location, intended_headcount, query_text, keywords, keyword_match)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_hunter_audit_job_id
    ON hunter_audit(job_id);

CREATE INDEX IF NOT EXISTS idx_hunter_audit_request_id
    ON hunter_audit(request_id);

CREATE INDEX IF NOT EXISTS idx_hunter_audit_created_at
    ON hunter_audit(created_at);

CREATE INDEX IF NOT EXISTS idx_hunter_audit_query_type
    ON hunter_audit(query_type);

-- JSONB index for applied_filters
CREATE INDEX IF NOT EXISTS idx_hunter_audit_applied_filters_gin
    ON hunter_audit USING GIN (applied_filters);

COMMIT;