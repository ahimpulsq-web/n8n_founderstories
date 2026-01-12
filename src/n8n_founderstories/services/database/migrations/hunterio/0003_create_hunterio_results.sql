-- Migration: 0003_create_hunterio_results.sql
-- Purpose: Create new simplified hunterio_results table to replace hunter_companies

BEGIN;

-- Needed for gen_random_uuid()
-- (PostgreSQL 13+ with pgcrypto available; safe to run even if already enabled)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS hunterio_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    job_id TEXT NOT NULL,
    request_id TEXT NOT NULL,

    organisation TEXT,
    domain TEXT,
    location TEXT,
    headcount TEXT,
    search_query TEXT,
    debug_filters TEXT
);

-- Idempotency (case-insensitive) for domain within same job/request
-- Postgres requires this as a UNIQUE INDEX (expression), not a table constraint.
CREATE UNIQUE INDEX IF NOT EXISTS uq_hunterio_results_domain_job_request
    ON hunterio_results (LOWER(domain), job_id, request_id)
    WHERE domain IS NOT NULL AND domain <> '';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_hunterio_results_job_id
    ON hunterio_results(job_id);

CREATE INDEX IF NOT EXISTS idx_hunterio_results_request_id
    ON hunterio_results(request_id);

CREATE INDEX IF NOT EXISTS idx_hunterio_results_domain_lower
    ON hunterio_results(LOWER(domain));

CREATE INDEX IF NOT EXISTS idx_hunterio_results_created_at
    ON hunterio_results(created_at);

COMMIT;