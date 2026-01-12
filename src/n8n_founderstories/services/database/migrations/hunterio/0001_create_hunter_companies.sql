-- Migration: 0001_create_hunter_companies.sql
-- Purpose: Store Hunter.io company rows mirrored from Google Sheets "HunterIO"

BEGIN;

-- Needed for gen_random_uuid()
-- (PostgreSQL 13+ with pgcrypto available; safe to run even if already enabled)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS hunter_companies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    job_id TEXT NOT NULL,
    request_id TEXT NOT NULL,

    domain TEXT,
    organization TEXT,
    applied_location TEXT,
    applied_headcount_bucket TEXT,
    intended_location TEXT,
    intended_headcount_bucket TEXT,
    source_query TEXT,
    query_type TEXT
);

-- Idempotency (case-insensitive) for domain within same job/request
-- Postgres requires this as a UNIQUE INDEX (expression), not a table constraint.
CREATE UNIQUE INDEX IF NOT EXISTS uq_hunter_companies_domain_job_request
    ON hunter_companies (LOWER(domain), job_id, request_id)
    WHERE domain IS NOT NULL AND domain <> '';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_hunter_companies_job_id
    ON hunter_companies(job_id);

CREATE INDEX IF NOT EXISTS idx_hunter_companies_request_id
    ON hunter_companies(request_id);

CREATE INDEX IF NOT EXISTS idx_hunter_companies_domain_lower
    ON hunter_companies(LOWER(domain));

CREATE INDEX IF NOT EXISTS idx_hunter_companies_created_at
    ON hunter_companies(created_at);

COMMIT;
