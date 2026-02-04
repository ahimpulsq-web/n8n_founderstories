-- Migration: 0001_create_web_search_results.sql
-- Purpose: Store web search leads (company hits + blog extracted companies)

BEGIN;

-- Needed for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS web_search_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Job/Request tracking
    job_id TEXT,
    request_id TEXT NOT NULL,

    -- Lead classification
    source_type TEXT NOT NULL,  -- 'company_hit' | 'blog_extracted'
    organisation TEXT NOT NULL,
    website TEXT,

    -- Search context
    query TEXT,
    country TEXT,  -- ISO2: DE/AT/CH
    location TEXT,  -- canonical location string
    language TEXT,  -- hl parameter
    domain TEXT,    -- google domain used

    -- Source provenance
    source_url TEXT NOT NULL,  -- company URL OR blog URL where extracted

    -- Classification metadata
    confidence DOUBLE PRECISION,
    reason TEXT,
    evidence TEXT,  -- blog evidence sentence
    snippet TEXT,   -- hit snippet

    -- Raw payload for debugging
    raw_json JSONB,

    -- Deduplication key
    dedupe_key TEXT NOT NULL
);

-- Uniqueness constraint for idempotency
-- Allows same request to be rerun without duplicates
CREATE UNIQUE INDEX IF NOT EXISTS uq_web_search_results_request_dedupe
    ON web_search_results (request_id, dedupe_key);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_web_search_results_job_id
    ON web_search_results(job_id)
    WHERE job_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_web_search_results_request_id
    ON web_search_results(request_id);

CREATE INDEX IF NOT EXISTS idx_web_search_results_created_at
    ON web_search_results(created_at);

CREATE INDEX IF NOT EXISTS idx_web_search_results_source_type
    ON web_search_results(source_type);

CREATE INDEX IF NOT EXISTS idx_web_search_results_organisation
    ON web_search_results(organisation);

CREATE INDEX IF NOT EXISTS idx_web_search_results_website
    ON web_search_results(website)
    WHERE website IS NOT NULL;

COMMIT;