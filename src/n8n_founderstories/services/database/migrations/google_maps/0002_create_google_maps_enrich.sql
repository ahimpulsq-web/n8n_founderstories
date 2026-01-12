-- Migration: 0002_create_google_maps_enrich.sql
-- Purpose: Store Google Maps enrichment results

BEGIN;

CREATE TABLE IF NOT EXISTS google_maps_enriched (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    job_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    place_id TEXT NOT NULL,

    -- Enrichment fields (only what enrichment actually produces today; keep flexible)
    opening_hours JSONB NULL,
    rating DOUBLE PRECISION NULL,
    reviews_count INTEGER NULL,
    photos_count INTEGER NULL,
    raw_json JSONB NULL,

    -- Unique constraint for upsert/no-duplicate
    CONSTRAINT uq_google_maps_enriched_unique
        UNIQUE (place_id, job_id, request_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_google_maps_enriched_job_id
    ON google_maps_enriched(job_id);

CREATE INDEX IF NOT EXISTS idx_google_maps_enriched_request_id
    ON google_maps_enriched(request_id);

CREATE INDEX IF NOT EXISTS idx_google_maps_enriched_place_id
    ON google_maps_enriched(place_id);

CREATE INDEX IF NOT EXISTS idx_google_maps_enriched_created_at
    ON google_maps_enriched(created_at);

COMMIT;