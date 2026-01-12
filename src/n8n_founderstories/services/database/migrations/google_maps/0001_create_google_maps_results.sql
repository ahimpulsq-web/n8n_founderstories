-- Migration: 0001_create_google_maps_results.sql
-- Purpose: Store Google Maps discovery results mirrored from Google Sheets "GoogleMaps"

BEGIN;

-- Needed for gen_random_uuid()
-- (PostgreSQL 13+ with pgcrypto available; safe to run even if already enabled)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS gmaps_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    job_id TEXT NOT NULL,
    request_id TEXT NOT NULL,

    -- Core Google Maps fields (discovery outputs)
    place_id TEXT,
    name TEXT,
    address TEXT,
    category TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    phone TEXT,
    website TEXT,

    -- Debug / provenance
    query_text TEXT,
    intended_location TEXT,
    source TEXT DEFAULT 'google_maps',
    raw_json JSONB,

    -- Additional fields from Sheets columns
    location_label TEXT,
    domain TEXT,
    business_status TEXT,
    google_maps_url TEXT
);

-- Uniqueness constraint: allow ON CONFLICT inserts
-- Use unique index like: (place_id, job_id, request_id) unique (place_id is main dedupe key)
-- if place_id is empty/null, do not enforce uniqueness
CREATE UNIQUE INDEX IF NOT EXISTS uq_gmaps_results_place_job_request
    ON gmaps_results (place_id, job_id, request_id)
    WHERE place_id IS NOT NULL AND place_id <> '';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_gmaps_results_job_id
    ON gmaps_results(job_id);

CREATE INDEX IF NOT EXISTS idx_gmaps_results_request_id
    ON gmaps_results(request_id);

CREATE INDEX IF NOT EXISTS idx_gmaps_results_created_at
    ON gmaps_results(created_at);

CREATE INDEX IF NOT EXISTS idx_gmaps_results_place_id
    ON gmaps_results(place_id);

COMMIT;