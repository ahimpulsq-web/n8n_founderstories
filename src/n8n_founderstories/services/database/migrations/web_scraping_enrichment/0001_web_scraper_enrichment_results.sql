-- web_scraper_enrichment_results_v2.sql

CREATE TABLE web_scraper_enrichment_results (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    -- identity
    request_id text NOT NULL,
    master_result_id uuid NOT NULL,

    -- snapshot from master
    organization text NULL,
    domain text NULL,
    source text NULL,

    -- =========================
    -- Crawl layer
    -- =========================
    contact_links JSONB NULL,   -- [url, url, ...]
    contact_case  TEXT  NULL,   -- "1", "2", "3", "5.3", etc
    about_links   JSONB NULL,   -- [url]
    about_case    TEXT  NULL,   -- about_anchor | about_link | about_not_found

    -- =========================
    -- Deterministic extraction
    -- =========================
    det_emails JSONB NULL,
    -- [
    --   { "email": "info@x.com", "source_url": "https://x.com/impressum" }
    -- ]

    -- =========================
    -- LLM extraction
    -- =========================
    llm_company JSONB NULL,
    -- { name, evidence: { url, quote } }

    llm_emails JSONB NULL,
    -- [
    --   { email, evidence: { url, quote } }
    -- ]

    llm_contacts JSONB NULL,
    -- [
    --   { name, role, evidence: { url, quote } }
    -- ]

    llm_about JSONB NULL,
    -- {
    --   short_description, short_evidence,
    --   long_description,  long_evidence
    -- }

    llm_status TEXT NULL,   -- ok | error
    llm_error  TEXT NULL,

    -- =========================
    -- Combine stage
    -- =========================
    combined_emails JSONB NULL,
    -- [
    --   { email, frequency, confidence, source_agreement }
    -- ]

    combined_company JSONB NULL,
    -- { name, confidence }

    combined_descriptions JSONB NULL,
    combined_people JSONB NULL,

    combine_status TEXT NULL,   -- ok | skipped | error
    combine_debug  TEXT NULL,

    -- =========================
    -- Job control
    -- =========================
    extraction_status TEXT NULL, -- pending | running | done | error
    debug_message     TEXT NULL,

    CONSTRAINT uq_web_enrichment_request_master
        UNIQUE (request_id, master_result_id)
);

-- =========================
-- Indexes
-- =========================

CREATE INDEX idx_web_enrichment_request
    ON web_scraper_enrichment_results (request_id);

CREATE INDEX idx_web_enrichment_request_domain
    ON web_scraper_enrichment_results (request_id, domain);

CREATE INDEX idx_web_enrichment_status
    ON web_scraper_enrichment_results (request_id, extraction_status);

-- =========================
-- Persisted crawl artifacts for downstream stages
-- =========================

-- Add columns for persisted crawl artifacts
ALTER TABLE web_scraper_enrichment_results
ADD COLUMN crawl_homepage JSONB NULL,
ADD COLUMN crawl_pages JSONB NULL;

-- Add deterministic extraction status columns
ALTER TABLE web_scraper_enrichment_results
ADD COLUMN det_status TEXT NULL,
ADD COLUMN det_error TEXT NULL;

-- Indexes to help claim queries
CREATE INDEX IF NOT EXISTS idx_web_enrichment_request_det_ready
ON web_scraper_enrichment_results (request_id)
WHERE extraction_status = 'crawl_ok' AND det_status IS NULL;

CREATE INDEX IF NOT EXISTS idx_web_enrichment_request_llm_ready
ON web_scraper_enrichment_results (request_id)
WHERE extraction_status = 'crawl_ok' AND llm_status IS NULL;
