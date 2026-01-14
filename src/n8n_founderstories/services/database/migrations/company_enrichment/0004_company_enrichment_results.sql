-- Migration: Add company_enrichment_results table
-- Purpose: Store deterministic enrichment data (emails, contacts, company description)
--          for companies in master_results table
-- Date: 2026-01-12

-- Create company_enrichment_results table
CREATE TABLE IF NOT EXISTS company_enrichment_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Keys (composite unique constraint)
    request_id TEXT NOT NULL,
    master_result_id UUID NOT NULL,
    
    -- Master data (denormalized for convenience)
    organization TEXT,
    domain TEXT,
    source TEXT,
    
    -- Enrichment data
    emails TEXT,  -- Formatted: (email1:url1), (email2:url2) - best first
    contacts TEXT,  -- Formatted: name1: role1; name2: role2
    extraction_status TEXT,  -- ok, not_found, skipped, blocked_or_forbidden, etc.
    debug_message TEXT,
    
    -- Unique constraint: one enrichment per (request_id, master_result_id)
    CONSTRAINT uq_company_enrichment_request_master UNIQUE (request_id, master_result_id)
);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_company_enrichment_request_id 
    ON company_enrichment_results(request_id);

CREATE INDEX IF NOT EXISTS idx_company_enrichment_master_result_id 
    ON company_enrichment_results(master_result_id);

CREATE INDEX IF NOT EXISTS idx_company_enrichment_status 
    ON company_enrichment_results(extraction_status);

-- Create updated_at trigger
CREATE OR REPLACE FUNCTION update_company_enrichment_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_company_enrichment_updated_at
    BEFORE UPDATE ON company_enrichment_results
    FOR EACH ROW
    EXECUTE FUNCTION update_company_enrichment_updated_at();

-- Add comments for documentation
COMMENT ON TABLE company_enrichment_results IS 
    'Stores deterministic enrichment data (emails, contacts) extracted from company websites. '
    'Keyed by (request_id, master_result_id) for idempotent upserts.';

COMMENT ON COLUMN company_enrichment_results.emails IS 
    'Formatted email list with best email first: (email1:url1), (email2:url2), ...';

COMMENT ON COLUMN company_enrichment_results.contacts IS 
    'Formatted contact list: name1: role1; name2: role2';

COMMENT ON COLUMN company_enrichment_results.extraction_status IS 
    'Status: ok, not_found, skipped, blocked_or_forbidden, server_unavailable, timeout, request_error, no_emails_found';

COMMENT ON COLUMN company_enrichment_results.debug_message IS 
    'Debug information from extraction process (pages visited, errors, etc.)';