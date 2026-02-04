-- Fix the combine index to only match successful det and llm extractions
-- This ensures combine worker only processes rows where both det and llm succeeded

-- Drop the old index
DROP INDEX IF EXISTS idx_web_enrichment_request_combine_ready;

-- Recreate with correct conditions
CREATE INDEX idx_web_enrichment_request_combine_ready
ON web_scraper_enrichment_results (request_id)
WHERE extraction_status = 'crawl_ok' 
  AND det_status = 'ok'
  AND llm_status = 'ok'
  AND combine_status IS NULL;