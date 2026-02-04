-- Add index to optimize COMBINE stage claim query
-- This index helps the combine_worker efficiently find rows ready for combining

CREATE INDEX IF NOT EXISTS idx_web_enrichment_request_combine_ready
ON web_scraper_enrichment_results (request_id)
WHERE extraction_status = 'crawl_ok'
  AND det_status = 'ok'
  AND llm_status = 'ok'
  AND combine_status IS NULL;