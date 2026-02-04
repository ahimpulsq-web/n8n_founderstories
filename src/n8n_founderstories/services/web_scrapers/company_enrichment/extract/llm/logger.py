# src/n8n_founderstories/services/web_scrapers/company_enrichment/llm/logger.py
import logging

logger = logging.getLogger("company_enrichment.llm")

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
