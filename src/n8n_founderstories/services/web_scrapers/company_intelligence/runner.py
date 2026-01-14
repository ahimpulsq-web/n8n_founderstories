# runner.py (UPDATED)

from __future__ import annotations

import asyncio
import os

from n8n_founderstories.services.web_scrapers.company_intelligence import (
    CompanyIntelligenceService,
    Crawl4AIClient,
    Crawl4AICompanyCrawler,
    EnsembleCompanyLLMExtractor,
    OpenRouterClient,
    OpenRouterCompanyLLMExtractor,
    OpenRouterConfig,
)

_service_lock = asyncio.Lock()
_service: CompanyIntelligenceService | None = None
_llm_sem: asyncio.Semaphore | None = None


def load_openrouter_models() -> list[str]:
    raw = os.environ.get("OPENROUTER_MODELS", "")
    models = [m.strip() for m in raw.split(",") if m.strip()]
    if not models:
        raise RuntimeError("OPENROUTER_MODELS is empty")
    return models


def _get_llm_semaphore() -> asyncio.Semaphore:
    global _llm_sem
    if _llm_sem is None:
        llm_concurrency = int(os.environ.get("LLM_CONCURRENCY", "4"))
        _llm_sem = asyncio.Semaphore(max(1, llm_concurrency))
    return _llm_sem


def build_extractor() -> object:
    """
    LLM mode:
    - COMPANY_INTEL_LLM_MODE=single   -> uses OPENROUTER_MODEL if set, else first OPENROUTER_MODELS
    - COMPANY_INTEL_LLM_MODE=ensemble -> uses all OPENROUTER_MODELS
    """
    mode = os.environ.get("COMPANY_INTEL_LLM_MODE", "ensemble").strip().lower()

    api_key = os.environ["OPENROUTER_API_KEY"]
    http_referer = os.environ.get("OPENROUTER_HTTP_REFERER")
    x_title = os.environ.get("OPENROUTER_X_TITLE", "n8n-founderStories")

    models = load_openrouter_models()
    llm_sem = _get_llm_semaphore()

    if mode == "single":
        model = (os.environ.get("OPENROUTER_MODEL") or models[0]).strip()
        client = OpenRouterClient(
            OpenRouterConfig(
                api_key=api_key,
                model=model,
                http_referer=http_referer,
                x_title=x_title,
            )
        )

        # throttle OpenRouter requests
        client._sem = llm_sem  # simple injection, see note below
        return OpenRouterCompanyLLMExtractor(client)

    extractors = []
    for model in models:
        client = OpenRouterClient(
            OpenRouterConfig(
                api_key=api_key,
                model=model,
                http_referer=http_referer,
                x_title=x_title,
            )
        )
        client._sem = llm_sem  # simple injection
        extractors.append((model, OpenRouterCompanyLLMExtractor(client)))

    return EnsembleCompanyLLMExtractor(extractors)


async def get_service() -> CompanyIntelligenceService:
    global _service
    async with _service_lock:
        if _service is not None:
            return _service

        crawl_client = Crawl4AIClient(
            timeout_s=float(os.environ.get("CRAWL4AI_TIMEOUT_S", "45")),
            max_concurrency=int(os.environ.get("CRAWL4AI_MAX_CONCURRENCY", "4")),
            user_agent=os.environ.get("CRAWL4AI_USER_AGENT"),
            min_domain_interval_s=float(os.environ.get("CRAWL4AI_MIN_DOMAIN_INTERVAL_S", "5")),
        )

        crawler = Crawl4AICompanyCrawler(crawl_client)
        extractor = build_extractor()
        _service = CompanyIntelligenceService(crawler=crawler, extractor=extractor)
        return _service


async def run_domain(domain: str, language: str | None = None):
    svc = await get_service()
    return await svc.run(domain, language=language or os.environ.get("LANGUAGE", "de"))
