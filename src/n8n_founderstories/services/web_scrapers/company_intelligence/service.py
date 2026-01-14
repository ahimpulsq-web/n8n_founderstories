from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List

from .providers import CompanyCrawler, CrawlResult, CompanyLLMExtractor
from .models import LLMExtractionResultModel



@dataclass(frozen=True)
class CompanyIntelligenceOutput:
    pages: List[CrawlResult]
    llm: LLMExtractionResultModel


class CompanyIntelligenceService:
    def __init__(self, crawler: CompanyCrawler, extractor: CompanyLLMExtractor):
        self._crawler = crawler
        self._extractor = extractor

    async def run(self, domain: str, language: str = "de") -> CompanyIntelligenceOutput:
        pages = await self._crawler.crawl(domain=domain, language=language)
        extracted = await self._extractor.extract(pages=pages, language=language)

        # Support both:
        # - single model extractor -> LLMExtractionResultModel
        # - ensemble extractor -> EnsembleResult(result=LLMExtractionResultModel, ...)
        if isinstance(extracted, LLMExtractionResultModel):
            llm = extracted
        else:
            llm = extracted.result

        return CompanyIntelligenceOutput(pages=pages, llm=llm)
