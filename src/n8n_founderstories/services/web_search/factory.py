from __future__ import annotations

from dataclasses import dataclass

from .google_search.serpapi_client import SerpApiClient
from .google_search.service import GoogleSearchService
from .llm_classifier.service import LLMWebsiteClassifier
from .blog_lead_extractor.service import BlogLeadExtractorService
from .openrouter_client import OpenRouterClient


@dataclass(frozen=True)
class WebSearchDeps:
    google: GoogleSearchService
    classifier: LLMWebsiteClassifier
    blog: BlogLeadExtractorService


def build_web_search_deps() -> WebSearchDeps:
    serp = SerpApiClient.from_env()
    google = GoogleSearchService(serp)

    llm = OpenRouterClient.from_env(
        tier_env="LINK_CLASSIFIER_TIER",
        fallback_env="LLM_PREMIUM_MODELS",
    )
    classifier = LLMWebsiteClassifier(llm)
    blog = BlogLeadExtractorService(llm)

    return WebSearchDeps(google=google, classifier=classifier, blog=blog)
