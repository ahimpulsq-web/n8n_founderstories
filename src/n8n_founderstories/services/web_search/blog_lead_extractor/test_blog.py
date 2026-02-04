# test_blog_pipeline.py
# Run from project root:
# python src/n8n_founderstories/services/blog_lead_extractor/test_blog_pipeline.py

import asyncio

from n8n_founderstories.services.web_search.openrouter_client import OpenRouterClient
from n8n_founderstories.services.web_search.llm_classifier.service import LLMWebsiteClassifier
from n8n_founderstories.services.web_search.blog_lead_extractor.service import BlogLeadExtractorService


RESULTS = [
    {
        "url": "https://www.seedtable.com/best-software-as-a-service-saas-startups",
        "title": "69 Best Software As A Service SaaS Startups to Watch in 2026",
        "snippet": "Sky Engine AI is an advanced data science technology and research company that develops evolutionary AI platform for deep learning in virtual reality for any ...",
    },
    {
        "url": "https://growthlist.co/list-of-funded-saas-startups/",
        "title": "List of Funded SaaS Startups For 2026 - Growth List",
        "snippet": "Over 40% of recently funded startups in our database incorporate AI or machine learning capabilities, from generative AI tools to predictive ...",
    },
    {
        "url": "https://qubit.capital/blog/software-ai-startup-funding",
        "title": "2026 AI-Driven SaaS Funding Trends & Strategies for Startups",
        "snippet": "AI-driven SaaS startups secure funding by pitching to venture capital firms, joining accelerator programs, and forging strategic partnerships.",
    },
    {
        "url": "https://www.f6s.com/companies/saas-ai/mo",
        "title": "80 Top SaaS AI Companies · January 2026 - F6S",
        "snippet": "Detailed info and reviews on 80 top SaaS AI companies and startups in 2026. Get the latest updates on their products, jobs, funding, investors, founders and ...",
    },
    {
        "url": "https://topstartups.io/?industries=Artificial%20Intelligence",
        "title": "Top 159 AI Startups 2026 | Funded by Sequoia, YC, A16Z",
        "snippet": "Top AI startups and new AI companies hiring now. Sort by valuation and recent funding. Funded by Sequoia, YC, A16Z, Benchmark – the very best.",
    },
    {
        "url": "https://www.trendingtopics.eu/ai-wird-saas-startups-wie-wir-sie-bisher-kannten-auf-den-kopf-stellen/",
        "title": "AI wird SaaS-Startups, wie wir sie bisher kannten, auf den Kopf stellen",
        "snippet": "Ziel ist es, Erkrankungen besser zu verstehen, schnellere und zuverlässigere Diagnosen zu ermöglichen und neue Therapien voranzutreiben.",
    },
    {
        "url": "https://www.hubspot.com/startups/reports/hypergrowth-startups/ai-startup-funding",
        "title": "These AI Startups & Their Funding Prove AI Is Overtaking SaaS",
        "snippet": "Largest AI startup funding deals in 2024 including Databricks, OpenAI, xAI, Waymo and Anthropic.",
    },
    {
        "url": "https://www.saastock.com/blog/startups-to-watch/",
        "title": "14 SaaS and AI startups to watch in 2025 - SaaStock",
        "snippet": "From analytics and automation to onboarding and sales, here are 14 AI and SaaS startups to watch at SaaStock Europe 2025.",
    },
    {
        "url": "https://www.ventureradar.com/startup/ai-powered_[pcnt]_20saas",
        "title": "Top AI-powered SaaS Start-ups | VentureRadar",
        "snippet": "Top start-ups for AI-powered SaaS at VentureRadar with Innovation Scores, Core Health Signals and more.",
    },
]



async def main():
    # Link classifier (free -> premium fallback)
    clf_client = OpenRouterClient.from_env(
        tier_env="LINK_CLASSIFIER_TIER",
        fallback_env="LLM_PREMIUM_MODELS",
    )
    clf = LLMWebsiteClassifier(clf_client)

    # Blog lead extractor (premium recommended)
    blog_llm = OpenRouterClient.from_env(
        tier_env="BLOG_EXTRACTOR_TIER",
        fallback_env="LLM_PREMIUM_MODELS",
    )
    extractor = BlogLeadExtractorService(blog_llm)

    # 1) classify all links
    classified = clf.classify_many(RESULTS, max_workers=5)

    blogs = [x for x in classified if x["classification"].get("type") == "blog"]

    print("\nCLASSIFIED COUNTS")
    print("  total:", len(classified))
    print("  blogs:", len(blogs))
    print("  non-blogs:", len(classified) - len(blogs))

    # 2) extract companies from blog links
    for b in blogs:
        url = b["url"]
        print("\n=== BLOG ===")
        print(url)
        res = await extractor.extract_from_url(url)
        print("companies_found:", len(res.get("companies", [])))
        for c in res.get("companies", [])[:10]:
            print(" -", c.get("name"), "|", c.get("website"))


if __name__ == "__main__":
    asyncio.run(main())
