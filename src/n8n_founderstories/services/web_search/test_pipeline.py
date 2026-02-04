import asyncio
import json

from n8n_founderstories.services.web_search.factory import build_web_search_deps
from n8n_founderstories.services.web_search.pipeline import run_pipeline



SEARCH_PLAN = {
  "raw_prompt": "Bio Vegan protein",
  "request_id": "c33d68c3-385e-49ff-a0a9-da2967382f9d",
  "provider_name": "openrouter/google/gemini-2.5-flash",
  "target_search": "Bio Vegan protein",
  "target_search_en": "Bio Vegan protein",
  "prompt_language": "en",
  "location": None,
  "industry": "Vegan Protein Products",
  "category": "Food & Beverage",
  "alternates": [],
  "keywords": [],
  "geo": "DACH",
  "geo_location_keywords": {
    "AT": {"hl": "de", "locations": ["Austria"]},
    "CH": {"hl": "de", "locations": ["Switzerland"]},
    "DE": {"hl": "de", "locations": ["Germany"]},
  },
  "sources_to_use": ["llm"],
  "web_queries": [],
  "maps_queries": [],
  "saved_at": "2026-01-26T20:53:09.642873+00:00"
}


async def main():
    deps = build_web_search_deps()

    out = await run_pipeline(
        deps=deps,
        search_plan=SEARCH_PLAN,
        max_pages=1,
        engine="google_light",
        resolve_locations=True,
        classify_workers=5,
        extract_blog_limit=5,
        job_id="test_job_001",  # Added for DB persistence testing
    )

    print(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"\n✅ Pipeline completed. Check database for persisted results with request_id: {SEARCH_PLAN['request_id']}")


if __name__ == "__main__":
    asyncio.run(main())
