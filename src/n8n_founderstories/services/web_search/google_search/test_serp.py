from n8n_founderstories.services.web_search.google_search.serpapi_client import SerpApiClient
from n8n_founderstories.services.web_search.google_search.service import GoogleSearchService

plan = {
  "web_queries": ["Saas AI startups"],
  "prompt_language": "en",
  "geo_location_keywords": {
    "DE": {"locations": ["Germany"]},
  }
}

client = SerpApiClient.from_env()
svc = GoogleSearchService(client)

hits = svc.run_search_plan(
    web_queries=plan["web_queries"],
    geo_location_keywords=plan["geo_location_keywords"],
    max_pages=1,
    engine="google_light",
    resolve_locations=True,
    prompt_language=plan.get("prompt_language"),
)

print("hits:", len(hits))
print(hits[0] if hits else "no results")
