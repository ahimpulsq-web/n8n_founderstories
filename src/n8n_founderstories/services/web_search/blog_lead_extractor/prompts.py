EXTRACT_COMPANIES_FROM_BLOG_PROMPT = """
You are extracting company leads from a blog article.

SEARCH INTENT:
{search_intent}

Rules:
- Extract ONLY companies that MATCH the search intent.
- The company must be a real business or startup.
- Ignore:
  - investors, banks, VCs (unless they ARE the subject company)
  - sponsors, partners, brands mentioned in passing
  - sports teams, people, acronyms, concepts
- If the article is about ONE company → return that company.
- If the article lists MULTIPLE relevant companies → return all of them.
- If NO relevant companies are present → return an empty list.

For each company, return:
- name
- website (if inferable)
- evidence: sentence proving relevance to the intent

Return STRICT JSON:
{
  "companies": [
    {
      "name": "...",
      "website": "... | null",
      "evidence": "..."
    }
  ]
}
"""
