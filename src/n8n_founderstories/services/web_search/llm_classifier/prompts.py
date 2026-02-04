CLASSIFY_WEBSITE_PROMPT = """
You are classifying a website link.

Given:
- URL
- Page title
- Snippet (if any)

Decide if the link is:
- "company" → official business / brand / product website
- "blog" → blog, magazine, news, review, content site, study, report
- "other" → marketplace, directory, forum, social, unknown

If type == "company":
- Extract company_name (best guess from title/domain)

Return STRICT JSON:
{
  "type": "company|blog|other",
  "confidence": 0.0-1.0,
  "reason": "short explanation",
  "company_name": "..." | null
}
"""
