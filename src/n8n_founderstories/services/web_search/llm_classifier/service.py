from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List, Any

from ..openrouter_client import OpenRouterClient
from .prompts import CLASSIFY_WEBSITE_PROMPT
from .run_log import append_classification_result


def _normalize_classification(res: Dict[str, Any], *, url: str) -> Dict[str, Any]:
    """
    Ensures stable schema:
    - always has: type, confidence, reason, company_name
    - if type != company -> company_name = None
    - if type == company -> company_name is kept as determined by LLM (or None if not provided)
    """
    if not isinstance(res, dict):
        res = {}

    t = (res.get("type") or "other").lower()
    res["type"] = t

    # keep numeric confidence if possible
    conf = res.get("confidence")
    if not isinstance(conf, (int, float)):
        res["confidence"] = 0.0

    res.setdefault("reason", "")

    if t == "company":
        # Keep company_name as determined by LLM, or None if not provided
        res.setdefault("company_name", None)
    else:
        res["company_name"] = None

    return res


class LLMWebsiteClassifier:
    def __init__(self, client: OpenRouterClient):
        self._client = client

    def classify(
        self,
        *,
        url: str,
        title: Optional[str] = None,
        snippet: Optional[str] = None,
    ) -> Dict[str, Any]:
        prompt = f"""
{CLASSIFY_WEBSITE_PROMPT}

URL: {url}
TITLE: {title or "-"}
SNIPPET: {snippet or "-"}
"""
        res = self._client.complete_json(prompt)
        return _normalize_classification(res, url=url)

    def classify_many(
        self,
        items: List[Dict[str, Any]],
        *,
        max_workers: int = 5,
        fail_type: str = "other",
    ) -> List[Dict[str, Any]]:
        """
        items: [{url, title, snippet}, ...]
        returns: [{url, title, snippet, classification: {...}}, ...]
        """
        out: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            fut_map = {
                ex.submit(
                    self.classify,
                    url=i["url"],
                    title=i.get("title"),
                    snippet=i.get("snippet"),
                ): i
                for i in items
            }

            for fut in as_completed(fut_map):
                item = fut_map[fut]
                try:
                    res = fut.result()

                    append_classification_result(
                        url=item["url"],
                        title=item.get("title"),
                        snippet=item.get("snippet"),
                        classification=res,
                        model=self._client.model,
                    )

                except Exception as e:
                    res = _normalize_classification(
                        {"type": fail_type, "confidence": 0.0, "reason": str(e)},
                        url=item["url"],
                    )

                out.append({**item, "classification": res})

        return out
