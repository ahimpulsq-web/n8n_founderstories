from __future__ import annotations

from typing import Optional
import trafilatura


def clean_html(html: str) -> str:
    if not html:
        return ""
    text: Optional[str] = trafilatura.extract(
        html,
        include_comments=False,
        include_tables=False,
        output_format="txt",
    )
    return text or ""
