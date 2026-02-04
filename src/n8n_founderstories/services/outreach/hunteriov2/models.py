from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence


@dataclass(frozen=True)
class HunterInput:
    request_id: str

    # At least one of these must be provided
    target_prompt: str | None = None          # used as "query" when keywords are empty
    keywords: Sequence[str] = ()              # used as "keywords" when non-empty

    industries: Sequence[str] | None = None

    # Prepared by search_plan_parser: list of location dicts.
    # Each item may have: country, city, continent, business_region
    locations: Sequence[dict[str, str]] = ()

    # NEW: optional Sheets export target
    sheet_id: str | None = None

    def validate(self) -> None:
        if not self.request_id.strip():
            raise ValueError("request_id is required")

        has_target = bool(self.target_prompt and self.target_prompt.strip())
        has_keywords = bool(self.keywords and any(k.strip() for k in self.keywords))

        if not (has_target or has_keywords):
            raise ValueError("Either target_prompt or keywords must be provided")

        # normalize / sanity-check locations
        for loc in self.locations or []:
            if not isinstance(loc, dict):
                raise ValueError(f"Invalid location entry: {loc!r}")
            if "city" in loc and "country" not in loc:
                raise ValueError(f"Location has city but no country: {loc!r}")

        if self.industries is not None:
            cleaned = [x.strip() for x in self.industries if x and x.strip()]
            if not cleaned:
                raise ValueError("industries was provided but is empty after cleaning")
