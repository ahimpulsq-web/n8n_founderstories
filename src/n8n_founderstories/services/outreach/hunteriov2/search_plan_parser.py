from __future__ import annotations

from typing import Any

from .models import HunterInput


def _clean_str(x: Any) -> str | None:
    if not isinstance(x, str):
        return None
    s = x.strip()
    return s or None


def _effective_location(loc: dict[str, Any]) -> dict[str, str] | None:
    """
    Choose the single most specific usable location filter per Hunter docs:
      - city requires country
      - state is US-only but we IGNORE state completely
      - if continent exists, ignore business_region even if present
    Priority:
      1) city+country
      2) country
      3) continent
      4) business_region
    """
    country = _clean_str(loc.get("country"))
    city = _clean_str(loc.get("city"))
    continent = _clean_str(loc.get("continent"))
    business_region = _clean_str(loc.get("region")) or _clean_str(loc.get("business_region"))

    # Ignore "state" entirely by design.
    # If caller provides only state+US, we fall back to country=US (below).

    if city and country:
        return {"country": country, "city": city}

    if country:
        return {"country": country}

    if continent:
        return {"continent": continent}

    if business_region:
        return {"business_region": business_region}

    return None


def _dedupe_locations(locs: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[tuple[str, str], ...]] = set()
    out: list[dict[str, str]] = []
    for loc in locs:
        key = tuple(sorted(loc.items()))
        if key in seen:
            continue
        seen.add(key)
        out.append(loc)
    return out


def parse_search_plan(plan: dict[str, Any]) -> HunterInput:
    request_id = _clean_str(plan.get("request_id")) or ""
    target_prompt = _clean_str(plan.get("prompt_target")) or _clean_str(plan.get("prompt_target_en")) or _clean_str(plan.get("normalized_prompt_en"))
    keywords = plan.get("prompt_keywords") or []
    industries = plan.get("matched_industries") or None

    sheet_id = _clean_str(plan.get("sheet_id")) or _clean_str(plan.get("google_sheet_id"))

    # Build effective locations
    resolved = plan.get("resolved_locations") or []
    effective: list[dict[str, str]] = []
    if isinstance(resolved, list):
        for item in resolved:
            if isinstance(item, dict):
                eff = _effective_location(item)
                if eff:
                    effective.append(eff)

    effective = _dedupe_locations(effective)

    return HunterInput(
        request_id=request_id,
        target_prompt=target_prompt,
        keywords=[k.strip() for k in keywords if isinstance(k, str) and k.strip()],
        industries=industries,
        locations=effective,
        sheet_id=sheet_id,
    )
