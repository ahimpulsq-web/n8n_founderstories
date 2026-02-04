from __future__ import annotations

from urllib.parse import urlparse


def _norm_domain(s: str) -> str:
    return (
        (s or "")
        .lower()
        .replace("http://", "")
        .replace("https://", "")
        .lstrip("www.")
        .rstrip("/")
    )





def _norm(s: str) -> str:
    return (
        (s or "")
        .lower()
        .replace("http://", "")
        .replace("https://", "")
        .lstrip("www.")
        .strip()
    )


def compute_company_confidence(
    *,
    name: str,
    domain: str,
    source_urls: list[str],
) -> float:
    score = 0

    norm_name = _norm(name)
    norm_domain = _norm(domain)

    name_token = norm_name.split()[0]
    domain_token = norm_domain.split(".")[0]

    # 1) company name matches domain token
    if name_token == domain_token:
        score += 1

    # 2) source url host matches domain
    for u in source_urls:
        try:
            host = _norm(urlparse(u).hostname or "")
            if host == norm_domain:
                score += 1
                break
        except Exception:
            continue

    # 3) frequency reinforcement
    if len(source_urls) >= 2:
        score += 1

    confidence_map = {
        0: 0.1,
        1: 0.4,
        2: 0.7,
        3: 1.0,
    }

    return confidence_map.get(score, 1.0)
