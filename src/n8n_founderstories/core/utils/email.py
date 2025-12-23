from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Optional

_EMAIL_RE = re.compile(r"([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})")

_PUBLIC_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
}

_BLACKLIST_LOCALPART = {
    "abuse", "webmaster", "postmaster", "noreply", "no-reply", "donotreply", "do-not-reply",
    "privacy", "legal", "security",
}


@dataclass(frozen=True)
class EmailPick:
    best: Optional[str]
    reason: str = ""


def extract_emails(text: str) -> list[str]:
    """Extract emails from text; preserves order; de-dupes; lowercases."""
    if not text:
        return []

    out: list[str] = []
    seen: set[str] = set()

    for m in _EMAIL_RE.finditer(text):
        e = (m.group(1) or "").strip().lower()
        if e and e not in seen:
            seen.add(e)
            out.append(e)

    return out


def pick_best_email(emails: Iterable[str], *, prefer_domain: Optional[str] = None) -> EmailPick:
    """Rank candidates for outreach use."""
    items = [e.strip().lower() for e in emails if e and "@" in e]
    if not items:
        return EmailPick(best=None, reason="no_candidates")

    def score(email: str) -> int:
        local, domain = email.split("@", 1)
        s = 0

        if prefer_domain and domain.endswith(prefer_domain.lower()):
            s += 30

        if local in {"partners", "partnerships"}:
            s += 25
        elif local in {"sales", "bizdev", "business", "bd"}:
            s += 20
        elif local in {"hello", "hi"}:
            s += 15
        elif local in {"info", "contact", "support"}:
            s += 10

        if local in _BLACKLIST_LOCALPART:
            s -= 50

        if domain in _PUBLIC_EMAIL_DOMAINS:
            s -= 15

        return s

    ranked = sorted(items, key=score, reverse=True)
    return EmailPick(best=ranked[0], reason="ranked")
