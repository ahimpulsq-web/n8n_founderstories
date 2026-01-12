from __future__ import annotations

import re
import html
from urllib.parse import unquote

from n8n_founderstories.core.utils.email import extract_emails


_ZERO_WIDTH = re.compile(r"[\u200B-\u200D\uFEFF\u00AD]")

_OBFUSCATIONS = [
    (re.compile(r"\s*\[\s*at\s*\]\s*", re.I), "@"),
    (re.compile(r"\s*\(\s*at\s*\)\s*", re.I), "@"),
    (re.compile(r"\s+at\s+", re.I), "@"),
    (re.compile(r"\s*\[\s*dot\s*\]\s*", re.I), "."),
    (re.compile(r"\s*\(\s*dot\s*\)\s*", re.I), "."),
    (re.compile(r"\s+dot\s+", re.I), "."),
]

_MAILTO_RE = re.compile(r"mailto:([^\"\'\s<>]+)", re.I)


def _dedupe_preserve(items: list[str]) -> list[str]:
    """Deduplicate while preserving order."""
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _normalize_email_text(s: str) -> str:
    """Normalize email text by removing obfuscations and zero-width characters."""
    s = html.unescape(s or "")
    s = _ZERO_WIDTH.sub("", s)
    s = s.replace("\n", " ").replace("\r", " ")
    for rx, repl in _OBFUSCATIONS:
        s = rx.sub(repl, s)
    s = s.replace("{at}", "@").replace("{dot}", ".")
    return s


def _extract_mailto_targets(html: str) -> list[str]:
    """Extract email addresses from mailto: links."""
    out: list[str] = []
    for m in _MAILTO_RE.finditer(html or ""):
        target = m.group(1)
        target = target.split("?", 1)[0]  # Remove query string
        target = unquote(target)  # URL decode
        out.append(target.strip())
    return out


def extract_emails_from_html(html: str, url: str = "") -> list[str]:
    """
    Extract email addresses from HTML content.
    
    Handles obfuscated emails (at/dot patterns, zero-width chars) and mailto links.
    
    Args:
        html: HTML content
        url: URL of the page (for context)
    
    Returns:
        List of unique email addresses found
    """
    if not html:
        return []
    normalized = _normalize_email_text(html)
    mailtos = _extract_mailto_targets(normalized)
    emails = extract_emails(normalized)
    return _dedupe_preserve(mailtos + emails)
