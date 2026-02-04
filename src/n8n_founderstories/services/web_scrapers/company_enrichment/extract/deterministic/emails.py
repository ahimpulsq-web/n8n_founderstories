# src/n8n_founderstories/services/web_scrapers/company_enrichment/extract/deterministic/emails.py
from __future__ import annotations

import html as _html
import re
from typing import Iterable, List
from urllib.parse import unquote

_EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b")
_MAILTO_RE = re.compile(r"mailto:([^\"\'\s<>]+)", re.IGNORECASE)
_ZERO_WIDTH = re.compile(r"[\u200B-\u200D\uFEFF\u00AD]")

_OBFUSCATIONS = [
    (re.compile(r"\s*\[\s*at\s*\]\s*", re.I), "@"),
    (re.compile(r"\s*\(\s*at\s*\)\s*", re.I), "@"),
    (re.compile(r"\s+at\s+", re.I), "@"),
    (re.compile(r"\s*\[\s*dot\s*\]\s*", re.I), "."),
    (re.compile(r"\s*\(\s*dot\s*\)\s*", re.I), "."),
    (re.compile(r"\s+dot\s+", re.I), "."),
]

# Reject “emails” that are actually filenames/assets (png/jpg/svg/etc.)
_ASSET_TLDS = {
    "png", "jpg", "jpeg", "gif", "svg", "webp", "ico",
    "css", "js", "map",
    "pdf", "zip", "rar", "7z", "gz", "tar",
    "woff", "woff2", "ttf", "eot", "otf",
    "mp3", "mp4", "mov", "avi", "mkv",
}

# Common local-part patterns that indicate image assets or similar (e.g., "logo2x", "icon@2x" style)
# Note: local-part never contains "@", so do NOT include "@" in this regex.
_BAD_LOCAL_PART = re.compile(r"(\b\d+x\b)|(\d+x)", re.IGNORECASE)

# Optional: strip trailing punctuation that frequently sticks to emails in text
_TRAILING_PUNCT = re.compile(r"[)\].,;:!]+$")


def _dedupe_preserve(items: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        x = (x or "").strip()
        if not x:
            continue
        x = _TRAILING_PUNCT.sub("", x)
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _normalize_text(s: str) -> str:
    s = _html.unescape(s or "")
    s = _ZERO_WIDTH.sub("", s)
    s = s.replace("\r", " ").replace("\n", " ")
    for rx, repl in _OBFUSCATIONS:
        s = rx.sub(repl, s)
    s = s.replace("{at}", "@").replace("{dot}", ".")
    return s


def _extract_mailtos(text: str) -> List[str]:
    out: List[str] = []
    for m in _MAILTO_RE.finditer(text or ""):
        target = (m.group(1) or "").strip()

        # Strip query params (subject/body/etc.)
        target = target.split("?", 1)[0].strip()

        # Decode percent-encoding (fixes "bei%20fragen...info@x.de" cases)
        target = unquote(target).strip()

        if not target:
            continue

        # Sometimes mailto contains extra text; pull the first valid email if present.
        mm = _EMAIL_RE.search(target)
        if mm:
            out.append(mm.group(0))
        else:
            # Fallback: keep raw target (might still be a plain email)
            out.append(target)

    return out


def _is_plausible_email(e: str) -> bool:
    """
    Post-filter to remove frequent false positives from HTML/CSS assets.
    """
    if not e:
        return False

    e = e.strip()
    if "/" in e or "\\" in e:
        return False

    if "@" not in e:
        return False

    local, _, domain = e.partition("@")
    if not local or not domain:
        return False

    # Reject obvious image-density patterns in local part (e.g. "logo2x", "icon-2x")
    if _BAD_LOCAL_PART.search(local):
        return False

    # Reject "TLDs" that are actually file extensions (png/svg/pdf/css/js/...)
    # Example: bild45@2x-min.png -> tld == "png" => reject
    parts = domain.rsplit(".", 1)
    if len(parts) == 2:
        tld = parts[1].lower()
        if tld in _ASSET_TLDS:
            return False

    return True


def extract_emails_from_text(text: str) -> List[str]:
    if not text:
        return []

    norm = _normalize_text(text)

    candidates: List[str] = []
    candidates.extend(_extract_mailtos(norm))
    candidates.extend(_EMAIL_RE.findall(norm) or [])

    cleaned: List[str] = []
    for e in candidates:
        e = (e or "").strip()
        if not e:
            continue

        e = _TRAILING_PUNCT.sub("", e)
        e = e.strip()

        # IMPORTANT FIX:
        # If the "email" contains percent-encoding, decode it and re-extract emails
        # from the decoded string. This prevents cases like:
        # "bei%20fragen...-%20info@danischpur.de" being treated as a single email.
        if "%" in e:
            dec = unquote(e)
            # pull all emails from the decoded version
            ms = _EMAIL_RE.findall(dec) or []
            for m in ms:
                m = (m or "").strip().lower()
                if m and _is_plausible_email(m):
                    cleaned.append(m)
            continue

        e = e.lower().strip()

        # If any candidate still contains junk around an email, extract the email substring.
        m = _EMAIL_RE.search(e)
        if m:
            e = m.group(0).lower()

        if _is_plausible_email(e):
            cleaned.append(e)

    return _dedupe_preserve(cleaned)

