from __future__ import annotations

import codecs
import html
import re
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import unquote

# Boundary-aware email match (still permissive enough for HTML)
_EMAIL_RE = re.compile(
    r"""
    (?<![\w@])
    ([a-zA-Z0-9._%+\-]{1,64}
     @
     (?:[a-zA-Z0-9\-]+\.)+
     [a-zA-Z]{2,})
    (?![\w@])
    """,
    re.VERBOSE,
)

_PUBLIC_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
}

_BLACKLIST_LOCALPART = {
    "abuse", "webmaster", "postmaster", "noreply", "no-reply", "donotreply", "do-not-reply",
    "privacy", "legal", "security",
}

# Prevent asset-filename false positives where "tld" is an image/font/script extension
_ASSET_TLDS = {
    "jpg", "jpeg", "png", "webp", "avif", "gif", "svg", "bmp", "ico",
    "css", "js", "map", "woff", "woff2", "ttf", "eot",
}

# Simple (at)/(dot) patterns (bounded; does not attempt aggressive reconstruction)
_AT_DOT_RE = re.compile(
    r"""
    (?P<local>[a-zA-Z0-9._%+\-]{1,64})\s*
    (?:\(|\[)?\s*(?:at|AT)\s*(?:\)|\])?\s*
    (?P<domain>[a-zA-Z0-9.\-]+)\s*
    (?:\(|\[)?\s*(?:dot|DOT)\s*(?:\)|\])?\s*
    (?P<tld>[a-zA-Z]{2,})
    """,
    re.VERBOSE,
)

# Matches: local (at) domain.tld   (no explicit "dot" token)
_AT_ONLY_RE = re.compile(
    r"""
    (?P<local>[a-zA-Z0-9._%+\-]{1,64})\s*
    (?:\(|\[)?\s*(?:at|AT)\s*(?:\)|\])?\s*
    (?P<domain>(?:[a-zA-Z0-9\-]+\.)+[a-zA-Z]{2,})
    """,
    re.VERBOSE,
)

_MAILTO_RE = re.compile(r"mailto:\s*([^\"\'\s>?]+)", re.IGNORECASE)

# Matches: local (at) domain.tld  (no explicit "dot")
_AT_ONLY_RE = re.compile(
    r"""
    (?P<local>[a-zA-Z0-9._%+\-]{1,64})\s*
    (?:\(|\[)?\s*(?:at|AT)\s*(?:\)|\])?\s*
    (?P<domain>(?:[a-zA-Z0-9\-]+\.)+[a-zA-Z]{2,})
    """,
    re.VERBOSE,
)



@dataclass(frozen=True)
class EmailPick:
    best: Optional[str]
    reason: str = ""


def _rot13(s: str) -> str:
    return codecs.decode(s, "rot_13")


def _looks_like_asset_email(e: str) -> bool:
    """
    Filters false positives like:
      image_1@2x.6d763021.png
      foo@2x.9ae8fad4.avif
    """
    if "@" not in e:
        return True
    local, domain = e.rsplit("@", 1)
    domain = domain.lower()
    local = local.lower()

    if "." in domain:
        tld = domain.rsplit(".", 1)[-1]
        if tld in _ASSET_TLDS:
            return True

    if "@2x" in local or "@3x" in local:
        return True

    # hashed asset patterns: something.<hex>
    if re.search(r"\.[0-9a-f]{6,}$", domain):
        return True

    # domains with no letters are nearly always junk
    if not re.search(r"[a-zA-Z]", domain):
        return True

    return False


def _maybe_decode_rot13_email(e: str) -> str:
    """
    Conservative ROT13 decoding for fruchtsaft-style obfuscation:
      vasb@sehpugfnsg.bet -> info@fruchtsaft.org
      oerhre@jcep.qr -> breuer@wprc.de
    """
    e = (e or "").strip().lower()
    if "@" not in e:
        return e

    _, dom = e.split("@", 1)
    tld = dom.rsplit(".", 1)[-1].lower()

    # Common rot13 results for real TLDs:
    # org->bet, de->qr, com->pbz, net->arg
    suspicious = {"bet", "qr", "pbz", "arg"}
    if tld not in suspicious:
        return e

    decoded = _rot13(e).strip().lower()
    if "@" not in decoded:
        return e

    _, dom2 = decoded.split("@", 1)
    tld2 = dom2.rsplit(".", 1)[-1].lower()
    if tld2 in {"de", "com", "org", "net", "eu", "info"}:
        return decoded

    return e


def _extract_at_dot_obfuscations(text: str) -> list[str]:
    """
    Extracts basic "name (at) domain (dot) tld" patterns.
    Keeps bounded scope; does not attempt complex transformations.
    """
    if not text:
        return []
    out: list[str] = []
    for m in _AT_DOT_RE.finditer(text):
        local = (m.group("local") or "").strip()
        dom = (m.group("domain") or "").strip()
        tld = (m.group("tld") or "").strip()
        if local and dom and tld:
            out.append(f"{local}@{dom}.{tld}".lower())
    return out

def _extract_at_only_obfuscations(text: str) -> list[str]:
    if not text:
        return []
    out: list[str] = []
    for m in _AT_ONLY_RE.finditer(text):
        local = (m.group("local") or "").strip()
        dom = (m.group("domain") or "").strip()
        if local and dom:
            out.append(f"{local}@{dom}".lower())
    return out

def _extract_mailto(text: str) -> list[str]:
    if not text:
        return []
    t = html.unescape(text)
    out: list[str] = []
    for m in _MAILTO_RE.finditer(t):
        addr = (m.group(1) or "").strip()
        addr = unquote(addr)
        addr = addr.split("?", 1)[0].strip().lower()
        if addr and "@" in addr:
            out.append(addr)
    return out


def _extract_at_only_obfuscations(text: str) -> list[str]:
    if not text:
        return []
    out: list[str] = []
    for m in _AT_ONLY_RE.finditer(text):
        local = (m.group("local") or "").strip()
        dom = (m.group("domain") or "").strip()
        if local and dom:
            out.append(f"{local}@{dom}".lower())
    return out




def extract_emails(text: str) -> list[str]:
    """
    Extract emails from text; preserves order; de-dupes; lowercases.
    - filters asset false positives
    - decodes ROT13 obfuscations conservatively
    - extracts mailto:
    - extracts (at)/(dot) and (at)domain.tld patterns
    - handles common German anti-spam token "spamVerhindern.de"
    - HTML entity unescape (&#64;, &commat;, etc.)
    """
    if not text:
        return []

    # Decode HTML entities
    text = html.unescape(text)

    # Normalize a common German anti-spam pattern:
    # "name @spamVerhindern.de @domain.tld" -> "name@domain.tld"
    text = re.sub(
        r"(\b[a-zA-Z0-9._%+\-]{1,64})\s*@spamverhindern\.de\s*@\s*",
        r"\1@",
        text,
        flags=re.IGNORECASE,
    )

    out: list[str] = []
    seen: set[str] = set()

    def add_email(raw: str) -> None:
        e = (raw or "").strip().lower()
        if not e:
            return
        e = _maybe_decode_rot13_email(e)
        if _looks_like_asset_email(e):
            return
        if e not in seen:
            seen.add(e)
            out.append(e)

    # mailto: first
    for e in _extract_mailto(text):
        add_email(e)

    for m in _EMAIL_RE.finditer(text):
        add_email(m.group(1) or "")

    for e in _extract_at_dot_obfuscations(text):
        add_email(e)

    for e in _extract_at_only_obfuscations(text):
        add_email(e)

    return out



def pick_best_email(emails: Iterable[str], *, prefer_domain: Optional[str] = None) -> EmailPick:
    """Rank candidates for outreach use."""
    items = [e.strip().lower() for e in emails if e and "@" in e]
    if not items:
        return EmailPick(best=None, reason="no_candidates")

    prefer = (prefer_domain or "").strip().lower()
    prefer = prefer[4:] if prefer.startswith("www.") else prefer

    def is_prefer_domain(dom: str) -> bool:
        dom = (dom or "").lower()
        dom = dom[4:] if dom.startswith("www.") else dom
        return dom == prefer or dom.endswith("." + prefer)

    def score(email: str) -> int:
        local, domain = email.split("@", 1)
        s = 0

        # Strong preference for exact domain or subdomain; strong penalty otherwise.
        if prefer:
            if is_prefer_domain(domain):
                s += 80
            else:
                s -= 40

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

        # Slight penalty for personal-looking addresses if we have a generic option
        if re.search(r"\d", local) or "." in local:
            s -= 2

        return s

    ranked = sorted(items, key=score, reverse=True)
    return EmailPick(best=ranked[0], reason="ranked")

