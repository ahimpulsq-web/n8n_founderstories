from __future__ import annotations

import re
from typing import Optional

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None  # type: ignore


# Title prefix patterns (Dr., Prof., Herr, Frau, etc.)
_TITLE_PREFIX_RE = re.compile(
    r"^(?:Dr\.?|Prof\.?|Mag\.?|Dipl\.-Ing\.?(?:\s+agr\.?)?|Dipl\.-Kfm\.?|Ing\.?|Herr|Frau|Herrn|Frauen)\s+",
    re.IGNORECASE,
)

# Remove trailing "legal/register/tax" tails commonly appended due to text flattening
_TRAILING_LEGAL_RE = re.compile(
    r"""
    \s+
    (?:USt|USt-ID|USt-IdNr|USTID|VAT|MwSt|
       HRB|HRA|VR|FN|CHE|
       Amtsgericht|Registergericht|
       Handelsregister|Firmenbuch)
    \b
    .*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Cut text after typical payment processor / disclaimer blocks that contain unrelated Geschäftsführer lists
_CUTOFF_MARKERS = [
    "kreditkartenabrechnungen",
    "payone",
    "payment",
    "zahlungs",
]


def _clean_person_name(name: str) -> str:
    """Clean person name by removing trailing legal terms and normalizing whitespace."""
    name = re.sub(r"\s+", " ", (name or "")).strip()
    name = _TRAILING_LEGAL_RE.sub("", name).strip()
    return name


def _strip_title_prefix(name: str) -> str:
    """Strip title prefixes (Dr., Prof., etc.) for matching stability."""
    # Keep titles in output if you prefer; currently we drop them for matching stability.
    # If you want to preserve: return name unchanged and only use this for matching.
    n = (name or "").strip()
    n = _TITLE_PREFIX_RE.sub("", n).strip()
    return n


def _cut_noise_blocks(text: str) -> str:
    """Cut text after payment processor/disclaimer markers to prevent pollution."""
    t = text or ""
    low = t.lower()
    # If a cutoff marker appears, cut from it onward (prevents pay processor Geschäftsführer pollution).
    cut_pos = None
    for m in _CUTOFF_MARKERS:
        p = low.find(m)
        if p != -1:
            cut_pos = p if cut_pos is None else min(cut_pos, p)
    return t[:cut_pos] if cut_pos is not None else t


# Keywords for finding contact person titles (priority order: higher = more important)
_CONTACT_TITLE_KEYWORDS: list[tuple[str, int]] = [
    ("gesetzliche anbieterkennung", 120),
    ("anbieterkennung", 115),
    ("angaben gemäß", 114),
    ("angaben gemaess", 114),  # without umlaut
    ("tmg", 113),  # Telemediengesetz
    ("vertreten durch", 112),
    ("vertretungsberechtigt", 112),
    ("inhaltlich verantwortlicher", 110),
    ("inhaltlich verantwortlich", 110),
    ("verantwortlicher gemäß", 105),
    ("verantwortlich", 95),  # keep lower than Geschäftsführer terms, but still used

    ("geschäftsführer", 100),
    ("geschäftsführerin", 100),
    ("geschäftsführung", 95),
    ("geschäftsleitung", 90),
    
    ("präsident", 88),
    ("präsidentin", 88),
    ("vize-präsident", 85),
    ("vizepräsident", 85),
    ("chairman", 87),
    ("vice chairman", 86),
    ("vice chairlady", 86),
    ("chairperson", 85),
    
    ("executive board", 90),
    ("vorstand", 60),
    ("board of directors", 85),
    ("board member", 70),
    
    ("secretary general", 88),
    ("generalsekretär", 88),
    ("generalsekretärin", 88),
    ("secretary", 75),
    ("sekretär", 75),
    ("sekretärin", 75),

    ("ceo", 85),
    ("chief executive officer", 80),
    ("managing director", 75),
    ("director", 70),

    ("vorstand", 60),
    ("vorstandsmitglied", 58),
    ("vorstandsmitglieder", 58),
    ("inhaber", 55),
    ("owner", 50),

    ("ansprechpartner", 55),
    ("ansprechperson", 55),
    ("kontaktperson", 55),

    ("kontakt", 30),
    ("contact", 25),
]

# Tolerant: allows particles (von/van/zu/de/di), hyphens, and multi-part names.
# We intentionally do NOT include all-caps tokens (VR/HRB) in the pattern.
_NAME_PATTERN = re.compile(
    r"\b"
    r"(?:Dr\.?|Prof\.?|Mag\.?|Dipl\.-Ing\.?(?:\s+agr\.?)?|Dipl\.-Kfm\.?|Ing\.?)?\s*"
    r"[A-ZÄÖÜ][a-zäöüß]+(?:[-'][A-ZÄÖÜ][a-zäöüß]+)*"
    r"(?:\s+(?:von|van|zu|de|di|del|der))?"
    r"(?:\s+[A-ZÄÖÜ][a-zäöüß]+(?:[-'][A-ZÄÖÜ][a-zäöüß]+)*){1,3}"
    r"\b"
)


def _element_contains_cutoff_marker(element) -> bool:
    """Check if element or its parent context contains cutoff markers."""
    # Check the element itself and its parent containers
    for elem in [element] + list(element.parents):
        if elem is None:
            continue
        text = elem.get_text(" ", strip=True).lower()
        for marker in _CUTOFF_MARKERS:
            if marker in text:
                return True
    return False


def _normalize_role_from_label(label_text: str) -> Optional[str]:
    """Extract and normalize role keyword from label text like 'Präsident:' or 'Geschäftsführer:'."""
    label_lower = label_text.lower().rstrip(":")
    
    # Find the matching keyword
    matched_keyword = None
    for kw, _ in _CONTACT_TITLE_KEYWORDS:
        if kw in label_lower:
            matched_keyword = kw
            break
    
    if not matched_keyword:
        return None
    
    # Skip very generic keywords that shouldn't be used as roles
    skip_keywords = {"kontakt", "contact", "verantwortlich", "verantwortlicher gemäß"}
    if matched_keyword in skip_keywords:
        return None
    
    # Normalize: map common keywords to proper capitalization
    role_mapping = {
        "geschäftsführer": "Geschäftsführer",
        "geschäftsführerin": "Geschäftsführerin",
        "geschäftsführung": "Geschäftsführung",
        "geschäftsleitung": "Geschäftsleitung",
        "präsident": "Präsident",
        "präsidentin": "Präsidentin",
        "vize-präsident": "Vizepräsident",
        "vizepräsident": "Vizepräsident",
        "chairman": "Chairman",
        "vice chairman": "Vice Chairman",
        "vice chairlady": "Vice Chairlady",
        "chairperson": "Chairperson",
        "executive board": "Executive Board",
        "vorstand": "Vorstand",
        "board of directors": "Board of Directors",
        "board member": "Board Member",
        "vorstandsmitglied": "Vorstandsmitglied",
        "vorstandsmitglieder": "Vorstandsmitglieder",
        "vorstandsvorsitzender": "Vorstandsvorsitzender",
        "secretary general": "Secretary General",
        "generalsekretär": "Generalsekretär",
        "generalsekretärin": "Generalsekretärin",
        "secretary": "Secretary",
        "sekretär": "Sekretär",
        "sekretärin": "Sekretärin",
        "ceo": "CEO",
        "chief executive officer": "CEO",
        "managing director": "Managing Director",
        "director": "Director",
        "inhaber": "Inhaber",
        "owner": "Owner",
        "gesetzliche anbieterkennung": "Anbieterkennung",
        "anbieterkennung": "Anbieterkennung",
        "angaben gemäß": "Angaben gemäß TMG",
        "angaben gemaess": "Angaben gemäß TMG",
        "tmg": "TMG",
        "vertreten durch": "Vertreten durch",
        "vertretungsberechtigt": "Vertretungsberechtigt",
        "inhaltlich verantwortlicher": "Verantwortlicher",
        "inhaltlich verantwortlich": "Verantwortlicher",
        "ansprechpartner": "Ansprechpartner",
        "ansprechperson": "Ansprechperson",
        "kontaktperson": "Kontaktperson",
    }
    
    normalized = role_mapping.get(matched_keyword)
    if normalized:
        return normalized
    
    # Fallback: capitalize first letter
    return matched_keyword.capitalize()


def _extract_name_and_role(part: str) -> Optional[tuple[str, str]]:
    """Extract name and optional role from text like 'Christian Weber, Homburg (Präsident)'."""
    part = part.strip()
    if not part:
        return None
    
    # Find name using pattern
    m = _NAME_PATTERN.search(part)
    if not m:
        return None
    
    name_match = m.group(0)
    
    # Try to extract role from parentheses
    role = None
    paren_match = re.search(r"\(([^)]+)\)", part)
    if paren_match:
        role_text = paren_match.group(1).strip()
        # Validate role: should not be a location or legal term
        role_lower = role_text.lower()
        invalid_roles = {
            "gmbh", "ag", "ltd", "inc", "corp", "company", "firma",
            "homburg", "korschenbroich", "kulmbach", "berlin", "munich", "münchen",
            "ust", "vat", "hrb", "hra", "vr", "fn", "amtsgericht",
        }
        if role_lower not in invalid_roles and len(role_text) < 50:
            role = role_text
    
    return (name_match, role)


def extract_contact_name(html: str, url: str = "") -> Optional[str]:
    """
    Extract contact name(s) from imprint/impressum page HTML.
    
    Uses structured-first approach: prefers dt/dd pairs and tables, falls back to regex.
    Returns all unique contact names found with their roles (if any), joined with "; " separator.
    
    Args:
        html: HTML content of the imprint page
        url: URL of the page (for debugging/logging)
    
    Returns:
        Contact name(s) found with roles in parentheses, joined with "; " if multiple, or None if not found
    """
    if not html or not BeautifulSoup:
        return None

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return None

    # Prefer keeping line boundaries for structured parsing quality
    text = soup.get_text(separator="\n", strip=True)
    text = _cut_noise_blocks(text)
    text_lower = text.lower()

    # Track all candidates with their priorities, normalized names, and full text (name + role)
    candidates: list[tuple[str, int, str]] = []  # (normalized_name, priority, full_display_text)

    def push(name_raw: str, priority: int, full_text: Optional[str] = None, role_from_label: Optional[str] = None) -> None:
        """Add candidate with optional role information."""
        text_to_parse = name_raw if full_text is None else full_text
        result = _extract_name_and_role(text_to_parse)
        if not result:
            return
        
        name, role_from_parentheses = result
        n = _clean_person_name(name)
        n2 = _strip_title_prefix(n)
        if _is_valid_name(n2):
            # Use role from label if available, otherwise use role from parentheses
            role = role_from_label if role_from_label else role_from_parentheses
            # Build full text: name + role in parentheses if available
            full_display = name
            if role:
                full_display = f"{name} ({role})"
            candidates.append((n2, priority, full_display))

    # ---------------------------------------------------------------------
    # Method A (best): definition lists and bold labels near values
    # Also check heading tags (h1-h6) which often contain section titles
    # ---------------------------------------------------------------------
    for dt in soup.find_all(["dt", "strong", "b", "h1", "h2", "h3", "h4", "h5", "h6"]):
        # Skip elements that appear in sections with cutoff markers (payment processors, etc.)
        if _element_contains_cutoff_marker(dt):
            continue
        
        # Skip "Kontakt" matches that are in navigation/header/menu areas (these are usually just links)
        parent_tag = dt.parent
        if parent_tag:
            parent_name = parent_tag.name.lower() if hasattr(parent_tag, 'name') else ""
            # Check if parent or grandparent is nav, header, menu, or a link
            is_in_nav = (parent_name in ["nav", "header", "menu", "a"] or 
                        (parent_tag.parent and hasattr(parent_tag.parent, 'name') and 
                         parent_tag.parent.name.lower() in ["nav", "header", "menu"]))
            if is_in_nav:
                label_lower = dt.get_text(" ", strip=True).lower()
                # Only skip if it's a low-priority keyword like "kontakt" or "contact"
                if label_lower in ["kontakt", "contact"]:
                    continue
        
        # Get text from the element and all its descendants (handles nested structures)
        label = dt.get_text(" ", strip=True).lower()
        if not label:
            continue

        matched_priority: Optional[int] = None
        matched_keyword = None
        for kw, pr in _CONTACT_TITLE_KEYWORDS:
            if kw in label:
                matched_priority = pr
                matched_keyword = kw
                break
        if matched_priority is None:
            continue

        # Extract role from label
        role_from_label = _normalize_role_from_label(dt.get_text(" ", strip=True))
        
        # Special handling: if dt is a <strong> or <b> tag and the keyword is in a nested child element,
        # we should also check the parent's siblings (the name might be in a sibling of the parent)
        # This handles: <span><strong><span>Gesetzliche Anbieterkennung:</span></strong></span><br /><strong>Name</strong>
        if dt.name in ["strong", "b"] and parent_tag:
            # Check if parent is a span/div and look for next sibling strong/b tags
            if parent_tag.name in ["span", "div", "p"]:
                # Look for the next <strong> or <b> tag that's a sibling of the parent
                next_strong = parent_tag.find_next_sibling(["strong", "b"])
                if next_strong and not _element_contains_cutoff_marker(next_strong):
                    strong_text = next_strong.get_text(" ", strip=True)
                    if strong_text and _NAME_PATTERN.search(strong_text):
                        invalid_indicators = ["@", "gmbh", "ag", "ltd", "inc", "corp", "company", "ust", "vat", "hrb", "telefon", "tel", "phone", "next generation", "gbR", "gbR"]
                        if not any(indicator in strong_text.lower() for indicator in invalid_indicators):
                            push(strong_text, matched_priority, strong_text, role_from_label)
                
                # Also check all following siblings more broadly (handles cases with <br /> between)
                current_sib = parent_tag.next_sibling
                checked_sibs = 0
                while current_sib and checked_sibs < 8:
                    if hasattr(current_sib, 'name') and current_sib.name in ["strong", "b"]:
                        if not _element_contains_cutoff_marker(current_sib):
                            sib_text = current_sib.get_text(" ", strip=True)
                            if sib_text and _NAME_PATTERN.search(sib_text):
                                invalid_indicators = ["@", "gmbh", "ag", "ltd", "inc", "corp", "company", "ust", "vat", "hrb", "telefon", "tel", "phone", "next generation", "gbR"]
                                if not any(indicator in sib_text.lower() for indicator in invalid_indicators):
                                    push(sib_text, matched_priority, sib_text, role_from_label)
                                    break
                    elif hasattr(current_sib, 'find_all'):
                        # Check for strong/b tags inside this sibling
                        for tag in current_sib.find_all(["strong", "b"], limit=3):
                            if not _element_contains_cutoff_marker(tag):
                                tag_text = tag.get_text(" ", strip=True)
                                if tag_text and _NAME_PATTERN.search(tag_text):
                                    invalid_indicators = ["@", "gmbh", "ag", "ltd", "inc", "corp", "company", "ust", "vat", "hrb", "telefon", "tel", "phone", "next generation", "gbR"]
                                    if not any(indicator in tag_text.lower() for indicator in invalid_indicators):
                                        push(tag_text, matched_priority, tag_text, role_from_label)
                                        checked_sibs = 999  # Signal to break
                                        break
                        if checked_sibs == 999:
                            break
                    current_sib = current_sib.next_sibling
                    checked_sibs += 1

        # Special handling: if the keyword is in a <strong> or <b> tag inside a <p>, get text from parent <p>
        # This handles: <p><strong>Vertreten durch</strong><br>Name1<br>Name2</p>
        if dt.name in ["strong", "b"] and dt.parent and dt.parent.name == "p":
            parent_p = dt.parent
            if not _element_contains_cutoff_marker(parent_p):
                # Get all text from the parent <p> after the keyword tag
                # Find the position of our tag in the parent's children
                parent_text = parent_p.get_text(separator="\n", strip=True)
                keyword_text = dt.get_text(" ", strip=True)
                # Find where the keyword appears in the parent text
                keyword_pos = parent_text.lower().find(keyword_text.lower())
                if keyword_pos != -1:
                    # Get text after the keyword
                    after_keyword = parent_text[keyword_pos + len(keyword_text):].strip()
                    # Remove leading colons, dashes, etc.
                    after_keyword = re.sub(r'^[:\-\s]+', '', after_keyword)
                    if after_keyword:
                        # Split by newlines (from <br> tags) and process each line
                        for line in after_keyword.splitlines():
                            line = line.strip()
                            if not line:
                                continue
                            # Extract names from each line (handles "Name (Role)" format)
                            push(line, matched_priority, line, role_from_label)

        # Special handling for heading tags (h1-h6): look for next <p> tag that contains names
        # This handles: <h3>Angaben gemäß § 5 TMG</h3><p>Company Name<br />Person Name</p>
        # Also handles: <h2>Executive Board</h2> followed by names in paragraphs or lists
        if dt.name and dt.name.startswith("h") and dt.name[1:].isdigit():
            # Find the next <p> tag after this heading
            next_p = dt.find_next_sibling("p")
            if next_p and not _element_contains_cutoff_marker(next_p):
                # Get text from the paragraph, split by <br> tags
                p_text = next_p.get_text(separator="\n", strip=True)
                for line in p_text.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    # Skip lines that look like addresses (contain numbers, "Straße", "Strasse", postal codes)
                    if re.search(r'\d{4,}', line) or any(word in line.lower() for word in ["straße", "strasse", "str.", "street", "platz", "weg"]):
                        continue
                    # Skip company names (contain "GmbH", "AG", "Ltd", etc.)
                    if any(word in line.lower() for word in ["gmbh", "ag", "ltd", "inc", "corp", "company", "firma", "metzgerei", "bäckerei"]):
                        continue
                    # Check if this line looks like a person name
                    if _NAME_PATTERN.search(line):
                        push(line, matched_priority, line, role_from_label)
            
            # Also check for following paragraphs, divs, or lists that might contain names
            # This handles Executive Board sections with multiple names
            next_elem = dt.find_next_sibling(["p", "div", "ul", "ol", "section"])
            checked_elems = 0
            while next_elem and checked_elems < 5:
                if not _element_contains_cutoff_marker(next_elem):
                    elem_text = next_elem.get_text(separator="\n", strip=True)
                    for line in elem_text.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        # Handle "Name (Role)" pattern
                        if "(" in line and ")" in line:
                            result = _extract_name_and_role(line)
                            if result:
                                name, role = result
                                push(name, matched_priority, f"{name} ({role})" if role else name, role)
                        # Or just check if it's a valid name
                        elif _NAME_PATTERN.search(line) and _is_valid_name(line):
                            # Skip addresses and company names
                            if not re.search(r'\d{4,}', line) and not any(word in line.lower() for word in ["straße", "strasse", "gmbh", "ag", "ltd", "@"]):
                                push(line, matched_priority, line, role_from_label)
                next_elem = next_elem.find_next_sibling(["p", "div", "ul", "ol", "section"])
                checked_elems += 1

        # Primary: next sibling value (including strong/b tags for names)
        nxt = dt.find_next_sibling(["dd", "p", "div", "span", "td", "li", "ul", "strong", "b"])
        if nxt and not _element_contains_cutoff_marker(nxt):
            val = nxt.get_text(" ", strip=True)
            # Split lists like "Max X, Erika Y" or bullet lists
            for part in re.split(r"[;,]\s*|\n", val):
                part = part.strip()
                if not part:
                    continue
                push(part, matched_priority, part, role_from_label)
            
            # Also check for names within the next sibling element (handles nested structures)
            # Look for text nodes or child elements that contain names
            if hasattr(nxt, 'get_text'):
                # Get text split by line breaks to handle <br> tags
                val_lines = nxt.get_text(separator="\n", strip=True)
                for line in val_lines.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    # Skip addresses and company names
                    if re.search(r'\d{4,}', line) or any(word in line.lower() for word in ["straße", "strasse", "str.", "street", "platz", "weg", "gmbh", "ag", "ltd"]):
                        continue
                    # Check if line contains a name pattern
                    if _NAME_PATTERN.search(line):
                        push(line, matched_priority, line, role_from_label)
            
            # Also check if the next sibling contains other keywords on separate lines
            # This handles cases like: "Gesetzliche Anbieterkennung:" followed by a paragraph
            # containing "Inhaber: Jörg Müller" on a separate line
            val_with_lines = nxt.get_text(separator="\n", strip=True)
            for line in val_with_lines.splitlines():
                line = line.strip()
                if not line:
                    continue
                # Check if this line contains another keyword
                line_lower = line.lower()
                for kw2, pr2 in _CONTACT_TITLE_KEYWORDS:
                    if kw2 in line_lower and ":" in line:
                        # Extract name from this line
                        after_colon = line.split(":", 1)[1].strip()
                        if after_colon:
                            role_from_label2 = _normalize_role_from_label(line.split(":", 1)[0])
                            for part in re.split(r"[;,]\s*", after_colon):
                                part = part.strip()
                                if part:
                                    push(part, pr2, part, role_from_label2)
                        break

        # Also check next few siblings (handles cases with <br /> tags between keyword and name)
        # Look for the next non-empty text element after the keyword
        current = dt.next_sibling
        checked_count = 0
        while current and checked_count < 5:  # Check up to 5 siblings
            if hasattr(current, 'name'):
                if current.name in ["strong", "b"] and not _element_contains_cutoff_marker(current):
                    val = current.get_text(" ", strip=True)
                    if val and len(val) > 2:  # Only process if there's actual content
                        for part in re.split(r"[;,]\s*|\n", val):
                            part = part.strip()
                            if part and _NAME_PATTERN.search(part):
                                push(part, matched_priority, part, role_from_label)
                                break  # Found a name, stop looking
                elif current.name in ["dd", "p", "div", "span"] and not _element_contains_cutoff_marker(current):
                    val = current.get_text(" ", strip=True)
                    if val and len(val) > 2:
                        for part in re.split(r"[;,]\s*|\n", val):
                            part = part.strip()
                            if part and _NAME_PATTERN.search(part):
                                push(part, matched_priority, part, role_from_label)
                                break
            current = current.next_sibling
            checked_count += 1

        # Also check parent container and its siblings for other strong/b tags that might contain the name
        # This handles cases where keyword and name are in the same parent but separated by <br />
        # First, try the parent's siblings (handles cases where keyword is in a nested structure)
        parent = dt.parent
        if parent:
            # Check parent's next siblings for strong/b tags (handles: <span><strong>keyword</strong></span><br /><strong>name</strong>)
            parent_sibling = parent.next_sibling
            checked_parent_siblings = 0
            while parent_sibling and checked_parent_siblings < 10:  # Increased limit for deeply nested structures
                # Skip text nodes and NavigableString
                if hasattr(parent_sibling, 'name'):
                    if parent_sibling.name in ["strong", "b"] and not _element_contains_cutoff_marker(parent_sibling):
                        tag_text = parent_sibling.get_text(" ", strip=True)
                        if tag_text and _NAME_PATTERN.search(tag_text):
                            invalid_indicators = ["@", "gmbh", "ag", "ltd", "inc", "corp", "company", "ust", "vat", "hrb", "telefon", "tel", "phone"]
                            if not any(indicator in tag_text.lower() for indicator in invalid_indicators):
                                push(tag_text, matched_priority, tag_text, role_from_label)
                                break
                    elif hasattr(parent_sibling, 'find_all'):
                        # Check for strong/b tags inside this sibling element
                        for tag in parent_sibling.find_all(["strong", "b"]):
                            if not _element_contains_cutoff_marker(tag):
                                tag_text = tag.get_text(" ", strip=True)
                                if tag_text and _NAME_PATTERN.search(tag_text):
                                    invalid_indicators = ["@", "gmbh", "ag", "ltd", "inc", "corp", "company", "ust", "vat", "hrb", "telefon", "tel", "phone"]
                                    if not any(indicator in tag_text.lower() for indicator in invalid_indicators):
                                        push(tag_text, matched_priority, tag_text, role_from_label)
                                        break
                # Also check if this sibling contains text that looks like a name (even if not in strong/b tags)
                elif hasattr(parent_sibling, 'get_text'):
                    sibling_text = parent_sibling.get_text(" ", strip=True)
                    if sibling_text and _NAME_PATTERN.search(sibling_text):
                        # Skip if it looks like an address or company
                        if not re.search(r'\d{4,}', sibling_text) and not any(word in sibling_text.lower() for word in ["straße", "strasse", "str.", "gmbh", "ag", "ltd", "@"]):
                            push(sibling_text, matched_priority, sibling_text, role_from_label)
                            break
                parent_sibling = parent_sibling.next_sibling
                checked_parent_siblings += 1
            
            # Also check all following siblings recursively (handles cases with multiple <br /> tags)
            # Look for the next element that contains a name, regardless of tag type
            current_elem = parent.next_sibling
            checked_elems = 0
            while current_elem and checked_elems < 10:
                if hasattr(current_elem, 'find_all'):
                    # Look for strong/b tags in this element or its children
                    for tag in current_elem.find_all(["strong", "b"]):
                        if not _element_contains_cutoff_marker(tag):
                            tag_text = tag.get_text(" ", strip=True)
                            if tag_text and _NAME_PATTERN.search(tag_text):
                                invalid_indicators = ["@", "gmbh", "ag", "ltd", "inc", "corp", "company", "ust", "vat", "hrb", "telefon", "tel", "phone"]
                                if not any(indicator in tag_text.lower() for indicator in invalid_indicators):
                                    push(tag_text, matched_priority, tag_text, role_from_label)
                                    checked_elems = 999  # Signal to break outer loop
                                    break
                    if checked_elems == 999:
                        break
                current_elem = current_elem.next_sibling
                checked_elems += 1
        
        # Also check all strong/b tags in the parent container that come after our keyword tag
        if parent and not _element_contains_cutoff_marker(parent):
            # Find all strong/b tags in the parent that come after our keyword tag
            all_strong_tags = parent.find_all(["strong", "b"])
            found_keyword = False
            for tag in all_strong_tags:
                # Check if this tag contains or is the keyword tag
                if tag == dt:
                    found_keyword = True
                    continue
                # Check if dt is a descendant of this tag
                if dt in tag.descendants:
                    found_keyword = True
                    continue
                if found_keyword and not _element_contains_cutoff_marker(tag):
                    tag_text = tag.get_text(" ", strip=True)
                    if tag_text and _NAME_PATTERN.search(tag_text):
                        # Check if this looks like a name (not an email, company name, etc.)
                        invalid_indicators = ["@", "gmbh", "ag", "ltd", "inc", "corp", "company", "ust", "vat", "hrb", "telefon", "tel", "phone"]
                        if not any(indicator in tag_text.lower() for indicator in invalid_indicators):
                            push(tag_text, matched_priority, tag_text, role_from_label)
                            break  # Found a name, stop looking
        
        # Fallback: Check grandparent container for strong/b tags (handles deeply nested structures)
        if parent and parent.parent and not _element_contains_cutoff_marker(parent.parent):
            grandparent = parent.parent
            # First check grandparent's siblings (handles: <span><strong>keyword</strong></span><br /><strong>name</strong>)
            gp_sibling = grandparent.next_sibling
            checked_gp_siblings = 0
            while gp_sibling and checked_gp_siblings < 10:
                if hasattr(gp_sibling, 'name'):
                    if gp_sibling.name in ["strong", "b"] and not _element_contains_cutoff_marker(gp_sibling):
                        tag_text = gp_sibling.get_text(" ", strip=True)
                        if tag_text and _NAME_PATTERN.search(tag_text):
                            invalid_indicators = ["@", "gmbh", "ag", "ltd", "inc", "corp", "company", "ust", "vat", "hrb", "telefon", "tel", "phone", "next generation", "gbR"]
                            if not any(indicator in tag_text.lower() for indicator in invalid_indicators):
                                push(tag_text, matched_priority, tag_text, role_from_label)
                                checked_gp_siblings = 999  # Signal to break
                                break
                    elif hasattr(gp_sibling, 'find_all'):
                        # Check for strong/b tags inside this sibling
                        for tag in gp_sibling.find_all(["strong", "b"], limit=3):
                            if not _element_contains_cutoff_marker(tag):
                                tag_text = tag.get_text(" ", strip=True)
                                if tag_text and _NAME_PATTERN.search(tag_text):
                                    invalid_indicators = ["@", "gmbh", "ag", "ltd", "inc", "corp", "company", "ust", "vat", "hrb", "telefon", "tel", "phone", "next generation", "gbR"]
                                    if not any(indicator in tag_text.lower() for indicator in invalid_indicators):
                                        push(tag_text, matched_priority, tag_text, role_from_label)
                                        checked_gp_siblings = 999
                                        break
                        if checked_gp_siblings == 999:
                            break
                gp_sibling = gp_sibling.next_sibling
                checked_gp_siblings += 1
            
            # Also check all strong/b tags within the grandparent that come after the keyword
            all_strong_tags = grandparent.find_all(["strong", "b"])
            found_keyword = False
            for tag in all_strong_tags:
                # Check if this tag contains or is the keyword tag
                if tag == dt:
                    found_keyword = True
                    continue
                # Check if dt is a descendant of this tag
                if dt in tag.descendants:
                    found_keyword = True
                    continue
                if found_keyword and not _element_contains_cutoff_marker(tag):
                    tag_text = tag.get_text(" ", strip=True)
                    if tag_text and _NAME_PATTERN.search(tag_text):
                        invalid_indicators = ["@", "gmbh", "ag", "ltd", "inc", "corp", "company", "ust", "vat", "hrb", "telefon", "tel", "phone", "next generation", "gbR"]
                        if not any(indicator in tag_text.lower() for indicator in invalid_indicators):
                            push(tag_text, matched_priority, tag_text, role_from_label)
                            break  # Found a name, stop looking

        # Secondary: same node after colon
        full = dt.get_text(" ", strip=True)
        if ":" in full:
            after = full.split(":", 1)[1].strip()
            for part in re.split(r"[;,]\s*|\n", after):
                part = part.strip()
                if not part:
                    continue
                push(part, matched_priority, part, role_from_label)
        
        # Aggressive search: After finding a keyword, search all following <strong>/<b> tags in the document
        # This handles deeply nested structures where the name might be far from the keyword
        if matched_priority and matched_priority >= 100:  # Only for high-priority keywords
            # First, try to find the common parent container (div, section, article, etc.)
            # This helps when keyword and name are in the same content block but not direct siblings
            container = dt
            found_name_in_container = False
            for _ in range(5):  # Go up max 5 levels
                if container and container.parent:
                    container = container.parent
                    container_name = container.name.lower() if hasattr(container, 'name') else ""
                    # Look for common content container classes/ids
                    if container_name in ["div", "section", "article", "main", "content"]:
                        # Check if this container has class/id that suggests it's a content area
                        classes = container.get("class", []) or []
                        class_str = " ".join(classes).lower()
                        if any(word in class_str for word in ["content", "text", "cms", "element", "block", "main", "article"]):
                            # Find all strong/b tags in this container that come after our keyword
                            all_strong_in_container = container.find_all(["strong", "b"])
                            found_keyword_in_container = False
                            for tag in all_strong_in_container:
                                if tag == dt or dt in tag.descendants:
                                    found_keyword_in_container = True
                                    continue
                                if found_keyword_in_container and not _element_contains_cutoff_marker(tag):
                                    tag_text = tag.get_text(" ", strip=True)
                                    if tag_text and _NAME_PATTERN.search(tag_text):
                                        invalid_indicators = ["@", "gmbh", "ag", "ltd", "inc", "corp", "company", "ust", "vat", "hrb", "telefon", "tel", "phone", "next generation", "gbR", "impressum", "datenschutz"]
                                        if not any(indicator in tag_text.lower() for indicator in invalid_indicators):
                                            # Check if this looks like a person name (not a company or address)
                                            if _is_valid_name(tag_text):
                                                push(tag_text, matched_priority, tag_text, role_from_label)
                                                found_name_in_container = True
                                                break  # Found a name, stop looking in this container
                            if found_name_in_container:
                                break  # Found name, stop going up
                else:
                    break
            
            # Fallback: Find all strong/b tags that come after this one in document order (only if not found in container)
            if not found_name_in_container:
                all_strong = soup.find_all(["strong", "b"])
                found_keyword_tag = False
                for tag in all_strong:
                    if tag == dt or dt in tag.descendants:
                        found_keyword_tag = True
                        continue
                    if found_keyword_tag and not _element_contains_cutoff_marker(tag):
                        tag_text = tag.get_text(" ", strip=True)
                        if tag_text and _NAME_PATTERN.search(tag_text):
                            invalid_indicators = ["@", "gmbh", "ag", "ltd", "inc", "corp", "company", "ust", "vat", "hrb", "telefon", "tel", "phone", "next generation", "gbR", "impressum", "datenschutz"]
                            if not any(indicator in tag_text.lower() for indicator in invalid_indicators):
                                # Check if this looks like a person name (not a company or address)
                                if _is_valid_name(tag_text):
                                    push(tag_text, matched_priority, tag_text, role_from_label)
                                    break  # Found a name, stop looking

    # ---------------------------------------------------------------------
    # Method B: table parsing (common on older Impressum pages)
    # ---------------------------------------------------------------------
    for td in soup.find_all("td"):
        # Skip elements that appear in sections with cutoff markers (payment processors, etc.)
        if _element_contains_cutoff_marker(td):
            continue
            
        lab = td.get_text(" ", strip=True).lower()
        matched_priority = None
        for kw, pr in _CONTACT_TITLE_KEYWORDS:
            if kw in lab:
                matched_priority = pr
                break
        if matched_priority is None:
            continue

        # Extract role from label
        role_from_label = _normalize_role_from_label(td.get_text(" ", strip=True))

        # name may be in next cell
        nxt = td.find_next_sibling("td")
        if nxt and not _element_contains_cutoff_marker(nxt):
            val = nxt.get_text(" ", strip=True)
            for part in re.split(r"[;,]\s*|\n", val):
                part = part.strip()
                if not part:
                    continue
                push(part, matched_priority, part, role_from_label)

    # Also handle list items (ul/ol) that might contain names
    for li in soup.find_all("li"):
        if _element_contains_cutoff_marker(li):
            continue
        li_text = li.get_text(" ", strip=True)
        # Check if this list item is under a heading that matches contact keywords
        parent_text = ""
        heading_elem = None
        for parent in li.parents:
            if parent and parent.name in ["ul", "ol", "div", "section", "article"]:
                # Check previous siblings for headings
                prev = parent.find_previous_sibling(["h1", "h2", "h3", "h4", "h5", "h6", "strong", "b"])
                if prev:
                    parent_text = prev.get_text(" ", strip=True).lower()
                    heading_elem = prev
                    break
                # Also check if parent itself has a heading as a child
                heading_in_parent = parent.find(["h1", "h2", "h3", "h4", "h5", "h6", "strong", "b"], recursive=False)
                if heading_in_parent:
                    parent_text = heading_in_parent.get_text(" ", strip=True).lower()
                    heading_elem = heading_in_parent
                    break
        
        if parent_text:
            matched_priority = None
            for kw, pr in _CONTACT_TITLE_KEYWORDS:
                if kw in parent_text:
                    matched_priority = pr
                    break
            if matched_priority is not None:
                role_from_label = _normalize_role_from_label(parent_text if heading_elem else parent_text)
                # Extract name and role from list item (handles "Name (Role)" format)
                result = _extract_name_and_role(li_text)
                if result:
                    name, role = result
                    # Use role from parentheses if available, otherwise use role from heading
                    final_role = role if role else role_from_label
                    push(name, matched_priority, f"{name} ({final_role})" if final_role else name, final_role)
                else:
                    # Fallback: push the whole text if it looks like a name
                    if _NAME_PATTERN.search(li_text) and _is_valid_name(li_text):
                        push(li_text, matched_priority, li_text, role_from_label)

    # ---------------------------------------------------------------------
    # Method C (fallback): regex on text lines, preserving line boundaries
    # ---------------------------------------------------------------------
    # Look for "Keyword: Name" on a single line
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in lines:
        ln_low = ln.lower()
        for kw, pr in _CONTACT_TITLE_KEYWORDS:
            if kw not in ln_low:
                continue

            # Common: "Geschäftsführer: Dr. Max Mustermann, Erika Musterfrau"
            # Also handles: "Managing Director: Johannes Bechtel"
            if ":" in ln:
                after = ln.split(":", 1)[1].strip()
                role_from_label = _normalize_role_from_label(ln.split(":", 1)[0])
                # Remove leading/trailing whitespace and common prefixes
                after = re.sub(r'^(Herr|Frau|Mr\.?|Mrs\.?|Ms\.?)\s+', '', after, flags=re.IGNORECASE).strip()
                for part in re.split(r"[;,]\s*|\n", after):
                    part = part.strip()
                    if not part:
                        continue
                    # Skip if it looks like an address or company name
                    if re.search(r'\d{4,}', part) or any(word in part.lower() for word in ["straße", "strasse", "str.", "street", "gmbh", "ag", "ltd", "inc", "corp"]):
                        continue
                    # Only push if it looks like a valid name
                    if _NAME_PATTERN.search(part) or _is_valid_name(part):
                        push(part, pr, part, role_from_label)
        
        # Also handle "Name (Role)" pattern on a single line
        # This handles: "Frank Schübel (Chairman)" or "Dr. Monika Beutgen (Secretary General)"
        if "(" in ln and ")" in ln:
            # Check if this line contains a name pattern followed by a role in parentheses
            name_match = _NAME_PATTERN.search(ln)
            if name_match:
                # Extract the full name and role
                result = _extract_name_and_role(ln)
                if result:
                    name, role = result
                    # Check if there's a keyword context (like "Executive Board" section)
                    # Look at previous lines for context
                    line_idx = lines.index(ln)
                    context_lines = lines[max(0, line_idx-3):line_idx]
                    context_text = " ".join(context_lines).lower()
                    # If we're in an "Executive Board" or similar section, extract the name
                    if any(kw in context_text for kw, _ in _CONTACT_TITLE_KEYWORDS if kw in ["executive board", "vorstand", "board of directors"]):
                        push(name, 90, f"{name} ({role})" if role else name, role)
                    # Or if the role itself matches a keyword
                    elif role and any(kw in role.lower() for kw, _ in _CONTACT_TITLE_KEYWORDS):
                        push(name, 90, f"{name} ({role})" if role else name, role)

    # Also handle 2-line pattern:
    # "Geschäftsleitung:" on one line, name on next line
    for i, ln in enumerate(lines[:-1]):
        ln_low = ln.lower().rstrip(":")
        for kw, pr in _CONTACT_TITLE_KEYWORDS:
            if kw in ln_low and ln.strip().endswith(":"):
                nxt = lines[i + 1]
                # Skip lines that start with legal/register terms (VR, HRB, USt-ID, etc.)
                nxt_stripped = nxt.strip()
                legal_starters = ["VR ", "HRB ", "HRA ", "FN ", "CHE ", "USt-ID", "USt-IdNr", "USt ", "USTID", "VAT ", "MwSt ", "Amtsgericht"]
                if any(nxt_stripped.startswith(starter) for starter in legal_starters):
                    continue
                role_from_label = _normalize_role_from_label(ln)
                push(nxt, pr, nxt, role_from_label)

    # Final fallback: If no candidates found, look for names in the main content area
    # This handles cases where the structure is unusual but names are present
    # Check if this looks like an impressum page at all (check both text and URL)
    page_text_lower = text_lower
    url_lower = (url or "").lower()
    is_impressum_page = (any(kw in page_text_lower for kw in ["impressum", "gesetzliche anbieterkennung", "anbieterkennung", "tmg", "verantwortlich"]) or
                         any(kw in url_lower for kw in ["impressum", "imprint", "legal"]))
    
    # If no valid candidates yet, try looking for all <strong> and <b> tags that contain names
    # This is a common pattern where names are in bold tags
    # Run this fallback if we're on an impressum page (deduplication will handle duplicates)
    if is_impressum_page:
        for strong_tag in soup.find_all(["strong", "b"]):
            # Skip cutoff markers only if they're very obvious (payment processors, etc.)
            # Don't skip based on parent context in fallback - be more permissive
            tag_text = strong_tag.get_text(" ", strip=True)
            if not tag_text or len(tag_text) < 3:
                continue
            tag_text_lower = tag_text.lower()
            # Skip obvious non-names
            if tag_text_lower in ["impressum", "gesetzliche anbieterkennung", "anbieterkennung", "verantwortlich", "datenschutz"]:
                continue
            # Skip if contains email or company indicators
            if "@" in tag_text_lower or any(word in tag_text_lower for word in ["gmbh", "ag", "ltd", "inc", "corp", "company"]):
                continue
            # Check if it matches name pattern and is valid
            if _NAME_PATTERN.search(tag_text) and _is_valid_name(tag_text):
                # Simple check: skip only if clearly in nav/footer by tag name
                parent = strong_tag.parent
                is_in_nav = False
                if parent and hasattr(parent, 'name'):
                    parent_name = parent.name.lower()
                    is_in_nav = parent_name in ["nav", "header", "footer", "menu"]
                if not is_in_nav:
                    push(tag_text, 50, tag_text, None)
        
        # Also try to find the main content area (article, main, or content div)
        if not candidates:
            main_content = soup.find("article") or soup.find("main") or soup.find(class_=re.compile("content|article|impressum", re.I))
            if main_content:
                # Look for all text that matches name patterns
                content_text = main_content.get_text(separator="\n", strip=True)
                for line in content_text.splitlines():
                    line = line.strip()
                    if not line or len(line) < 5:
                        continue
                    # Skip lines that are clearly not names
                    if any(word in line.lower() for word in [
                        "telefon", "tel", "phone", "fax", "e-mail", "email", "@",
                        "straße", "strasse", "str.", "street", "platz", "weg",
                        "gmbh", "ag", "ltd", "inc", "corp", "company", "firma",
                        "ust", "vat", "hrb", "hra", "vr", "fn", "amtsgericht",
                        "impressum", "datenschutz", "privacy", "copyright"
                    ]):
                        continue
                    # Skip lines that are mostly numbers (addresses, postal codes)
                    if re.search(r'\d{4,}', line):
                        continue
                    # Check if line looks like a name
                    if _NAME_PATTERN.search(line) and _is_valid_name(line):
                        # Use a lower priority since we don't have a keyword match
                        push(line, 40, line, None)

    if not candidates:
        return None

    # Deduplicate by normalized name, keeping the highest priority for each
    seen: dict[str, tuple[int, str]] = {}  # normalized_name -> (priority, full_display_text)
    for norm_name, priority, full_display in candidates:
        if norm_name not in seen or priority > seen[norm_name][0]:
            seen[norm_name] = (priority, full_display)

    # Sort by priority (highest first), then by name length
    unique_candidates = [(norm, prio, display) for norm, (prio, display) in seen.items()]
    unique_candidates.sort(key=lambda x: (-x[1], -len(x[0])))

    # Return all unique names with roles joined with "; "
    names = [display for _, _, display in unique_candidates]
    return "; ".join(names)


def _is_valid_name(name: str) -> bool:
    """Check if a string looks like a valid person name."""
    if not name or len(name) < 3:
        return False
    
    # Should have at least 2 words (first + last name)
    words = name.split()
    if len(words) < 2:
        return False
    
    # Each word should start with uppercase
    if not all(word[0].isupper() for word in words if word):
        return False
    
    # Reject short all-caps tokens inside the candidate (VR, HRB, USt, etc.)
    if any(w.isupper() and len(w) <= 4 for w in words):
        return False
    
    # Should not contain common non-name words
    invalid_words = {
        "gmbh", "ag", "ltd", "inc", "corp", "company", "firma",
        "straße", "strasse", "str", "street", "platz", "place",
        "telefon", "tel", "phone", "fax", "email", "e-mail",
        "impressum", "imprint", "datenschutz", "privacy",
        "amtsgericht", "handelsregister", "registergericht",
        "ust", "ust-id", "ust-idnr", "mwst", "vat",
        "hrb", "hra", "vr", "fn", "che",
    }
    name_lower = name.lower()
    if any(word in name_lower for word in invalid_words):
        return False
    
    # Should not be too long (probably not a name)
    if len(name) > 100:
        return False
    
    return True
