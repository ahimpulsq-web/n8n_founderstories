from __future__ import annotations

import re
from typing import Optional

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None  # type: ignore


_MAX_DESCRIPTION_LENGTH = 300


def extract_company_description(html: str, url: str = "") -> Optional[str]:
    """
    Extract company description from HTML (prefer about pages, fallback to homepage).
    
    Args:
        html: HTML content of the page
        url: URL of the page (for debugging/logging)
    
    Returns:
        Company description text, or None if not found
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
    
    # Method 1: Try meta description tag
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        desc = meta_desc["content"].strip()
        if desc and len(desc) > 20:  # Valid description
            return _clean_and_truncate(desc)
    
    # Method 2: Try Open Graph description
    og_desc = soup.find("meta", attrs={"property": "og:description"})
    if og_desc and og_desc.get("content"):
        desc = og_desc["content"].strip()
        if desc and len(desc) > 20:
            return _clean_and_truncate(desc)
    
    # Method 3: Extract from page content
    # Remove script, style, nav, header, footer
    for tag in soup.find_all(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    
    # Try to find main content area
    main_content = None
    
    # Look for semantic HTML5 elements first
    for selector in ["main", "article", "[role='main']"]:
        elem = soup.select_one(selector)
        if elem:
            main_content = elem
            break
    
    # Look for common class names
    if not main_content:
        for class_name in ["about", "about-us", "content", "main-content", "description"]:
            elem = soup.find(class_=re.compile(class_name, re.I))
            if elem:
                main_content = elem
                break
    
    # Fallback to body if no main content found
    if not main_content:
        main_content = soup.find("body")
    
    if not main_content:
        return None
    
    # Extract text and find first substantial paragraph
    text = main_content.get_text(separator=" ", strip=True)
    
    # Split into sentences/paragraphs
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n|\.\s+", text) if p.strip()]
    
    # Find first paragraph that's substantial (at least 50 chars, not too long)
    for para in paragraphs:
        if 50 <= len(para) <= 500:
            # Check if it looks like a description (not navigation, copyright, etc.)
            para_lower = para.lower()
            skip_keywords = [
                "cookie", "datenschutz", "privacy", "impressum", "imprint",
                "copyright", "all rights reserved", "alle rechte vorbehalten",
                "login", "register", "sign up", "anmelden",
                "navigation", "menu", "menü",
            ]
            if not any(keyword in para_lower for keyword in skip_keywords):
                return _clean_and_truncate(para)
    
    # Fallback: take first 300 chars of cleaned text
    cleaned = re.sub(r"\s+", " ", text).strip()
    if len(cleaned) > 50:
        return _clean_and_truncate(cleaned)
    
    return None


def _clean_and_truncate(text: str) -> str:
    """Clean text and truncate to max length, preferring complete sentences."""
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()
    
    if len(text) <= _MAX_DESCRIPTION_LENGTH:
        return text
    
    # Truncate at sentence boundary if possible
    truncated = text[:_MAX_DESCRIPTION_LENGTH]
    
    # Try to find last sentence ending before the truncation point
    last_period = truncated.rfind(".")
    last_exclamation = truncated.rfind("!")
    last_question = truncated.rfind("?")
    
    last_sentence_end = max(last_period, last_exclamation, last_question)
    
    if last_sentence_end > _MAX_DESCRIPTION_LENGTH * 0.7:  # If sentence end is reasonably close
        return truncated[: last_sentence_end + 1].strip()
    
    # Otherwise truncate at word boundary
    last_space = truncated.rfind(" ")
    if last_space > _MAX_DESCRIPTION_LENGTH * 0.8:
        return truncated[:last_space].strip() + "..."
    
    return truncated.strip() + "..."
