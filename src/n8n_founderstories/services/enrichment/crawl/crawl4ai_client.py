"""
═══════════════════════════════════════════════════════════════════════════════
CRAWL4AI CLIENT - Adapter Layer for Browser Automation
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [INFRASTRUCTURE] - Adapter between engine and business logic

PURPOSE:
    Thin adapter that wraps Crawl4AIEngine and converts its results into
    PageArtifact objects used by the business logic layer.

ARCHITECTURE:
    ┌─────────────────────────────────────────────────────────────────────┐
    │  service.py (Business Logic)                                        │
    │  └─> Uses PageArtifact objects                                      │
    └─────────────────────────────────────────────────────────────────────┘
                                    ↓
    ┌─────────────────────────────────────────────────────────────────────┐
    │  crawl4ai_client.py (This File - Adapter)                           │
    │  └─> Converts CrawlResult → PageArtifact                            │
    └─────────────────────────────────────────────────────────────────────┘
                                    ↓
    ┌─────────────────────────────────────────────────────────────────────┐
    │  crawl4ai_engine.py (Infrastructure)                                │
    │  └─> Returns CrawlResult objects                                    │
    └─────────────────────────────────────────────────────────────────────┘

RESPONSIBILITIES:
    ✓ Config adaptation (Crawl4AIClientConfig → Crawl4AIEngineConfig)
    ✓ Result conversion (CrawlResult → PageArtifact)
    ✓ Link normalization (preserve both flat list and raw links)
    ✓ Error extraction (clean, concise error messages)
    ✓ Lifecycle management (start/stop engine)

PAGEARTIFACT CONTRACT:
    The client maintains backward compatibility with PageArtifact:
    
    PageArtifact {
        url: str                    # Page URL
        cleaned_html: str | None    # Sanitized HTML
        markdown: str | None        # Markdown content
        links: List[str]            # Flat list of URLs (http/https only)
        meta: Dict                  # Metadata including:
            - crawl4ai_links_raw: List[Dict] (href + text for anchor analysis)
            - page_type: str (home/contact/impressum/privacy/about/other)
        error: str | None           # Clean error message if failed
    }

LINK HANDLING:
    1. Flat Links (PageArtifact.links):
       - Simple List[str] of URLs
       - Only http/https URLs (filters out mailto:, tel:, etc.)
       - Used for basic link discovery
    
    2. Raw Links (PageArtifact.meta["crawl4ai_links_raw"]):
       - List[Dict] with {href, text} structure
       - Preserves anchor text for intelligent matching
       - Used by text_link_finder.py for keyword-based discovery

ERROR HANDLING:
    - Extracts clean, concise error messages from verbose Crawl4AI errors
    - Handles SSL errors, timeouts, network failures
    - Provides user-friendly error descriptions

CONFIGURATION:
    - headless: Run browser in headless mode (default: True)
    - timeout_s: Page load timeout in seconds (default: 30.0)
    - max_concurrency: Max concurrent browser tabs (default: 15)
    - wait_after_load_s: Wait time after page load (default: 0.1)
    - text_mode: Extract text content only (default: True)
    - light_mode: Minimal rendering overhead (default: True)

USAGE:
    config = Crawl4AIClientConfig(headless=True, timeout_s=30.0)
    async with Crawl4AIClient(config) as client:
        page = await client.fetch("https://example.com")
        print(page.links)  # Flat list of URLs
        print(page.meta["crawl4ai_links_raw"])  # Raw links with text

DEPENDENCIES:
    - crawl4ai_engine.py: Low-level browser automation
    - models.py: PageArtifact definition

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, List
from urllib.parse import urlparse

from ..models import PageArtifact
from .crawl4ai_engine import Crawl4AIEngine, Crawl4AIEngineConfig

# Import Playwright's TimeoutError for proper exception handling
try:
    from playwright._impl._errors import TimeoutError as PlaywrightTimeoutError
except ImportError:
    # Fallback if import fails
    PlaywrightTimeoutError = TimeoutError


@dataclass(frozen=True)
class Crawl4AIClientConfig:
    """
    Runtime configuration for the Crawl4AI client.
    
    This config is adapted to Crawl4AIEngineConfig internally.
    See module docstring for parameter descriptions.
    """
    headless: bool = True
    timeout_s: float = 30.0
    max_concurrency: int = 15
    wait_after_load_s: float = 0.1
    text_mode: bool = True
    light_mode: bool = True


class Crawl4AIClient:
    """
    Adapter that converts Crawl4AIEngine results into PageArtifact objects.
    
    See module docstring for detailed architecture and usage.
    """

    def __init__(self, cfg: Crawl4AIClientConfig):
        self._cfg = cfg
        
        # Create engine config from client config
        # Pass through text_mode and light_mode from config instead of hardcoding
        engine_config = Crawl4AIEngineConfig(
            headless=cfg.headless,
            timeout_s=cfg.timeout_s,
            max_concurrency=cfg.max_concurrency,
            wait_after_load_s=cfg.wait_after_load_s,
            text_mode=cfg.text_mode,
            light_mode=cfg.light_mode
        )
        
        self._engine = Crawl4AIEngine(engine_config)

    async def start(self) -> None:
        """Start the underlying engine."""
        await self._engine.start()

    async def close(self) -> None:
        """Close the underlying engine."""
        await self._engine.close()

    async def __aenter__(self) -> "Crawl4AIClient":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # -----------------------------
    # URL filtering & normalization
    # -----------------------------

    @staticmethod
    def _is_http_url(u: str) -> bool:
        """
        Keep only http(s) URLs in PageArtifact.links.
        Prevents mailto:, tel:, javascript:, #fragment, etc.
        """
        if not u:
            return False
        low = u.strip().lower()
        return low.startswith("http://") or low.startswith("https://")

    @classmethod
    def _normalize_links_flat(cls, links_raw: Any) -> List[str]:
        """
        Normalize Crawl4AI result.links into a flat List[str].

        Supported inputs:
          - list[str]
          - dict[str, list[str]]
          - dict[str, list[dict]]  (href/text objects)

        Important:
          - Filters out non-http(s) schemes like mailto:, tel:, javascript:
        """
        out: List[str] = []

        def add(x: Any) -> None:
            if not x:
                return
            s = str(x).strip()
            if not s:
                return
            if cls._is_http_url(s):
                out.append(s)

        if isinstance(links_raw, list):
            for x in links_raw:
                add(x)
            return out

        if isinstance(links_raw, dict):
            for v in links_raw.values():
                if isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict):
                            href = item.get("href") or item.get("url") or item.get("link")
                            add(href)
                        else:
                            add(item)

        return out

    @staticmethod
    def _clean_error_message(error: str) -> str:
        """
        Extract clean, concise error message from verbose error strings.
        
        Based on crawl4aitest/crawl_domains.py lines 146-186.
        Enhanced with additional Playwright error patterns.
        
        Args:
            error: Raw error message (may be verbose)
            
        Returns:
            Cleaned, shortened error message
        """
        if not error:
            return ""
        
        # Common error patterns to extract (order matters - most specific first)
        if "ERR_NAME_NOT_RESOLVED" in error:
            return "Domain not found (DNS error)"
        elif "ERR_CONNECTION_REFUSED" in error:
            return "Connection refused"
        elif "ERR_CONNECTION_TIMED_OUT" in error or "Timeout" in error:
            return "Connection timeout"
        elif "ERR_NETWORK_CHANGED" in error:
            return "Network changed during request"
        elif "ERR_CONNECTION_RESET" in error:
            return "Connection reset"
        elif "ERR_CONNECTION_CLOSED" in error:
            return "Connection closed"
        elif "ERR_EMPTY_RESPONSE" in error:
            return "Empty response from server"
        elif "ERR_SSL" in error or "SSL" in error:
            return "SSL certificate error"
        elif "ERR_CERT" in error:
            return "Certificate error"
        elif "ERR_TOO_MANY_REDIRECTS" in error:
            return "Too many redirects"
        elif "ERR_UNSAFE_REDIRECT" in error:
            return "Unsafe redirect"
        elif "ERR_BLOCKED_BY_CLIENT" in error:
            return "Blocked by client"
        elif "ERR_BLOCKED_BY_RESPONSE" in error:
            return "Blocked by response"
        elif "404" in error:
            return "Page not found (404)"
        elif "403" in error:
            return "Access forbidden (403)"
        elif "500" in error:
            return "Server error (500)"
        elif "502" in error:
            return "Bad gateway (502)"
        elif "503" in error:
            return "Service unavailable (503)"
        elif "504" in error:
            return "Gateway timeout (504)"
        elif "Empty content" in error:
            return error  # Already clean
        
        # Extract first meaningful line if it's a multi-line error
        lines = error.split('\n')
        for line in lines:
            line = line.strip()
            if line and not line.startswith(('Error:', 'Call log:', 'Code context:', '-', '→')):
                # Take first 80 chars of meaningful content
                return line[:80] + "..." if len(line) > 80 else line
        
        # Fallback: take first 80 chars of original error
        return error[:80] + "..." if len(error) > 80 else error

    # -----------------------------
    # Public API
    # -----------------------------

    async def fetch_page(self, url: str) -> PageArtifact:
        """
        Fetch a page and return a PageArtifact.

        Contract:
        - Always returns PageArtifact.meta as a dict (never None).
        - Never attempts to fetch non-http(s) URLs.
        - Enforces a hard timeout to prevent indefinite hangs
        - Properly checks result.success and result.error_message (like prototype)
        """
        # Defensive: never attempt to fetch non-http(s)
        if not self._is_http_url(str(url)):
            return PageArtifact(
                url=url,
                cleaned_html="",
                markdown="",
                error="invalid_url_scheme",
                links=[],
                meta={},
            )

        t0 = time.perf_counter()
        
        try:
            # Use engine to fetch the page
            res = await self._engine.fetch(url)

            # CRITICAL FIX 1: Check result.success first (like prototype does)
            # Based on crawl4aitest/crawl_domains.py lines 244-248
            if not res.success:
                error_msg = res.error_message or "Unknown error"
                # Clean error message like prototype does (lines 146-186)
                error_msg = self._clean_error_message(error_msg)
                return PageArtifact(
                    url=url,
                    cleaned_html="",
                    markdown="",
                    error=error_msg,
                    links=[],
                    meta={},
                )

            # CRITICAL FIX 2: Check if cleaned_html is None explicitly (like prototype)
            # Based on crawl4aitest/crawl_domains.py lines 251-254
            if res.cleaned_html is None:
                return PageArtifact(
                    url=url,
                    cleaned_html="",
                    markdown="",
                    error="Null HTML content (cleaned_html is None)",
                    links=[],
                    meta={},
                )

            html = getattr(res, "html", "") or ""
            
            # Extract markdown - use raw_markdown for full content including emails
            md_obj = getattr(res, "markdown", None)
            md = ""
            if md_obj:
                if isinstance(md_obj, str):
                    md = md_obj
                elif hasattr(md_obj, 'raw_markdown'):
                    # MarkdownGenerationResult object - use raw_markdown for complete content
                    md = md_obj.raw_markdown or ""
                else:
                    md = str(md_obj)
            
            cleaned = res.cleaned_html or ""
            links_raw = getattr(res, "links", None)

            # CRITICAL FIX 3: Check for empty content (like prototype)
            # Based on crawl4aitest/crawl_domains.py lines 257-275
            has_html = cleaned and len(cleaned.strip()) > 0
            has_markdown = False
            
            if md:
                # Handle both string and MarkdownGenerationResult object
                if isinstance(md, str):
                    has_markdown = len(md.strip()) > 0
                else:
                    # MarkdownGenerationResult object
                    has_markdown = (
                        hasattr(md, 'raw_markdown') and
                        md.raw_markdown and
                        len(md.raw_markdown.strip()) > 0
                    )
            
            if not has_html and not has_markdown:
                return PageArtifact(
                    url=url,
                    cleaned_html="",
                    markdown="",
                    error="Empty content (no HTML or markdown)",
                    links=[],
                    meta={},
                )

            cleaned_html = cleaned if cleaned.strip() else html

            final_url = getattr(res, "final_url", None)
            if final_url:
                final_url = _strip_fragment_query(str(final_url))

            links = self._normalize_links_flat(links_raw)

            meta: dict[str, Any] = {}
            if html:
                meta["raw_html"] = html
            if links_raw is not None:
                meta["crawl4ai_links_raw"] = links_raw

            return PageArtifact(
                url=url,
                final_url=final_url,
                cleaned_html=str(cleaned_html),
                markdown=str(md),
                title=getattr(res, "title", None),
                links=links,
                meta=meta,
            )

        except PlaywrightTimeoutError as e:
            # Playwright-specific timeout error
            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            error_msg = str(e)[:200] if str(e) else f"playwright_timeout_{elapsed_ms}ms"
            return PageArtifact(
                url=url,
                cleaned_html="",
                markdown="",
                error=error_msg,
                links=[],
                meta={},
            )

        except asyncio.TimeoutError:
            # asyncio timeout error
            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            return PageArtifact(
                url=url,
                cleaned_html="",
                markdown="",
                error=f"timeout_after_{elapsed_ms}ms",
                links=[],
                meta={},
            )

        except Exception as e:
            # All other exceptions
            error_msg = f"{type(e).__name__}: {str(e)[:200]}"
            return PageArtifact(
                url=url,
                cleaned_html="",
                markdown="",
                error=error_msg,
                links=[],
                meta={},
            )


def _strip_fragment_query(u: str) -> str:
    """Drop #fragment and ?query from URL (used for final_url normalization)."""
    try:
        p = urlparse(u)
        return p._replace(fragment="", query="").geturl()
    except Exception:
        return u
