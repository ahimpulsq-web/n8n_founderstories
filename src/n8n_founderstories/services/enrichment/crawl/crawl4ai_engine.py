"""
═══════════════════════════════════════════════════════════════════════════════
CRAWL4AI ENGINE - Browser Automation Infrastructure
═══════════════════════════════════════════════════════════════════════════════

CLASSIFICATION: [INFRASTRUCTURE] - Low-level browser automation layer

PURPOSE:
    Manages the Crawl4AI AsyncWebCrawler lifecycle and configuration.
    Provides a clean async API for fetching web pages with optimal performance.

RESPONSIBILITIES:
    ✓ AsyncWebCrawler lifecycle management (start/stop)
    ✓ Browser configuration (headless, timeout, concurrency)
    ✓ Performance optimizations (resource blocking, rendering)
    ✓ Concurrency control (global + per-domain limits)
    ✓ Rich console output suppression
    
    ✗ Business logic (handled by service.py)
    ✗ Case logic (handled by service.py)
    ✗ Database access (handled by repo.py)
    ✗ Orchestration (handled by runner.py)

CONFIGURATION:
    Environment Variables (set at module import):
    - CRAWL4AI_LOG_LEVEL: ERROR (suppress verbose logs)
    - CRAWL4AI_VERBOSE: false (disable verbose mode)
    - CRAWL4AI_NO_RICH: true (disable Rich console output)
    - CRAWL4AI_CONSOLE_OUTPUT: false (disable console output)

PERFORMANCE OPTIMIZATIONS:
    1. Resource Blocking:
       - Blocks images, fonts, stylesheets, media
       - Reduces bandwidth and improves speed
    
    2. Rendering Mode:
       - text_mode: Extract text content only
       - light_mode: Minimal rendering overhead
    
    3. Concurrency:
       - Global limiter: Controls total concurrent requests
       - Per-domain: Prevents overwhelming single domains

RICH CONSOLE SUPPRESSION:
    The engine monkey-patches Rich Console to suppress crawl4ai's
    verbose output ([FETCH], [SCRAPE], [COMPLETE] messages).
    This keeps logs clean and focused on application-level events.

BROWSER CONFIGURATION:
    - Chromium browser (headless mode)
    - Custom user agent
    - Resource blocking for performance
    - Configurable timeouts and wait times

USAGE:
    config = Crawl4AIEngineConfig(headless=True, timeout_s=30.0)
    engine = Crawl4AIEngine(config)
    await engine.start()
    result = await engine.fetch("https://example.com")
    await engine.close()

DEPENDENCIES:
    - crawl4ai: AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
    - playwright: Browser automation
    - global_limiter.py: Global concurrency control

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from typing import Any, Optional
from io import StringIO

from .global_limiter import get_global_limiter

# Configure Crawl4AI logging BEFORE any imports
os.environ['CRAWL4AI_LOG_LEVEL'] = 'ERROR'
os.environ['CRAWL4AI_VERBOSE'] = 'false'
os.environ['CRAWL4AI_NO_RICH'] = 'true'
os.environ['CRAWL4AI_CONSOLE_OUTPUT'] = 'false'

# Disable Rich globally by monkey-patching
import sys
_original_stdout = sys.stdout
_original_stderr = sys.stderr

# Create a null output stream
class NullOutput:
    def write(self, *args, **kwargs):
        pass
    def flush(self, *args, **kwargs):
        pass
    def isatty(self):
        return False

# Temporarily redirect output during crawl4ai import
_null_output = NullOutput()

# Fix Windows console encoding
os.environ['PYTHONIOENCODING'] = 'utf-8'
if os.name == 'nt':  # Windows
    if hasattr(_original_stdout, 'reconfigure'):
        _original_stdout.reconfigure(encoding='utf-8')
    if hasattr(_original_stderr, 'reconfigure'):
        _original_stderr.reconfigure(encoding='utf-8')

import logging

# Disable crawl4ai logging completely
logging.getLogger("crawl4ai").setLevel(logging.CRITICAL)
logging.getLogger("crawl4ai").propagate = False
logging.getLogger("crawl4ai").disabled = True
logging.getLogger("crawl4ai.async_webcrawler").setLevel(logging.CRITICAL)
logging.getLogger("crawl4ai.async_webcrawler").propagate = False
logging.getLogger("crawl4ai.async_webcrawler").disabled = True
logging.getLogger("crawl4ai.async_crawler_strategy").setLevel(logging.CRITICAL)
logging.getLogger("crawl4ai.async_crawler_strategy").propagate = False
logging.getLogger("crawl4ai.async_crawler_strategy").disabled = True

# Disable Rich console before importing crawl4ai
try:
    import rich.console
    # Monkey-patch Rich Console to do nothing
    _original_rich_print = rich.console.Console.print
    def _silent_print(self, *args, **kwargs):
        pass
    rich.console.Console.print = _silent_print
except ImportError:
    pass

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from playwright.async_api import Page, BrowserContext


@dataclass(frozen=True)
class Crawl4AIEngineConfig:
    """
    Configuration for Crawl4AI engine.
    
    Note: text_mode and light_mode are enabled for performance.
    Resource blocking is handled via a single route handler in the
    on_page_context_created hook to avoid EPIPE errors.
    """
    headless: bool = True
    timeout_s: float = 30.0
    max_concurrency: int = 3
    wait_after_load_s: float = 0.1
    text_mode: bool = True
    light_mode: bool = True


class Crawl4AIEngine:
    """
    High-performance AsyncWebCrawler engine.
    
    Features (from crawl4aitest/crawl_domains.py):
    - Resource blocking (images, fonts, media, tracking)
    - CSS animation disabling
    - text_mode and light_mode for speed
    - Controlled concurrency via semaphore
    - Performance-optimized browser config
    
    Lifecycle:
    - start() → creates and initializes AsyncWebCrawler
    - fetch(url) → fetches a single page
    - close() → cleans up browser
    """
    
    def __init__(self, config: Crawl4AIEngineConfig):
        self._config = config
        self._crawler: Optional[AsyncWebCrawler] = None
        self._semaphore = asyncio.Semaphore(config.max_concurrency)
        self._domain_semaphores: dict[str, asyncio.Semaphore] = {}
        self._global_limiter = get_global_limiter()
        self._closed = False
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger(__name__)
        
        # Configure browser for maximum performance
        # text_mode and light_mode enabled for speed (like prototype)
        self._browser_config = BrowserConfig(
            headless=config.headless,
            verbose=False,
            browser_type="chromium",
            text_mode=config.text_mode,
            light_mode=config.light_mode,
            extra_args=[
                "--disable-extensions",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
        )
        
        # Configure markdown generator to preserve all content including emails
        # Documentation: https://docs.crawl4ai.com/core/markdown-generation
        markdown_generator = DefaultMarkdownGenerator(
            options={
                "ignore_links": False,  # Keep all links including mailto:
                "body_width": 0  # No line wrapping to preserve formatting
            }
        )
        
        # Configure crawler run to capture ALL content including emails and contact info
        # CRITICAL: Minimal filtering to preserve emails in cleaned_html and markdown
        # Based on updated crawl4aitest/crawl_domains.py
        self._run_config = CrawlerRunConfig(
            # MINIMAL content filtering to preserve emails and contact info
            excluded_tags=["script", "style"],  # Only exclude scripts/styles, keep everything else
            exclude_external_links=False,  # Keep external links (emails might be in mailto: links)
            exclude_external_images=True,  # Still block images for speed
            word_count_threshold=0,  # Don't filter out small content blocks (emails might be there)
            only_text=False,  # Preserve HTML attributes where emails might be
            
            # Markdown generation
            markdown_generator=markdown_generator,  # Use configured markdown generator
            
            # Timing optimizations
            page_timeout=int(config.timeout_s * 1000),
            delay_before_return_html=config.wait_after_load_s,
            
            # Disable resource monitoring for speed
            capture_network_requests=False,
            capture_console_messages=False,
            
            # Cache mode
            cache_mode=CacheMode.BYPASS
        )
    
    async def start(self) -> None:
        """
        Initialize the AsyncWebCrawler and attach performance hooks.
        
        Based on crawl4aitest/crawl_domains.py lines 395-398.
        """
        async with self._lock:
            if self._closed:
                raise RuntimeError("Crawl4AIEngine is closed")
            if self._crawler is not None:
                return
            
            try:
                self._crawler = AsyncWebCrawler(config=self._browser_config)
                
                # Initialize browser with timeout
                await asyncio.wait_for(self._crawler.__aenter__(), timeout=60.0)
                
                # Attach the resource blocking hook (like prototype line 398)
                # This hook blocks heavy resources and disables CSS animations
                self._crawler.crawler_strategy.set_hook("on_page_context_created", self._on_page_context_created)
                
                # Log configuration for verification
                global_limit_info = f"global_limit={self._global_limiter.max_concurrency}" if self._global_limiter.enabled else "global_limit=disabled"
                self._logger.info(
                    f"Crawl4AI engine started - "
                    f"timeout={self._config.timeout_s}s, "
                    f"text_mode={self._config.text_mode}, "
                    f"light_mode={self._config.light_mode}, "
                    f"max_concurrency={self._config.max_concurrency}, "
                    f"{global_limit_info}, "
                    f"hook=attached"
                )
                
            except asyncio.TimeoutError:
                self._crawler = None
                raise RuntimeError("Crawl4AI browser initialization timed out after 60 seconds")
            except Exception as e:
                self._crawler = None
                raise RuntimeError(f"Failed to create AsyncWebCrawler: {e}")
    
    async def _on_page_context_created(
        self,
        page: Page,
        context: BrowserContext,
        **kwargs
    ) -> Page:
        """
        Performance optimization hook - blocks heavy resources and disables animations.
        
        Based on crawl4aitest/crawl_domains.py lines 290-326 and Crawl4AI docs.
        
        Called right after page + context are created, before navigation.
        
        Note: Using a single route handler with pattern matching to avoid
        Playwright EPIPE errors from too many route handlers.
        """
        # Define blocked patterns
        blocked_extensions = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg', '.ico',
                            '.woff', '.woff2', '.ttf', '.otf', '.eot',
                            '.mp4', '.webm', '.ogg', '.mp3', '.wav', '.flac', '.avi', '.mov'}
        
        blocked_domains = {'google-analytics.com', 'googletagmanager.com', 'doubleclick.net',
                          'facebook.com/tr', 'facebook.net', 'connect.facebook.net',
                          'hotjar.com', 'segment.com', 'segment.io',
                          'amazon-adsystem.com', 'googlesyndication.com'}
        
        # Single route handler for all blocking
        async def route_handler(route):
            url = route.request.url.lower()
            
            # Check if URL ends with blocked extension
            if any(url.endswith(ext) for ext in blocked_extensions):
                await route.abort()
                return
            
            # Check if URL contains blocked domain
            if any(domain in url for domain in blocked_domains):
                await route.abort()
                return
            
            # Allow all other requests
            await route.continue_()
        
        # Register single route handler for all patterns
        await context.route("**/*", route_handler)
        
        # Disable CSS animations for faster rendering
        await page.add_style_tag(content='''
            *, *::before, *::after {
                animation-duration: 0s !important;
                transition-duration: 0s !important;
            }
        ''')
        
        return page
    
    async def fetch(self, url: str) -> Any:
        """
        Fetch a single page using the configured crawler.
        
        Implements three-tier concurrency limiting:
        1. Global process-wide limit (cross-job coordination)
        2. Engine-level limit (per-client max_concurrency)
        3. Per-domain limit (reduce throttling per domain)
        
        Args:
            url: URL to fetch
            
        Returns:
            CrawlResult from Crawl4AI
            
        Raises:
            RuntimeError: If engine is not started or closed
            asyncio.TimeoutError: If fetch times out
            Exception: Any other exception from Playwright/Crawl4AI
        """
        if self._closed:
            raise RuntimeError("Crawl4AIEngine is closed")
        
        await self.start()
        
        if self._crawler is None:
            raise RuntimeError("Crawler not initialized")
        
        # Extract base domain for per-domain limiting
        from urllib.parse import urlparse
        try:
            parsed = urlparse(url)
            base_domain = parsed.netloc or url
        except Exception:
            base_domain = url
        
        # Get or create per-domain semaphore (limit 2 concurrent requests per domain)
        if base_domain not in self._domain_semaphores:
            self._domain_semaphores[base_domain] = asyncio.Semaphore(2)
        
        domain_sem = self._domain_semaphores[base_domain]
        
        # Apply three-tier concurrency limits: global → engine → per-domain
        async with self._global_limiter():
            async with self._semaphore:
                async with domain_sem:
                    # Don't use asyncio.wait_for as it can create orphaned futures
                    # Let Crawl4AI's internal timeout handle it (configured in BrowserConfig and CrawlerRunConfig)
                    result = await self._crawler.arun(url=url, config=self._run_config)
                    return result
    
    async def close(self) -> None:
        """
        Close the AsyncWebCrawler and clean up resources.
        """
        async with self._lock:
            if self._crawler is None:
                self._closed = True
                return
            
            crawler = self._crawler
            self._crawler = None
            self._closed = True
            
            await crawler.__aexit__(None, None, None)
    
    async def __aenter__(self) -> "Crawl4AIEngine":
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Async context manager exit."""
        await self.close()