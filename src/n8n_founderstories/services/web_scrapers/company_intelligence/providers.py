from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, Tuple
from urllib.parse import urlparse

import httpx
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

from .models import LLMExtractionResultModel, Contact
from .prompts import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE
from .utils import RateLimiter, is_allowed_by_robots, log_event, to_markdown


# -----------------------------
# LLM payload sanitizer (NEW)
# -----------------------------

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _is_valid_email(value: Any) -> bool:
    if value is None:
        return False
    s = str(value).strip().lower()
    if not s:
        return False
    return bool(_EMAIL_RE.match(s))


def _fallback_summary(language: str) -> str:
    if str(language).lower().startswith("de"):
        return "Keine verlässliche Beschreibung aus den bereitgestellten Seiten extrahierbar."
    return "No reliable company description could be extracted from the provided pages."


def sanitize_llm_payload(data: Any, language: str) -> Dict[str, Any]:
    """
    Keep role text as-is (NO role mapping), but prevent validation failures:
    - emails: keep only valid emails
    - contacts: drop items with empty name or invalid/empty email
    - about.summary: ensure >= 20 chars (fallback)
    """
    if not isinstance(data, dict):
        return {"language": language, "emails": [], "contacts": [], "about": {"summary": _fallback_summary(language)}}

    out: Dict[str, Any] = {}

    # language
    out["language"] = data.get("language") or language

    # emails
    emails_in = data.get("emails") or []
    if not isinstance(emails_in, list):
        emails_in = []
    emails_out: List[str] = []
    for e in emails_in:
        s = str(e).strip().lower()
        if _is_valid_email(s):
            emails_out.append(s)
    out["emails"] = sorted(set(emails_out))

    # contacts
    contacts_in = data.get("contacts") or []
    if not isinstance(contacts_in, list):
        contacts_in = []
    contacts_out: List[Dict[str, Any]] = []
    seen = set()

    for c in contacts_in:
        if not isinstance(c, dict):
            continue

        name = str(c.get("name") or "").strip()
        email = str(c.get("email") or "").strip().lower()
        role = str(c.get("role") or "other").strip()  # keep raw role text
        title = str(c.get("title") or "").strip() or None
        source_url = c.get("source_url") or None
        if source_url is not None:
            source_url = str(source_url).strip() or None

        # enforce schema constraints that caused failures
        if not name:
            continue
        if not _is_valid_email(email):
            continue

        key = f"{email}|{name.lower()}|{role.lower()}"
        if key in seen:
            continue
        seen.add(key)

        contacts_out.append(
            {
                "name": name,
                "email": email,
                "role": role,
                "title": title,
                "source_url": source_url,
            }
        )

    out["contacts"] = contacts_out[:5]

    # about
    about = data.get("about")
    if not isinstance(about, dict):
        about = {}
    summary = str(about.get("summary") or "").strip()
    if len(summary) < 20:
        summary = _fallback_summary(out["language"])
    out["about"] = {"summary": summary}

    return out


# -----------------------------
# Core crawling abstractions
# -----------------------------

class CrawlResult:
    def __init__(self, url: str, html: str, text: str):
        self.url = url
        self.html = html
        self.text = text


class CompanyCrawler(Protocol):
    async def crawl(self, domain: str, language: str = "de") -> List[CrawlResult]:
        ...


# -----------------------------
# Crawl4AI provider
# -----------------------------

@dataclass(frozen=True)
class Crawl4AIPage:
    url: str
    html: str
    text: str  # markdown for LLM

class Crawl4AIClient:
    """
    Reusable Crawl4AI browser instance.

    Usage patterns:
    - In production/service: keep one instance alive and call crawl() many times.
    - In scripts: wrap it in 'async with client:' to ensure close() is called.
    """

    def __init__(
        self,
        headless: bool = True,
        timeout_s: float = 45.0,
        max_concurrency: int = 4,
        user_agent: Optional[str] = None,
        min_domain_interval_s: float = 5.0,
    ):
        self._headless = headless
        self._timeout_s = timeout_s
        self._max_concurrency = max_concurrency
        self._user_agent = user_agent or "Mozilla/5.0"
        self._rl = RateLimiter(min_interval_s=min_domain_interval_s)
        self._min_domain_interval_s = min_domain_interval_s

        self._browser_cfg = BrowserConfig(headless=self._headless, user_agent=self._user_agent)
        self._run_cfg = CrawlerRunConfig(page_timeout=self._timeout_s * 1000)

        self._crawler: Optional[AsyncWebCrawler] = None
        self._crawler_lock = asyncio.Lock()  # protect start/close
        self._page_sem = asyncio.Semaphore(self._max_concurrency)

    async def start(self) -> None:
        async with self._crawler_lock:
            if self._crawler is not None:
                return
            self._crawler = AsyncWebCrawler(config=self._browser_cfg)
            await self._crawler.__aenter__()  # opens browser once

    async def close(self) -> None:
        async with self._crawler_lock:
            if self._crawler is None:
                return
            crawler = self._crawler
            self._crawler = None
            await crawler.__aexit__(None, None, None)

    async def __aenter__(self) -> "Crawl4AIClient":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def crawl(self, start_urls: List[str]) -> List[Crawl4AIPage]:
        if not start_urls:
            return []

        await self.start()  # ensure browser is up

        domain = urlparse(start_urls[0]).netloc.lower()
        if not self._rl.allow(domain):
            log_event(
                "company_intel.domain_rate_limited",
                domain=domain,
                min_interval_s=self._min_domain_interval_s,
                urls=len(start_urls),
            )
            return []

        crawler = self._crawler
        if crawler is None:
            return []

        out: List[Crawl4AIPage] = []

        async def fetch(url: str) -> None:
            decision = await is_allowed_by_robots(url, user_agent=self._user_agent)
            if not decision.allowed:
                log_event(
                    "company_intel.crawl_skipped",
                    reason="robots_disallowed",
                    url=url,
                    domain=domain,
                )
                return

            try:
                async with self._page_sem:
                    result = await crawler.arun(url=url, config=self._run_cfg)

                md = getattr(result, "markdown", "") or ""
                html = getattr(result, "html", "") or ""

                if not str(md).strip():
                    log_event("company_intel.page_empty", url=url, domain=domain)
                    return

                out.append(Crawl4AIPage(url=url, html=html, text=str(md)))

            except asyncio.TimeoutError:
                log_event(
                    "company_intel.crawl_timeout",
                    url=url,
                    domain=domain,
                    timeout_s=self._timeout_s,
                )
            except Exception as e:
                log_event(
                    "company_intel.crawl_error",
                    url=url,
                    domain=domain,
                    error_type=type(e).__name__,
                    error=str(e)[:300],
                )

        await asyncio.gather(*(fetch(u) for u in start_urls))

        log_event(
            "company_intel.crawl_done",
            domain=domain,
            fetched=len(out),
            attempted=len(start_urls),
        )
        return out



class Crawl4AICompanyCrawler:
    def __init__(self, client: Crawl4AIClient):
        self._client = client

    async def crawl(self, domain: str, language: str = "de") -> List[CrawlResult]:
        base = f"https://{domain}".rstrip("/")

        start_urls = [
            f"{base}/impressum",
            f"{base}/imprint",
            f"{base}/kontakt",
            f"{base}/ueber-uns",
            f"{base}/datenschutz",
            f"{base}/team",
            f"{base}/about",
            base,
        ]

        pages = await self._client.crawl(start_urls)
        return [CrawlResult(url=p.url, html=p.html, text=p.text) for p in pages]


# -----------------------------
# OpenRouter provider
# -----------------------------

OPENROUTER_CHAT_URL = "https://openrouter.ai/api/v1/chat/completions"


@dataclass(frozen=True)
class OpenRouterConfig:
    api_key: str
    model: str
    http_referer: Optional[str] = None
    x_title: Optional[str] = None
    timeout_s: float = 60.0


def _extract_json_object(text: str) -> Dict[str, Any]:
    s = text.strip()

    s = re.sub(r"^```json\s*", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"^```\s*", "", s).strip()
    s = re.sub(r"\s*```$", "", s).strip()

    try:
        return json.loads(s)
    except Exception:
        pass

    m = re.search(r"\{.*\}", s, flags=re.DOTALL)
    if not m:
        raise ValueError("LLM did not return JSON object")
    return json.loads(m.group(0))


class OpenRouterClient:
    def __init__(self, cfg: OpenRouterConfig, sem: asyncio.Semaphore | None = None):
        self._cfg = cfg
        self._sem = sem
        self._client: httpx.AsyncClient | None = None
        self._client_lock = asyncio.Lock()

    async def _get_client(self) -> httpx.AsyncClient:
        # Ensure exactly one shared client exists
        async with self._client_lock:
            if self._client is None:
                self._client = httpx.AsyncClient(timeout=self._cfg.timeout_s)
            return self._client

    async def aclose(self) -> None:
        async with self._client_lock:
            if self._client is not None:
                await self._client.aclose()
                self._client = None

    async def chat_json(self, system: str, user: str, temperature: float = 0.2) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self._cfg.api_key}",
            "Content-Type": "application/json",
        }
        if self._cfg.http_referer:
            headers["HTTP-Referer"] = self._cfg.http_referer
        if self._cfg.x_title:
            headers["X-Title"] = self._cfg.x_title

        payload = {
            "model": self._cfg.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
        }

        client = await self._get_client()

        if self._sem is None:
            resp = await client.post(OPENROUTER_CHAT_URL, headers=headers, json=payload)
        else:
            async with self._sem:
                resp = await client.post(OPENROUTER_CHAT_URL, headers=headers, json=payload)

        resp.raise_for_status()
        data = resp.json()

        content = data["choices"][0]["message"]["content"]
        return _extract_json_object(content)


# -----------------------------
# LLM extractors + ensemble
# -----------------------------

def _build_pages_blob(pages: List[CrawlResult]) -> str:
    parts: List[str] = []
    for p in pages:
        md = to_markdown(p.text)
        if not md:
            continue
        parts.append(f"URL: {p.url}\nCONTENT:\n{md}\n")
    blob = "\n---\n".join(parts)
    return blob[:80_000]


class CompanyLLMExtractor(Protocol):
    async def extract(self, pages: List[CrawlResult], language: str = "de") -> LLMExtractionResultModel:
        ...


class JSONLLMExtractor:
    def __init__(self, llm_client: Any):
        self._llm = llm_client

    async def extract(self, pages: List[CrawlResult], language: str = "de") -> LLMExtractionResultModel:
        pages_blob = _build_pages_blob(pages)
        user_prompt = (
            USER_PROMPT_TEMPLATE
            .replace("{language}", language)
            .replace("{pages_text}", pages_blob)
        )

        raw = await self._llm.chat(
            system=SYSTEM_PROMPT,
            user=user_prompt,
            temperature=0.2,
        )

        data = json.loads(raw)
        data = sanitize_llm_payload(data, language=language)
        return LLMExtractionResultModel.model_validate(data)


class OpenRouterCompanyLLMExtractor:
    def __init__(self, client: OpenRouterClient):
        self._client = client

    async def extract(self, pages: List[CrawlResult], language: str = "de") -> LLMExtractionResultModel:
        pages_blob = _build_pages_blob(pages)

        user_prompt = (
            USER_PROMPT_TEMPLATE
            .replace("{language}", language)
            .replace("{pages_text}", pages_blob)
        )

        data = await self._client.chat_json(
            system=SYSTEM_PROMPT,
            user=user_prompt,
            temperature=0.2,
        )

        data = sanitize_llm_payload(data, language=language)
        return LLMExtractionResultModel.model_validate(data)


@dataclass(frozen=True)
class EnsembleItemMeta:
    source_models: List[str]
    confidence: float


@dataclass(frozen=True)
class EnsembleResult:
    result: LLMExtractionResultModel
    email_meta: Dict[str, EnsembleItemMeta]
    contact_meta: Dict[str, EnsembleItemMeta]


class EnsembleCompanyLLMExtractor:
    def __init__(self, extractors: List[Tuple[str, OpenRouterCompanyLLMExtractor]]):
        self._extractors = extractors

    async def extract(self, pages: List[CrawlResult], language: str = "de") -> EnsembleResult:
        tasks = [ex.extract(pages=pages, language=language) for _, ex in self._extractors]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        ok: List[Tuple[str, LLMExtractionResultModel]] = []
        failures: List[str] = []

        for (model, _), r in zip(self._extractors, results):
            if isinstance(r, Exception):
                failures.append(f"{model}: {type(r).__name__}: {str(r)[:250]}")
                continue
            ok.append((model, r))

        if not ok:
            details = "\n".join(failures) if failures else "No details"
            raise RuntimeError(f"All LLM extractors failed:\n{details}")

        return self._merge(ok, language=language)

    def _merge(self, runs: List[Tuple[str, LLMExtractionResultModel]], language: str) -> EnsembleResult:
        total = len(runs)

        email_votes: Dict[str, List[str]] = {}
        for model, r in runs:
            for e in r.emails:
                k = str(e).lower()
                email_votes.setdefault(k, []).append(model)

        contact_votes: Dict[str, List[str]] = {}
        best_contact_by_key: Dict[str, Contact] = {}

        for model, r in runs:
            for c in r.contacts:
                key = str(c.email).lower() if c.email else f"{c.name.lower()}|{c.role}"
                contact_votes.setdefault(key, []).append(model)
                best_contact_by_key.setdefault(key, c)

        def score(res: LLMExtractionResultModel) -> int:
            return len(res.contacts) + len(res.emails)

        _, best_run = max(runs, key=lambda mr: score(mr[1]))

        merged_emails = sorted(email_votes.keys())
        merged_contacts = list(best_contact_by_key.values())

        email_meta = {
            e: EnsembleItemMeta(source_models=mods, confidence=len(mods) / total)
            for e, mods in email_votes.items()
        }
        contact_meta = {
            k: EnsembleItemMeta(source_models=mods, confidence=len(mods) / total)
            for k, mods in contact_votes.items()
        }

        merged = LLMExtractionResultModel(
            language=language,
            emails=merged_emails,
            contacts=merged_contacts,
            about=best_run.about,
        )
        return EnsembleResult(result=merged, email_meta=email_meta, contact_meta=contact_meta)
