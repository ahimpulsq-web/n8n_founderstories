# batch_runner.py (UPDATED)

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

from n8n_founderstories.services.web_scrapers.company_intelligence.runner import run_domain


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def setup_logging() -> None:
    log_file = os.environ.get("BATCH_LOG_FILE", "output/batch_runner.log")
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logging.info("batch_runner started; log_file=%s", log_file)


def load_domains(txt_path: str) -> list[str]:
    p = Path(txt_path)
    if not p.exists():
        raise FileNotFoundError(f"Domains file not found: {txt_path}")

    domains: list[str] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        s = s.replace("https://", "").replace("http://", "")
        s = s.strip("/").lower()
        domains.append(s)

    seen = set()
    out: list[str] = []
    for d in domains:
        if d not in seen:
            out.append(d)
            seen.add(d)
    return out


def ensure_csv_header(csv_path: str, fieldnames: list[str]) -> None:
    p = Path(csv_path)
    if p.exists() and p.stat().st_size > 0:
        return
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        f.flush()


def output_row(domain: str, out) -> dict:
    llm = out.llm
    emails = ";".join(sorted({str(e).lower() for e in llm.emails}))

    contacts = []
    for c in llm.contacts:
        contacts.append(
            {
                "name": c.name,
                "role": c.role,
                "email": str(c.email),
                "title": c.title or "",
                "source_url": str(c.source_url) if c.source_url else "",
            }
        )

    return {
        "timestamp_utc": utc_now_iso(),
        "domain": domain,
        "company_description": llm.about.summary,
        "emails": emails,
        "contacts_json": json.dumps(contacts, ensure_ascii=False),
        "status": "ok",
        "error": "",
    }


def error_row(domain: str, status: str, error: str) -> dict:
    return {
        "timestamp_utc": utc_now_iso(),
        "domain": domain,
        "company_description": "",
        "emails": "",
        "contacts_json": "[]",
        "status": status,
        "error": error[:300],
    }


async def main() -> None:
    setup_logging()

    domains_file = os.environ.get("DOMAINS_TXT", "domains.txt")
    csv_out = os.environ.get("OUTPUT_CSV", "output/company_intelligence_results.csv")
    language = os.environ.get("LANGUAGE", "de")

    domain_concurrency = int(os.environ.get("DOMAIN_CONCURRENCY", "10"))
    sem = asyncio.Semaphore(max(1, domain_concurrency))

    domains = load_domains(domains_file)
    logging.info("loaded_domains=%d domain_concurrency=%d", len(domains), domain_concurrency)

    fieldnames = [
        "timestamp_utc",
        "domain",
        "company_description",
        "emails",
        "contacts_json",
        "status",
        "error",
    ]
    ensure_csv_header(csv_out, fieldnames)

    write_lock = asyncio.Lock()

    # open CSV once (faster)
    Path(csv_out).parent.mkdir(parents=True, exist_ok=True)
    f = open(csv_out, "a", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=fieldnames)

    async def process_one(domain: str) -> None:
        async with sem:
            t0 = perf_counter()
            logging.info("domain_start domain=%s", domain)

            try:
                out = await run_domain(domain, language=language)
                row = output_row(domain, out)
                elapsed = perf_counter() - t0
                logging.info("domain_ok domain=%s elapsed_s=%.2f", domain, elapsed)

            except Exception as e:
                elapsed = perf_counter() - t0
                row = error_row(domain, "error", f"{type(e).__name__}: {str(e)}")
                logging.error("domain_error domain=%s elapsed_s=%.2f err=%s", domain, elapsed, row["error"])

            async with write_lock:
                w.writerow(row)
                f.flush()

    try:
        await asyncio.gather(*(process_one(d) for d in domains))
    finally:
        f.close()
        logging.info("batch_runner finished; output_csv=%s", csv_out)


if __name__ == "__main__":
    asyncio.run(main())
