# FounderStories Backend

A Python backend that powers the FounderStories lead discovery and enrichment pipeline.  
Designed to be orchestrated by **n8n**, and structured as modular services for:
- lead discovery (e.g., Google Maps / web search),
- enrichment (e.g., Hunter.io),
- website-based contact extraction (ethical crawling posture),
- job tracking and exports (e.g., Google Sheets or other sinks).

> This repository is under active development. Interfaces and modules may evolve as the pipeline hardens.

---

## Table of Contents
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Local Development](#local-development)
- [Configuration](#configuration)
- [Running the Service](#running-the-service)
- [n8n Integration](#n8n-integration)
- [Operational Concerns](#operational-concerns)
- [Testing](#testing)
- [Deployment](#deployment)
- [Security](#security)
- [Ethics and Compliance](#ethics-and-compliance)
- [Roadmap](#roadmap)

---

## Architecture

**High-level flow**

1. n8n triggers the backend (HTTP calls).
2. The backend runs one pipeline action (discovery / enrichment / extraction).
3. Results are returned to n8n (and optionally persisted by the backend depending on configuration).
4. n8n writes results to Google Sheets / DB / CRM and continues the workflow.

**Design goals**
- Modular domain services (clean boundaries between discovery, enrichment, scraping).
- Strong observability (structured logs; job/run tracking).
- Safe-by-default crawling posture (rate limits, robots awareness where applicable).
- Configuration-driven execution for multiple environments (dev/staging/prod).

---

## Repository Structure
src/
n8n_founderstories/
api/ # API layer (routing, request/response)
core/ # config, logging, shared utilities
services/ # pipeline services (maps, hunter, scraping, jobs, exports)
requirements.txt
pyproject.toml

## Local Development

### Prerequisites
- Python 3.10+ (recommended)
- Git
- A virtual environment tool (`venv` / `virtualenv`)

### Setup (Windows PowerShell / CMD)

Create and activate venv (example):

```bat
python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt


