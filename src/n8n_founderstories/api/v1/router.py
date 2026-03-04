from __future__ import annotations

# ============================================================================
# router.py
# API v1 router aggregator.
# ============================================================================

from fastapi import APIRouter

from .search_plan import router as prompt_router
from .jobs import router as jobs_router
from .google_maps import router as google_maps_router
from .hunter import router as hunter_router
from .mailer import router as mailer_router

router = APIRouter()

router.include_router(prompt_router, tags=["search_plan"])
router.include_router(jobs_router, tags=["jobs"])
router.include_router(google_maps_router, tags=["location"])
router.include_router(hunter_router, tags=["enrichment"])
router.include_router(mailer_router, tags=["mailer"])
