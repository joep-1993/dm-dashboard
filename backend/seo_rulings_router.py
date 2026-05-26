"""SEO Rulings router — exposes a single POST endpoint that runs all checks."""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter

from backend.seo_rulings_service import run_all_checks

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/seo-rulings", tags=["seo-rulings"])

# One background worker is plenty — the whole run is <12 HTTP fetches.
_executor = ThreadPoolExecutor(max_workers=1)


@router.get("/health")
def health():
    return {"status": "healthy", "service": "seo_rulings"}


@router.post("/run")
async def run():
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(_executor, run_all_checks)
    return result
