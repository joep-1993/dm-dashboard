"""SEO Rulings router — exposes a single POST endpoint that runs all checks."""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, HTTPException

from backend.seo_rulings_service import (
    delete_run,
    get_last_run,
    get_recent_runs,
    get_run_by_id,
    run_all_checks,
)

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


@router.get("/last")
def last():
    """Return the most-recently-completed run so the page can rehydrate on
    refresh. Shape: {has_run: bool, run: {...} | null}."""
    row = get_last_run()
    return {"has_run": row is not None, "run": row}


@router.get("/runs")
def runs(limit: int = 20):
    """List recent runs (newest first), without the full result payload."""
    return {"runs": get_recent_runs(limit=limit)}


@router.get("/runs/{run_id}")
def run_detail(run_id: int):
    """Return one run including the full result payload (for export / view)."""
    row = get_run_by_id(run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return row


@router.delete("/runs/{run_id}")
def remove_run(run_id: int):
    """Delete a run from history."""
    if not delete_run(run_id):
        raise HTTPException(status_code=404, detail="Run not found")
    return {"deleted": True, "run_id": run_id}
