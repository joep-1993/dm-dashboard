"""
GSD Budgets router — mirrors the DMA Bidding router shape.

POST /api/gsd-budgets/run is the entry point. Everything else is stats/history
so the frontend can pre-populate and replay previous runs.
"""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend.gsd_budgets_service import (
    _run_history,
    get_stats,
    run_gsd_budgets,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/gsd-budgets", tags=["gsd-budgets"])
executor = ThreadPoolExecutor(max_workers=2)


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "gsd_budgets"}


@router.get("/stats")
async def stats(country: str = Query("NL", description="Country code: NL or BE")):
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(executor, lambda: get_stats(country=country))
    except Exception as e:
        logger.error(f"Error fetching GSD Budgets stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run")
async def run(
    country: str = Query("NL", description="Country code: NL or BE"),
    dry_run: bool = Query(True, description="If true, no budget mutations and no missed-shops insert"),
    start_days_ago: int = Query(9, description="SA360 marge window start (days ago)"),
    end_days_ago: int = Query(3, description="SA360 marge window end (days ago)"),
    limit_shops: Optional[int] = Query(None, description="Limit the Redshift shop-list SIZE (useful for smoke tests)"),
    shop_names: Optional[str] = Query(None, description="Comma-separated shop names to include/exclude"),
    shop_names_excluded: bool = Query(False, description="If true, shop_names acts as an exclude-list; otherwise include-only"),
    skip_missed_upload: bool = Query(False, description="If true, skip upload to pa.jvs_gsd_missed_shops (always skipped in dry-run)"),
):
    try:
        loop = asyncio.get_event_loop()
        shop_list = (
            [s.strip() for s in shop_names.split(",") if s.strip()]
            if shop_names
            else None
        )
        result = await loop.run_in_executor(
            executor,
            lambda: run_gsd_budgets(
                country=country,
                dry_run=dry_run,
                start_days_ago=start_days_ago,
                end_days_ago=end_days_ago,
                limit_shops=limit_shops,
                shop_names=shop_list,
                shop_names_excluded=shop_names_excluded,
                skip_missed_upload=skip_missed_upload,
            ),
        )
        return result
    except Exception as e:
        logger.error(f"Error running GSD Budgets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
async def history():
    return {"runs": _run_history}


@router.get("/history/{run_id}")
async def history_detail(run_id: int):
    for run_entry in _run_history:
        if run_entry.get("run_id") == run_id:
            return run_entry
    raise HTTPException(status_code=404, detail=f"Run #{run_id} not found")


@router.delete("/history")
async def clear_history():
    from backend.gsd_budgets_service import _history_clear

    count = _history_clear()
    return {"cleared": count}
