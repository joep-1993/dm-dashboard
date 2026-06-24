from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from backend.shop_campaigns_service import get_performance, get_inventory, get_top_performers

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/shop-campaigns", tags=["shop-campaigns"])
executor = ThreadPoolExecutor(max_workers=2)


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "shop_campaigns"}


@router.get("/performance")
async def performance(
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD (default: 30 days ago)"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD (default: today)"),
    force: bool = Query(False, description="Bypass the cache and re-query SA360"),
):
    """Per-day aggregated performance of all SHOP/ campaigns from SA360."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, get_performance, start_date, end_date, force)
        return result
    except Exception as e:
        logger.error(f"Error fetching shop-campaigns performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-performers")
async def top_performers(
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD (default: 30 days ago)"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD (default: today)"),
    limit: int = Query(10000, ge=1, le=100000, description="Max rows per table (default: all, ranked by revenue)"),
    force: bool = Query(False, description="Bypass the cache and re-query SA360"),
):
    """Top campaigns and ad groups (by revenue) over a date range from SA360."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, get_top_performers, start_date, end_date, limit, force)
        return result
    except Exception as e:
        logger.error(f"Error fetching shop-campaigns top performers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory")
async def inventory(
    force: bool = Query(False, description="Bypass the cache and re-query SA360"),
):
    """All SHOP/ campaigns with status counts (which campaigns are being tracked)."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, get_inventory, force)
        return result
    except Exception as e:
        logger.error(f"Error fetching shop-campaigns inventory: {e}")
        raise HTTPException(status_code=500, detail=str(e))
