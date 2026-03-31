from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from backend.gsd_campaigns_service import (
    get_all_gsd_stats,
    pause_campaign,
    enable_campaign,
    remove_campaign,
    get_redshift_shop_changes,
    run_gsd_script,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/gsd-campaigns", tags=["gsd-campaigns"])
executor = ThreadPoolExecutor(max_workers=2)


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "gsd_campaigns"}


@router.get("/stats")
async def get_stats():
    """Get campaign counts per account and full campaign list."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, get_all_gsd_stats)
        return result
    except Exception as e:
        logger.error(f"Error fetching GSD stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/campaigns")
async def get_campaigns(
    country: Optional[str] = Query(None, description="Filter by country (NL, BE, DE)"),
    status: Optional[str] = Query(None, description="Filter by status (ENABLED, PAUSED)"),
    search: Optional[str] = Query(None, description="Search by shop name or campaign name"),
):
    """Get all campaigns with optional filters."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, get_all_gsd_stats)
        campaigns = result.get("campaigns", [])

        # Apply filters
        if country:
            campaigns = [c for c in campaigns if c.get("country", "").upper() == country.upper()]
        if status:
            campaigns = [c for c in campaigns if c.get("status", "").upper() == status.upper()]
        if search:
            search_lower = search.lower()
            campaigns = [
                c for c in campaigns
                if search_lower in c.get("shop_name", "").lower()
                or search_lower in c.get("campaign_name", "").lower()
                or search_lower in str(c.get("shop_id", "")).lower()
            ]

        return {"campaigns": campaigns, "total": len(campaigns)}
    except Exception as e:
        logger.error(f"Error fetching GSD campaigns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/campaigns/{customer_id}/{campaign_id}/pause")
async def pause_campaign_endpoint(customer_id: str, campaign_id: str):
    """Pause a specific campaign."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, pause_campaign, customer_id, campaign_id)
        return result
    except Exception as e:
        logger.error(f"Error pausing campaign {campaign_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/campaigns/{customer_id}/{campaign_id}/enable")
async def enable_campaign_endpoint(customer_id: str, campaign_id: str):
    """Enable a specific campaign."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, enable_campaign, customer_id, campaign_id)
        return result
    except Exception as e:
        logger.error(f"Error enabling campaign {campaign_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/campaigns/{customer_id}/{campaign_id}")
async def remove_campaign_endpoint(customer_id: str, campaign_id: str):
    """Remove a specific campaign."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, remove_campaign, customer_id, campaign_id)
        return result
    except Exception as e:
        logger.error(f"Error removing campaign {campaign_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shop-changes")
async def get_shop_changes(
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
    shop_names: Optional[str] = Query(None, description="Comma-separated shop names"),
    included: bool = Query(False, description="If true, only include listed shops; if false, exclude them"),
):
    """Get shop changes from Redshift."""
    try:
        loop = asyncio.get_event_loop()
        shop_list = [s.strip() for s in shop_names.split(",") if s.strip()] if shop_names else None
        result = await loop.run_in_executor(
            executor, get_redshift_shop_changes, date, shop_list, included
        )
        return result
    except Exception as e:
        logger.error(f"Error fetching shop changes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run")
async def run_gsd_script_endpoint(
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
    shop_names: Optional[str] = Query(None, description="Comma-separated shop names"),
    included: bool = Query(False, description="If true, only include listed shops; if false, exclude them"),
):
    """Run the GSD script. This is a long-running operation."""
    try:
        loop = asyncio.get_event_loop()
        shop_list = [s.strip() for s in shop_names.split(",") if s.strip()] if shop_names else None
        result = await loop.run_in_executor(
            executor, run_gsd_script, date, shop_list, included
        )
        return result
    except Exception as e:
        logger.error(f"Error running GSD script: {e}")
        raise HTTPException(status_code=500, detail=str(e))
