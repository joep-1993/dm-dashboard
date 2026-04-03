from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from backend.mc_id_finder_service import search_mc_ids

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/mc-id-finder", tags=["mc-id-finder"])
executor = ThreadPoolExecutor(max_workers=2)


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "mc_id_finder"}


@router.get("/search")
async def search(
    shop_names: Optional[str] = Query(None, description="Comma-separated shop names (partial match)"),
    countries: Optional[str] = Query(None, description="Comma-separated country codes (nl,be,de)"),
):
    """Search for Merchant Center IDs."""
    try:
        name_list = [s.strip() for s in shop_names.split(",") if s.strip()] if shop_names else None
        country_list = [c.strip().lower() for c in countries.split(",") if c.strip()] if countries else None
        # Validate country codes
        if country_list:
            country_list = [c for c in country_list if c in ("nl", "be", "de")]

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            executor, search_mc_ids, name_list, country_list
        )
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result["error"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in MC ID search: {e}")
        raise HTTPException(status_code=500, detail=str(e))
