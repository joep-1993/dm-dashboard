from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from backend.gsd_check_service import search_gsd

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/gsd-check", tags=["gsd-check"])
executor = ThreadPoolExecutor(max_workers=2)


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "gsd_check"}


@router.get("/search")
async def search(
    shop_names: Optional[str] = Query(None, description="Comma-separated shop names (partial match)"),
):
    """Search GSD flags + shop metadata as of yesterday."""
    try:
        name_list = [s.strip() for s in shop_names.split(",") if s.strip()] if shop_names else None

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, search_gsd, name_list)
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result["error"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in GSD search: {e}")
        raise HTTPException(status_code=500, detail=str(e))
