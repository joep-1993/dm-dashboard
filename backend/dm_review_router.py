"""FastAPI router for the DM Review refresh tool (slide 2 feeds)."""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, HTTPException

from backend.dm_review_service import run_dm_review

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dm-review", tags=["dm-review"])
executor = ThreadPoolExecutor(max_workers=1)


@router.get("/health")
def health():
    return {"status": "healthy", "service": "dm_review"}


@router.post("/run")
async def run():
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(executor, run_dm_review)
    except Exception as e:
        logger.exception("dm-review run failed")
        raise HTTPException(status_code=500, detail=str(e))
    if result.get("status") == "error":
        raise HTTPException(status_code=400, detail=result["error"])
    return result
