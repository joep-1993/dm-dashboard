from datetime import date
from concurrent.futures import ThreadPoolExecutor
import asyncio
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.performance_standup_service import run_standup

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/performance-standup", tags=["performance-standup"])
executor = ThreadPoolExecutor(max_workers=1)


class RunRequest(BaseModel):
    start_date: date
    end_date: date


@router.get("/health")
def health():
    return {"status": "healthy", "service": "performance_standup"}


@router.post("/run")
async def run(req: RunRequest):
    if req.start_date > req.end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(executor, run_standup, req.start_date, req.end_date)
    except Exception as e:
        logger.exception("performance-standup run failed")
        raise HTTPException(status_code=500, detail=str(e))
    if result.get("status") == "error":
        raise HTTPException(status_code=400, detail=result["error"])
    return result
