"""FastAPI router for the DM Review refresh tool (slide 2 feeds)."""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.dm_review_service import run_dm_review

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dm-review", tags=["dm-review"])
executor = ThreadPoolExecutor(max_workers=1)


class RunRequest(BaseModel):
    # "YYYY-MM" (e.g. "2026-05"); omit/null to process the current month.
    target_month: Optional[str] = None


def _parse_target_yyyymm(target_month: Optional[str]) -> Optional[int]:
    if not target_month:
        return None
    try:
        y, m = target_month.split("-")
        y, m = int(y), int(m)
        if not (1 <= m <= 12) or y < 2000:
            raise ValueError
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400,
                            detail=f"Invalid target_month '{target_month}', expected YYYY-MM")
    return y * 100 + m


@router.get("/health")
def health():
    return {"status": "healthy", "service": "dm_review"}


@router.post("/run")
async def run(req: Optional[RunRequest] = None):
    target_yyyymm = _parse_target_yyyymm(req.target_month if req else None)
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(executor, run_dm_review, target_yyyymm)
    except Exception as e:
        logger.exception("dm-review run failed")
        raise HTTPException(status_code=500, detail=str(e))
    if result.get("status") == "error":
        raise HTTPException(status_code=400, detail=result["error"])
    return result
