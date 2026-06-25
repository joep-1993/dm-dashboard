from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from backend.seo_stats_service import get_daily, get_deltas, get_notes, set_note


class NoteIn(BaseModel):
    date: str
    note: str = ""
    color: Optional[str] = None

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/seo-stats", tags=["seo-stats"])
executor = ThreadPoolExecutor(max_workers=2)


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "seo_stats"}


@router.get("/daily")
async def daily(
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD (default: 30 days ago)"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD (default: yesterday)"),
    force: bool = Query(False, description="Bypass the 5-min cache and re-query Redshift"),
):
    """Per-day visits + revenue for SEO / DMA organic / GSAAS."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, get_daily, start_date, end_date, force)
        return result
    except Exception as e:
        logger.error(f"Error fetching seo-stats daily: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/deltas")
async def deltas(
    ref_date: Optional[str] = Query(None, description="Reference 'yesterday' YYYY-MM-DD (default: yesterday)"),
    force: bool = Query(False, description="Bypass the 5-min cache and re-query Redshift"),
):
    """Channel %-deltas + top maincats/subcats by most-positive delta.

    Visits compare ref vs ref-7d; revenue compares ref-1 vs ref-8d.
    """
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, get_deltas, ref_date, force)
        return result
    except Exception as e:
        logger.error(f"Error fetching seo-stats deltas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/notes")
async def notes(
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
):
    """Per-date notes/labels for the given range."""
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(executor, get_notes, start_date, end_date)
    except Exception as e:
        logger.error(f"Error fetching seo-stats notes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/notes")
async def save_note(payload: NoteIn):
    """Upsert (or clear, if empty) the note for a single date."""
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(executor, set_note, payload.date, payload.note, payload.color)
    except Exception as e:
        logger.error(f"Error saving seo-stats note: {e}")
        raise HTTPException(status_code=500, detail=str(e))
