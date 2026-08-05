from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from backend.seo_stats_service import (
    get_daily, get_dashboard, get_deltas, get_notes, set_note,
)


class NoteIn(BaseModel):
    date: str
    note: str = ""
    color: Optional[str] = None

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/seo-stats", tags=["seo-stats"])
executor = ThreadPoolExecutor(max_workers=2)


# AUDIT LOW — a malformed ?date= used to surface as 500 "time data 'abc' does not match
# format '%Y-%m-%d'": the service's strptime raised deep inside run_in_executor and every
# handler's `except Exception` turned it into a server error. It is a client error.
#
# CALL THIS BEFORE THE `try`. HTTPException is an Exception, so `except Exception` below
# would catch a 400 raised inside the block and re-raise it as a 500 — the very bug this
# fixes, reintroduced one indent level deeper.
def _check_date(value: Optional[str], field: str, required: bool = False) -> None:
    if value is None or value == "":
        # Query params default to None and the service substitutes "yesterday"; a note's
        # date is the row key, so there it must actually be there.
        if required:
            raise HTTPException(status_code=400, detail=f"{field} is required (YYYY-MM-DD)")
        return
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"{field} must be YYYY-MM-DD, got {value!r}",
        )


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
    _check_date(start_date, "start_date")
    _check_date(end_date, "end_date")
    try:
        loop = asyncio.get_running_loop()
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
    _check_date(ref_date, "ref_date")
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(executor, get_deltas, ref_date, force)
        return result
    except Exception as e:
        logger.error(f"Error fetching seo-stats deltas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard")
async def dashboard(
    date: Optional[str] = Query(None, description="Day YYYY-MM-DD (default: yesterday)"),
    force: bool = Query(False, description="Bypass the 5-min cache and re-query Redshift"),
):
    """Single-day SEO tiles: device split (visits + revenue), CTR, Bounce, OPB,
    and week-over-week deltas against the same weekday 7 days earlier."""
    _check_date(date, "date")
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(executor, get_dashboard, date, force)
        return result
    except Exception as e:
        logger.error(f"Error fetching seo-stats dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/notes")
async def notes(
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
):
    """Per-date notes/labels for the given range."""
    _check_date(start_date, "start_date")
    _check_date(end_date, "end_date")
    try:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, get_notes, start_date, end_date)
    except Exception as e:
        logger.error(f"Error fetching seo-stats notes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/notes")
async def save_note(payload: NoteIn):
    """Upsert (or clear, if empty) the note for a single date."""
    # A note is keyed BY date, so a malformed one would write an unreachable row rather
    # than just failing a read.
    _check_date(payload.date, "date", required=True)
    try:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, set_note, payload.date, payload.note, payload.color)
    except Exception as e:
        logger.error(f"Error saving seo-stats note: {e}")
        raise HTTPException(status_code=500, detail=str(e))
