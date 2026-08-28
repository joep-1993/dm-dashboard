"""
Facet Watch Router — daily insight into recently created/changed facets per main
category, from the Taxonomy API audit log. Logic lives in facet_watch_service.
"""
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend import facet_watch_service as fw

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/facet-watch", tags=["facet-watch"])


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.post("/init")
async def init():
    """Create the tables (idempotent) and fill the main-category names."""
    fw.init_tables()
    return fw.refresh_maincats()


@router.get("/status")
async def status():
    try:
        return {"store": fw.get_status(), "run": fw.get_run_state()}
    except Exception as e:
        logger.exception("facet-watch status failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/main-categories")
async def main_categories():
    return fw.get_main_categories()


@router.get("/overview")
async def overview(days: int = Query(1, ge=1, le=365),
                   exclude_auto: bool = Query(True)):
    """`exclude_auto` hides the auto-created product-line facet family, which
    otherwise puts a phantom "1 new facet" in almost every main category."""
    try:
        return fw.get_overview(days, exclude_auto)
    except Exception as e:
        logger.exception("facet-watch overview failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/facets")
async def facets(days: int = Query(1, ge=1, le=365),
                 main_cat_id: Optional[int] = None,
                 limit: int = Query(300, ge=1, le=5000),
                 exclude_auto: bool = Query(True)):
    try:
        return fw.get_facets(days, main_cat_id, limit, exclude_auto)
    except Exception as e:
        logger.exception("facet-watch facets failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/events")
async def events(days: int = Query(1, ge=1, le=365),
                 main_cat_id: Optional[int] = None,
                 facet_id: Optional[int] = None,
                 entity_name: Optional[str] = None,
                 action: Optional[str] = None,
                 actor: Optional[str] = None,
                 limit: int = Query(500, ge=1, le=5000),
                 offset: int = Query(0, ge=0)):
    try:
        return fw.get_events(days, main_cat_id, facet_id, entity_name,
                             action, actor, limit, offset)
    except Exception as e:
        logger.exception("facet-watch events failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/deletions")
async def deletions(days: int = Query(1, ge=1, le=30),
                    limit: int = Query(300, ge=1, le=1000)):
    """Route C (garbage bin) — deletions WITH their name and restore window."""
    return fw.get_deletions(days, limit)


@router.post("/ingest")
async def ingest(from_date: Optional[str] = None, to_date: Optional[str] = None):
    """Start an ingest in the background. Omit both dates for the daily default:
    one day back past the newest stored event. Idempotent on audit id."""
    fw.init_tables()
    return fw.start_ingest_async(from_date, to_date)


@router.post("/stop")
async def stop():
    return fw.stop_run()


@router.post("/seed-values")
async def seed_values():
    """One-off (or occasional) refresh of the value -> facet cache from the full
    `/api/Facets/values` dump: 555k rows, ~146 MB, ~60 s. Blocks."""
    fw.init_tables()
    try:
        return fw.seed_value_facet_map()
    except Exception as e:
        logger.exception("facet-watch seed failed")
        raise HTTPException(status_code=500, detail=str(e))
