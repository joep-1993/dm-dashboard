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


@router.get("/facet-values")
async def facet_values(facet_id: int,
                       days: int = Query(30, ge=1, le=365),
                       with_urls: bool = Query(True),
                       limit: int = Query(300, ge=1, le=2000)):
    """De waarden van één facet: welke er in dit venster veranderden (uit de
    eventstore, live verrijkt) en of er een pagina op bestaat. `limit` begrenst
    de gewijzigde waarden; `with_urls=false` slaat de pa.urls-scan over, die bij
    een veelgebruikte slug als `merk` ~2,5 s kost."""
    try:
        return fw.get_facet_values(facet_id, days, with_urls, limit)
    except Exception as e:
        logger.exception("facet-watch facet-values failed")
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


@router.get("/product-lines")
async def product_lines(days: int = Query(30, ge=1, le=365),
                        limit: int = Query(500, ge=1, le=5000)):
    """Nieuwe productlijnen — facetwaarden uit de productlijn-familie, ontdubbeld op
    (merk, naam), met de main categorieën en het gecachte zoekvolume."""
    try:
        return fw.get_product_lines(days, limit)
    except Exception as e:
        logger.exception("facet-watch product-lines failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/product-lines/volumes")
async def product_line_volumes(days: int = Query(30, ge=1, le=365),
                               limit: int = Query(500, ge=1, le=5000),
                               only_missing: bool = Query(True)):
    """Zoekvolume ophalen bij de Keyword Planner voor de productlijnen in dit
    venster, en cachen. Blokkeert: ~1 call per 500 termen.

    De zoektermen komen uit `get_product_lines` zelf en worden hier niet opnieuw
    samengesteld — anders kan de term die wordt OPGEHAALD afwijken van de term
    waarop de tabel zijn cache JOINt, en blijft de kolom leeg terwijl het volume
    wel is opgehaald.
    """
    try:
        rows = fw.get_product_lines(days, limit).get("product_lines", [])
        kws = [r["keyword"] for r in rows
               if r.get("keyword") and (not only_missing or r.get("search_volume") is None)]
        if not kws:
            return {"success": True, "requested": 0, "fetched": 0,
                    "message": "Alle productlijnen in dit venster hebben al een zoekvolume."}
        res = fw.fetch_product_line_volumes(kws)
        return {"success": True, **res}
    except Exception as e:
        logger.exception("facet-watch product-line volumes failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/moved-facets")
async def moved_facets(days: int = Query(30, ge=1, le=365),
                       with_url_counts: bool = Query(True)):
    """Verhuisde facetten — facetten waarvan de URL-slug veranderde, per locale,
    met het aantal bestaande URL's dat daardoor van adres wisselt."""
    try:
        return fw.get_moved_facets(days, with_url_counts)
    except Exception as e:
        logger.exception("facet-watch moved-facets failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/moved-facets/redirects")
async def moved_facet_redirects(facet_id: int,
                                old_slug: str,
                                new_slug: str,
                                limit: int = Query(5000, ge=1, le=50000)):
    """De concrete oude -> nieuwe URL-paren voor één slug-wijziging, klaar om in de
    Redirect Tool te zetten."""
    try:
        return fw.build_moved_facet_redirects(facet_id, old_slug, new_slug, limit)
    except Exception as e:
        logger.exception("facet-watch moved-facet redirects failed")
        raise HTTPException(status_code=500, detail=str(e))


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
