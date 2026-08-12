"""Bot Hits — FastAPI routes for the crawler-log dashboard."""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend.bothits_service import (
    clear_cache, get_categories, get_daily, get_ingest_log, get_meta,
    get_summary, get_top_urls, get_top_waste, get_url_detail,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/bothits", tags=["bothits"])
executor = ThreadPoolExecutor(max_workers=4)


def _check_date(value: Optional[str], field: str) -> None:
    """Reject a malformed date at the edge.

    Call this BEFORE the try block: HTTPException is an Exception, so a 400
    raised inside would be swallowed by `except Exception` and re-raised as a
    500 — the same trap documented in seo_stats_router.
    """
    if not value:
        return
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400,
                            detail=f"{field} must be YYYY-MM-DD, got {value!r}")


async def _run(fn, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, fn, *args)


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "bothits"}


@router.get("/meta")
async def meta(force: bool = Query(False)):
    """Date coverage, hosts, bot taxonomy and url types for the filter bar."""
    try:
        return await _run(get_meta, force)
    except Exception as e:
        logger.error("bothits meta failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/daily")
async def daily(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    host: Optional[str] = Query(None, description="comma-separated hosts"),
    bot_class: Optional[str] = Query(None, description="ai,search,seo-tool,social,monitoring,other"),
    bot_family: Optional[str] = Query(None, description="comma-separated families"),
    url_type: Optional[str] = Query(None, description="comma-separated url types"),
    known: Optional[str] = Query(None, description="known | unknown"),
    group_by: str = Query("bot_class"),
    force: bool = Query(False),
):
    """Per-day hit counts split by the chosen dimension."""
    _check_date(start_date, "start_date")
    _check_date(end_date, "end_date")
    try:
        return await _run(get_daily, start_date, end_date, host, bot_class,
                          bot_family, url_type, known, group_by, force)
    except Exception as e:
        logger.error("bothits daily failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
async def summary(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    host: Optional[str] = Query(None),
    bot_class: Optional[str] = Query(None),
    bot_family: Optional[str] = Query(None),
    url_type: Optional[str] = Query(None),
    known: Optional[str] = Query(None),
    force: bool = Query(False),
):
    """Totals per bot family plus url-type / facet-depth / host breakdowns."""
    _check_date(start_date, "start_date")
    _check_date(end_date, "end_date")
    try:
        return await _run(get_summary, start_date, end_date, host, bot_class,
                          bot_family, url_type, known, force)
    except Exception as e:
        logger.error("bothits summary failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-urls")
async def top_urls(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    host: Optional[str] = Query(None),
    bot_class: Optional[str] = Query(None),
    bot_family: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    main_cat: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    force: bool = Query(False),
):
    """Most-crawled URLs that exist in pa.urls."""
    _check_date(start_date, "start_date")
    _check_date(end_date, "end_date")
    try:
        return await _run(get_top_urls, start_date, end_date, host, bot_class,
                          bot_family, limit, main_cat, search, force)
    except Exception as e:
        logger.error("bothits top-urls failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-waste")
async def top_waste(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    host: Optional[str] = Query(None),
    bot_family: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    force: bool = Query(False),
):
    """Most-crawled URLs absent from pa.urls — the crawl-budget leak."""
    _check_date(start_date, "start_date")
    _check_date(end_date, "end_date")
    try:
        return await _run(get_top_waste, start_date, end_date, host,
                          bot_family, limit, force)
    except Exception as e:
        logger.error("bothits top-waste failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/url")
async def url_detail(
    url: str = Query(..., description="exact path, e.g. /products/mode/..."),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    force: bool = Query(False),
):
    """Per-day, per-bot crawl history for a single URL."""
    _check_date(start_date, "start_date")
    _check_date(end_date, "end_date")
    try:
        return await _run(get_url_detail, url, start_date, end_date, force)
    except Exception as e:
        logger.error("bothits url detail failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/categories")
async def categories(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    host: Optional[str] = Query(None),
    bot_class: Optional[str] = Query(None),
    bot_family: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    force: bool = Query(False),
):
    """Crawl volume per main category (known URLs only)."""
    _check_date(start_date, "start_date")
    _check_date(end_date, "end_date")
    try:
        return await _run(get_categories, start_date, end_date, host,
                          bot_class, bot_family, limit, force)
    except Exception as e:
        logger.error("bothits categories failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ingest/log")
async def ingest_log(limit: int = Query(200, ge=1, le=1000),
                     force: bool = Query(False)):
    """Which log dates are loaded, how complete, and how long they took."""
    try:
        return await _run(get_ingest_log, limit, force)
    except Exception as e:
        logger.error("bothits ingest log failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ingest/status")
def ingest_status():
    from backend.bothits_ingest import (AUTO_INGEST, AUTO_INGEST_AT, DROP_DIR,
                                        ingest_state)
    return {**ingest_state(), "drop_dir": DROP_DIR,
            "auto_ingest": AUTO_INGEST, "auto_ingest_at": AUTO_INGEST_AT}


@router.post("/ingest/run")
def ingest_run():
    """Kick off a drop-folder ingest in the background.

    Shares its lock with the scheduled run, so clicking Verwerk while the
    nightly pass is going returns already_running instead of double-processing.
    """
    from backend.bothits_ingest import start_ingest_async
    started, state = start_ingest_async("manual", on_done=clear_cache)
    return {"status": "started" if started else "already_running", **state}


@router.get("/s3/preview")
async def s3_preview(days: int = Query(3, ge=1, le=45)):
    """Wat een ophaal van `days` dagen zou downloaden — zonder te downloaden.

    Voedt de confirm-dialog van "Nieuwe logs ophalen": één dag is ~2.900 bestanden
    en ~900 MB, dus die klik hoort een volume te quoten. Max 45 dagen omdat de
    bucket ~42 dagen retentie heeft; ouder vragen levert alleen lege datums op.
    """
    from backend.bothits_s3 import S3NotConfigured, preview
    try:
        return await _run(preview, days)
    except S3NotConfigured as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("bothits s3 preview failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/s3/fetch")
def s3_fetch(days: int = Query(3, ge=1, le=45)):
    """Download nieuwe CloudFront-logs uit S3 en verwerk ze meteen.

    Deelt lock én statusveld met de dropfolder-ingest, dus dit is dezelfde poller
    als de Verwerk-knop en twee gelijktijdige runs kunnen niet bestaan.
    """
    from backend.bothits_ingest import start_ingest_async
    from backend.bothits_s3 import S3_DIR, S3NotConfigured, fetch, is_configured
    if not is_configured():
        raise HTTPException(
            status_code=400,
            detail="S3-credentials ontbreken: zet BOTHITS_S3_ACCESS_KEY_ID en "
                   "BOTHITS_S3_SECRET_ACCESS_KEY in .env")

    def before(progress, should_cancel):
        return fetch(days, progress=progress, should_cancel=should_cancel)

    try:
        started, state = start_ingest_async("s3", on_done=clear_cache,
                                           src=S3_DIR, before=before)
    except S3NotConfigured as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "started" if started else "already_running",
            "days": days, **state}


@router.post("/ingest/cancel")
def ingest_cancel():
    """Vraag de lopende ophaal/ingest-run te stoppen op de eerstvolgende veilige grens.

    Geen harde stop: de worker kijkt zelf tussen bestanden (download) en tussen
    logdatums (verwerken). Alles wat al in de cube staat blijft geldig — een datum is
    óf helemaal geladen óf helemaal niet, dus de ledger blijft kloppen en een volgende
    run pakt de rest gewoon op.
    """
    from backend.bothits_ingest import request_cancel
    return {"status": "cancelling" if request_cancel() else "niets_actief"}


@router.post("/cache/clear")
def cache_clear():
    clear_cache()
    return {"status": "cleared"}
