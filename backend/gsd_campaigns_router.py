from fastapi import APIRouter, HTTPException, Query, Request, Body
from typing import Optional, List
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from backend.gsd_campaigns_service import (
    get_all_gsd_stats,
    pause_campaign,
    enable_campaign,
    remove_campaign,
    get_redshift_shop_changes,
    run_gsd_script,
    cancel_run,
    get_run_progress,
    preview_gsd_script,
    get_preview_progress,
    undo_run,
    reconstruct_run,
    backfill_campaign_created_dates,
)
from backend.gsd_ll_service import (
    start_ll_run,
    start_ll_apply,
    get_ll_progress,
    get_history as get_ll_history,
    get_shop_cycles as get_ll_shop_cycles,
    get_excel_schedule_status,
    toggle_excel_schedule,
    load_excel_data,
    get_excel_data_status,
    save_activity,
    get_activity_log,
    mark_activity_reset,
    backfill_activity_from_ll,
    backfill_activity_from_gsd,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/gsd-campaigns", tags=["gsd-campaigns"])
executor = ThreadPoolExecutor(max_workers=2)


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "gsd_campaigns"}


@router.get("/stats")
async def get_stats():
    """Get campaign counts per account and full campaign list."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, get_all_gsd_stats)
        return result
    except Exception as e:
        logger.error(f"Error fetching GSD stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/campaigns")
async def get_campaigns(
    country: Optional[str] = Query(None, description="Filter by country (NL, BE, DE)"),
    status: Optional[str] = Query(None, description="Filter by status (ENABLED, PAUSED)"),
    search: Optional[str] = Query(None, description="Search by shop name or campaign name"),
):
    """Get all campaigns with optional filters."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, get_all_gsd_stats)
        campaigns = result.get("campaigns", [])

        # Apply filters
        if country:
            campaigns = [c for c in campaigns if c.get("country", "").upper() == country.upper()]
        if status:
            campaigns = [c for c in campaigns if c.get("status", "").upper() == status.upper()]
        if search:
            search_lower = search.lower()
            campaigns = [
                c for c in campaigns
                if search_lower in c.get("shop_name", "").lower()
                or search_lower in c.get("campaign_name", "").lower()
                or search_lower in str(c.get("shop_id", "")).lower()
            ]

        return {"campaigns": campaigns, "total": len(campaigns)}
    except Exception as e:
        logger.error(f"Error fetching GSD campaigns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/campaigns/backfill-created-dates")
async def backfill_created_dates_endpoint(
    days: int = Query(30, description="Look back this many days in change_event (~30 max retained)"),
    dry_run: bool = Query(False, description="If true, report what would be inserted without writing"),
):
    """Seed per-campaign creation dates from the Google Ads change_event log."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, backfill_campaign_created_dates, days, dry_run)
        return result
    except Exception as e:
        logger.error(f"Error backfilling created dates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/campaigns/{customer_id}/{campaign_id}/pause")
async def pause_campaign_endpoint(customer_id: str, campaign_id: str):
    """Pause a specific campaign."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, pause_campaign, customer_id, campaign_id)
        return result
    except Exception as e:
        logger.error(f"Error pausing campaign {campaign_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/campaigns/{customer_id}/{campaign_id}/enable")
async def enable_campaign_endpoint(customer_id: str, campaign_id: str):
    """Enable a specific campaign."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, enable_campaign, customer_id, campaign_id)
        return result
    except Exception as e:
        logger.error(f"Error enabling campaign {campaign_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/campaigns/{customer_id}/{campaign_id}")
async def remove_campaign_endpoint(customer_id: str, campaign_id: str):
    """Remove a specific campaign."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, remove_campaign, customer_id, campaign_id)
        return result
    except Exception as e:
        logger.error(f"Error removing campaign {campaign_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shop-changes")
async def get_shop_changes(
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
    shop_names: Optional[str] = Query(None, description="Comma-separated shop names"),
    included: bool = Query(False, description="If true, only include listed shops; if false, exclude them"),
):
    """Get shop changes from Redshift."""
    try:
        loop = asyncio.get_event_loop()
        shop_list = [s.strip() for s in shop_names.split(",") if s.strip()] if shop_names else None
        result = await loop.run_in_executor(
            executor, get_redshift_shop_changes, date, shop_list, included
        )
        return result
    except Exception as e:
        logger.error(f"Error fetching shop changes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ll/run")
async def run_low_linkage_endpoint(
    request: Request,
    dry_run: bool = Query(False, description="If true, preview only — no Ads mutations or DB writes"),
    date: Optional[str] = Query(None, description="Evaluate shop_list GSD flags as of this date (YYYY-MM-DD)"),
    shop_names: Optional[str] = Query(None, description="Comma-separated feed shop names to scope the run"),
    included: bool = Query(False, description="With shop_names: True = only these shops, False = all except"),
    source: str = Query("feed", description="Data source: 'feed' (pixel-monitor CSV) or 'excel' (local Excel file)"),
):
    """Start a low-linkage run in the background; poll /ll/progress for status."""
    client_ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "unknown")
    logger.info("GSD LL /ll/run called — ip=%s  dry_run=%s  source=%s  user-agent=%s",
                client_ip, dry_run, source, user_agent)
    try:
        shop_list = [s.strip() for s in shop_names.split(",") if s.strip()] if shop_names else None
        return start_ll_run(dry_run, date, shop_list, included, source)
    except Exception as e:
        logger.error(f"Error starting GSD low-linkage process: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ll/apply")
async def apply_low_linkage_endpoint(payload: dict):
    """Apply pause/enable for an explicit selection of preview rows.

    Body: {"entries": [ {action, customer_id, campaign_id, shop_id, shop_name,
    country, campaign_name, linkage, campaign_label_resource?}, ... ]}.
    Starts a background run; poll /ll/progress for status.
    """
    try:
        entries = payload.get("entries") if isinstance(payload, dict) else None
        if not isinstance(entries, list) or not entries:
            raise HTTPException(status_code=400, detail="No entries provided.")
        return start_ll_apply(entries)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting GSD low-linkage apply: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ll/progress")
def ll_progress_endpoint():
    """Return the current/last low-linkage run progress for the UI to poll."""
    return get_ll_progress()


@router.get("/ll/history")
async def ll_history_endpoint(
    limit: int = Query(500, ge=1, le=5000, description="Max audit rows to return"),
):
    """Return the pause/enable audit trail from pa.jvs_gsd_ll_campaigns."""
    try:
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(executor, get_ll_history, limit)
        return {"rows": rows, "total": len(rows)}
    except Exception as e:
        logger.error(f"Error fetching GSD LL history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ll/shop-cycles")
async def ll_shop_cycles_endpoint(
    limit: int = Query(1000, ge=1, le=10000, description="Max shop-cycle rows to return"),
):
    """Per-(shop, country) pause/enable cycle counts from pa.jvs_gsd_ll_shop_cycles."""
    try:
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(executor, get_ll_shop_cycles, limit)
        return {"rows": rows, "total": len(rows)}
    except Exception as e:
        logger.error(f"Error fetching GSD LL shop cycles: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/activity-log")
async def activity_log_get(
    limit: int = Query(100, ge=1, le=500, description="Max entries to return"),
):
    """Return the server-side activity log."""
    try:
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(executor, get_activity_log, limit)
        return {"entries": rows}
    except Exception as e:
        logger.error(f"Error fetching activity log: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/activity-log")
async def activity_log_post(entry: dict = Body(...)):
    """Save or update an activity log entry from the frontend."""
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(executor, save_activity, entry)
        return {"ok": True}
    except Exception as e:
        logger.error(f"Error saving activity log entry: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/activity-log/{entry_id}/reset")
async def activity_log_mark_reset(entry_id: str):
    """Mark an activity log entry as reset."""
    try:
        loop = asyncio.get_event_loop()
        found = await loop.run_in_executor(executor, mark_activity_reset, entry_id)
        return {"ok": True, "found": found}
    except Exception as e:
        logger.error(f"Error marking activity reset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/activity-log/backfill")
async def activity_log_backfill():
    """Reconstruct Activity Log entries from both LL audit table and Google Ads change history."""
    try:
        loop = asyncio.get_event_loop()
        ll_result = await loop.run_in_executor(executor, backfill_activity_from_ll)
        gsd_result = await loop.run_in_executor(executor, backfill_activity_from_gsd)
        return {"ll": ll_result, "gsd": gsd_result}
    except Exception as e:
        logger.error(f"Error backfilling activity log: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/preview")
async def preview_gsd_script_endpoint(
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
    shop_names: Optional[str] = Query(None, description="Comma-separated shop names"),
    included: bool = Query(False, description="If true, only include listed shops; if false, exclude them"),
):
    """Dry-run the GSD script: report how many campaigns would be created/paused. Read-only."""
    try:
        loop = asyncio.get_event_loop()
        shop_list = [s.strip() for s in shop_names.split(",") if s.strip()] if shop_names else None
        result = await loop.run_in_executor(
            executor, preview_gsd_script, date, shop_list, included
        )
        return result
    except Exception as e:
        logger.error(f"Error previewing GSD script: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/preview/progress")
def preview_progress_endpoint():
    """Current GSD preview progress {current, total, running} for the progress bar."""
    return get_preview_progress()


@router.post("/reconstruct")
async def reconstruct_run_endpoint(payload: dict):
    """
    Reconstruct a past run's changes from Google Ads change history (read-only),
    keyed off a log entry timestamp. Body: {"at": "<iso>", "before_minutes"?,
    "after_minutes"?}. Returns {created, paused, window, errors} to feed /undo.
    """
    try:
        at = payload.get("at")
        if not at:
            raise HTTPException(status_code=400, detail="Missing 'at' timestamp")
        before = int(payload.get("before_minutes", 60))
        after = int(payload.get("after_minutes", 10))
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, reconstruct_run, at, before, after)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reconstructing GSD run: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/undo")
async def undo_run_endpoint(payload: dict):
    """
    Reverse a previous run: pause the campaigns it created and re-enable the
    campaigns it paused. Body: {"created": [...], "paused": [...]} where each
    item has customer_id and campaign_id.
    """
    try:
        created = payload.get("created") or []
        paused = payload.get("paused") or []
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, undo_run, created, paused)
        return result
    except Exception as e:
        logger.error(f"Error undoing GSD run: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run/cancel")
def cancel_run_endpoint():
    """Request the active GSD run to stop at the next shop boundary."""
    cancel_run()
    return {"ok": True}


@router.get("/run/progress")
def run_progress_endpoint():
    """Current GSD run progress {current, total, running} for the progress bar."""
    return get_run_progress()


@router.post("/run")
async def run_gsd_script_endpoint(
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
    shop_names: Optional[str] = Query(None, description="Comma-separated shop names"),
    included: bool = Query(False, description="If true, only include listed shops; if false, exclude them"),
):
    """Run the GSD script. This is a long-running operation."""
    try:
        loop = asyncio.get_event_loop()
        shop_list = [s.strip() for s in shop_names.split(",") if s.strip()] if shop_names else None
        result = await loop.run_in_executor(
            executor, run_gsd_script, date, shop_list, included
        )
        return result
    except Exception as e:
        logger.error(f"Error running GSD script: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ll/excel-schedule")
def excel_schedule_endpoint():
    """Return the daily Excel data-load schedule status."""
    return get_excel_schedule_status()


@router.post("/ll/excel-schedule/toggle")
def excel_schedule_toggle_endpoint(
    enabled: bool = Query(..., description="Enable or disable the daily schedule"),
):
    """Enable or disable the daily Excel data-load at 9:50 CET."""
    return toggle_excel_schedule(enabled)


@router.post("/ll/excel-load")
async def excel_load_endpoint():
    """Load (cache) the newest Excel file for use by Preview/Run.

    Called daily by the Windows Scheduled Task or manually from the UI.
    Does NOT pause/enable any campaigns.
    """
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, load_excel_data)
        return result
    except Exception as e:
        logger.error(f"Error loading Excel data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ll/excel-data")
def excel_data_endpoint():
    """Return the cached Excel data status (file, counts, load time)."""
    return get_excel_data_status()
