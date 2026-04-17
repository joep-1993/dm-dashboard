from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from typing import Optional
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from backend.dma_plus_service import (
    start_operation, get_task_status, cancel_task,
    get_history, clear_history, remove_history_entry, COUNTRY_CONFIG,
    _cat_ids_cache, _cat_ids_cache_time, _CAT_IDS_TTL,
    _ensure_maincat_mapping, _fetch_all_cat_ids_from_taxonomy_api,
)
import time as _time

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dma-plus", tags=["dma-plus"])
executor = ThreadPoolExecutor(max_workers=2)


@router.get("/health")
def health():
    return {"status": "healthy", "service": "dma_plus"}


@router.get("/countries")
def countries():
    return {code: {"customer_id": cfg["customer_id"]} for code, cfg in COUNTRY_CONFIG.items()}


# --- Start operations ---

@router.post("/start")
async def start(
    operation: str = Form(...),
    country: str = Form("NL"),
    shop_name: Optional[str] = Form(None),
    maincat: Optional[str] = Form(None),
    maincat_id: Optional[str] = Form(None),
    cl1: Optional[str] = Form(None),
    budget: Optional[float] = Form(None),
    campaign_pattern: Optional[str] = Form(None),
    dry_run: bool = Form(False),
    fix: bool = Form(False),
    file: Optional[UploadFile] = File(None),
):
    """Start a DMA+ operation. Accepts Excel upload or shop name input."""
    valid_ops = ["inclusion", "exclusion", "reverse_inclusion", "reverse_exclusion", "validate_cl1", "validate_ads", "validate_trees"]
    if operation not in valid_ops:
        raise HTTPException(400, f"Invalid operation. Expected one of: {valid_ops}")

    if country not in COUNTRY_CONFIG:
        raise HTTPException(400, f"Invalid country. Expected NL or BE")

    wb_bytes = None
    if file:
        wb_bytes = await file.read()

    # Validate input
    if operation in ("inclusion", "exclusion", "reverse_inclusion", "reverse_exclusion") and not wb_bytes and not shop_name:
        raise HTTPException(400, "Provide either an Excel file or a shop name")

    task_id = start_operation(
        operation=operation,
        country=country,
        wb_bytes=wb_bytes,
        shop_name=shop_name,
        maincat=maincat,
        maincat_id=maincat_id,
        cl1=cl1,
        budget=budget,
        campaign_pattern=campaign_pattern,
        dry_run=dry_run,
        fix=fix,
    )

    return {"task_id": task_id, "status": "started"}


@router.get("/status/{task_id}")
def status(task_id: str):
    """Poll task progress."""
    task = get_task_status(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task


@router.post("/cancel/{task_id}")
def cancel(task_id: str):
    if cancel_task(task_id):
        return {"status": "cancelling"}
    raise HTTPException(400, "Task not running or not found")


# --- Category cache ---

@router.get("/cat-cache-status")
def cat_cache_status():
    """Check if the category index is cached."""
    import backend.dma_plus_service as svc
    age = _time.time() - svc._cat_ids_cache_time if svc._cat_ids_cache_time else None
    return {
        "cached": len(svc._cat_ids_cache) > 0,
        "rows": len(svc._cat_ids_cache),
        "age_seconds": round(age) if age else None,
        "ttl_seconds": _CAT_IDS_TTL,
    }


@router.post("/warm-cat-cache")
async def warm_cat_cache():
    """Pre-load the category index from Taxonomy API (takes ~5 min first time)."""
    import backend.dma_plus_service as svc
    if svc._cat_ids_cache and (_time.time() - svc._cat_ids_cache_time) < _CAT_IDS_TTL:
        return {"status": "already_cached", "rows": len(svc._cat_ids_cache)}

    loop = asyncio.get_event_loop()

    def _warm():
        svc._ensure_maincat_mapping()
        rows = svc._fetch_all_cat_ids_from_taxonomy_api()
        if rows:
            svc._cat_ids_cache = rows
            svc._cat_ids_cache_time = _time.time()
        return len(rows)

    count = await loop.run_in_executor(executor, _warm)
    return {"status": "loaded", "rows": count}


# --- History ---

@router.get("/history")
def history():
    return get_history()


@router.delete("/history")
def delete_history():
    clear_history()
    return {"status": "cleared"}


@router.delete("/history/{task_id}")
def delete_history_entry(task_id: str):
    if remove_history_entry(task_id):
        return {"status": "removed", "task_id": task_id}
    raise HTTPException(404, f"History entry {task_id} not found")
