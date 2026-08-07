"""Routes voor de GSD Tag Toppers-tool.

Upload een Excel met kandidaten, draai eerst een dry-run (leest alleen), bekijk
de tabel en draai daarna 'voor het echt'. De echte run schrijft naar Google Ads
en vereist daarom een expliciete `confirm`.
"""
import logging

from fastapi import APIRouter, File, HTTPException, UploadFile

from backend.gsd_tag_toppers_service import (
    cancel_run,
    get_progress,
    get_results,
    get_run_results,
    get_runs,
    get_uploaded,
    parse_workbook,
    set_uploaded,
    start_run,
    uploaded_rows,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/gsd-tag-toppers", tags=["gsd-tag-toppers"])

MAX_UPLOAD_BYTES = 25 * 1024 * 1024


@router.post("/upload")
async def upload(file: UploadFile = File(...)):
    if not (file.filename or "").lower().endswith((".xlsx", ".xlsm")):
        raise HTTPException(status_code=400, detail="Alleen .xlsx of .xlsm")
    data = await file.read()
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=400, detail="Bestand is groter dan 25 MB")
    try:
        parsed = parse_workbook(data)
    except Exception as ex:
        logger.exception("Tag Toppers upload kon niet gelezen worden")
        raise HTTPException(status_code=400, detail=f"Kon de Excel niet lezen: {ex}")
    if not parsed["rows"]:
        raise HTTPException(status_code=400, detail="Geen bruikbare rijen gevonden")
    set_uploaded(parsed, file.filename)
    return {
        "filename": file.filename,
        "rows": parsed["total_rows"],
        "total_ids": parsed["total_ids"],
        "warnings": parsed["warnings"],
    }


@router.get("/uploaded")
async def uploaded():
    return get_uploaded()


@router.post("/dry-run")
async def dry_run():
    rows = uploaded_rows()
    if not rows:
        raise HTTPException(status_code=400, detail="Upload eerst een Excel")
    try:
        return start_run(rows, dry_run=True)
    except RuntimeError as ex:
        raise HTTPException(status_code=409, detail=str(ex))


@router.post("/run")
async def run(confirm: bool = False):
    # Deze route schrijft naar Google Ads. Zonder expliciete confirm gebeurt er niets,
    # zodat een losse POST (of een dubbele klik op de dry-run knop) nooit muteert.
    if not confirm:
        raise HTTPException(status_code=400, detail="confirm=true is verplicht voor een echte run")
    rows = uploaded_rows()
    if not rows:
        raise HTTPException(status_code=400, detail="Upload eerst een Excel")
    try:
        return start_run(rows, dry_run=False)
    except RuntimeError as ex:
        raise HTTPException(status_code=409, detail=str(ex))


@router.get("/progress")
async def progress():
    return get_progress()


@router.get("/results")
async def results():
    return get_results()


@router.post("/cancel")
async def cancel():
    cancel_run()
    return {"cancelled": True}


@router.get("/runs")
async def runs(limit: int = 100):
    """Afgeronde runs uit de database — overleeft een herstart van de backend."""
    return {"runs": get_runs(max(1, min(limit, 500)))}


@router.get("/runs/{run_id}/results")
async def run_results(run_id: int):
    """De rijen van één run, voor de export. Runs van vóór deze feature hebben
    er geen; die geven een lege lijst in plaats van een 404."""
    rows = get_run_results(run_id)
    if rows is None:
        raise HTTPException(status_code=404, detail="Run niet gevonden")
    return {"run_id": run_id, "results": rows}
