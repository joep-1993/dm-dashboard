"""Routes voor de GSD Tag Toppers-tool.

Upload een Excel met kandidaten, draai eerst een dry-run (leest alleen), bekijk
de tabel en draai daarna 'voor het echt'. De echte run schrijft naar Google Ads
en vereist daarom een expliciete `confirm`.
"""
import logging

from fastapi import APIRouter, File, HTTPException, UploadFile

from backend.gsd_tag_toppers_service import (
    cancel_run,
    get_seed_progress,
    import_items,
    items_for_run,
    items_summary,
    start_seed_from_ads,
    get_progress,
    get_results,
    get_run_detail,
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
    """Rijen + samenvatting van één run, voor de export en om hem terug te zetten
    in het resultatenscherm. Runs van vóór deze feature hebben geen rijen; die
    geven een lege lijst in plaats van een 404."""
    detail = get_run_detail(run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Run niet gevonden")
    return {"run_id": run_id, **detail}


# ---------------------------------------------------------------------------
# Beheerde ids — de gewenste staat per shop/land
# ---------------------------------------------------------------------------

@router.get("/items/summary")
async def items_overzicht():
    return items_summary()


@router.post("/items/import-excel")
async def items_import_excel():
    """Zet de geüploade Excel in de beheerde staat. Schrijft niets naar Google Ads."""
    rows = uploaded_rows()
    if not rows:
        raise HTTPException(status_code=400, detail="Upload eerst een Excel")
    naam = get_uploaded().get("filename") or "excel"
    return import_items(rows, f"excel:{naam}")


@router.post("/items/import-live")
async def items_import_live():
    """Vult de beheerde staat met wat er nu in Google Ads getarget wordt."""
    try:
        return start_seed_from_ads()
    except RuntimeError as ex:
        raise HTTPException(status_code=409, detail=str(ex))


@router.get("/items/import-live/progress")
async def items_import_live_progress():
    return get_seed_progress()


@router.post("/items/to-upload")
async def items_to_upload():
    """Laadt de beheerde staat als 'geüploade' rijen, zodat Preview/Run erop draait."""
    rows = items_for_run()
    if not rows:
        raise HTTPException(status_code=400, detail="De beheerde staat is nog leeg")
    set_uploaded({"rows": rows, "warnings": []}, "beheerde staat (tabel)")
    return {"rows": len(rows), "total_ids": sum(len(r["item_ids"]) for r in rows)}
