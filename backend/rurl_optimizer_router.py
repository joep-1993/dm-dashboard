"""
HTTP surface for the R-URL Optimizer tool.
"""
from __future__ import annotations

import os
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import FileResponse
from typing import Optional

from backend import rurl_optimizer_service as svc

router = APIRouter(prefix="/api/rurl", tags=["rurl"])


@router.get("/health")
def health():
    return {"status": "healthy", "service": "rurl_optimizer"}


@router.post("/optimize")
async def optimize(
    file: Optional[UploadFile] = File(None),
    workers: int = Form(0),
    threshold: int = Form(80),
    multi_facet: bool = Form(True),
    url_column: str = Form("r_url"),
    also_global: bool = Form(False),
    source: str = Form("upload"),
    lookback_days: int = Form(365),
    force_reprocess: bool = Form(False),
):
    if source not in ("upload", "redshift"):
        raise HTTPException(400, "source must be 'upload' or 'redshift'")

    body: Optional[bytes] = None
    filename: Optional[str] = None
    if source == "upload":
        if not file or not file.filename:
            raise HTTPException(400, "File required when source=upload")
        body = await file.read()
        if not body:
            raise HTTPException(400, "Empty file")
        if not file.filename.lower().endswith(".csv"):
            raise HTTPException(400, "Expected a .csv file")
        filename = os.path.basename(file.filename)

    task_id = svc.start_optimize(
        csv_bytes=body,
        filename=filename,
        workers=workers or None,
        threshold=threshold,
        multi_facet=multi_facet,
        url_column=url_column,
        also_global=also_global,
        source=source,
        lookback_days=lookback_days,
        force_reprocess=force_reprocess,
    )
    return {"task_id": task_id, "status": "started"}


@router.get("/status/{task_id}")
def status(task_id: str):
    t = svc.get_status(task_id)
    if not t:
        raise HTTPException(404, "Task not found")
    return t


@router.post("/cancel/{task_id}")
def cancel(task_id: str):
    ok = svc.cancel(task_id)
    if not ok:
        raise HTTPException(400, "Task not cancellable")
    return {"cancelled": True}


@router.get("/download/{task_id}")
def download(task_id: str):
    path = svc.get_output_path(task_id)
    if not path:
        raise HTTPException(404, "No output for this task")
    if not os.path.exists(path):
        raise HTTPException(410, "Output missing on disk")
    return FileResponse(path, media_type="text/csv", filename=os.path.basename(path))


@router.get("/history")
def history():
    return svc.get_history()
