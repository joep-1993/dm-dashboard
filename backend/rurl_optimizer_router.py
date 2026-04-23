"""
HTTP surface for the R-URL Optimizer tool.
"""
from __future__ import annotations

import os
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import FileResponse

from backend import rurl_optimizer_service as svc

router = APIRouter(prefix="/api/rurl", tags=["rurl"])


@router.get("/health")
def health():
    return {"status": "healthy", "service": "rurl_optimizer"}


@router.post("/optimize")
async def optimize(
    file: UploadFile = File(...),
    workers: int = Form(0),
    threshold: int = Form(80),
    multi_facet: bool = Form(True),
    url_column: str = Form("r_url"),
    also_global: bool = Form(False),
):
    body = await file.read()
    if not body:
        raise HTTPException(400, "Empty file")
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "Expected a .csv file")

    task_id = svc.start_optimize(
        csv_bytes=body,
        filename=os.path.basename(file.filename),
        workers=workers or None,
        threshold=threshold,
        multi_facet=multi_facet,
        url_column=url_column,
        also_global=also_global,
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
