"""
HTTP surface for the R-URL Optimizer tool — v2.

Mirrors rurl_optimizer_router but mounted at /api/rurl-v2 and backed by
rurl_optimizer_v2_service. Persistence (rurl_processed table) is shared with v1.
"""
from __future__ import annotations

import os
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import FileResponse, Response
from typing import Optional

from backend import rurl_optimizer_v2_service as svc

router = APIRouter(prefix="/api/rurl-v2", tags=["rurl-v2"])


@router.get("/health")
def health():
    return {"status": "healthy", "service": "rurl_optimizer_v2"}


@router.post("/optimize")
async def optimize(
    file: Optional[UploadFile] = File(None),
    workers: int = Form(0),
    threshold: int = Form(80),
    multi_facet: bool = Form(True),
    also_global: bool = Form(False),
    source: str = Form("upload"),
    lookback_days: int = Form(365),
    row_limit: Optional[int] = Form(None),
    force_reprocess: bool = Form(False),
    exclude_shopnames: bool = Form(False),
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
        url_column="r_url",
        also_global=also_global,
        source=source,
        lookback_days=lookback_days,
        row_limit=row_limit,
        force_reprocess=force_reprocess,
        exclude_shopnames=exclude_shopnames,
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
    blob = svc.get_output_bytes(task_id)
    if not blob:
        raise HTTPException(404, "No output for this task")
    filename, mime, content = blob
    return Response(
        content=content,
        media_type=mime,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/history")
def history():
    return svc.get_history()


@router.delete("/history/{task_id}")
def delete_history(task_id: str):
    if not svc.delete_history_entry(task_id):
        raise HTTPException(404, "Run not found")
    return {"deleted": True}
