"""
HTTP surface for the R-URL Optimizer tool.
"""
from __future__ import annotations

import os
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import FileResponse, Response
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
    also_global: bool = Form(False),
    source: str = Form("upload"),
    lookback_days: int = Form(365),
    row_limit: Optional[int] = Form(None),
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
        url_column="r_url",
        also_global=also_global,
        source=source,
        lookback_days=lookback_days,
        row_limit=row_limit,
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


@router.get("/export-all")
def export_all():
    """Export every URL ever processed (deduped, from rurl_processed).

    One row per unique original_url, projected to the v2 user-facing
    schema. visits/revenue are intentionally omitted because they aren't
    cached per-URL (they're time-dependent, only meaningful per-run).
    """
    import io
    import pandas as pd
    from datetime import datetime
    from backend import rurl_optimizer_persistence as pers
    from backend.rurl_optimizer_v2_service import (
        _main_category_from_redirect,
        _deepest_category_from_redirect,
    )

    df = pers.load_all_processed()
    if df.empty:
        raise HTTPException(404, "No processed URLs available")

    out = pd.DataFrame()
    out["old url"] = df["original_url"]
    out["new url"] = df["redirect_url"]
    out["score"] = df["reliability_score"]
    out["main_category"] = df["redirect_url"].apply(_main_category_from_redirect)
    out["deepest_category"] = df["redirect_url"].apply(_deepest_category_from_redirect)
    out["reason"] = df["reason"]
    out["processed_at"] = df["processed_at"]

    out["__s"] = pd.to_numeric(out["score"], errors="coerce")
    out = out.sort_values("__s", ascending=False, na_position="last").drop(columns="__s")

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        out.to_excel(w, index=False, sheet_name="all_processed")
        # Center-align score column for visual consistency with per-run xlsx.
        ws = w.sheets["all_processed"]
        from openpyxl.styles import Alignment
        center = Alignment(horizontal="center", vertical="center")
        score_col = list(out.columns).index("score") + 1
        for row in range(1, ws.max_row + 1):
            ws.cell(row=row, column=score_col).alignment = center

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Response(
        content=buf.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="rurl_all_processed_{ts}.xlsx"'},
    )
