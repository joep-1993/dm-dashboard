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
from backend import rurl_results

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
    tier_a_limit: Optional[int] = Form(None),
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
        tier_a_limit=tier_a_limit,
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


@router.get("/results/{task_id}")
def results(task_id: str, min_score: int = 90, limit: int = 5000):
    """Rows of a finished run, for the "doorvoeren" selection screen.

    Reads the run's stored xlsx back (same bytes /download serves), so a run
    from any point in the history can be pushed, not just the one on screen.
    """
    try:
        data = rurl_results.run_results(task_id, min_score=min_score, limit=limit)
    except ValueError as e:
        raise HTTPException(422, str(e))
    if data is None:
        raise HTTPException(404, "No output for this task")
    return data


@router.get("/export")
def export_runs(task_ids: str):
    """Combined export of the runs selected in Recent runs.

    `task_ids` is a comma-separated list. The stored-output table is shared
    between the engine versions, so this serves a mixed v1/v2 selection.
    """
    ids = [t.strip() for t in task_ids.split(",") if t.strip()]
    if not ids:
        raise HTTPException(400, "No task_ids given")
    blob = rurl_results.export_runs(ids)
    if not blob:
        raise HTTPException(404, "None of the selected runs has stored output")
    filename, mime, content = blob
    return Response(
        content=content,
        media_type=mime,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@router.get("/coverage")
def coverage():
    """Per-tier: how much of what the optimizer proposed is actually live.

    Answers the two standing questions in one table — how many Tier A/B
    redirects are still waiting to be pushed, and how many need regenerating
    because the optimizer never found a target, the push failed, or the
    suggestion changed after it went live.
    """
    from backend import rurl_push_tracking
    return rurl_push_tracking.coverage()


@router.get("/coverage/rows")
def coverage_rows(tiers: str = "A,B", states: str = "never,stale,failed",
                  limit: int = 5000):
    """The individual URLs behind the coverage buckets."""
    from backend import rurl_push_tracking
    rows = rurl_push_tracking.outstanding(
        tiers=[t for t in tiers.split(",") if t.strip()],
        states=[s for s in states.split(",") if s.strip()],
        limit=limit,
    )
    return {"tiers": tiers, "states": states, "returned": len(rows), "rows": rows}


@router.get("/coverage/export")
def coverage_export(tiers: str = "A,B", states: str = "never,stale,failed",
                    limit: int = 50000):
    """The same rows as an xlsx, in the Push-screen column order.

    `old url` / `new url` first, so the file can be pasted straight into the
    Redirect Tool's text input without reshaping it.
    """
    import io
    import pandas as pd
    from datetime import datetime
    from backend import rurl_push_tracking

    rows = rurl_push_tracking.outstanding(
        tiers=[t for t in tiers.split(",") if t.strip()],
        states=[s for s in states.split(",") if s.strip()],
        limit=limit,
    )
    if not rows:
        raise HTTPException(404, "No rows match this selection")

    df = pd.DataFrame(rows)
    out = pd.DataFrame({
        "old url": df["original_url"],
        "new url": df["redirect_url"],
        "score": df["reliability_score"],
        "tier": df["reliability_tier"],
        "state": df["state"],
        "reason": df.get("reason"),
        "push_status": df.get("push_status"),
        "push_detail": df.get("push_detail"),
        "pushed_target": df.get("pushed_target"),
        "pushed_at": df.get("pushed_at"),
        "processed_at": df.get("processed_at"),
    })
    # ISO strings with a +00:00 offset in, Amsterdam wall-clock out. openpyxl
    # rejects a tz-aware datetime outright, and a raw offset string is not
    # something you can sort or filter on in Excel.
    for col in ("pushed_at", "processed_at"):
        s = pd.to_datetime(out[col], errors="coerce", utc=True)
        out[col] = s.dt.tz_convert("Europe/Amsterdam").dt.tz_localize(None)
    buf = io.BytesIO()
    out.to_excel(buf, index=False)
    name = f"rurl_outstanding_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    return Response(
        content=buf.getvalue(),
        media_type=("application/vnd.openxmlformats-officedocument"
                    ".spreadsheetml.sheet"),
        headers={"Content-Disposition": f'attachment; filename="{name}"'},
    )


@router.post("/refresh-facets")
def refresh_facets():
    """Kick off a background rebuild of facets.csv from the Search API.

    No-op (already_running=True) if a refresh is already in progress. Poll
    /refresh-facets/status for completion.
    """
    return svc.start_facets_refresh()


@router.get("/refresh-facets/status")
def refresh_facets_status():
    return svc.get_facets_status()


@router.get("/history")
def history():
    return svc.get_history()


@router.delete("/history/{task_id}")
def delete_history(task_id: str):
    if not svc.delete_history_entry(task_id):
        raise HTTPException(404, "Run not found")
    return {"deleted": True}
