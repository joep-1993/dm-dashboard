"""
Healthscore 2.0 — FastAPI router (Phase 6 frontend backend).

Read endpoints are Postgres-only and return instantly (coverage history,
sitemap composition, feature snapshot, shadow runs). The heavy pipeline steps
(features / sitemap / coverage / shadow) all touch Redshift and take minutes, so
they run as in-process background jobs and the UI polls /jobs/{id}.

Shadow-only: nothing here repoints the live HTML-sitemap renderer. It selects
into pa.hs2_sitemap and measures the projected win against the current live set.
"""
from __future__ import annotations

import uuid
import logging
import threading
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel

from backend import healthscore_runs as hr
from backend import healthscore_service as hs

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/healthscore", tags=["healthscore"])

# One worker: the pipeline steps are Redshift-bound and must not run concurrently
# (features feeds sitemap; shadow is a big scan). Serialize them.
_executor = ThreadPoolExecutor(max_workers=1)
_JOBS: dict[str, dict] = {}
_JOBS_LOCK = threading.Lock()
_MAX_JOBS = 50


def _month_end(month: str) -> date:
    from datetime import timedelta
    y, m = (int(x) for x in month.split("-"))
    first_next = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    return first_next - timedelta(days=1)


def _new_job(action: str, params: dict) -> str:
    job_id = uuid.uuid4().hex[:12]
    with _JOBS_LOCK:
        _JOBS[job_id] = {
            "id": job_id, "action": action, "params": params,
            "status": "queued", "created_at": datetime.now().isoformat(timespec="seconds"),
            "started_at": None, "finished_at": None, "result": None, "error": None,
        }
        # Trim oldest finished jobs so the dict can't grow without bound.
        if len(_JOBS) > _MAX_JOBS:
            done = sorted((j for j in _JOBS.values() if j["status"] in ("done", "error")),
                          key=lambda j: j["created_at"])
            for j in done[: len(_JOBS) - _MAX_JOBS]:
                _JOBS.pop(j["id"], None)
    return job_id


def _run_job(job_id: str, fn, *args) -> None:
    with _JOBS_LOCK:
        j = _JOBS.get(job_id)
        if j:
            j["status"] = "running"
            j["started_at"] = datetime.now().isoformat(timespec="seconds")
    try:
        result = fn(*args)
        with _JOBS_LOCK:
            j = _JOBS.get(job_id)
            if j:
                j["status"] = "done"
                j["result"] = result
                j["finished_at"] = datetime.now().isoformat(timespec="seconds")
    except Exception as e:  # noqa: BLE001 — surface any failure to the UI
        logger.exception("healthscore job %s (%s) failed", job_id, args)
        with _JOBS_LOCK:
            j = _JOBS.get(job_id)
            if j:
                j["status"] = "error"
                j["error"] = str(e)
                j["finished_at"] = datetime.now().isoformat(timespec="seconds")


# --------------------------------------------------------------------------- #
# Read endpoints (instant, Postgres-only)
# --------------------------------------------------------------------------- #
@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "healthscore"}


@router.get("/coverage-history")
def coverage_history():
    """Persisted Phase-1 coverage runs (per month, per type_url + __ALL__)."""
    try:
        return {"rows": hs.get_coverage_history()}
    except Exception as e:
        logger.error("coverage-history failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sitemap-summary")
def sitemap_summary():
    """Composition of the latest pa.hs2_sitemap snapshot."""
    try:
        return hs.get_sitemap_summary()
    except Exception as e:
        logger.error("sitemap-summary failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/features-summary")
def features_summary():
    """Row/coverage summary of the latest pa.hs2_features snapshot."""
    try:
        return hs.get_features_summary()
    except Exception as e:
        logger.error("features-summary failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shadow")
def shadow_history():
    """All persisted shadow comparison runs (projected HS2.0 vs live)."""
    try:
        return {"rows": hs.get_shadow_history()}
    except Exception as e:
        logger.error("shadow history failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# --------------------------------------------------------------------------- #
# Run triggers (background jobs)
# --------------------------------------------------------------------------- #
class RunIn(BaseModel):
    action: str                      # coverage | features | sitemap | shadow
    month: Optional[str] = None      # 'YYYY-MM' (default: previous complete month)
    cap_n: int = hs.CAP_N_DEFAULT    # sitemap/shadow per-category cap


def _coverage_and_write(month: str) -> dict:
    """Compute Phase-1 coverage and persist it so the dashboard history refreshes."""
    result = hs.compute_coverage(month)
    hs.write_coverage(result)
    return result


def _prev_month() -> str:
    t = date.today()
    y, m = (t.year, t.month - 1) if t.month > 1 else (t.year - 1, 12)
    return f"{y:04d}-{m:02d}"


@router.post("/run")
def run(body: RunIn):
    """Kick off a pipeline step in the background. Returns a job id to poll."""
    action = body.action
    month = body.month or _prev_month()
    if action not in ("coverage", "features", "sitemap", "shadow"):
        raise HTTPException(status_code=400, detail=f"unknown action '{action}'")

    # Guard: only one pipeline job in flight at a time (single worker anyway, but
    # give the UI a clear signal rather than silently queueing).
    with _JOBS_LOCK:
        active = [j for j in _JOBS.values() if j["status"] in ("queued", "running")]
    if active:
        raise HTTPException(status_code=409,
                            detail=f"a {active[0]['action']} job is already running")

    params = {"month": month, "cap_n": body.cap_n}
    job_id = _new_job(action, params)

    if action == "coverage":
        _executor.submit(_run_job, job_id, _coverage_and_write, month)
    elif action == "features":
        _executor.submit(_run_job, job_id, hs.build_features, _month_end(month))
    elif action == "sitemap":
        _executor.submit(_run_job, job_id, hs.build_sitemaps, _month_end(month), body.cap_n)
    else:  # shadow
        _executor.submit(_run_job, job_id, hs.compute_shadow, month, body.cap_n)

    return {"job_id": job_id, "action": action, "params": params}


# --------------------------------------------------------------------------- #
# Category runs (preview -> push) + run history
#
# Two endpoints, two clicks, on purpose: a preview sends nothing, a push replays
# what that preview stored. See healthscore_runs for why the push does not
# rebuild. Both are background jobs on the same single worker as the pipeline
# steps — get_live() alone moves ~17k records for a maincat.
# --------------------------------------------------------------------------- #
class CategoryIn(BaseModel):
    id: int
    name: str
    scope: str = "deepest"           # deepest | maincat


class PreviewIn(BaseModel):
    categories: list[CategoryIn]
    country: str = "nl"
    include_plp: bool = False
    preserve_cross_category: bool = True
    label: Optional[str] = None


class PushIn(BaseModel):
    run_id: int
    confirm: str                     # must be the literal "REPLACE"
    category_ids: Optional[list[int]] = None


def _guard_idle() -> None:
    """One heavy job at a time; tell the UI instead of silently queueing."""
    with _JOBS_LOCK:
        active = [j for j in _JOBS.values() if j["status"] in ("queued", "running")]
    if active:
        raise HTTPException(status_code=409,
                            detail=f"a {active[0]['action']} job is already running")


@router.get("/categories")
def categories(scope: str = Query("deepest", pattern="^(deepest|maincat)$")):
    """Picker options. `as_of` is the selection build every run gets pinned to."""
    try:
        return {"scope": scope, "as_of": str(hr.latest_as_of() or ""),
                "rows": hr.list_categories(scope)}
    except Exception as e:
        logger.error("categories failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/test-bucket")
def test_bucket():
    """The 12 buckets HS2.0 is live on (10 deepest categories + 2 maincats)."""
    return {"rows": hr.test_bucket(), "as_of": str(hr.latest_as_of() or "")}


@router.post("/categories/preview")
def categories_preview(body: PreviewIn):
    _guard_idle()
    cats = [c.model_dump() for c in body.categories]
    if not cats:
        raise HTTPException(status_code=400, detail="pick at least one category")
    params = {"categories": len(cats), "country": body.country,
              "label": body.label or (cats[0]["name"] if len(cats) == 1 else f"{len(cats)} cats")}
    job_id = _new_job("preview", params)
    _executor.submit(_run_job, job_id, hr.preview, cats, None, body.country,
                     body.include_plp, body.preserve_cross_category, body.label)
    return {"job_id": job_id, "action": "preview", "params": params}


@router.post("/categories/push")
def categories_push(body: PushIn):
    """Live write: replaces the sitemap set of every category in the run."""
    _guard_idle()
    if body.confirm != "REPLACE":
        raise HTTPException(status_code=400, detail="type REPLACE to confirm the push")
    params = {"run_id": body.run_id,
              "categories": len(body.category_ids or []) or None}
    job_id = _new_job("push", params)
    _executor.submit(_run_job, job_id, hr.push_run, body.run_id, body.confirm,
                     body.category_ids)
    return {"job_id": job_id, "action": "push", "params": params}


@router.get("/runs")
def runs(limit: int = Query(200, ge=1, le=1000)):
    try:
        return {"rows": hr.list_runs(limit)}
    except Exception as e:
        logger.error("runs failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/runs/export.csv")
def runs_export():
    try:
        return Response(content=hr.runs_csv(), media_type="text/csv; charset=utf-8",
                        headers={"Content-Disposition":
                                 'attachment; filename="healthscore_runs.csv"'})
    except Exception as e:
        logger.error("runs export failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/runs/{run_id}")
def run_detail(run_id: int, include_payload: bool = False):
    """Full run. The payloads are stripped unless asked for: a maincat payload is
    ~17k records and the UI only renders the stats, the diff and the coverage."""
    try:
        row = hr.get_run(run_id)
    except Exception as e:
        logger.error("run detail failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    if not row:
        raise HTTPException(status_code=404, detail="run not found")
    if not include_payload:
        for c in ((row.get("detail") or {}).get("categories") or []):
            c.pop("payload", None)
    return row


@router.get("/jobs/{job_id}")
def job_status(job_id: str):
    with _JOBS_LOCK:
        j = _JOBS.get(job_id)
        if not j:
            raise HTTPException(status_code=404, detail="job not found")
        return dict(j)


@router.get("/jobs")
def jobs():
    with _JOBS_LOCK:
        return {"jobs": sorted(_JOBS.values(), key=lambda j: j["created_at"], reverse=True)}
