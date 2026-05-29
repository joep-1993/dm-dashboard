"""HTTP routes for the Redirect Tool."""

from __future__ import annotations

import csv
import io
import logging
from typing import Any

import pandas as pd
from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from backend import redirect_tool_service as svc

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/redirect-tool", tags=["redirect-tool"])

EXPECTED_COLUMNS = ["old", "new", "statuscode", "country", "label"]


# ---------------------------------------------------------------------------
# Parse — accepts file (csv/xlsx) or pasted text
# ---------------------------------------------------------------------------

def _df_to_rows(df: pd.DataFrame) -> list[dict]:
    df.columns = [str(c).strip().lower() for c in df.columns]
    # Fill missing expected columns with empty strings
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    rows: list[dict] = []
    for _, r in df.iterrows():
        row = {col: ("" if pd.isna(r[col]) else str(r[col]).strip()) for col in EXPECTED_COLUMNS}
        if row["old"] or row["new"]:
            rows.append(row)
    return rows


def _parse_text(text: str) -> list[dict]:
    """Parse pasted text — auto-detects CSV or TSV with a header row, falls back
    to two-column `old<sep>new` lines without a header."""
    text = text.strip()
    if not text:
        return []

    first_line = text.splitlines()[0]
    sep = "\t" if "\t" in first_line else ","
    # Header detection must match WHOLE TOKENS, not substrings — otherwise URLs
    # like /products/autos/... fire on the "to" in "auto" and the URL is parsed
    # as a column name (giving zero data rows).
    HEADER_TOKENS = {"old", "new", "from", "to", "fromurl", "tourl",
                     "statuscode", "country", "label"}
    tokens = [p.strip().lower() for p in first_line.split(sep)]
    has_header = any(t in HEADER_TOKENS for t in tokens)

    if has_header:
        reader = csv.DictReader(io.StringIO(text), delimiter=sep)
        df = pd.DataFrame(list(reader))
        # Map common aliases
        df.columns = [str(c).strip().lower() for c in df.columns]
        rename = {"from": "old", "fromurl": "old", "to": "new", "tourl": "new"}
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        return _df_to_rows(df)

    rows: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(sep)]
        row = {col: "" for col in EXPECTED_COLUMNS}
        if len(parts) >= 1:
            row["old"] = parts[0]
        if len(parts) >= 2:
            row["new"] = parts[1]
        if len(parts) >= 3:
            row["statuscode"] = parts[2]
        if len(parts) >= 4:
            row["country"] = parts[3]
        if len(parts) >= 5:
            row["label"] = parts[4]
        if row["old"] or row["new"]:
            rows.append(row)
    return rows


def _read_csv_any_encoding(content: bytes, **kwargs) -> tuple[pd.DataFrame, str]:
    """pd.read_csv with encoding fallback. Returns (df, encoding_used).

    Windows exports are commonly cp1252. utf-8 is tried first so well-formed
    files take the fast path.
    """
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            df = pd.read_csv(io.BytesIO(content), encoding=encoding, **kwargs)
            return df, encoding
        except UnicodeDecodeError:
            continue
    df = pd.read_csv(io.BytesIO(content), encoding="utf-8", encoding_errors="replace", **kwargs)
    return df, "utf-8-replace"


def _encoding_warning(encoding: str, df: pd.DataFrame) -> str | None:
    """Build a human warning if the file wasn't clean utf-8."""
    if encoding == "utf-8-sig":
        return None
    has_replacement = any(
        "�" in str(v)
        for v in df.astype(str).values.ravel()
    )
    if encoding == "utf-8-replace" or has_replacement:
        return (
            "File contained bytes that could not be decoded — some characters "
            "were replaced with \"?\". Check non-ASCII URLs carefully before uploading."
        )
    return (
        f"File was not utf-8 — decoded as {encoding} (typical for Windows-exported CSVs). "
        "Double-check any rows with non-ASCII characters (é, ë, etc.) in the preview."
    )


@router.post("/parse-file")
async def parse_file(file: UploadFile = File(...)) -> dict:
    content = await file.read()
    filename = (file.filename or "").lower()
    warning: str | None = None
    try:
        if filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(content))
        elif filename.endswith(".csv"):
            df, enc = _read_csv_any_encoding(content, sep=None, engine="python", dtype=str, keep_default_na=False)
            warning = _encoding_warning(enc, df)
        elif filename.endswith(".tsv"):
            df, enc = _read_csv_any_encoding(content, sep="\t", dtype=str, keep_default_na=False)
            warning = _encoding_warning(enc, df)
        else:
            text = content.decode("utf-8", errors="replace")
            rows = _parse_text(text)
            return {"rows": rows, "count": len(rows)}
    except Exception as exc:
        raise HTTPException(400, f"Could not parse file: {exc}") from exc
    rows = _df_to_rows(df)
    payload: dict[str, Any] = {"rows": rows, "count": len(rows)}
    if warning:
        payload["warning"] = warning
    return payload


class ParseTextRequest(BaseModel):
    text: str


@router.post("/parse-text")
def parse_text(req: ParseTextRequest) -> dict:
    rows = _parse_text(req.text)
    return {"rows": rows, "count": len(rows)}


# ---------------------------------------------------------------------------
# Preview (preflight)
# ---------------------------------------------------------------------------

class PreviewRequest(BaseModel):
    rows: list[dict]


@router.post("/preview")
def preview(req: PreviewRequest) -> dict:
    """Start preflight in the background; the client polls
    /preview-status/{task_id} to drive the Upload progress bar and pick
    up the final preflight result on completion."""
    if not req.rows:
        raise HTTPException(400, "No rows provided")
    task_id = svc.start_preflight(req.rows)
    return {"task_id": task_id, "status": "started"}


@router.get("/preview-status/{task_id}")
def preview_status(task_id: str) -> dict:
    task = svc.get_preflight_status(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task


# ---------------------------------------------------------------------------
# Submit
# ---------------------------------------------------------------------------

class SubmitRequest(BaseModel):
    processed: list[dict]
    label: str = ""
    input_method: str = "file"
    replace_existing: bool = False


@router.post("/submit")
def submit(req: SubmitRequest) -> dict:
    """Kick off the submission in the background; the client polls
    /submit-status/{task_id} to drive a progress bar and pick up the final
    run_id + counts on completion."""
    if not req.processed:
        raise HTTPException(400, "Nothing to submit")
    task_id = svc.start_submit(
        req.processed, req.label, req.input_method,
        replace_existing=req.replace_existing,
    )
    return {"task_id": task_id, "status": "started"}


@router.get("/submit-status/{task_id}")
def submit_status(task_id: str) -> dict:
    task = svc.get_submit_status(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task


# ---------------------------------------------------------------------------
# Individual URL check
# ---------------------------------------------------------------------------

@router.get("/check-url")
def check_url(url: str, country: str = "nl") -> dict:
    if not url.strip():
        raise HTTPException(400, "url is required")
    path = svc.strip_domain(url)
    outgoing = svc.check_url_is_fromUrl(path, country)
    incoming = svc.check_url_incoming(path)
    return {
        "input": url,
        "normalized_path": svc.normalize_path(path),
        "variants_checked": svc.url_variants(path),
        "is_homepage": svc.is_homepage(path),
        "outgoing": outgoing,
        "incoming": incoming,
        "incoming_count": len(incoming),
    }


# ---------------------------------------------------------------------------
# Runs (recent results)
# ---------------------------------------------------------------------------

@router.get("/runs")
def runs() -> dict:
    return {"runs": svc.list_runs()}


@router.get("/runs/{run_id}")
def run_detail(run_id: int) -> dict:
    run = svc.get_run(run_id)
    if not run:
        raise HTTPException(404, "Run not found")
    return run


@router.delete("/runs/{run_id}")
def remove_run(run_id: int) -> dict:
    ok = svc.delete_run(run_id)
    if not ok:
        raise HTTPException(404, "Run not found")
    return {"deleted": run_id}


@router.get("/runs/{run_id}/export")
def export_run(run_id: int) -> StreamingResponse:
    run = svc.get_run(run_id)
    if not run:
        raise HTTPException(404, "Run not found")
    results = run["results"] or []
    df_rows = []
    for r in results:
        api_resp = r.get("api_response") or {}
        if isinstance(api_resp, dict):
            msg = api_resp.get("message", "") or api_resp.get("error", "")
        else:
            msg = str(api_resp)
        df_rows.append({
            "status": r.get("status", ""),
            "old": r.get("input_old", ""),
            "new_original": r.get("input_new", ""),
            "new_submitted": r.get("final_new", ""),
            "flattened_from": r.get("flatten_from") or "",
            "country": r.get("country", ""),
            "statusCode": r.get("statusCode", ""),
            "label": r.get("label", ""),
            "skip_reason": r.get("skip_reason", "") or "",
            "api_message": msg,
        })
    df = pd.DataFrame(df_rows)
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    fname = f"redirect_tool_run_{run_id}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )
