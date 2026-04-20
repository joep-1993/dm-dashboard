from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from typing import List
from pydantic import BaseModel
import asyncio
import logging
import pandas as pd
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

from backend.url_validator_service import validate_urls, get_cache_stats, clear_cache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/url-validator", tags=["url-validator"])
executor = ThreadPoolExecutor(max_workers=2)


class ValidateRequest(BaseModel):
    urls: List[str]


@router.get("/health")
def health():
    return {"status": "healthy", "service": "url_validator"}


@router.get("/cache-status")
def cache_status():
    return get_cache_stats()


@router.post("/cache-refresh")
def cache_refresh():
    return clear_cache()


@router.post("/validate")
async def validate(req: ValidateRequest):
    """Validate a list of URLs against the Taxonomy API."""
    if not req.urls:
        raise HTTPException(400, "No URLs provided")
    if len(req.urls) > 50000:
        raise HTTPException(400, "Maximum 50,000 URLs per request")

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, validate_urls, req.urls)
    return result


@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload Excel/CSV/TXT and extract URLs."""
    content = await file.read()
    filename = (file.filename or "").lower()

    urls = []
    try:
        if filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(BytesIO(content))
            # Find URL column (case-insensitive)
            url_col = None
            for col in df.columns:
                if col.strip().lower() == "url":
                    url_col = col
                    break
            if not url_col:
                url_col = df.columns[0]
            urls = [str(v).strip() for v in df[url_col].dropna() if str(v).strip()]

        elif filename.endswith(".csv"):
            df = pd.read_csv(BytesIO(content), sep=None, engine="python")
            url_col = None
            for col in df.columns:
                if col.strip().lower() == "url":
                    url_col = col
                    break
            if not url_col:
                url_col = df.columns[0]
            urls = [str(v).strip() for v in df[url_col].dropna() if str(v).strip()]

        else:
            # Plain text: one URL per line
            text = content.decode("utf-8", errors="replace")
            urls = [line.strip() for line in text.splitlines() if line.strip()]

    except Exception as e:
        raise HTTPException(400, f"Could not parse file: {e}")

    return {"urls": urls, "count": len(urls)}


@router.post("/download")
async def download_results(req: dict):
    """Download validation results as Excel."""
    results = req.get("results", [])
    if not results:
        raise HTTPException(400, "No results to download")

    rows = []
    for r in results:
        issue_msgs = "; ".join(
            f"[{i['severity'].upper()}] {i['message']}" for i in r.get("issues", [])
        )
        rows.append({
            "URL": r.get("url", ""),
            "Status": r.get("status", ""),
            "Maincat": r.get("maincat_name", ""),
            "Category": r.get("category_name", ""),
            "Issues": issue_msgs,
        })

    df = pd.DataFrame(rows)
    buf = BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=url_validation_results.xlsx"},
    )
