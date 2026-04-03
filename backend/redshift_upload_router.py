from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from typing import Optional
import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from backend.redshift_upload_service import (
    upload_to_redshift,
    parse_xlsx,
    parse_pasted_data,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/redshift-upload", tags=["redshift-upload"])
executor = ThreadPoolExecutor(max_workers=2)


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "redshift_upload"}


@router.post("/xlsx")
async def upload_xlsx(
    file: UploadFile = File(...),
    table_name: str = Form(...),
    chunk_size: int = Form(5000),
):
    """Upload an .xlsx file to Redshift pa.<table_name>."""
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only .xlsx/.xls files are supported.")
    try:
        contents = await file.read()
        df = parse_xlsx(contents)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            executor, upload_to_redshift, df, table_name, chunk_size
        )
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result["error"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"XLSX upload error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/paste")
async def upload_paste(
    table_name: str = Form(...),
    chunk_size: int = Form(5000),
    headers: str = Form(...),
    rows: str = Form(...),
):
    """Upload pasted data to Redshift pa.<table_name>.
    headers: JSON array of column names
    rows: JSON array of arrays (row data)
    """
    try:
        header_list = json.loads(headers)
        row_list = json.loads(rows)
        if not header_list or not row_list:
            raise HTTPException(status_code=400, detail="Headers and rows are required.")
        df = parse_pasted_data(header_list, row_list)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            executor, upload_to_redshift, df, table_name, chunk_size
        )
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result["error"])
        return result
    except HTTPException:
        raise
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON in headers or rows.")
    except Exception as e:
        logger.error(f"Paste upload error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
