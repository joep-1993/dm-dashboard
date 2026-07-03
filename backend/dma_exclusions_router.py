"""DMA Exclusions API router (/api/dma-exclusions)."""
import asyncio
import io
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from backend.dma_exclusions_service import (
    lookup as svc_lookup,
    preview as svc_preview,
    apply as svc_apply,
    enable as svc_enable,
    list_exclusions as svc_list,
    backfill_headline_shops as svc_backfill_headline_shops,
    cleanup_enabled as svc_cleanup_enabled,
    exclusion_targets as svc_exclusion_targets,
    oos_scan as svc_oos_scan,
    oos_exclude as svc_oos_exclude,
    oos_recovered as svc_oos_recovered,
    oos_reenable as svc_oos_reenable,
)
from pydantic import BaseModel


class OosExcludeBody(BaseModel):
    market: str = "NL"
    item_ids: list[str]

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dma-exclusions", tags=["dma-exclusions"])
executor = ThreadPoolExecutor(max_workers=2)


async def _run(fn, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, lambda: fn(*args))


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "dma_exclusions"}


@router.get("/lookup")
async def lookup_endpoint(
    item_id: str = Query(..., description="Product / item id"),
    market: str = Query("NL", description="Market: NL or BE"),
):
    """Resolve the bid category + serving campaigns for an item id (read-only)."""
    try:
        return await _run(svc_lookup, item_id, market)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("lookup failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/preview")
async def preview_endpoint(
    item_id: str = Query(..., description="Product / item id"),
    market: str = Query("NL", description="Market: NL or BE"),
    shop: Optional[str] = Query(None, description="Headline-offer shop (optional)"),
    campaign_filter: Optional[str] = Query(None, description="Restrict to campaigns containing this text"),
):
    """Dry-run: show every campaign/ad-group/tree change apply() would make."""
    try:
        return await _run(svc_preview, item_id, market, shop, campaign_filter)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("preview failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/apply")
async def apply_endpoint(
    item_id: str = Query(..., description="Product / item id"),
    market: str = Query("NL", description="Market: NL or BE"),
    shop: Optional[str] = Query(None, description="Headline-offer shop (optional)"),
    campaign_filter: Optional[str] = Query(None, description="Restrict to campaigns containing this text"),
):
    """Apply the exclusion live and persist it for later re-enable."""
    try:
        return await _run(svc_apply, item_id, market, shop, campaign_filter)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("apply failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/list")
async def list_endpoint():
    """List saved exclusions with their status."""
    try:
        return {"exclusions": await _run(svc_list)}
    except Exception as e:
        logger.exception("list failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/backfill-headline-shops")
async def backfill_headline_shops_endpoint(
    only_missing: bool = Query(True, description="Only fill rows that have no headline offer yet"),
):
    """Populate the headline-offer shop for existing exclusions from the live ES
    index (rows created before the column existed). Returns counts."""
    try:
        return await _run(svc_backfill_headline_shops, only_missing)
    except Exception as e:
        logger.exception("backfill headline shops failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cleanup-enabled")
async def cleanup_enabled_endpoint(market: str = Query("NL", description="Market: NL or BE")):
    """Delete resolved (status='enabled') records for a market — history cleanup only."""
    try:
        return await _run(svc_cleanup_enabled, market)
    except Exception as e:
        logger.exception("cleanup enabled failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/exclusion/{record_id}/targets")
async def exclusion_targets_endpoint(record_id: int):
    """The campaigns/ad-groups a saved exclusion was added to as a negative."""
    try:
        return await _run(svc_exclusion_targets, record_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("exclusion targets failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/oos/scan")
async def oos_scan_endpoint(
    market: str = Query("NL", description="Market: NL or BE"),
    limit: Optional[int] = Query(None, ge=1, description="Cap the number of OOS EANs pulled from the monitor"),
):
    """List OOS products that are live in DMA, with 30d spend/clicks/conversions."""
    try:
        return await _run(svc_oos_scan, market, limit)
    except Exception as e:
        logger.exception("oos scan failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/oos/exclude")
async def oos_exclude_endpoint(body: OosExcludeBody):
    """Exclude a selected set of OOS item ids (tagged source=oos)."""
    if not body.item_ids:
        raise HTTPException(status_code=400, detail="item_ids is empty")
    try:
        return await _run(svc_oos_exclude, body.item_ids, body.market)
    except Exception as e:
        logger.exception("oos exclude failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/oos/recovered")
async def oos_recovered_endpoint(market: str = Query("NL", description="Market: NL or BE")):
    """OOS exclusions whose product has recovered (re-enable candidates)."""
    try:
        return {"recovered": await _run(svc_oos_recovered, market)}
    except Exception as e:
        logger.exception("oos recovered failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/oos/reenable")
async def oos_reenable_endpoint(market: str = Query("NL", description="Market: NL or BE")):
    """Re-enable every recovered OOS exclusion for a market."""
    try:
        return await _run(svc_oos_reenable, market)
    except Exception as e:
        logger.exception("oos reenable failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export/xlsx")
async def export_xlsx():
    """Download all saved exclusions as an Excel file."""
    try:
        rows = await _run(svc_list)
        cols = [
            ("item_id", "Item ID"), ("market", "Market"), ("category", "Category"),
            ("cl0", "Cat id (CL0)"), ("headline_shop", "Headline offer"),
            ("campaign_filter", "Campaign filter"),
            ("status", "Status"), ("target_count", "Targets"),
            ("created_at", "Created"), ("applied_at", "Applied"), ("enabled_at", "Enabled"),
        ]
        df = pd.DataFrame([{label: r.get(key) for key, label in cols} for r in rows],
                          columns=[label for _, label in cols])
        # Items excluded only via bestsellers/APlus have no resolvable category.
        df["Category"] = df["Category"].fillna("").replace("", "n/a")
        plp_urls = [r.get("plp_url") for r in rows]
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="exclusions")
            from openpyxl.styles import Alignment, Font
            ws = w.sheets["exclusions"]
            center = Alignment(horizontal="center", vertical="center")
            left = Alignment(horizontal="left", vertical="center")
            labels = [label for _, label in cols]
            left_cols = {labels.index("Category") + 1, labels.index("Shop") + 1}
            itemid_col = labels.index("Item ID") + 1
            link_font = Font(color="0563C1", underline="single")
            for row in ws.iter_rows():
                for cell in row:
                    cell.alignment = left if cell.column in left_cols else center
            # Turn Item ID cells into hyperlinks to the product PLP url.
            for i, url in enumerate(plp_urls):
                if not url:
                    continue
                cell = ws.cell(row=i + 2, column=itemid_col)  # +2: header + 1-indexed
                cell.hyperlink = url
                cell.font = link_font
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return Response(
            content=buf.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="dma_exclusions_{ts}.xlsx"'},
        )
    except Exception as e:
        logger.exception("export failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/enable/{record_id}")
async def enable_endpoint(record_id: int):
    """Re-enable (remove the negative + prune) a saved exclusion."""
    try:
        return await _run(svc_enable, record_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("enable failed")
        raise HTTPException(status_code=500, detail=str(e))
