from fastapi import APIRouter, Body, HTTPException, Query
from typing import List, Optional
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from backend.dma_bidding_service import (
    get_level_stats,
    run_dma_bidding,
    get_bid_strategies,
    get_campaigns_with_strategies,
    change_bid_strategy,
    _run_history,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dma-bidding", tags=["dma-bidding"])
executor = ThreadPoolExecutor(max_workers=2)


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "dma_bidding"}


@router.get("/stats")
async def get_stats(
    country: str = Query("NL", description="Country code: NL or BE"),
):
    """Get campaign counts per DMA bid strategy level."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, lambda: get_level_stats(country=country))
        return result
    except Exception as e:
        logger.error(f"Error fetching DMA bidding stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run")
async def run_dma_bidding_endpoint(
    dry_run: bool = Query(True, description="If true, no actual changes are made"),
    start_days_ago: int = Query(9, description="Start of date range (days ago)"),
    end_days_ago: int = Query(3, description="End of date range (days ago)"),
    exclude_campaigns: Optional[str] = Query(None, description="Comma-separated campaign name substrings to exclude"),
    include_campaigns: Optional[str] = Query(None, description="Comma-separated campaign name substrings to include (only these will be processed)"),
    country: str = Query("NL", description="Country code: NL or BE"),
):
    """Run the DMA bidding analysis and (optionally) apply bid strategy changes."""
    try:
        loop = asyncio.get_event_loop()
        exclude_list = (
            [s.strip() for s in exclude_campaigns.split(",") if s.strip()]
            if exclude_campaigns
            else None
        )
        include_list = (
            [s.strip() for s in include_campaigns.split(",") if s.strip()]
            if include_campaigns
            else None
        )
        result = await loop.run_in_executor(
            executor,
            lambda: run_dma_bidding(
                start_days_ago=start_days_ago,
                end_days_ago=end_days_ago,
                dry_run=dry_run,
                exclude_campaigns=exclude_list,
                include_campaigns=include_list,
                country=country,
            ),
        )
        return result
    except Exception as e:
        logger.error(f"Error running DMA bidding: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
async def get_history():
    """Return recent DMA bidding runs."""
    return {"runs": _run_history}


@router.post("/revert")
async def revert_campaigns(
    revert_list: List[dict] = Body(...),
    country: str = Query("NL", description="Country code: NL or BE"),
):
    """Revert campaigns to specified bid strategy levels.

    Expects a JSON list of: [{"campaign_name": "...", "target_level": 1}, ...]
    """
    try:
        loop = asyncio.get_event_loop()

        def do_revert():
            level_to_strategy, _ = get_bid_strategies(country=country)
            campaigns = get_campaigns_with_strategies(country=country)

            # Build lookup: campaign_name -> resource_name
            name_to_resource = {c["campaign_name"]: c["resource_name"] for c in campaigns}

            success = 0
            failed = 0
            errors = []

            for item in revert_list:
                name = item["campaign_name"]
                target_level = item["target_level"]
                resource_name = name_to_resource.get(name)
                strategy_resource = level_to_strategy.get(target_level)

                if not resource_name:
                    errors.append({"campaign": name, "error": "Campaign not found"})
                    failed += 1
                    continue
                if not strategy_resource:
                    errors.append({"campaign": name, "error": f"No strategy for level {target_level}"})
                    failed += 1
                    continue

                result = change_bid_strategy(resource_name, strategy_resource, dry_run=False, country=country)
                if result.get("status") == "success":
                    success += 1
                else:
                    errors.append({"campaign": name, "error": str(result)})
                    failed += 1

            return {"success": success, "failed": failed, "errors": errors}

        result = await loop.run_in_executor(executor, do_revert)
        return result
    except Exception as e:
        logger.error(f"Error reverting campaigns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/history")
async def clear_history():
    """Clear all DMA bidding run history."""
    count = len(_run_history)
    _run_history.clear()
    return {"cleared": count}


@router.get("/history/{run_id}")
async def get_history_detail(run_id: int):
    """Return details for a specific DMA bidding run."""
    for run in _run_history:
        if run.get("run_id") == run_id:
            return run
    raise HTTPException(status_code=404, detail=f"Run #{run_id} not found")
