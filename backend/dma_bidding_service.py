"""
DMA Bidding Service

Manages DMA (Direct Marketing Advertising) bid strategy level changes for campaigns.
Analyzes campaign performance metrics (marge, OPB, ROAS) and moves campaigns between
bid strategy levels (L1/L2/L3) based on configurable thresholds.
"""
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CUSTOMER_ID = "3800751597"
MCC_CUSTOMER_ID = "3011145605"

BID_STRATEGIES = {
    1: "DMA: Level 1 - 0,07",
    2: "DMA: Level 2 - 0,11",
    3: "DMA: Level 3 - 0,15",
}

DMA_CLA_CONVERSION_ACTION = "Omzet DMA en CLA"

# ---------------------------------------------------------------------------
# In-memory run history (capped at 50)
# ---------------------------------------------------------------------------

_run_history: List[Dict[str, Any]] = []

# ---------------------------------------------------------------------------
# Client helper
# ---------------------------------------------------------------------------


def _get_client() -> GoogleAdsClient:
    """Initialize Google Ads client from environment variables."""
    config = {
        "developer_token": os.environ.get("GOOGLE_DEVELOPER_TOKEN", ""),
        "refresh_token": os.environ.get("GOOGLE_REFRESH_TOKEN", ""),
        "client_id": os.environ.get("GOOGLE_CLIENT_ID", ""),
        "client_secret": os.environ.get("GOOGLE_CLIENT_SECRET", ""),
        "login_customer_id": os.environ.get("GOOGLE_LOGIN_CUSTOMER_ID", MCC_CUSTOMER_ID),
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config)


# ---------------------------------------------------------------------------
# Google Ads query helpers
# ---------------------------------------------------------------------------


def get_bid_strategies() -> Tuple[Dict[int, str], Dict[str, int]]:
    """
    Query MCC for DMA Level 1/2/3 bid strategy resource names.
    Returns:
        (level_to_strategy_id, strategy_id_to_level) dicts.
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT bidding_strategy.name, bidding_strategy.resource_name
        FROM bidding_strategy
    """

    response = ga_service.search(customer_id=CUSTOMER_ID, query=query)

    level_to_strategy_id: Dict[int, str] = {}
    strategy_id_to_level: Dict[str, int] = {}

    for row in response:
        name = row.bidding_strategy.name
        resource_name = row.bidding_strategy.resource_name
        # Extract strategy ID from resource_name: customers/{cid}/biddingStrategies/{sid}
        strategy_id = resource_name.split("/")[-1]

        for level, level_name in BID_STRATEGIES.items():
            if name == level_name:
                level_to_strategy_id[level] = strategy_id
                strategy_id_to_level[strategy_id] = level
                break

    logger.info(f"Found bid strategies: {level_to_strategy_id}")
    return level_to_strategy_id, strategy_id_to_level


def get_campaigns_with_strategies() -> List[Dict[str, Any]]:
    """
    Get all ENABLED campaigns with their bid strategy.
    Returns list of dicts with campaign_name, resource_name, bidding_strategy.
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT campaign.name, campaign.resource_name, campaign.bidding_strategy
        FROM campaign
        WHERE campaign.status = 'ENABLED'
    """

    response = ga_service.search(customer_id=CUSTOMER_ID, query=query)

    campaigns = []
    for row in response:
        campaigns.append({
            "campaign_name": row.campaign.name,
            "resource_name": row.campaign.resource_name,
            "bidding_strategy": row.campaign.bidding_strategy,
        })

    logger.info(f"Found {len(campaigns)} enabled campaigns")
    return campaigns


def get_campaign_metrics(start_days_ago: int = 9, end_days_ago: int = 3) -> Dict[str, Dict[str, Any]]:
    """
    Get per-campaign metrics: clicks, conversions_value, cost_micros.
    Also calculates OPB (conversions_value / clicks) and cost in EUR.
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")

    start_date = (datetime.now() - timedelta(days=start_days_ago)).strftime("%Y-%m-%d")
    end_date = (datetime.now() - timedelta(days=end_days_ago)).strftime("%Y-%m-%d")

    query = f"""
        SELECT campaign.name, metrics.clicks, metrics.conversions_value, metrics.cost_micros
        FROM campaign
        WHERE campaign.status = 'ENABLED'
            AND segments.date BETWEEN '{start_date}' AND '{end_date}'
    """

    response = ga_service.search(customer_id=CUSTOMER_ID, query=query)

    metrics: Dict[str, Dict[str, Any]] = {}
    for row in response:
        name = row.campaign.name
        if name not in metrics:
            metrics[name] = {"clicks": 0, "conversions_value": 0.0, "cost_micros": 0}

        metrics[name]["clicks"] += row.metrics.clicks
        metrics[name]["conversions_value"] += row.metrics.conversions_value
        metrics[name]["cost_micros"] += row.metrics.cost_micros

    # Calculate derived metrics
    for name, m in metrics.items():
        m["cost"] = m["cost_micros"] / 1_000_000
        m["opb"] = m["conversions_value"] / m["clicks"] if m["clicks"] > 0 else 0.0

    logger.info(f"Got metrics for {len(metrics)} campaigns (range: {start_date} to {end_date})")
    return metrics


def get_dma_cla_omzet(start_days_ago: int = 9, end_days_ago: int = 3) -> Dict[str, float]:
    """
    Get DMA/CLA conversion value per campaign.
    Returns dict mapping campaign_name -> all_conversions_value.
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")

    start_date = (datetime.now() - timedelta(days=start_days_ago)).strftime("%Y-%m-%d")
    end_date = (datetime.now() - timedelta(days=end_days_ago)).strftime("%Y-%m-%d")

    query = f"""
        SELECT campaign.name, segments.conversion_action_name, metrics.all_conversions_value
        FROM campaign
        WHERE campaign.status = 'ENABLED'
            AND segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND segments.conversion_action_name = '{DMA_CLA_CONVERSION_ACTION}'
    """

    response = ga_service.search(customer_id=CUSTOMER_ID, query=query)

    omzet: Dict[str, float] = {}
    for row in response:
        name = row.campaign.name
        omzet[name] = omzet.get(name, 0.0) + row.metrics.all_conversions_value

    logger.info(f"Got DMA/CLA omzet for {len(omzet)} campaigns")
    return omzet


def change_bid_strategy(campaign_resource_name: str, new_strategy_id: str, dry_run: bool = True) -> Dict[str, Any]:
    """
    Mutate a campaign's bidding_strategy to a new strategy.
    Skips mutation if dry_run is True.
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would change {campaign_resource_name} to strategy {new_strategy_id}")
        return {"status": "dry_run", "campaign": campaign_resource_name, "new_strategy_id": new_strategy_id}

    client = _get_client()
    campaign_service = client.get_service("CampaignService")

    operation = client.get_type("CampaignOperation")
    campaign = operation.update
    campaign.resource_name = campaign_resource_name
    campaign.bidding_strategy = f"customers/{CUSTOMER_ID}/biddingStrategies/{new_strategy_id}"

    field_mask = client.get_type("FieldMask")
    field_mask.paths.append("bidding_strategy")
    operation.update_mask.CopyFrom(field_mask)

    try:
        response = campaign_service.mutate_campaigns(
            customer_id=CUSTOMER_ID, operations=[operation]
        )
        logger.info(f"Changed bid strategy for {campaign_resource_name} to {new_strategy_id}")
        return {
            "status": "success",
            "campaign": campaign_resource_name,
            "new_strategy_id": new_strategy_id,
            "result": response.results[0].resource_name,
        }
    except GoogleAdsException as ex:
        logger.error(f"Failed to change bid strategy: {ex}")
        return {"status": "error", "campaign": campaign_resource_name, "error": str(ex)}


# ---------------------------------------------------------------------------
# High-level functions
# ---------------------------------------------------------------------------


def get_level_stats() -> Dict[str, Any]:
    """
    Get campaign counts per DMA bid strategy level + full campaign list.
    """
    level_to_strategy_id, strategy_id_to_level = get_bid_strategies()
    campaigns = get_campaigns_with_strategies()

    level_counts = {1: 0, 2: 0, 3: 0}
    campaign_list = []

    for c in campaigns:
        strategy_resource = c.get("bidding_strategy", "")
        if strategy_resource:
            strategy_id = strategy_resource.split("/")[-1]
            level = strategy_id_to_level.get(strategy_id)
            if level:
                level_counts[level] += 1
                campaign_list.append({
                    "campaign_name": c["campaign_name"],
                    "resource_name": c["resource_name"],
                    "level": level,
                    "strategy_name": BID_STRATEGIES.get(level, "Unknown"),
                })

    total = sum(level_counts.values())

    return {
        "level_counts": level_counts,
        "total": total,
        "campaigns": campaign_list,
    }


def run_dma_bidding(
    start_days_ago: int = 9,
    end_days_ago: int = 3,
    dry_run: bool = True,
    exclude_campaigns: Optional[List[str]] = None,
    include_campaigns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Main DMA bidding flow:
    1. Get bid strategies, campaigns, metrics, DMA/CLA omzet
    2. For each campaign on a DMA level, evaluate rules and change bid strategy
    3. Return structured result with changes
    """
    run_id = len(_run_history) + 1
    start_time = datetime.now()

    logger.info(f"Starting DMA bidding run #{run_id} (dry_run={dry_run}, range={start_days_ago}-{end_days_ago})")

    # Step 1: Gather data
    level_to_strategy_id, strategy_id_to_level = get_bid_strategies()
    campaigns = get_campaigns_with_strategies()
    metrics = get_campaign_metrics(start_days_ago, end_days_ago)
    dma_cla_omzet = get_dma_cla_omzet(start_days_ago, end_days_ago)

    # Step 2: Process campaigns
    changes = {
        "1_to_2": [],
        "2_to_3": [],
        "3_to_2": [],
        "2_to_1": [],
        "stuck_l1": [],
    }
    skipped = []
    no_data = []
    unchanged = []

    exclude_list = [e.lower().strip() for e in (exclude_campaigns or []) if e.strip()]
    include_list = [e.lower().strip() for e in (include_campaigns or []) if e.strip()]

    for c in campaigns:
        campaign_name = c["campaign_name"]
        resource_name = c["resource_name"]
        strategy_resource = c.get("bidding_strategy", "")

        if not strategy_resource:
            continue

        strategy_id = strategy_resource.split("/")[-1]
        current_level = strategy_id_to_level.get(strategy_id)

        if current_level is None:
            continue  # Not a DMA campaign

        # Check include filter (if set, only process matching campaigns)
        if include_list and not any(incl in campaign_name.lower() for incl in include_list):
            skipped.append({"campaign_name": campaign_name, "level": current_level, "reason": "not included"})
            continue

        # Check exclusions (case-insensitive substring)
        if exclude_list and any(excl in campaign_name.lower() for excl in exclude_list):
            skipped.append({"campaign_name": campaign_name, "level": current_level, "reason": "excluded"})
            continue

        # Get metrics
        m = metrics.get(campaign_name)
        omzet = dma_cla_omzet.get(campaign_name, 0.0)

        if not m or m["clicks"] == 0:
            no_data.append({"campaign_name": campaign_name, "level": current_level})
            continue

        cost = m["cost"]
        clicks = m["clicks"]
        conversions_value = m["conversions_value"]
        opb = m["opb"]
        marge = omzet - cost
        roas = omzet / cost if cost > 0 else 0.0

        campaign_info = {
            "campaign_name": campaign_name,
            "resource_name": resource_name,
            "current_level": current_level,
            "marge": round(marge, 2),
            "opb": round(opb, 4),
            "clicks": clicks,
            "roas": round(roas, 2),
            "cost": round(cost, 2),
            "dma_cla_omzet": round(omzet, 2),
        }

        new_level = current_level

        # Rule: Decrease if marge < -10
        if marge < -10:
            if current_level == 3:
                new_level = 2
                campaign_info["change"] = "3_to_2"
                changes["3_to_2"].append(campaign_info)
            elif current_level == 2:
                new_level = 1
                campaign_info["change"] = "2_to_1"
                changes["2_to_1"].append(campaign_info)
            elif current_level == 1:
                campaign_info["change"] = "stuck_l1"
                changes["stuck_l1"].append(campaign_info)
                continue  # No strategy change possible

        # Rule: Increase if marge > 10 AND clicks > 39 AND roas >= 1.30
        elif marge > 10 and clicks > 39 and roas >= 1.30:
            if current_level == 1 and opb > 0.15:
                new_level = 2
                campaign_info["change"] = "1_to_2"
                changes["1_to_2"].append(campaign_info)
            elif current_level == 2 and opb > 0.20:
                new_level = 3
                campaign_info["change"] = "2_to_3"
                changes["2_to_3"].append(campaign_info)
            else:
                unchanged.append(campaign_info)
                continue
        else:
            unchanged.append(campaign_info)
            continue

        # Apply bid strategy change
        if new_level != current_level:
            new_strategy_id = level_to_strategy_id.get(new_level)
            if new_strategy_id:
                result = change_bid_strategy(resource_name, new_strategy_id, dry_run=dry_run)
                campaign_info["mutation_result"] = result

    # Step 3: Build summary
    end_time = datetime.now()
    duration_s = (end_time - start_time).total_seconds()

    summary = {
        "1_to_2": len(changes["1_to_2"]),
        "2_to_3": len(changes["2_to_3"]),
        "3_to_2": len(changes["3_to_2"]),
        "2_to_1": len(changes["2_to_1"]),
        "stuck_l1": len(changes["stuck_l1"]),
        "skipped": len(skipped),
        "no_data": len(no_data),
        "unchanged": len(unchanged),
        "total_changes": (
            len(changes["1_to_2"]) + len(changes["2_to_3"]) +
            len(changes["3_to_2"]) + len(changes["2_to_1"]) +
            len(changes["stuck_l1"])
        ),
    }

    run_result = {
        "run_id": run_id,
        "dry_run": dry_run,
        "start_days_ago": start_days_ago,
        "end_days_ago": end_days_ago,
        "date_range": {
            "start": (datetime.now() - timedelta(days=start_days_ago)).strftime("%Y-%m-%d"),
            "end": (datetime.now() - timedelta(days=end_days_ago)).strftime("%Y-%m-%d"),
        },
        "exclude_campaigns": exclude_list,
        "timestamp": start_time.isoformat(),
        "duration_seconds": round(duration_s, 1),
        "summary": summary,
        "changes": changes,
        "skipped": skipped,
        "no_data": no_data,
        "status": "completed",
    }

    # Store in history (capped at 50)
    _run_history.insert(0, run_result)
    if len(_run_history) > 50:
        _run_history.pop()

    logger.info(f"DMA bidding run #{run_id} completed in {duration_s:.1f}s - {summary}")
    return run_result
