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
from google.protobuf import field_mask_pb2

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COUNTRY_CUSTOMER_IDS = {
    "NL": "3800751597",
    "BE": "9920951707",
}
DEFAULT_COUNTRY = "NL"
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


def _resolve_customer_id(country: str = DEFAULT_COUNTRY) -> str:
    """Return the Google Ads CUSTOMER_ID for the given country code."""
    cid = COUNTRY_CUSTOMER_IDS.get(country.upper())
    if not cid:
        raise ValueError(f"Unknown country '{country}'. Expected one of: {list(COUNTRY_CUSTOMER_IDS.keys())}")
    return cid


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


def get_bid_strategies(country: str = DEFAULT_COUNTRY) -> Tuple[Dict[int, str], Dict[str, int]]:
    """
    Query DMA Level 1/2/3 bid strategies via accessible_bidding_strategy.
    This finds both account-owned and MCC-owned (cross-account) strategies.
    Returns:
        (level_to_strategy_resource, strategy_id_to_level) dicts.
        level_to_strategy_resource maps level -> full bidding_strategy resource name for mutations.
    """
    customer_id = _resolve_customer_id(country)
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT accessible_bidding_strategy.name,
               accessible_bidding_strategy.id,
               accessible_bidding_strategy.owner_customer_id
        FROM accessible_bidding_strategy
    """

    response = ga_service.search(customer_id=customer_id, query=query)

    level_to_strategy_resource: Dict[int, str] = {}
    strategy_id_to_level: Dict[str, int] = {}

    for row in response:
        name = row.accessible_bidding_strategy.name
        strategy_id = str(row.accessible_bidding_strategy.id)
        owner_id = str(row.accessible_bidding_strategy.owner_customer_id)

        for level, level_name in BID_STRATEGIES.items():
            if name == level_name:
                # Build resource name using the owner's customer ID for mutations
                level_to_strategy_resource[level] = f"customers/{owner_id}/biddingStrategies/{strategy_id}"
                strategy_id_to_level[strategy_id] = level
                break

    logger.info(f"Found bid strategies: {level_to_strategy_resource}")
    return level_to_strategy_resource, strategy_id_to_level


def get_campaigns_with_strategies(country: str = DEFAULT_COUNTRY) -> List[Dict[str, Any]]:
    """
    Get all ENABLED campaigns with their accessible bid strategy.
    Returns list of dicts with campaign_name, resource_name, accessible_bidding_strategy.
    """
    customer_id = _resolve_customer_id(country)
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT campaign.name, campaign.resource_name, campaign.accessible_bidding_strategy
        FROM campaign
        WHERE campaign.status = 'ENABLED'
    """

    response = ga_service.search(customer_id=customer_id, query=query)

    campaigns = []
    for row in response:
        campaigns.append({
            "campaign_name": row.campaign.name,
            "resource_name": row.campaign.resource_name,
            "accessible_bidding_strategy": str(row.campaign.accessible_bidding_strategy),
        })

    logger.info(f"Found {len(campaigns)} enabled campaigns")
    return campaigns


def get_campaign_metrics(start_days_ago: int = 9, end_days_ago: int = 3, country: str = DEFAULT_COUNTRY) -> Dict[str, Dict[str, Any]]:
    """
    Get per-campaign metrics: clicks, conversions_value, cost_micros.
    Also calculates OPB (conversions_value / clicks) and cost in EUR.
    """
    customer_id = _resolve_customer_id(country)
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

    response = ga_service.search(customer_id=customer_id, query=query)

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


def get_dma_cla_omzet(start_days_ago: int = 9, end_days_ago: int = 3, country: str = DEFAULT_COUNTRY) -> Dict[str, float]:
    """
    Get DMA/CLA conversion value per campaign.
    Returns dict mapping campaign_name -> all_conversions_value.
    """
    customer_id = _resolve_customer_id(country)
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

    response = ga_service.search(customer_id=customer_id, query=query)

    omzet: Dict[str, float] = {}
    for row in response:
        name = row.campaign.name
        omzet[name] = omzet.get(name, 0.0) + row.metrics.all_conversions_value

    logger.info(f"Got DMA/CLA omzet for {len(omzet)} campaigns")
    return omzet


def change_bid_strategy(campaign_resource_name: str, new_strategy_resource: str, dry_run: bool = True, country: str = DEFAULT_COUNTRY) -> Dict[str, Any]:
    """
    Mutate a campaign's bidding_strategy to a new strategy.
    new_strategy_resource is the full resource name (e.g. customers/{owner_id}/biddingStrategies/{id}).
    Skips mutation if dry_run is True.
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would change {campaign_resource_name} to strategy {new_strategy_resource}")
        return {"status": "dry_run", "campaign": campaign_resource_name, "new_strategy": new_strategy_resource}

    customer_id = _resolve_customer_id(country)
    client = _get_client()
    campaign_service = client.get_service("CampaignService")

    operation = client.get_type("CampaignOperation")
    campaign = operation.update
    campaign.resource_name = campaign_resource_name
    campaign.bidding_strategy = new_strategy_resource

    field_mask = field_mask_pb2.FieldMask(paths=["bidding_strategy"])
    operation.update_mask.CopyFrom(field_mask)

    try:
        response = campaign_service.mutate_campaigns(
            customer_id=customer_id, operations=[operation]
        )
        logger.info(f"Changed bid strategy for {campaign_resource_name} to {new_strategy_resource}")
        return {
            "status": "success",
            "campaign": campaign_resource_name,
            "new_strategy": new_strategy_resource,
            "result": response.results[0].resource_name,
        }
    except GoogleAdsException as ex:
        logger.error(f"Failed to change bid strategy: {ex}")
        return {"status": "error", "campaign": campaign_resource_name, "error": str(ex)}


# ---------------------------------------------------------------------------
# High-level functions
# ---------------------------------------------------------------------------


def get_level_stats(country: str = DEFAULT_COUNTRY) -> Dict[str, Any]:
    """
    Get campaign counts per DMA bid strategy level + full campaign list.
    """
    level_to_strategy_id, strategy_id_to_level = get_bid_strategies(country=country)
    campaigns = get_campaigns_with_strategies(country=country)

    level_counts = {1: 0, 2: 0, 3: 0}
    campaign_list = []

    for c in campaigns:
        strategy_resource = c.get("accessible_bidding_strategy", "")
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
    country: str = DEFAULT_COUNTRY,
) -> Dict[str, Any]:
    """
    Main DMA bidding flow:
    1. Get bid strategies, campaigns, metrics, DMA/CLA omzet
    2. For each campaign on a DMA level, evaluate rules and change bid strategy
    3. Return structured result with changes
    """
    run_id = len(_run_history) + 1
    start_time = datetime.now()

    logger.info(f"Starting DMA bidding run #{run_id} (country={country}, dry_run={dry_run}, range={start_days_ago}-{end_days_ago})")

    # Step 1: Gather data
    level_to_strategy_id, strategy_id_to_level = get_bid_strategies(country=country)
    campaigns = get_campaigns_with_strategies(country=country)
    metrics = get_campaign_metrics(start_days_ago, end_days_ago, country=country)
    dma_cla_omzet = get_dma_cla_omzet(start_days_ago, end_days_ago, country=country)

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
        strategy_resource = c.get("accessible_bidding_strategy", "")

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
            new_strategy_resource = level_to_strategy_id.get(new_level)
            if new_strategy_resource:
                result = change_bid_strategy(resource_name, new_strategy_resource, dry_run=dry_run, country=country)
                campaign_info["mutation_result"] = result
            else:
                # Strategy for target level not configured — record as not-applied
                # so the campaign row isn't silently claimed as moved.
                campaign_info["mutation_result"] = {
                    "success": False,
                    "skipped": True,
                    "reason": f"No bid strategy configured for level {new_level}",
                }

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
        "country": country,
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
