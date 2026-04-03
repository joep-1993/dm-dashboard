"""RSA management operations for smart ad slot handling."""

import asyncio
import logging
from typing import List, Dict, Optional
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from utils.retry import async_retry

logger = logging.getLogger(__name__)

# Maximum RSAs allowed per ad group
MAX_RSAS_PER_AD_GROUP = 3


@async_retry(max_attempts=3, delay=1.0)
async def get_ad_group_rsas(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_id: str
) -> List[Dict]:
    """Get all RSAs in an ad group with their labels.

    Args:
        client: Google Ads client
        customer_id: Customer ID
        ad_group_id: Ad group ID

    Returns:
        List of dictionaries with RSA information:
        [
            {
                "resource_name": str,
                "status": str,
                "labels": List[str],  # Label resource names
                "created_at": str
            },
            ...
        ]
    """

    def _fetch():
        ga_service = client.get_service("GoogleAdsService")

        query = f"""
            SELECT
                ad_group_ad.resource_name,
                ad_group_ad.status,
                ad_group_ad.ad.id,
                ad_group_ad_label.label
            FROM ad_group_ad
            WHERE ad_group.id = {ad_group_id}
              AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
              AND ad_group_ad.status != REMOVED
            ORDER BY ad_group_ad.ad.id ASC
        """

        ads_map = {}  # ad_resource -> ad_info
        try:
            response = ga_service.search(customer_id=customer_id, query=query)

            for row in response:
                ad_resource = row.ad_group_ad.resource_name
                status = str(row.ad_group_ad.status)

                # Initialize ad entry if not exists
                if ad_resource not in ads_map:
                    ads_map[ad_resource] = {
                        "resource_name": ad_resource,
                        "status": status,
                        "labels": [],
                        "ad_id": str(row.ad_group_ad.ad.id)
                    }

                # Add label if present
                if hasattr(row, 'ad_group_ad_label') and row.ad_group_ad_label.label:
                    ads_map[ad_resource]["labels"].append(row.ad_group_ad_label.label)

        except Exception as e:
            logger.error(f"Failed to fetch RSAs for ad group {ad_group_id}: {e}")
            return []

        ads_list = list(ads_map.values())
        logger.debug(f"Found {len(ads_list)} RSAs in ad group {ad_group_id}")
        return ads_list

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch)


@async_retry(max_attempts=3, delay=1.0)
async def remove_ads_by_resource(
    client: GoogleAdsClient,
    customer_id: str,
    ad_resources: List[str]
) -> int:
    """Remove specific ads by resource name.

    Args:
        client: Google Ads client
        customer_id: Customer ID
        ad_resources: List of ad resource names to remove

    Returns:
        Count of successfully removed ads
    """

    def _remove():
        if not ad_resources:
            return 0

        service = client.get_service("AdGroupAdService")
        operations = []

        for ad_resource in ad_resources:
            op = client.get_type("AdGroupAdOperation")
            op.remove = ad_resource
            operations.append(op)

        try:
            response = service.mutate_ad_group_ads(
                customer_id=customer_id,
                operations=operations
            )
            logger.info(f"Removed {len(response.results)} ads")
            return len(response.results)

        except GoogleAdsException as e:
            logger.warning(f"Failed to remove some ads: {e}")
            return 0

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _remove)


async def manage_ad_slots(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_id: str,
    theme_labels: List[str],
    original_ad_label: str = "THEMA_ORIGINAL"
) -> Optional[str]:
    """Manage ad slots to make room for a new theme ad.

    This function ensures there's space for a new RSA by:
    1. Checking current RSA count
    2. If >= 3 RSAs, removing old theme ads (by label)
    3. If no theme ads found, removing oldest PAUSED ad
    4. If all ads ENABLED, returns error

    Args:
        client: Google Ads client
        customer_id: Customer ID
        ad_group_id: Ad group ID
        theme_labels: List of theme label resource names to look for
        original_ad_label: Label resource name for THEMA_ORIGINAL

    Returns:
        None if successful, error message if unable to make room
    """

    # Get all RSAs in the ad group
    rsas = await get_ad_group_rsas(client, customer_id, ad_group_id)

    if len(rsas) < MAX_RSAS_PER_AD_GROUP:
        logger.info(f"Ad group {ad_group_id} has {len(rsas)} RSAs, space available")
        return None  # Space available

    logger.info(f"Ad group {ad_group_id} has {len(rsas)} RSAs, need to make room")

    # Find theme ads (ads with any theme label)
    theme_ads = []
    original_ads = []
    paused_ads = []
    enabled_ads = []

    for ad in rsas:
        # Check if ad has any theme label
        has_theme_label = any(label in ad["labels"] for label in theme_labels)
        has_original_label = original_ad_label in ad["labels"]

        if has_theme_label:
            theme_ads.append(ad)
        elif has_original_label:
            original_ads.append(ad)

        # Also categorize by status
        if ad["status"] == "PAUSED":
            paused_ads.append(ad)
        else:
            enabled_ads.append(ad)

    # Strategy 1: Remove old theme ads (not original ad)
    if theme_ads:
        # Remove oldest theme ad (by ad_id, assuming lower ID = older)
        theme_ads_sorted = sorted(theme_ads, key=lambda x: int(x["ad_id"]))
        ad_to_remove = theme_ads_sorted[0]
        logger.info(f"Removing old theme ad {ad_to_remove['resource_name']} from ad group {ad_group_id}")

        removed = await remove_ads_by_resource(client, customer_id, [ad_to_remove["resource_name"]])
        if removed > 0:
            return None  # Successfully made room
        else:
            return "Failed to remove old theme ad"

    # Strategy 2: Remove oldest paused ad (but not original ad)
    paused_non_original = [ad for ad in paused_ads if original_ad_label not in ad["labels"]]
    if paused_non_original:
        # Remove oldest paused ad
        paused_sorted = sorted(paused_non_original, key=lambda x: int(x["ad_id"]))
        ad_to_remove = paused_sorted[0]
        logger.info(f"Removing paused ad {ad_to_remove['resource_name']} from ad group {ad_group_id}")

        removed = await remove_ads_by_resource(client, customer_id, [ad_to_remove["resource_name"]])
        if removed > 0:
            return None  # Successfully made room
        else:
            return "Failed to remove paused ad"

    # Strategy 3: All ads are enabled or protected - cannot proceed
    logger.warning(
        f"Cannot make room in ad group {ad_group_id}: "
        f"{len(enabled_ads)} enabled ads, {len(original_ads)} original ads"
    )
    return f"Cannot remove ads: {len(enabled_ads)} enabled, {len(original_ads)} protected (THEMA_ORIGINAL)"


async def check_needs_room(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_id: str
) -> bool:
    """Check if an ad group needs room (has >= 3 RSAs).

    Args:
        client: Google Ads client
        customer_id: Customer ID
        ad_group_id: Ad group ID

    Returns:
        True if ad group has >= 3 RSAs and needs management
    """
    rsas = await get_ad_group_rsas(client, customer_id, ad_group_id)
    return len(rsas) >= MAX_RSAS_PER_AD_GROUP
