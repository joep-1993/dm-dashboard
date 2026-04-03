#!/usr/bin/env python3
"""Script to remove all ads with SINGLES_DAY label and SD_DONE labels for all customers.

DEPRECATED: This script is for legacy Singles Day campaigns only.
For new multi-theme support, use remove_theme_ads.py instead:

    python remove_theme_ads.py singles_day [customer_ids...]

This script still works but uses the old SD_DONE label instead of THEME_SD_DONE.
"""

import asyncio
import logging
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from google.ads.googleads.client import GoogleAdsClient
from config import load_config_from_env
from google_ads_client import initialize_client
from operations.labels import (
    get_ads_by_label,
    remove_ads_batch,
    get_ad_groups_by_label,
    get_ad_group_label_resources,
    remove_ad_group_labels_batch
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def remove_singles_day_ads_for_customer(client: GoogleAdsClient, customer_id: str):
    """Remove all ads with SINGLES_DAY label for a single customer.

    Args:
        client: Google Ads client
        customer_id: Customer ID (without hyphens)
    """

    logger.info(f"\n{'='*60}")
    logger.info(f"Processing customer: {customer_id}")
    logger.info(f"{'='*60}\n")

    # Step 1: Find all ads with SINGLES_DAY label
    logger.info("Step 1: Finding ads with SINGLES_DAY label...")
    try:
        ad_resources = await get_ads_by_label(client, customer_id, "SINGLES_DAY")
    except Exception as e:
        logger.error(f"Failed to find ads with SINGLES_DAY label: {e}")
        return False

    if not ad_resources:
        logger.info("No ads found with SINGLES_DAY label")
    else:
        # Step 2: Remove the ads
        logger.info(f"Step 2: Removing {len(ad_resources)} ads with SINGLES_DAY label...")
        try:
            removed_count = await remove_ads_batch(client, customer_id, ad_resources)
            logger.info(f"✓ Successfully removed {removed_count} ads")
        except Exception as e:
            logger.error(f"Failed to remove ads: {e}")
            return False

    # Step 3: Remove SD_DONE labels (AFTER ads are removed)
    logger.info("Step 3: Finding ad groups with SD_DONE label...")
    try:
        ad_group_resources = await get_ad_groups_by_label(client, customer_id, "SD_DONE")
    except Exception as e:
        logger.error(f"Failed to find ad groups with SD_DONE label: {e}")
        return False

    if not ad_group_resources:
        logger.info("No ad groups found with SD_DONE label")
    else:
        logger.info(f"Step 4: Getting ad_group_label associations for {len(ad_group_resources)} ad groups...")
        try:
            label_resources = await get_ad_group_label_resources(
                client, customer_id, ad_group_resources, "SD_DONE"
            )

            if label_resources:
                logger.info(f"Step 5: Removing {len(label_resources)} SD_DONE label associations...")
                removed_labels = await remove_ad_group_labels_batch(client, customer_id, label_resources)
                logger.info(f"✓ Successfully removed {removed_labels} SD_DONE label associations")
            else:
                logger.info("No label associations found to remove")
        except Exception as e:
            logger.error(f"Failed to remove SD_DONE labels: {e}")
            return False

    logger.info(f"✓ Removal process complete for customer {customer_id}!")
    return True


async def main():
    """Main entry point."""

    # Load configuration and initialize client
    try:
        config = load_config_from_env()
        client = initialize_client(config.google_ads)
    except Exception as e:
        logger.error(f"Failed to initialize Google Ads client: {e}")
        logger.error("Make sure all required environment variables are set in .env file")
        sys.exit(1)

    # Get customer IDs from command line or use default list
    if len(sys.argv) > 1:
        customer_ids = [arg.replace("-", "") for arg in sys.argv[1:]]
    else:
        # Default customer IDs from database
        customer_ids = [
            "1351439239",
            "1496704472",
            "2237802672",
            "3114657125",
            "3273661472"
        ]

    logger.info(f"\n{'='*60}")
    logger.info(f"BATCH REMOVAL OF SINGLES_DAY ADS AND SD_DONE LABELS")
    logger.info(f"{'='*60}")
    logger.info(f"Processing {len(customer_ids)} customer(s)")
    logger.info(f"Customer IDs: {', '.join(customer_ids)}")
    logger.info(f"{'='*60}\n")

    # Process each customer
    success_count = 0
    for customer_id in customer_ids:
        try:
            success = await remove_singles_day_ads_for_customer(client, customer_id)
            if success:
                success_count += 1
        except Exception as e:
            logger.error(f"Unexpected error processing customer {customer_id}: {e}")

    logger.info(f"\n{'='*60}")
    logger.info(f"BATCH PROCESSING COMPLETE")
    logger.info(f"{'='*60}")
    logger.info(f"Successfully processed: {success_count}/{len(customer_ids)} customers")
    logger.info(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
