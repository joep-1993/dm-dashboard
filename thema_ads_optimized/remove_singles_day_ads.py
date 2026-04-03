#!/usr/bin/env python3
"""Script to remove all ads with SINGLES_DAY label and optionally SD_DONE labels.

DEPRECATED: This script is for legacy Singles Day campaigns only.
For new multi-theme support, use remove_theme_ads.py instead:

    python remove_theme_ads.py singles_day [customer_ids...]

This script still works but uses the old SD_DONE label instead of THEME_SD_DONE.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from google.ads.googleads.client import GoogleAdsClient
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


async def remove_singles_day_ads(client: GoogleAdsClient, customer_id: str, remove_sd_done: bool = False):
    """Remove all ads with SINGLES_DAY label.

    Args:
        client: Google Ads client
        customer_id: Customer ID (without hyphens)
        remove_sd_done: If True, also remove SD_DONE labels from ad groups
    """

    logger.info(f"Starting removal process for customer {customer_id}")

    # Step 1: Find all ads with SINGLES_DAY label
    logger.info("Step 1: Finding ads with SINGLES_DAY label...")
    ad_resources = await get_ads_by_label(client, customer_id, "SINGLES_DAY")

    if not ad_resources:
        logger.info("No ads found with SINGLES_DAY label")
    else:
        # Step 2: Remove the ads
        logger.info(f"Step 2: Removing {len(ad_resources)} ads with SINGLES_DAY label...")
        removed_count = await remove_ads_batch(client, customer_id, ad_resources)
        logger.info(f"Successfully removed {removed_count} ads")

    # Step 3: Optionally remove SD_DONE labels
    if remove_sd_done:
        logger.info("Step 3: Finding ad groups with SD_DONE label...")
        ad_group_resources = await get_ad_groups_by_label(client, customer_id, "SD_DONE")

        if not ad_group_resources:
            logger.info("No ad groups found with SD_DONE label")
        else:
            logger.info(f"Step 4: Getting ad_group_label associations for {len(ad_group_resources)} ad groups...")
            label_resources = await get_ad_group_label_resources(
                client, customer_id, ad_group_resources, "SD_DONE"
            )

            if label_resources:
                logger.info(f"Step 5: Removing {len(label_resources)} SD_DONE label associations...")
                removed_labels = await remove_ad_group_labels_batch(client, customer_id, label_resources)
                logger.info(f"Successfully removed {removed_labels} SD_DONE label associations")

    logger.info("Removal process complete!")


async def main():
    """Main entry point."""

    # Load Google Ads client
    try:
        client = GoogleAdsClient.load_from_storage("google-ads.yaml")
    except Exception as e:
        logger.error(f"Failed to load Google Ads client: {e}")
        logger.error("Make sure google-ads.yaml exists in the current directory")
        sys.exit(1)

    # Get customer ID from environment or command line
    import os
    customer_id = os.getenv("CUSTOMER_ID")

    if len(sys.argv) > 1:
        customer_id = sys.argv[1].replace("-", "")

    if not customer_id:
        logger.error("Please provide CUSTOMER_ID via environment variable or command line argument")
        logger.error("Usage: python remove_singles_day_ads.py <customer_id>")
        sys.exit(1)

    # Ask for confirmation
    print(f"\n{'='*60}")
    print(f"CUSTOMER ID: {customer_id}")
    print(f"{'='*60}")
    print("\nThis script will:")
    print("  1. Find all ads with SINGLES_DAY label")
    print("  2. Remove (set status to REMOVED) those ads")
    print("\nOptionally:")
    print("  3. Remove SD_DONE labels from ad groups")
    print(f"{'='*60}\n")

    response = input("Do you want to proceed? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        logger.info("Operation cancelled by user")
        sys.exit(0)

    remove_sd_done_response = input("\nAlso remove SD_DONE labels? (yes/no): ").strip().lower()
    remove_sd_done = remove_sd_done_response in ['yes', 'y']

    # Execute removal
    await remove_singles_day_ads(client, customer_id, remove_sd_done=remove_sd_done)


if __name__ == "__main__":
    asyncio.run(main())
