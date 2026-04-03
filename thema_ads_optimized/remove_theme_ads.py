#!/usr/bin/env python3
"""Script to remove theme ads and their DONE labels for specified customers.

This script supports all themes and automatically removes both:
1. Ads with the theme's label (e.g., THEME_BF for Black Friday)
2. The theme's DONE label from ad groups (e.g., THEME_BF_DONE)

This allows ad groups to be re-processed with different themes.
"""

import asyncio
import logging
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from google.ads.googleads.client import GoogleAdsClient
from config import load_config_from_env
from google_ads_client import initialize_client
from themes import get_theme_label, SUPPORTED_THEMES
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


async def remove_theme_ads_for_customer(
    client: GoogleAdsClient,
    customer_id: str,
    theme_name: str
):
    """Remove all ads with a theme label and the DONE label for a customer.

    Args:
        client: Google Ads client
        customer_id: Customer ID (without hyphens)
        theme_name: Name of the theme (e.g., 'black_friday', 'cyber_monday')

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing customer: {customer_id}")
    logger.info(f"Theme: {theme_name}")
    logger.info(f"{'='*60}\n")

    # Get theme labels
    theme_label = get_theme_label(theme_name)
    done_label = f"{theme_label}_DONE"

    # Step 1: Find all ads with theme label
    logger.info(f"Step 1: Finding ads with {theme_label} label...")
    try:
        ad_resources = await get_ads_by_label(client, customer_id, theme_label)
    except Exception as e:
        logger.error(f"Failed to find ads with {theme_label} label: {e}")
        return False

    if not ad_resources:
        logger.info(f"No ads found with {theme_label} label")
    else:
        # Step 2: Remove the ads
        logger.info(f"Step 2: Removing {len(ad_resources)} ads with {theme_label} label...")
        try:
            removed_count = await remove_ads_batch(client, customer_id, ad_resources)
            logger.info(f"✓ Successfully removed {removed_count} ads")
        except Exception as e:
            logger.error(f"Failed to remove ads: {e}")
            return False

    # Step 3: Remove DONE labels (AFTER ads are removed)
    logger.info(f"Step 3: Finding ad groups with {done_label} label...")
    try:
        ad_group_resources = await get_ad_groups_by_label(client, customer_id, done_label)
    except Exception as e:
        logger.error(f"Failed to find ad groups with {done_label} label: {e}")
        return False

    if not ad_group_resources:
        logger.info(f"No ad groups found with {done_label} label")
    else:
        logger.info(f"Step 4: Getting ad_group_label associations for {len(ad_group_resources)} ad groups...")
        try:
            label_resources = await get_ad_group_label_resources(
                client, customer_id, ad_group_resources, done_label
            )

            if label_resources:
                logger.info(f"Step 5: Removing {len(label_resources)} {done_label} label associations...")
                removed_labels = await remove_ad_group_labels_batch(client, customer_id, label_resources)
                logger.info(f"✓ Successfully removed {removed_labels} {done_label} label associations")
            else:
                logger.info("No label associations found to remove")
        except Exception as e:
            logger.error(f"Failed to remove {done_label} labels: {e}")
            return False

    logger.info(f"✓ Removal process complete for customer {customer_id}!")
    return True


async def main():
    """Main entry point."""

    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: python remove_theme_ads.py <theme_name> [customer_id1] [customer_id2] ...")
        print("\nSupported themes:")
        for theme_name, theme_info in SUPPORTED_THEMES.items():
            print(f"  - {theme_name}: {theme_info['display_name']}")
        print("\nIf no customer IDs are provided, will process all default customers.")
        sys.exit(1)

    theme_name = sys.argv[1].lower()

    # Validate theme
    if theme_name not in SUPPORTED_THEMES:
        print(f"Error: Unknown theme '{theme_name}'")
        print("\nSupported themes:")
        for theme_name_opt, theme_info in SUPPORTED_THEMES.items():
            print(f"  - {theme_name_opt}: {theme_info['display_name']}")
        sys.exit(1)

    # Load configuration and initialize client
    try:
        config = load_config_from_env()
        client = initialize_client(config.google_ads)
    except Exception as e:
        logger.error(f"Failed to initialize Google Ads client: {e}")
        logger.error("Make sure all required environment variables are set in .env file")
        sys.exit(1)

    # Get customer IDs from command line or use default list
    if len(sys.argv) > 2:
        customer_ids = [arg.replace("-", "") for arg in sys.argv[2:]]
    else:
        # Default customer IDs from account ids file
        account_ids_file = Path(__file__).parent / "account ids"
        if account_ids_file.exists():
            with open(account_ids_file, 'r') as f:
                customer_ids = [line.strip() for line in f if line.strip()]
        else:
            logger.error("No customer IDs provided and 'account ids' file not found")
            sys.exit(1)

    theme_label = get_theme_label(theme_name)
    theme_display = SUPPORTED_THEMES[theme_name]['display_name']

    logger.info(f"\n{'='*60}")
    logger.info(f"BATCH REMOVAL OF {theme_display.upper()} ADS AND LABELS")
    logger.info(f"{'='*60}")
    logger.info(f"Theme: {theme_name}")
    logger.info(f"Label: {theme_label}")
    logger.info(f"DONE Label: {theme_label}_DONE")
    logger.info(f"Processing {len(customer_ids)} customer(s)")
    logger.info(f"Customer IDs: {', '.join(customer_ids)}")
    logger.info(f"{'='*60}\n")

    # Process each customer
    success_count = 0
    for customer_id in customer_ids:
        try:
            success = await remove_theme_ads_for_customer(client, customer_id, theme_name)
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
