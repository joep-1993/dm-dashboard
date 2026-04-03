#!/usr/bin/env python3
"""
Validate that all ad groups with theme ads also have THEMA_ORIGINAL ads.

This script checks the integrity of theme ad creation by ensuring that:
- Every ad group with theme-labeled ads (THEME_BF, THEME_CM, etc.) also has THEMA_ORIGINAL ads
- Reports any ad groups missing original ads (which would indicate incomplete setup)
"""

import asyncio
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
env_path = Path(__file__).parent / '.env'
load_dotenv(env_path)

# Constants
THEME_LABELS = ['THEME_BF', 'THEME_CM', 'THEME_SK', 'THEME_KM']  # Excluding THEME_SD
THEMA_ORIGINAL_LABEL = 'THEMA_ORIGINAL'

# Valid customer IDs (from whitelist)
VALID_CUSTOMERS = [
    '4056770576', '1496704472', '4964513580', '3114657125', '5807833423',
    '3273661472', '9251309631', '8273243429', '8696777335', '5930401821',
    '6213822688', '6379322129', '2237802672', '8338942127', '9525057729',
    '8431844135', '6511658729', '4675585929', '5105960927', '4567815835',
    '1351439239', '5122292229', '7346695290', '5550062935', '4761604080',
    '6044293584', '6271552035', '8755979133'
]


def initialize_client() -> GoogleAdsClient:
    """Initialize Google Ads API client."""
    try:
        client = GoogleAdsClient.load_from_dict({
            'developer_token': os.getenv('GOOGLE_DEVELOPER_TOKEN'),
            'refresh_token': os.getenv('GOOGLE_REFRESH_TOKEN'),
            'client_id': os.getenv('GOOGLE_CLIENT_ID'),
            'client_secret': os.getenv('GOOGLE_CLIENT_SECRET'),
            'login_customer_id': os.getenv('GOOGLE_LOGIN_CUSTOMER_ID'),
            'token_uri': 'https://oauth2.googleapis.com/token',
            'use_proto_plus': True
        })
        logger.info("Google Ads client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Google Ads client: {e}")
        raise


def get_label_ids(client: GoogleAdsClient, customer_id: str) -> Dict[str, str]:
    """Get label IDs for all relevant labels."""
    ga_service = client.get_service("GoogleAdsService")

    # Get all label names we care about
    all_labels = THEME_LABELS + [THEMA_ORIGINAL_LABEL]

    query = f"""
        SELECT
            label.id,
            label.name
        FROM label
        WHERE label.name IN ({','.join(f"'{label}'" for label in all_labels)})
    """

    label_map = {}
    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            label_map[row.label.name] = row.label.resource_name

        logger.info(f"[{customer_id}] Found {len(label_map)} labels: {list(label_map.keys())}")
        return label_map
    except GoogleAdsException as ex:
        logger.error(f"[{customer_id}] Failed to fetch labels: {ex}")
        return {}


def get_ad_groups_by_label(
    client: GoogleAdsClient,
    customer_id: str,
    label_ids: Dict[str, str]
) -> Tuple[Set[str], Set[str]]:
    """
    Get ad groups that have theme ads and ad groups that have original ads.

    Returns:
        Tuple of (theme_ad_groups, original_ad_groups)
    """
    if not label_ids:
        logger.info(f"[{customer_id}] No labels found, skipping")
        return set(), set()

    ga_service = client.get_service("GoogleAdsService")

    # Query for all ad groups with labeled ads
    query = f"""
        SELECT
            ad_group.id,
            ad_group.name,
            ad_group.resource_name,
            label.name
        FROM ad_group_ad_label
        WHERE label.name IN ({','.join(f"'{label}'" for label in label_ids.keys())})
    """

    # Build sets of ad groups
    theme_ad_groups: Set[str] = set()
    original_ad_groups: Set[str] = set()
    ad_group_names: Dict[str, str] = {}

    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            ad_group_resource = row.ad_group.resource_name
            ad_group_name = row.ad_group.name
            label_name = row.label.name

            ad_group_names[ad_group_resource] = ad_group_name

            if label_name == THEMA_ORIGINAL_LABEL:
                original_ad_groups.add(ad_group_resource)
            elif label_name in THEME_LABELS:
                theme_ad_groups.add(ad_group_resource)

        logger.info(
            f"[{customer_id}] Found {len(theme_ad_groups)} ad groups with theme ads, "
            f"{len(original_ad_groups)} with original ads"
        )
        return theme_ad_groups, original_ad_groups, ad_group_names

    except GoogleAdsException as ex:
        logger.error(f"[{customer_id}] Failed to query ad groups: {ex}")
        return set(), set(), {}


async def validate_customer(
    client: GoogleAdsClient,
    customer_id: str
) -> Dict[str, int]:
    """Validate a single customer."""
    logger.info(f"[{customer_id}] Starting validation...")

    # Get label IDs
    label_ids = get_label_ids(client, customer_id)
    if THEMA_ORIGINAL_LABEL not in label_ids:
        logger.info(f"[{customer_id}] No THEMA_ORIGINAL label found, skipping")
        return {'theme_ad_groups': 0, 'missing_original': 0}

    theme_label_ids = {name: rid for name, rid in label_ids.items() if name in THEME_LABELS}
    if not theme_label_ids:
        logger.info(f"[{customer_id}] No theme labels found, skipping")
        return {'theme_ad_groups': 0, 'missing_original': 0}

    # Get ad groups with theme and original ads
    theme_ad_groups, original_ad_groups, ad_group_names = get_ad_groups_by_label(
        client, customer_id, label_ids
    )

    if not theme_ad_groups:
        logger.info(f"[{customer_id}] No theme ad groups found")
        return {'theme_ad_groups': 0, 'missing_original': 0}

    # Find ad groups with theme ads but missing original ads
    missing_original = theme_ad_groups - original_ad_groups

    if missing_original:
        logger.warning(
            f"[{customer_id}] Found {len(missing_original)} ad groups with theme ads "
            f"but NO original ads!"
        )
        # Log first 3 examples
        for ag_resource in list(missing_original)[:3]:
            ag_name = ad_group_names.get(ag_resource, 'Unknown')
            logger.warning(f"  - {ag_name} ({ag_resource})")
        if len(missing_original) > 3:
            logger.warning(f"  ... and {len(missing_original) - 3} more")
    else:
        logger.info(f"[{customer_id}] ✓ All {len(theme_ad_groups)} theme ad groups have original ads")

    return {
        'theme_ad_groups': len(theme_ad_groups),
        'missing_original': len(missing_original)
    }


async def main(limit: int = None):
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("Theme Ad Groups Validation Script")
    logger.info("=" * 80)
    logger.info(f"Processing: {len(VALID_CUSTOMERS)} customers")
    if limit:
        logger.info(f"Limit: First {limit} customers only")
    logger.info("=" * 80)

    # Initialize client
    client = initialize_client()

    # Process customers
    customers_to_process = VALID_CUSTOMERS[:limit] if limit else VALID_CUSTOMERS

    total_theme_ad_groups = 0
    total_missing_original = 0
    customers_with_issues = 0

    for customer_id in customers_to_process:
        try:
            result = await validate_customer(client, customer_id)
            total_theme_ad_groups += result['theme_ad_groups']
            total_missing_original += result['missing_original']
            if result['missing_original'] > 0:
                customers_with_issues += 1
        except Exception as e:
            logger.error(f"[{customer_id}] Unexpected error: {e}")
            continue

    # Summary
    logger.info("=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total ad groups with theme ads: {total_theme_ad_groups}")
    logger.info(f"Ad groups missing original ads: {total_missing_original}")
    logger.info(f"Customers with issues: {customers_with_issues}")
    if total_missing_original == 0:
        logger.info("✓ ALL THEME AD GROUPS HAVE ORIGINAL ADS!")
    else:
        logger.warning(f"⚠ {total_missing_original} ad groups need attention")
    logger.info("=" * 80)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Validate that all ad groups with theme ads also have original ads'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit to first N customers (for testing)'
    )

    args = parser.parse_args()

    asyncio.run(main(limit=args.limit))
