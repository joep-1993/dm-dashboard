#!/usr/bin/env python3
"""
Remove THEMA_ORIGINAL labels from ads that also have theme labels.

This script finds ads that have both a THEMA_ORIGINAL label and a theme label
(THEME_BF, THEME_CM, THEME_SK, THEME_KM, THEME_SD), and removes the incorrect
THEMA_ORIGINAL label.

Logic:
- Theme ads should ONLY have theme labels (not THEMA_ORIGINAL)
- Original ads should ONLY have THEMA_ORIGINAL (not theme labels)
- This fixes mislabeled ads that have both
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
THEME_LABELS = ['THEME_BF', 'THEME_CM', 'THEME_SK', 'THEME_KM']  # Excluding THEME_SD (Singles Day)
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


def find_ads_with_conflicting_labels(
    client: GoogleAdsClient,
    customer_id: str,
    label_ids: Dict[str, str]
) -> List[Tuple[str, Set[str]]]:
    """
    Find ads that have both THEMA_ORIGINAL and at least one theme label.

    Returns:
        List of (ad_resource_name, set_of_theme_labels) tuples
    """
    if THEMA_ORIGINAL_LABEL not in label_ids:
        logger.info(f"[{customer_id}] No THEMA_ORIGINAL label found, skipping")
        return []

    theme_label_ids = {name: rid for name, rid in label_ids.items() if name in THEME_LABELS}
    if not theme_label_ids:
        logger.info(f"[{customer_id}] No theme labels found, skipping")
        return []

    ga_service = client.get_service("GoogleAdsService")

    # Query for all ads with THEMA_ORIGINAL label
    query = f"""
        SELECT
            ad_group_ad.ad.id,
            ad_group_ad.ad.name,
            ad_group_ad.resource_name,
            label.name
        FROM ad_group_ad_label
        WHERE label.name IN ({','.join(f"'{label}'" for label in label_ids.keys())})
    """

    # Build map of ad -> labels
    ad_labels: Dict[str, Set[str]] = defaultdict(set)

    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            ad_resource = row.ad_group_ad.resource_name
            label_name = row.label.name
            ad_labels[ad_resource].add(label_name)

        # Find ads with both THEMA_ORIGINAL and theme labels
        conflicting_ads = []
        for ad_resource, labels in ad_labels.items():
            if THEMA_ORIGINAL_LABEL in labels:
                theme_labels_present = labels & set(THEME_LABELS)
                if theme_labels_present:
                    conflicting_ads.append((ad_resource, theme_labels_present))

        logger.info(
            f"[{customer_id}] Found {len(conflicting_ads)} ads with both "
            f"THEMA_ORIGINAL and theme labels (out of {len(ad_labels)} total labeled ads)"
        )
        return conflicting_ads

    except GoogleAdsException as ex:
        logger.error(f"[{customer_id}] Failed to query ads: {ex}")
        return []


def remove_thema_original_labels(
    client: GoogleAdsClient,
    customer_id: str,
    ads_to_fix: List[Tuple[str, Set[str]]],
    thema_original_label_id: str,
    dry_run: bool = False
) -> Tuple[int, int]:
    """
    Remove THEMA_ORIGINAL labels from the specified ads.

    Returns:
        Tuple of (success_count, failure_count)
    """
    if not ads_to_fix:
        return 0, 0

    ad_group_ad_label_service = client.get_service("AdGroupAdLabelService")

    success_count = 0
    failure_count = 0

    # Build operations to remove THEMA_ORIGINAL label
    operations = []
    for ad_resource, theme_labels in ads_to_fix:
        # Construct the ad_group_ad_label resource name
        # Format: customers/{customer_id}/adGroupAdLabels/{ad_group_id}~{ad_id}~{label_id}
        label_id = thema_original_label_id.split('/')[-1]
        ad_parts = ad_resource.split('~')
        if len(ad_parts) >= 2:
            ad_group_id = ad_parts[0].split('/')[-1]
            ad_id = ad_parts[1]

            label_resource = (
                f"customers/{customer_id}/adGroupAdLabels/"
                f"{ad_group_id}~{ad_id}~{label_id}"
            )

            operation = client.get_type("AdGroupAdLabelOperation")
            operation.remove = label_resource
            operations.append(operation)

    if dry_run:
        logger.info(
            f"[{customer_id}] DRY RUN: Would remove THEMA_ORIGINAL from "
            f"{len(operations)} ads"
        )
        return len(operations), 0

    # Process in batches of 1000
    BATCH_SIZE = 1000
    for i in range(0, len(operations), BATCH_SIZE):
        batch = operations[i:i+BATCH_SIZE]
        try:
            response = ad_group_ad_label_service.mutate_ad_group_ad_labels(
                customer_id=customer_id,
                operations=batch
            )
            batch_success = len(response.results)
            success_count += batch_success
            logger.info(
                f"[{customer_id}] Removed THEMA_ORIGINAL from {batch_success} ads "
                f"(batch {i//BATCH_SIZE + 1}/{(len(operations)-1)//BATCH_SIZE + 1})"
            )
        except GoogleAdsException as ex:
            batch_failure = len(batch)
            failure_count += batch_failure
            logger.error(
                f"[{customer_id}] Failed to remove labels from batch "
                f"{i//BATCH_SIZE + 1}: {ex}"
            )

    return success_count, failure_count


async def process_customer(
    client: GoogleAdsClient,
    customer_id: str,
    dry_run: bool = False
) -> Dict[str, int]:
    """Process a single customer."""
    logger.info(f"[{customer_id}] Starting cleanup...")

    # Get label IDs
    label_ids = get_label_ids(client, customer_id)
    if THEMA_ORIGINAL_LABEL not in label_ids:
        return {'checked': 0, 'fixed': 0, 'failed': 0}

    # Find ads with conflicting labels
    conflicting_ads = find_ads_with_conflicting_labels(client, customer_id, label_ids)

    if not conflicting_ads:
        logger.info(f"[{customer_id}] No conflicting labels found ✓")
        return {'checked': 0, 'fixed': 0, 'failed': 0}

    # Log examples
    for ad_resource, theme_labels in conflicting_ads[:3]:
        logger.info(
            f"[{customer_id}] Example: {ad_resource} has "
            f"THEMA_ORIGINAL + {', '.join(theme_labels)}"
        )

    # Remove THEMA_ORIGINAL labels
    success, failure = remove_thema_original_labels(
        client,
        customer_id,
        conflicting_ads,
        label_ids[THEMA_ORIGINAL_LABEL],
        dry_run=dry_run
    )

    logger.info(
        f"[{customer_id}] Completed: {success} fixed, {failure} failed"
    )

    return {
        'checked': len(conflicting_ads),
        'fixed': success,
        'failed': failure
    }


async def main(dry_run: bool = False, limit: int = None):
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("THEMA_ORIGINAL Label Cleanup Script")
    logger.info("=" * 80)
    logger.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    logger.info(f"Processing: {len(VALID_CUSTOMERS)} customers")
    if limit:
        logger.info(f"Limit: First {limit} customers only")
    logger.info("=" * 80)

    # Initialize client
    client = initialize_client()

    # Process customers
    customers_to_process = VALID_CUSTOMERS[:limit] if limit else VALID_CUSTOMERS

    total_checked = 0
    total_fixed = 0
    total_failed = 0

    for customer_id in customers_to_process:
        try:
            result = await process_customer(client, customer_id, dry_run=dry_run)
            total_checked += result['checked']
            total_fixed += result['fixed']
            total_failed += result['failed']
        except Exception as e:
            logger.error(f"[{customer_id}] Unexpected error: {e}")
            continue

    # Summary
    logger.info("=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total ads with conflicting labels: {total_checked}")
    logger.info(f"Successfully fixed: {total_fixed}")
    logger.info(f"Failed: {total_failed}")
    logger.info("=" * 80)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Remove THEMA_ORIGINAL labels from ads that have theme labels'
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        help='Execute the cleanup (default is dry-run mode)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit to first N customers (for testing)'
    )

    args = parser.parse_args()

    asyncio.run(main(dry_run=not args.execute, limit=args.limit))
