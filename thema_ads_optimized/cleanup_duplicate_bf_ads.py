#!/usr/bin/env python3
"""
Find and remove duplicate Black Friday RSAs.

This script identifies ad groups that have multiple RSAs with Black Friday content,
keeping the ones with THEME_BF label and removing the unlabeled duplicates from Job 172.
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Dict, Set
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Load .env file first
from dotenv import load_dotenv
load_dotenv()

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google_ads_client import initialize_client
from config import load_config_from_env

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DuplicateAdCleaner:
    """Clean up duplicate Black Friday RSAs."""

    def __init__(self, dry_run: bool = True):
        config = load_config_from_env()
        self.client = initialize_client(config.google_ads)
        self.dry_run = dry_run

    async def find_duplicates(self, customer_id: str) -> Dict[str, List[dict]]:
        """Find ad groups with duplicate RSAs.

        Returns:
            Dict mapping ad_group_id -> list of duplicate ads (unlabeled ones)
        """
        logger.info(f"Scanning customer {customer_id} for duplicate Black Friday RSAs...")

        ga_service = self.client.get_service("GoogleAdsService")

        # Query for all RSAs in ad groups that have THEME_BF_DONE label
        query = """
            SELECT
                ad_group.id,
                ad_group.name,
                ad_group_ad.ad.id,
                ad_group_ad.ad.responsive_search_ad.headlines,
                ad_group_ad.ad.responsive_search_ad.descriptions,
                ad_group_ad.ad.final_urls,
                ad_group_ad.status,
                ad_group_ad.labels,
                ad_group.labels
            FROM ad_group_ad
            WHERE
                ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                AND ad_group_ad.status != REMOVED
                AND ad_group.status != REMOVED
        """

        def _query():
            try:
                response = ga_service.search(customer_id=customer_id, query=query)
                return list(response)
            except GoogleAdsException as e:
                logger.error(f"Query failed for customer {customer_id}: {e}")
                return []

        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(None, _query)

        logger.info(f"Found {len(rows)} RSAs to analyze")

        # Group ads by ad group
        ad_groups = {}
        bf_labeled_ad_groups = set()

        for row in rows:
            ag_id = str(row.ad_group.id)
            ad_id = str(row.ad_group_ad.ad.id)

            # Check if ad group has THEME_BF_DONE label
            ag_labels = [label.split('/')[-1] for label in row.ad_group.labels]
            if 'THEME_BF_DONE' in ag_labels:
                bf_labeled_ad_groups.add(ag_id)

            # Check ad labels
            ad_labels = [label.split('/')[-1] for label in row.ad_group_ad.labels]
            has_theme_bf = 'THEME_BF' in ad_labels

            # Check if RSA contains Black Friday content
            headlines = [h.text for h in row.ad_group_ad.ad.responsive_search_ad.headlines]
            descriptions = [d.text for d in row.ad_group_ad.ad.responsive_search_ad.descriptions]

            # Simple check: look for Black Friday related keywords in headlines
            bf_keywords = ['Black Friday', 'black friday', 'BLACK FRIDAY', 'BF', 'Black-Friday']
            has_bf_content = any(
                any(keyword in text for keyword in bf_keywords)
                for text in headlines + descriptions
            )

            if has_bf_content:
                if ag_id not in ad_groups:
                    ad_groups[ag_id] = {
                        'name': row.ad_group.name,
                        'ads': []
                    }

                ad_groups[ag_id]['ads'].append({
                    'ad_id': ad_id,
                    'resource_name': row.ad_group_ad.resource_name,
                    'has_theme_bf_label': has_theme_bf,
                    'status': row.ad_group_ad.status.name,
                    'headlines': headlines[:3],  # Sample
                    'final_urls': list(row.ad_group_ad.ad.final_urls)
                })

        # Find ad groups with duplicates
        duplicates = {}
        for ag_id, data in ad_groups.items():
            # Only process ad groups with THEME_BF_DONE label (were processed)
            if ag_id not in bf_labeled_ad_groups:
                continue

            if len(data['ads']) > 1:
                # Find ads without THEME_BF label (duplicates from Job 172)
                unlabeled_ads = [ad for ad in data['ads'] if not ad['has_theme_bf_label']]
                labeled_ads = [ad for ad in data['ads'] if ad['has_theme_bf_label']]

                if unlabeled_ads and labeled_ads:
                    duplicates[ag_id] = {
                        'name': data['name'],
                        'total_ads': len(data['ads']),
                        'unlabeled': unlabeled_ads,
                        'labeled': labeled_ads
                    }

        logger.info(f"Found {len(duplicates)} ad groups with duplicate RSAs")
        return duplicates

    async def remove_duplicates(self, customer_id: str, duplicates: Dict[str, List[dict]]) -> dict:
        """Remove unlabeled duplicate RSAs.

        Returns:
            Statistics dict with counts
        """
        if not duplicates:
            logger.info("No duplicates to remove")
            return {'removed': 0, 'failed': 0}

        # Collect all unlabeled ad resource names
        ads_to_remove = []
        for ag_id, data in duplicates.items():
            for ad in data['unlabeled']:
                ads_to_remove.append(ad['resource_name'])

        logger.info(f"Planning to remove {len(ads_to_remove)} unlabeled duplicate RSAs")

        if self.dry_run:
            logger.info("DRY RUN: Would remove the following ads:")
            for ag_id, data in list(duplicates.items())[:10]:  # Show first 10
                logger.info(f"  Ad Group {ag_id} ({data['name']}): {len(data['unlabeled'])} unlabeled ads")

            if len(duplicates) > 10:
                logger.info(f"  ... and {len(duplicates) - 10} more ad groups")

            return {'removed': len(ads_to_remove), 'failed': 0, 'dry_run': True}

        # Actually remove ads in batches
        service = self.client.get_service("AdGroupAdService")
        removed = 0
        failed = 0

        BATCH_SIZE = 100
        for i in range(0, len(ads_to_remove), BATCH_SIZE):
            batch = ads_to_remove[i:i + BATCH_SIZE]

            def _remove_batch():
                operations = []
                for resource_name in batch:
                    op = self.client.get_type("AdGroupAdOperation")
                    op.remove = resource_name
                    operations.append(op)

                try:
                    response = service.mutate_ad_group_ads(
                        customer_id=customer_id,
                        operations=operations
                    )
                    return len(response.results), 0
                except GoogleAdsException as e:
                    logger.error(f"Batch removal failed: {e}")
                    return 0, len(batch)

            loop = asyncio.get_event_loop()
            batch_removed, batch_failed = await loop.run_in_executor(None, _remove_batch)

            removed += batch_removed
            failed += batch_failed

            logger.info(f"Progress: {removed}/{len(ads_to_remove)} removed, {failed} failed")

            # Small delay between batches
            await asyncio.sleep(1.0)

        return {'removed': removed, 'failed': failed}


async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Clean up duplicate Black Friday RSAs')
    parser.add_argument('--customer-id', required=True, help='Customer ID to process')
    parser.add_argument('--execute', action='store_true', help='Actually remove duplicates (default is dry-run)')
    parser.add_argument('--all-customers', action='store_true', help='Process all customers (8338942127, 9525057729)')

    args = parser.parse_args()

    dry_run = not args.execute

    if dry_run:
        logger.info("="*60)
        logger.info("DRY RUN MODE - No ads will be removed")
        logger.info("Use --execute flag to actually remove duplicates")
        logger.info("="*60)
    else:
        logger.warning("="*60)
        logger.warning("EXECUTE MODE - Ads will be PERMANENTLY REMOVED")
        logger.warning("="*60)
        response = input("Are you sure you want to continue? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Cancelled by user")
            return

    cleaner = DuplicateAdCleaner(dry_run=dry_run)

    # Determine which customers to process
    if args.all_customers:
        customer_ids = ['8338942127', '9525057729']
    else:
        customer_ids = [args.customer_id]

    total_stats = {'removed': 0, 'failed': 0}

    for customer_id in customer_ids:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing Customer: {customer_id}")
        logger.info(f"{'='*60}\n")

        # Find duplicates
        duplicates = await cleaner.find_duplicates(customer_id)

        if duplicates:
            # Show sample
            logger.info(f"\nSample duplicate ad groups (first 5):")
            for ag_id, data in list(duplicates.items())[:5]:
                logger.info(f"\n  Ad Group {ag_id}: {data['name']}")
                logger.info(f"    Total RSAs: {data['total_ads']}")
                logger.info(f"    Unlabeled (to remove): {len(data['unlabeled'])}")
                logger.info(f"    Labeled (to keep): {len(data['labeled'])}")

                if data['unlabeled']:
                    logger.info(f"    Sample unlabeled ad:")
                    ad = data['unlabeled'][0]
                    logger.info(f"      Status: {ad['status']}")
                    logger.info(f"      Headlines: {', '.join(ad['headlines'])}")

            # Remove duplicates
            stats = await cleaner.remove_duplicates(customer_id, duplicates)

            total_stats['removed'] += stats['removed']
            total_stats['failed'] += stats['failed']
        else:
            logger.info("No duplicates found for this customer")

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY")
    logger.info(f"{'='*60}")

    if dry_run:
        logger.info(f"DRY RUN: Would remove {total_stats['removed']} duplicate RSAs")
    else:
        logger.info(f"Removed: {total_stats['removed']} duplicate RSAs")
        logger.info(f"Failed: {total_stats['failed']} RSAs")

    logger.info(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
