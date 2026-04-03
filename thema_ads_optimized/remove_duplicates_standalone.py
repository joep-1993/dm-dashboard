#!/usr/bin/env python3
"""
Standalone script to remove duplicate ads across all customers.
This bypasses the API to avoid timeout issues for long-running operations.
"""
import os
import sys
import asyncio
import logging
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set, Tuple

from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Add parent directory to path
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

# Valid customer IDs
VALID_CUSTOMERS = [
    '4056770576', '1496704472', '4964513580', '3114657125', '5807833423',
    '3273661472', '9251309631', '8273243429', '8696777335', '5930401821',
    '6213822688', '6379322129', '2237802672', '8338942127', '9525057729',
    '8431844135', '6511658729', '4675585929', '5105960927', '4567815835',
    '1351439239', '5122292229', '7346695290', '5550062935', '4761604080',
    '6044293584', '6271552035', '8755979133'
]

THEME_LABELS = {'THEME_BF', 'THEME_CM', 'THEME_SK', 'THEME_KM', 'THEME_SD'}
CHECKED_LABEL_NAME = 'THEME_DUPLICATES_CHECK'


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


def get_or_create_label(client: GoogleAdsClient, customer_id: str, label_name: str) -> str:
    """Get or create a label and return its resource name."""
    ga_service = client.get_service("GoogleAdsService")

    # Try to find existing label
    label_query = f"SELECT label.resource_name FROM label WHERE label.name = '{label_name}'"
    try:
        response = ga_service.search(customer_id=customer_id, query=label_query)
        for row in response:
            logger.info(f"[{customer_id}] Found existing {label_name} label")
            return row.label.resource_name
    except:
        pass

    # Create new label
    label_service = client.get_service("LabelService")
    label_operation = client.get_type("LabelOperation")
    label = label_operation.create
    label.name = label_name

    try:
        response = label_service.mutate_labels(
            customer_id=customer_id,
            operations=[label_operation]
        )
        resource_name = response.results[0].resource_name
        logger.info(f"[{customer_id}] Created {label_name} label")
        return resource_name
    except Exception as e:
        logger.error(f"[{customer_id}] Failed to create {label_name} label: {e}")
        return None


def get_ad_groups(client: GoogleAdsClient, customer_id: str,
                  checked_label_resource: str = None,
                  reset_labels: bool = False,
                  limit: int = None) -> List[Tuple[str, str]]:
    """Get ENABLED ad groups in HS/ campaigns."""
    ga_service = client.get_service("GoogleAdsService")

    # Get all ENABLED ad groups in HS/ campaigns
    ag_query = """
        SELECT
            ad_group.id,
            ad_group.name
        FROM ad_group
        WHERE ad_group.status = 'ENABLED'
        AND campaign.status = 'ENABLED'
        AND campaign.name LIKE 'HS/%'
    """

    ag_response = ga_service.search(customer_id=customer_id, query=ag_query)
    all_ad_groups = [(str(row.ad_group.id), row.ad_group.name) for row in ag_response]

    # Filter out already-checked ad groups unless reset_labels
    if not reset_labels and checked_label_resource:
        checked_ags = set()
        ag_label_query = f"""
            SELECT ad_group_label.ad_group
            FROM ad_group_label
            WHERE ad_group_label.label = '{checked_label_resource}'
        """
        try:
            response = ga_service.search(customer_id=customer_id, query=ag_label_query)
            for row in response:
                ag_id = row.ad_group_label.ad_group.split('/')[-1]
                checked_ags.add(ag_id)
            logger.info(f"[{customer_id}] Skipping {len(checked_ags)} already-checked ad groups")
        except:
            pass

        ad_groups = [(ag_id, ag_name) for ag_id, ag_name in all_ad_groups if ag_id not in checked_ags]
    else:
        ad_groups = all_ad_groups

    if limit:
        ad_groups = ad_groups[:limit]

    return ad_groups


def get_ads_for_ad_groups(client: GoogleAdsClient, customer_id: str,
                          ad_groups: List[Tuple[str, str]]) -> Dict:
    """Batch fetch all ads for given ad groups."""
    ga_service = client.get_service("GoogleAdsService")
    all_ads_by_ag = {}
    ad_group_names = {ag_id: ag_name for ag_id, ag_name in ad_groups}

    batch_size = 1000
    for i in range(0, len(ad_groups), batch_size):
        batch_ad_groups = ad_groups[i:i + batch_size]
        ag_ids_in_batch = [ag_id for ag_id, _ in batch_ad_groups]

        ag_resources = [f"'customers/{customer_id}/adGroups/{ag_id}'" for ag_id in ag_ids_in_batch]
        in_clause = ", ".join(ag_resources)

        ads_query = f"""
            SELECT
                ad_group_ad.ad_group,
                ad_group_ad.ad.id,
                ad_group_ad.resource_name,
                ad_group_ad.status,
                ad_group_ad.ad.responsive_search_ad.headlines,
                ad_group_ad.ad.responsive_search_ad.descriptions
            FROM ad_group_ad
            WHERE ad_group_ad.ad_group IN ({in_clause})
            AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
            AND ad_group_ad.status != REMOVED
        """

        try:
            ads_response = ga_service.search(customer_id=customer_id, query=ads_query)

            for row in ads_response:
                ag_resource = row.ad_group_ad.ad_group
                ag_id = ag_resource.split('/')[-1]
                ad_id = str(row.ad_group_ad.ad.id)
                rsa = row.ad_group_ad.ad.responsive_search_ad

                ad_data = {
                    'ad_id': ad_id,
                    'resource_name': row.ad_group_ad.resource_name,
                    'status': str(row.ad_group_ad.status),
                    'headlines': [h.text for h in rsa.headlines] if rsa.headlines else [],
                    'descriptions': [d.text for d in rsa.descriptions] if rsa.descriptions else []
                }

                if ag_id not in all_ads_by_ag:
                    all_ads_by_ag[ag_id] = {'name': ad_group_names.get(ag_id, 'Unknown'), 'ads': []}

                all_ads_by_ag[ag_id]['ads'].append(ad_data)

        except Exception as e:
            logger.error(f"[{customer_id}] Error fetching ads for batch: {e}")
            continue

    return all_ads_by_ag


def get_ad_labels(client: GoogleAdsClient, customer_id: str,
                  ad_ids: List[str]) -> Dict[str, Set[str]]:
    """Get labels for all ads."""
    ga_service = client.get_service("GoogleAdsService")
    ad_labels = defaultdict(set)

    batch_size = 10000
    for i in range(0, len(ad_ids), batch_size):
        batch_ids = ad_ids[i:i + batch_size]
        ad_resources = [f"'customers/{customer_id}/ads/{ad_id}'" for ad_id in batch_ids]
        in_clause = ", ".join(ad_resources)

        label_query = f"""
            SELECT
                ad_group_ad_label.ad_group_ad,
                label.name
            FROM ad_group_ad_label
            WHERE ad_group_ad_label.ad_group_ad IN (
                SELECT ad_group_ad.resource_name
                FROM ad_group_ad
                WHERE ad_group_ad.ad.id IN ({','.join(batch_ids)})
            )
        """

        try:
            response = ga_service.search(customer_id=customer_id, query=label_query)
            for row in response:
                ad_resource = row.ad_group_ad_label.ad_group_ad
                ad_id = ad_resource.split('~')[-1]
                label_name = row.label.name
                ad_labels[ad_id].add(label_name)
        except Exception as e:
            logger.warning(f"[{customer_id}] Error fetching labels: {e}")
            continue

    return ad_labels


def find_duplicates(ads_by_ag: Dict, ad_labels: Dict) -> List[Dict]:
    """Find duplicate ads within each ad group."""
    duplicates_to_remove = []

    for ag_id, ag_data in ads_by_ag.items():
        ads = ag_data['ads']
        if len(ads) < 2:
            continue

        # Group ads by content
        content_map = defaultdict(list)
        for ad in ads:
            headlines = tuple(sorted(ad['headlines']))
            descriptions = tuple(sorted(ad['descriptions']))
            content_key = (headlines, descriptions)
            content_map[content_key].append(ad)

        # Find duplicates
        for content_key, duplicate_ads in content_map.items():
            if len(duplicate_ads) <= 1:
                continue

            # Score ads (higher score = keep)
            scored_ads = []
            for ad in duplicate_ads:
                ad_id = ad['ad_id']
                labels = ad_labels.get(ad_id, set())

                score = 0
                # Theme-labeled ads score higher
                if any(label in THEME_LABELS for label in labels):
                    score += 10
                # ENABLED ads score higher than PAUSED
                if 'ENABLED' in ad['status']:
                    score += 1

                scored_ads.append((score, ad))

            # Sort by score (highest first)
            scored_ads.sort(key=lambda x: x[0], reverse=True)

            # Mark all but the highest-scored for removal
            for score, ad in scored_ads[1:]:
                duplicates_to_remove.append({
                    'ad_group_id': ag_id,
                    'ad_group_name': ag_data['name'],
                    'ad_id': ad['ad_id'],
                    'resource_name': ad['resource_name'],
                    'score': score
                })

    return duplicates_to_remove


def remove_ads(client: GoogleAdsClient, customer_id: str,
               ads_to_remove: List[Dict], dry_run: bool = True) -> int:
    """Remove duplicate ads."""
    if dry_run:
        logger.info(f"[{customer_id}] DRY RUN: Would remove {len(ads_to_remove)} ads")
        return len(ads_to_remove)

    ad_service = client.get_service("AdGroupAdService")
    removed_count = 0

    # Remove in batches
    batch_size = 500
    for i in range(0, len(ads_to_remove), batch_size):
        batch = ads_to_remove[i:i + batch_size]
        operations = []

        for ad in batch:
            operation = client.get_type("AdGroupAdOperation")
            operation.remove = ad['resource_name']
            operations.append(operation)

        try:
            response = ad_service.mutate_ad_group_ads(
                customer_id=customer_id,
                operations=operations
            )
            removed_count += len(response.results)
            logger.info(f"[{customer_id}] Removed {len(response.results)} ads")
        except GoogleAdsException as ex:
            logger.error(f"[{customer_id}] Failed to remove ads: {ex}")
            continue

    return removed_count


def label_ad_groups(client: GoogleAdsClient, customer_id: str,
                   ad_group_ids: List[str], label_resource: str, dry_run: bool = True):
    """Label ad groups as checked."""
    if dry_run or not label_resource:
        return

    ag_label_service = client.get_service("AdGroupLabelService")
    operations = []

    for ag_id in ad_group_ids:
        operation = client.get_type("AdGroupLabelOperation")
        ag_label = operation.create
        ag_label.ad_group = f"customers/{customer_id}/adGroups/{ag_id}"
        ag_label.label = label_resource
        operations.append(operation)

    if not operations:
        return

    # Apply in batches
    batch_size = 5000
    for i in range(0, len(operations), batch_size):
        batch = operations[i:i + batch_size]
        try:
            ag_label_service.mutate_ad_group_labels(
                customer_id=customer_id,
                operations=batch
            )
        except:
            pass  # Label might already exist


async def process_customer(client: GoogleAdsClient, customer_id: str,
                          dry_run: bool = True, reset_labels: bool = False,
                          limit: int = None) -> Dict:
    """Process a single customer."""
    logger.info(f"[{customer_id}] Starting processing...")

    stats = {
        'ad_groups_checked': 0,
        'ad_groups_with_duplicates': 0,
        'duplicate_sets_found': 0,
        'ads_removed': 0
    }

    try:
        # Get or create checked label
        checked_label_resource = None
        if not dry_run:
            checked_label_resource = get_or_create_label(client, customer_id, CHECKED_LABEL_NAME)

        # Get ad groups
        ad_groups = get_ad_groups(client, customer_id, checked_label_resource, reset_labels, limit)
        stats['ad_groups_checked'] = len(ad_groups)

        if not ad_groups:
            logger.info(f"[{customer_id}] No ad groups to check")
            return stats

        logger.info(f"[{customer_id}] Checking {len(ad_groups)} ad groups")

        # Get all ads
        ads_by_ag = get_ads_for_ad_groups(client, customer_id, ad_groups)

        # Get all ad IDs
        all_ad_ids = []
        for ag_data in ads_by_ag.values():
            all_ad_ids.extend([ad['ad_id'] for ad in ag_data['ads']])

        # Get labels for all ads
        ad_labels = get_ad_labels(client, customer_id, all_ad_ids)

        # Find duplicates
        duplicates_to_remove = find_duplicates(ads_by_ag, ad_labels)

        # Count ad groups with duplicates
        ag_ids_with_duplicates = set(d['ad_group_id'] for d in duplicates_to_remove)
        stats['ad_groups_with_duplicates'] = len(ag_ids_with_duplicates)
        stats['duplicate_sets_found'] = len(ag_ids_with_duplicates)  # Approximate

        if duplicates_to_remove:
            logger.info(f"[{customer_id}] Found {len(duplicates_to_remove)} duplicate ads in {len(ag_ids_with_duplicates)} ad groups")

            # Remove duplicates
            removed = remove_ads(client, customer_id, duplicates_to_remove, dry_run)
            stats['ads_removed'] = removed
        else:
            logger.info(f"[{customer_id}] No duplicates found")

        # Label ad groups as checked
        if not dry_run and checked_label_resource:
            label_ad_groups(client, customer_id, [ag_id for ag_id, _ in ad_groups],
                          checked_label_resource, dry_run)

        logger.info(f"[{customer_id}] Completed: {stats}")
        return stats

    except Exception as e:
        logger.error(f"[{customer_id}] Failed: {e}")
        return stats


async def main(dry_run: bool = True, reset_labels: bool = False,
               limit: int = None, customer_ids: List[str] = None,
               workers: int = 1):
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("Remove Duplicates - Standalone Script")
    logger.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    logger.info(f"Reset labels: {reset_labels}")
    logger.info(f"Limit per customer: {limit if limit else 'None (all ad groups)'}")
    logger.info(f"Parallel workers: {workers}")
    logger.info("=" * 80)

    # Initialize client
    client = initialize_client()

    # Determine customers to process
    customers = customer_ids if customer_ids else VALID_CUSTOMERS
    logger.info(f"Processing {len(customers)} customers")

    # Process all customers
    total_stats = {
        'customers_processed': 0,
        'ad_groups_checked': 0,
        'ad_groups_with_duplicates': 0,
        'duplicate_sets_found': 0,
        'ads_removed': 0
    }

    if workers > 1:
        # Parallel processing with semaphore
        semaphore = asyncio.Semaphore(workers)

        async def process_with_semaphore(customer_id):
            async with semaphore:
                try:
                    return await process_customer(client, customer_id, dry_run, reset_labels, limit)
                except Exception as e:
                    logger.error(f"[{customer_id}] Unexpected error: {e}")
                    return {
                        'ad_groups_checked': 0,
                        'ad_groups_with_duplicates': 0,
                        'duplicate_sets_found': 0,
                        'ads_removed': 0
                    }

        # Process all customers in parallel (limited by semaphore)
        results = await asyncio.gather(*[process_with_semaphore(cid) for cid in customers])

        # Aggregate results
        for stats in results:
            if stats['ad_groups_checked'] > 0:
                total_stats['customers_processed'] += 1
            total_stats['ad_groups_checked'] += stats['ad_groups_checked']
            total_stats['ad_groups_with_duplicates'] += stats['ad_groups_with_duplicates']
            total_stats['duplicate_sets_found'] += stats['duplicate_sets_found']
            total_stats['ads_removed'] += stats['ads_removed']
    else:
        # Sequential processing
        for customer_id in customers:
            try:
                stats = await process_customer(client, customer_id, dry_run, reset_labels, limit)
                total_stats['customers_processed'] += 1
                total_stats['ad_groups_checked'] += stats['ad_groups_checked']
                total_stats['ad_groups_with_duplicates'] += stats['ad_groups_with_duplicates']
                total_stats['duplicate_sets_found'] += stats['duplicate_sets_found']
                total_stats['ads_removed'] += stats['ads_removed']
            except Exception as e:
                logger.error(f"[{customer_id}] Unexpected error: {e}")
                continue

    # Summary
    logger.info("=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Customers processed: {total_stats['customers_processed']}/{len(customers)}")
    logger.info(f"Ad groups checked: {total_stats['ad_groups_checked']}")
    logger.info(f"Ad groups with duplicates: {total_stats['ad_groups_with_duplicates']}")
    logger.info(f"Duplicate sets found: {total_stats['duplicate_sets_found']}")
    logger.info(f"Ads {'that would be' if dry_run else ''} removed: {total_stats['ads_removed']}")
    logger.info("=" * 80)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Remove duplicate ads across all customers'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=True,
        help='Run in dry-run mode (no actual changes)'
    )
    parser.add_argument(
        '--live',
        action='store_true',
        help='Run in LIVE mode (actually remove duplicates)'
    )
    parser.add_argument(
        '--reset-labels',
        action='store_true',
        help='Reset and re-check already-checked ad groups'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit ad groups per customer (for testing)'
    )
    parser.add_argument(
        '--customers',
        nargs='+',
        help='Specific customer IDs to process'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=1,
        help='Number of parallel workers (default: 1)'
    )

    args = parser.parse_args()

    # Determine dry_run mode
    dry_run = not args.live

    asyncio.run(main(
        dry_run=dry_run,
        reset_labels=args.reset_labels,
        limit=args.limit,
        customer_ids=args.customers,
        workers=args.workers
    ))
