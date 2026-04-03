"""Label operations for campaigns, ad groups, and ads."""

import asyncio
import logging
from typing import List, Dict
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from utils.retry import async_retry

logger = logging.getLogger(__name__)


@async_retry(max_attempts=3, delay=1.0)
async def ensure_labels_exist(
    client: GoogleAdsClient,
    customer_id: str,
    label_names: List[str],
    existing_labels: Dict[str, str]
) -> Dict[str, str]:
    """Ensure labels exist, create missing ones. Returns {label_name: resource_name}."""

    def _ensure():
        # Check which labels need to be created
        needed = [name for name in label_names if name not in existing_labels]

        if not needed:
            return existing_labels

        # Batch create missing labels
        label_service = client.get_service("LabelService")
        operations = []

        for label_name in needed:
            op = client.get_type("LabelOperation")
            op.create.name = label_name
            operations.append(op)

        try:
            response = label_service.mutate_labels(
                customer_id=customer_id,
                operations=operations
            )

            # Update the map
            result = existing_labels.copy()
            for i, res in enumerate(response.results):
                result[needed[i]] = res.resource_name

            logger.info(f"Created {len(needed)} new labels for customer {customer_id}")
            return result

        except GoogleAdsException as e:
            logger.error(f"Failed to create labels: {e}")
            # Return what we have
            return existing_labels

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _ensure)


@async_retry(max_attempts=3, delay=1.0)
async def label_ads_batch(
    client: GoogleAdsClient,
    customer_id: str,
    ad_label_pairs: List[tuple]  # [(ad_group_ad_resource, label_resource), ...]
) -> int:
    """Label multiple ads in batch. Returns count of successful labels.

    Google Ads API limits to 10,000 operations per request.
    This function automatically chunks larger batches.
    """

    def _label():
        if not ad_label_pairs:
            return 0

        # DEDUPLICATE: Remove duplicate ad-label pairs to avoid "Cannot mutate the same resource twice" error
        unique_pairs = list(set(ad_label_pairs))
        if len(unique_pairs) < len(ad_label_pairs):
            logger.info(f"Deduplicated ad labels: {len(ad_label_pairs)} -> {len(unique_pairs)} pairs")

        service = client.get_service("AdGroupAdLabelService")
        total_labeled = 0

        # Google Ads API limit: 10,000 operations per request
        BATCH_LIMIT = 10000

        # Process in chunks
        for chunk_start in range(0, len(unique_pairs), BATCH_LIMIT):
            chunk = unique_pairs[chunk_start:chunk_start + BATCH_LIMIT]
            operations = []

            for ad_resource, label_resource in chunk:
                op = client.get_type("AdGroupAdLabelOperation")
                op.create.ad_group_ad = ad_resource
                op.create.label = label_resource
                operations.append(op)

            try:
                response = service.mutate_ad_group_ad_labels(
                    customer_id=customer_id,
                    operations=operations
                )
                total_labeled += len(response.results)
                logger.debug(f"Labeled {len(response.results)} ads in chunk {chunk_start//BATCH_LIMIT + 1}")

            except GoogleAdsException as e:
                logger.warning(f"Some ad labels failed in chunk {chunk_start//BATCH_LIMIT + 1}: {e}")

        logger.info(f"Labeled {total_labeled} ads total")
        return total_labeled

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _label)


@async_retry(max_attempts=3, delay=1.0)
async def label_ad_groups_batch(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_label_pairs: List[tuple]  # [(ad_group_resource, label_resource), ...]
) -> int:
    """Label multiple ad groups in batch. Returns count of successful labels.

    Google Ads API limits to 10,000 operations per request.
    This function automatically chunks larger batches.
    """

    def _label():
        if not ad_group_label_pairs:
            return 0

        # DEDUPLICATE: Remove duplicate ad group-label pairs to avoid "Cannot mutate the same resource twice" error
        unique_pairs = list(set(ad_group_label_pairs))
        if len(unique_pairs) < len(ad_group_label_pairs):
            logger.info(f"Deduplicated ad group labels: {len(ad_group_label_pairs)} -> {len(unique_pairs)} pairs")

        service = client.get_service("AdGroupLabelService")
        total_labeled = 0

        # Google Ads API limit: 10,000 operations per request
        BATCH_LIMIT = 10000

        # Process in chunks
        for chunk_start in range(0, len(unique_pairs), BATCH_LIMIT):
            chunk = unique_pairs[chunk_start:chunk_start + BATCH_LIMIT]
            operations = []

            for ag_resource, label_resource in chunk:
                op = client.get_type("AdGroupLabelOperation")
                op.create.ad_group = ag_resource
                op.create.label = label_resource
                operations.append(op)

            try:
                response = service.mutate_ad_group_labels(
                    customer_id=customer_id,
                    operations=operations
                )
                total_labeled += len(response.results)
                logger.debug(f"Labeled {len(response.results)} ad groups in chunk {chunk_start//BATCH_LIMIT + 1}")

            except GoogleAdsException as e:
                logger.warning(f"Some ad group labels failed in chunk {chunk_start//BATCH_LIMIT + 1}: {e}")

        logger.info(f"Labeled {total_labeled} ad groups total")
        return total_labeled

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _label)


@async_retry(max_attempts=3, delay=1.0)
async def get_ads_by_label(
    client: GoogleAdsClient,
    customer_id: str,
    label_name: str,
    exclude_removed: bool = True
) -> List[str]:
    """Get all ad resource names that have a specific label.

    Args:
        exclude_removed: If True, exclude ads that are already REMOVED

    Returns:
        List of ad_group_ad resource names
    """

    def _fetch():
        ga_service = client.get_service("GoogleAdsService")

        # First, get the label resource name
        label_query = f"""
            SELECT label.resource_name, label.name
            FROM label
            WHERE label.name = '{label_name}'
        """

        label_resource = None
        try:
            label_response = ga_service.search(customer_id=customer_id, query=label_query)
            for row in label_response:
                label_resource = row.label.resource_name
                break
        except Exception as e:
            logger.warning(f"Failed to find label '{label_name}': {e}")
            return []

        if not label_resource:
            logger.info(f"Label '{label_name}' does not exist")
            return []

        # Now get all ads with this label, filtering by status
        status_filter = "AND ad_group_ad.status != REMOVED" if exclude_removed else ""
        ads_query = f"""
            SELECT ad_group_ad_label.ad_group_ad, ad_group_ad.status
            FROM ad_group_ad_label
            WHERE ad_group_ad_label.label = '{label_resource}'
              {status_filter}
        """

        ad_resources = []
        try:
            ads_response = ga_service.search(customer_id=customer_id, query=ads_query)
            for row in ads_response:
                ad_resources.append(row.ad_group_ad_label.ad_group_ad)

            logger.info(f"Found {len(ad_resources)} ads with label '{label_name}' (excluded removed: {exclude_removed})")
        except Exception as e:
            logger.error(f"Failed to fetch ads with label '{label_name}': {e}")
            return []

        return ad_resources

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch)


@async_retry(max_attempts=3, delay=1.0)
async def remove_ads_batch(
    client: GoogleAdsClient,
    customer_id: str,
    ad_resource_names: List[str]
) -> int:
    """Remove multiple ads in batch using remove operation.

    Returns:
        Count of successfully removed ads
    """

    def _remove():
        if not ad_resource_names:
            return 0

        service = client.get_service("AdGroupAdService")
        total_removed = 0

        # Google Ads API limit: 10,000 operations per request
        BATCH_LIMIT = 10000

        # Process in chunks
        for chunk_start in range(0, len(ad_resource_names), BATCH_LIMIT):
            chunk = ad_resource_names[chunk_start:chunk_start + BATCH_LIMIT]
            operations = []

            for ad_resource in chunk:
                op = client.get_type("AdGroupAdOperation")
                op.remove = ad_resource
                operations.append(op)

            try:
                response = service.mutate_ad_group_ads(
                    customer_id=customer_id,
                    operations=operations
                )
                total_removed += len(response.results)
                logger.info(f"Removed {len(response.results)} ads in chunk {chunk_start//BATCH_LIMIT + 1}")

            except GoogleAdsException as e:
                # Check if error is about already removed ads
                error_msg = str(e)
                if "CANNOT_OPERATE_ON_REMOVED_ADGROUPAD" in error_msg:
                    logger.warning(f"Chunk {chunk_start//BATCH_LIMIT + 1}: Some ads were already removed, skipping...")
                else:
                    logger.error(f"Failed to remove ads in chunk {chunk_start//BATCH_LIMIT + 1}: {e}")

        logger.info(f"Removed {total_removed} ads total")
        return total_removed

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _remove)


@async_retry(max_attempts=3, delay=1.0)
async def get_ad_groups_by_label(
    client: GoogleAdsClient,
    customer_id: str,
    label_name: str
) -> List[str]:
    """Get all ad group resource names that have a specific label.

    Returns:
        List of ad_group resource names
    """

    def _fetch():
        ga_service = client.get_service("GoogleAdsService")

        # First, get the label resource name
        label_query = f"""
            SELECT label.resource_name, label.name
            FROM label
            WHERE label.name = '{label_name}'
        """

        label_resource = None
        try:
            label_response = ga_service.search(customer_id=customer_id, query=label_query)
            for row in label_response:
                label_resource = row.label.resource_name
                break
        except Exception as e:
            logger.warning(f"Failed to find label '{label_name}': {e}")
            return []

        if not label_resource:
            logger.info(f"Label '{label_name}' does not exist")
            return []

        # Now get all ad groups with this label
        ad_groups_query = f"""
            SELECT ad_group_label.ad_group
            FROM ad_group_label
            WHERE ad_group_label.label = '{label_resource}'
        """

        ad_group_resources = []
        try:
            response = ga_service.search(customer_id=customer_id, query=ad_groups_query)
            for row in response:
                ad_group_resources.append(row.ad_group_label.ad_group)

            logger.info(f"Found {len(ad_group_resources)} ad groups with label '{label_name}'")
        except Exception as e:
            logger.error(f"Failed to fetch ad groups with label '{label_name}': {e}")
            return []

        return ad_group_resources

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch)


@async_retry(max_attempts=3, delay=1.0)
async def remove_ad_group_labels_batch(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_label_resources: List[str]
) -> int:
    """Remove ad group label associations in batch.

    Args:
        ad_group_label_resources: List of ad_group_label resource names to remove

    Returns:
        Count of successfully removed label associations
    """

    def _remove():
        if not ad_group_label_resources:
            return 0

        service = client.get_service("AdGroupLabelService")
        total_removed = 0

        # Google Ads API limit: 10,000 operations per request
        BATCH_LIMIT = 10000

        # Process in chunks
        for chunk_start in range(0, len(ad_group_label_resources), BATCH_LIMIT):
            chunk = ad_group_label_resources[chunk_start:chunk_start + BATCH_LIMIT]
            operations = []

            for label_resource in chunk:
                op = client.get_type("AdGroupLabelOperation")
                op.remove = label_resource
                operations.append(op)

            try:
                response = service.mutate_ad_group_labels(
                    customer_id=customer_id,
                    operations=operations
                )
                total_removed += len(response.results)
                logger.info(f"Removed {len(response.results)} ad group labels in chunk {chunk_start//BATCH_LIMIT + 1}")

            except GoogleAdsException as e:
                logger.warning(f"Some ad group labels failed to remove in chunk {chunk_start//BATCH_LIMIT + 1}: {e}")

        logger.info(f"Removed {total_removed} ad group labels total")
        return total_removed

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _remove)


@async_retry(max_attempts=3, delay=1.0)
async def get_ad_group_label_resources(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resources: List[str],
    label_name: str,
    batch_size: int = 1000
) -> List[str]:
    """Get ad_group_label resource names for specific ad groups and label.

    Args:
        ad_group_resources: List of ad group resource names
        label_name: Name of the label
        batch_size: Max number of ad groups to query at once (API limit)

    Returns:
        List of ad_group_label resource names that can be used for removal
    """

    def _fetch():
        if not ad_group_resources:
            return []

        ga_service = client.get_service("GoogleAdsService")

        # First, get the label resource name
        label_query = f"""
            SELECT label.resource_name, label.name
            FROM label
            WHERE label.name = '{label_name}'
        """

        label_resource = None
        try:
            label_response = ga_service.search(customer_id=customer_id, query=label_query)
            for row in label_response:
                label_resource = row.label.resource_name
                break
        except Exception as e:
            logger.warning(f"Failed to find label '{label_name}': {e}")
            return []

        if not label_resource:
            logger.info(f"Label '{label_name}' does not exist")
            return []

        # Process ad groups in batches to avoid query filter limit
        all_label_resources = []
        total_batches = (len(ad_group_resources) - 1) // batch_size + 1

        for i in range(0, len(ad_group_resources), batch_size):
            batch = ad_group_resources[i:i + batch_size]
            batch_num = i // batch_size + 1

            # Build query for this batch
            resources_str = ", ".join(f"'{r}'" for r in batch)
            query = f"""
                SELECT ad_group_label.resource_name, ad_group_label.ad_group, ad_group_label.label
                FROM ad_group_label
                WHERE ad_group_label.ad_group IN ({resources_str})
                  AND ad_group_label.label = '{label_resource}'
            """

            try:
                response = ga_service.search(customer_id=customer_id, query=query)
                batch_results = []
                for row in response:
                    batch_results.append(row.ad_group_label.resource_name)

                all_label_resources.extend(batch_results)
                logger.info(f"Batch {batch_num}/{total_batches}: Found {len(batch_results)} ad_group_label associations")

            except Exception as e:
                logger.error(f"Failed to fetch ad_group_label resources for batch {batch_num}: {e}")

        logger.info(f"Found {len(all_label_resources)} ad_group_label associations total to remove")
        return all_label_resources

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch)
