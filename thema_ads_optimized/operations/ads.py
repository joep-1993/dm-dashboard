"""Ad operations for creating and managing RSAs."""

import asyncio
import logging
from typing import List, Dict
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from utils.retry import async_retry
from utils.rate_limiter import AdaptiveRateLimiter

logger = logging.getLogger(__name__)

# Global rate limiter instance (shared across all ad creation operations)
# BALANCED settings: Good balance between speed and reliability (2-3x faster than CONSERVATIVE)
_rate_limiter = AdaptiveRateLimiter(
    initial_delay=1.0,      # BALANCED: Moderate start (was 2.0 CONSERVATIVE)
    min_delay=0.5,          # BALANCED: Lower minimum for faster processing (was 1.0 CONSERVATIVE)
    max_delay=10.0,         # BALANCED: Moderate maximum (was 15.0 CONSERVATIVE)
    increase_factor=2.0,    # BALANCED: Normal backoff (was 2.5 CONSERVATIVE)
    decrease_factor=0.95    # BALANCED: Faster reduction when stable (was 0.98 CONSERVATIVE)
)


@async_retry(max_attempts=5, delay=2.0, backoff=2.0)
async def create_rsa_batch(
    client: GoogleAdsClient,
    customer_id: str,
    ad_data_list: List[dict]  # List of ad configurations
) -> dict:
    """Create multiple RSAs in batch. Returns dict with resource names and failures.

    Google Ads API limits to 10,000 operations per request.
    This function automatically chunks larger batches.

    Returns:
        {
            "resources": List[str],  # Successfully created ad resources
            "failures": List[dict]   # Failed items with error info
        }
    """

    def _create_chunk_with_retry(service, chunk, chunk_size):
        """Create a chunk with automatic size reduction on REQUEST_TOO_LARGE."""
        operations = []

        for ad_data in chunk:
            op = client.get_type("AdGroupAdOperation")
            aga = op.create

            aga.ad_group = ad_data["ad_group_resource"]
            aga.status = client.enums.AdGroupAdStatusEnum.PAUSED

            ad = aga.ad
            ad.final_urls.append(ad_data["final_url"])

            rsa = ad.responsive_search_ad

            # Add headlines
            for headline in ad_data.get("headlines", [])[:15]:
                if headline:
                    asset = client.get_type("AdTextAsset")
                    asset.text = headline
                    rsa.headlines.append(asset)

            # Add descriptions
            for description in ad_data.get("descriptions", [])[:4]:
                if description:
                    asset = client.get_type("AdTextAsset")
                    asset.text = description
                    rsa.descriptions.append(asset)

            # Add paths
            if ad_data.get("path1"):
                rsa.path1 = ad_data["path1"]
            if ad_data.get("path2"):
                rsa.path2 = ad_data["path2"]

            operations.append(op)

        try:
            response = service.mutate_ad_group_ads(
                customer_id=customer_id,
                operations=operations
            )
            resource_names = [res.resource_name for res in response.results]
            return {"resources": resource_names, "failures": []}

        except GoogleAdsException as e:
            # Check if it's REQUEST_TOO_LARGE error
            error_msg = str(e)
            is_too_large = "REQUEST_TOO_LARGE" in error_msg or "too large" in error_msg.lower()

            if is_too_large and chunk_size > 100:
                # Retry with smaller chunks (half the size)
                new_chunk_size = chunk_size // 2
                logger.warning(f"REQUEST_TOO_LARGE error, retrying with chunk size {new_chunk_size} (was {chunk_size})")

                all_resources = []
                all_failures = []

                # Split into smaller sub-chunks
                for sub_start in range(0, len(chunk), new_chunk_size):
                    sub_chunk = chunk[sub_start:sub_start + new_chunk_size]
                    result = _create_chunk_with_retry(service, sub_chunk, new_chunk_size)
                    all_resources.extend(result["resources"])
                    all_failures.extend(result["failures"])

                return {"resources": all_resources, "failures": all_failures}
            else:
                # Non-recoverable error or chunk too small - mark all as failed
                failures = []
                for ad_data in chunk:
                    failures.append({
                        "ad_group_resource": ad_data["ad_group_resource"],
                        "error": str(e)
                    })
                return {"resources": [], "failures": failures}

    def _create():
        if not ad_data_list:
            return {"resources": [], "failures": []}

        service = client.get_service("AdGroupAdService")
        all_resource_names = []
        all_failures = []

        # Reduced batch size to avoid overwhelming Google's ad policy crawler
        # Google Ads API limit: 10,000 operations per request
        # But using smaller batches (20) to prevent "Too long" errors with large RSAs
        # and DESTINATION_NOT_WORKING errors caused by CloudFront rate limiting Google's crawler
        BATCH_LIMIT = 20

        # Process in chunks
        for chunk_start in range(0, len(ad_data_list), BATCH_LIMIT):
            chunk = ad_data_list[chunk_start:chunk_start + BATCH_LIMIT]
            chunk_num = chunk_start//BATCH_LIMIT + 1
            total_chunks = (len(ad_data_list)-1)//BATCH_LIMIT + 1

            # Use adaptive delay before processing chunk (except for first chunk)
            if chunk_num > 1:
                _rate_limiter.wait()

            result = _create_chunk_with_retry(service, chunk, BATCH_LIMIT)

            all_resource_names.extend(result["resources"])
            all_failures.extend(result["failures"])

            # Update rate limiter based on results
            if result["resources"]:
                logger.info(f"Created {len(result['resources'])} RSAs in chunk {chunk_num}/{total_chunks}")
                _rate_limiter.on_success()
            if result["failures"]:
                logger.warning(f"Failed to create {len(result['failures'])} RSAs in chunk {chunk_num}/{total_chunks}")
                # Only increase delay if all items failed AND it's a rate limit error (not policy violation)
                if len(result["failures"]) == len(chunk):
                    # Check if this is a policy violation (should not trigger rate limiting)
                    error_messages = [f.get("error", "") for f in result["failures"]]
                    is_policy_violation = any(
                        "PROHIBITED" in msg or "policy" in msg.lower() or "disapproved" in msg.lower()
                        for msg in error_messages
                    )
                    is_rate_limit = any(
                        "RATE_EXCEEDED" in msg or "RESOURCE_EXHAUSTED" in msg or "503" in msg
                        for msg in error_messages
                    )

                    if is_rate_limit:
                        _rate_limiter.on_error("rate_limit")
                        logger.warning(f"Rate limit detected in chunk {chunk_num}/{total_chunks}, increasing delay")
                    elif is_policy_violation:
                        # Policy violation - don't trigger rate limiting
                        logger.info(f"Policy violation in chunk {chunk_num}/{total_chunks}, NOT increasing delay")
                    else:
                        # Unknown error type - apply rate limiting to be safe
                        _rate_limiter.on_error("batch_failure")
                        logger.warning(f"Unknown error type in chunk {chunk_num}/{total_chunks}, increasing delay")

        logger.info(f"Created {len(all_resource_names)} RSAs total, {len(all_failures)} failures across {total_chunks} chunks")

        # Log rate limiter stats
        stats = _rate_limiter.get_stats()
        logger.info(f"Rate limiter stats: delay={stats['current_delay']:.2f}s, success_rate={stats['success_rate']:.2%}")

        return {"resources": all_resource_names, "failures": all_failures}

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _create)


@async_retry(max_attempts=3, delay=1.0)
async def pause_ads_batch(
    client: GoogleAdsClient,
    customer_id: str,
    ad_resource_names: List[str]
) -> int:
    """Pause multiple ads in batch. Returns count of successfully paused ads."""

    def _pause():
        if not ad_resource_names:
            return 0

        service = client.get_service("AdGroupAdService")
        operations = []

        for ad_resource in ad_resource_names:
            op = client.get_type("AdGroupAdOperation")
            op.update.resource_name = ad_resource
            op.update.status = client.enums.AdGroupAdStatusEnum.PAUSED
            op.update_mask.paths.append("status")
            operations.append(op)

        try:
            response = service.mutate_ad_group_ads(
                customer_id=customer_id,
                operations=operations
            )
            logger.debug(f"Paused {len(response.results)} ads")
            return len(response.results)

        except GoogleAdsException as e:
            logger.warning(f"Some ads failed to pause: {e}")
            return 0

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _pause)


def build_ad_data(
    ad_group_resource: str,
    final_url: str,
    base_headlines: List[str],
    base_description: str,
    extra_headlines: List[str],
    extra_descriptions: List[str],
    path1: str,
    path2: str
) -> dict:
    """Build ad data structure for batch creation."""

    all_headlines = (base_headlines or []) + (extra_headlines or [])
    all_headlines = [h for h in all_headlines if h][:15]

    all_descriptions = ([base_description] if base_description else []) + (extra_descriptions or [])
    all_descriptions = [d for d in all_descriptions if d][:4]

    # Add campaign_theme=1 query parameter to the final URL
    if "?" in final_url:
        final_url_with_param = f"{final_url}&campaign_theme=1"
    else:
        final_url_with_param = f"{final_url}?campaign_theme=1"

    return {
        "ad_group_resource": ad_group_resource,
        "final_url": final_url_with_param,
        "headlines": all_headlines,
        "descriptions": all_descriptions,
        "path1": path1,
        "path2": path2
    }
