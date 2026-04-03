"""
Optimized Thema Ads Script - High Performance Version

This script processes Google Ads themed campaigns with the following optimizations:
- Async/parallel processing (20-50x faster)
- Bulk API operations (10x fewer API calls)
- Smart prefetching and caching
- Batch mutations
- Retry logic with exponential backoff
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Dict
from collections import defaultdict
import time

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import load_config_from_env
from google_ads_client import initialize_client
from models import AdGroupInput, ProcessingResult
from processors.data_loader import load_data
from operations.prefetch import prefetch_customer_data
from operations.labels import ensure_labels_exist, label_ads_batch, label_ad_groups_batch
from operations.ads import create_rsa_batch, build_ad_data
from templates.generators import generate_themed_content
from themes import get_theme_label, get_all_theme_labels


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('thema_ads_optimized.log')
    ]
)
logger = logging.getLogger(__name__)


class ThemaAdsProcessor:
    """High-performance processor for themed ad campaigns."""

    def __init__(self, config, batch_size: int = 5000, skip_sd_done_check: bool = False):
        self.config = config
        self.client = initialize_client(config.google_ads)
        self.theme = "singles_day"  # Default theme (legacy)
        # Get all theme labels dynamically + standard labels
        theme_labels = get_all_theme_labels()
        # Generate DONE labels for each theme
        done_labels = [f"{label}_DONE" for label in theme_labels]
        self.label_names = theme_labels + done_labels + ["THEMA_AD", "THEMA_ORIGINAL"]
        self.batch_size = batch_size
        self.skip_sd_done_check = skip_sd_done_check
        logger.info(f"Initialized ThemaAdsProcessor with batch_size={batch_size}, skip_sd_done_check={skip_sd_done_check}")
        logger.info(f"Theme labels: {theme_labels}")
        logger.info(f"DONE labels: {done_labels}")

    async def process_all(self, inputs: List[AdGroupInput]) -> List[ProcessingResult]:
        """Process all ad groups with maximum parallelization."""

        logger.info(f"Starting processing of {len(inputs)} ad groups")
        start_time = time.time()

        # Group by customer_id for optimal batching
        by_customer = defaultdict(list)
        for inp in inputs:
            by_customer[inp.customer_id].append(inp)

        logger.info(f"Processing {len(by_customer)} customers")

        # Process customers in parallel with semaphore
        semaphore = asyncio.Semaphore(self.config.performance.max_concurrent_customers)

        async def process_with_limit(customer_id, customer_inputs):
            async with semaphore:
                result = await self.process_customer(customer_id, customer_inputs)
                # Add delay between customers to avoid rate limits
                await asyncio.sleep(self.config.performance.customer_delay)
                return result

        tasks = [
            process_with_limit(cid, inputs_list)
            for cid, inputs_list in by_customer.items()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten results
        all_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Customer processing failed: {result}")
            else:
                all_results.extend(result)

        elapsed = time.time() - start_time
        success_count = sum(1 for r in all_results if r.success)

        logger.info(
            f"Processing complete: {success_count}/{len(all_results)} successful "
            f"in {elapsed:.2f}s ({len(all_results)/elapsed:.1f} ad groups/sec)"
        )

        return all_results

    async def _resolve_ad_group_ids(
        self,
        customer_id: str,
        inputs: List[AdGroupInput]
    ) -> List[AdGroupInput]:
        """Resolve ad_group_id from ad_group_name when name is provided.
        Excel scientific notation corrupts IDs, so we look up correct IDs by name.

        Optimized: Pre-fetches ALL ad group IDs in a single query for the entire customer,
        then uses dictionary lookup instead of repeated queries.
        """
        # Separate inputs that need lookup vs those that don't
        inputs_needing_lookup = [inp for inp in inputs if inp.ad_group_name]
        inputs_ready = [inp for inp in inputs if not inp.ad_group_name]

        if not inputs_needing_lookup:
            return inputs  # No lookups needed

        # Optimization: Pre-fetch all ad group ID mappings for this customer
        async def _prefetch_all_ad_groups():
            """Pre-fetch ALL ad group IDs for this customer in one query."""
            def _fetch():
                ga_service = self.client.get_service("GoogleAdsService")

                # Query ALL ad groups for this customer (no filter)
                # This is faster than multiple filtered queries
                query = """
                    SELECT ad_group.id, ad_group.name
                    FROM ad_group
                """

                try:
                    response = ga_service.search(customer_id=customer_id, query=query)
                    name_to_id = {row.ad_group.name: str(row.ad_group.id) for row in response}
                    logger.info(f"Pre-fetched {len(name_to_id)} ad group IDs for customer {customer_id}")
                    return name_to_id
                except Exception as e:
                    logger.error(f"Failed to pre-fetch ad group IDs: {e}")
                    return {}

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _fetch)

        # Pre-fetch all ad group mappings
        name_to_id = await _prefetch_all_ad_groups()

        if not name_to_id:
            logger.warning(f"No ad groups found for customer {customer_id}")
            return inputs_ready + inputs_needing_lookup

        # Fast dictionary lookup for all inputs
        corrected_inputs = []
        for inp in inputs_needing_lookup:
            if inp.ad_group_name in name_to_id:
                # Create new input with correct ID
                corrected_inp = AdGroupInput(
                    customer_id=inp.customer_id,
                    campaign_name=inp.campaign_name,
                    campaign_id=inp.campaign_id,
                    ad_group_id=name_to_id[inp.ad_group_name],
                    ad_group_name=inp.ad_group_name,
                    theme_name=inp.theme_name
                )
                corrected_inputs.append(corrected_inp)
            else:
                logger.warning(f"Could not find ad group '{inp.ad_group_name}' for customer {customer_id}")
                corrected_inputs.append(inp)  # Use original (will likely fail)

        logger.info(f"Resolved {len(corrected_inputs)}/{len(inputs_needing_lookup)} ad group IDs from names")

        # Combine corrected and ready inputs
        return corrected_inputs + inputs_ready

    async def process_customer(
        self,
        customer_id: str,
        inputs: List[AdGroupInput]
    ) -> List[ProcessingResult]:
        """Process all ad groups for a single customer."""

        logger.info(f"Processing customer {customer_id}: {len(inputs)} ad groups")

        try:
            # Resolve ad group names to correct IDs (Excel scientific notation corrupts IDs)
            inputs_with_correct_ids = await self._resolve_ad_group_ids(customer_id, inputs)

            # Build ad group resource names
            ag_service = self.client.get_service("AdGroupService")
            ad_group_resources = [
                ag_service.ad_group_path(customer_id, inp.ad_group_id)
                for inp in inputs_with_correct_ids
            ]

            # Step 1: Prefetch all data (2-3 API calls)
            cached_data = await prefetch_customer_data(
                self.client,
                customer_id,
                ad_group_resources,
                batch_size=self.batch_size
            )

            # Step 2: Ensure all labels exist (1 API call)
            labels = await ensure_labels_exist(
                self.client,
                customer_id,
                self.label_names,
                cached_data.labels
            )

            # Step 2.5: Auto-remove old theme ads when switching themes
            await self._remove_conflicting_theme_ads(
                customer_id,
                inputs,
                ad_group_resources,
                cached_data,
                labels
            )

            # Step 3: Build operations in memory (no API calls)
            ad_operations = []
            label_operations_ads = []
            label_operations_ad_groups = []
            old_ads_to_label = []

            skipped_ags = []
            processed_inputs = []
            failed_inputs = []  # Track inputs that failed pre-checks

            for inp, ag_resource in zip(inputs, ad_group_resources):
                # Skip ad groups that already have this theme's DONE label (unless this is a repair job)
                if not self.skip_sd_done_check and cached_data.ad_group_labels:
                    theme_label = get_theme_label(inp.theme_name)
                    done_label_name = f"{theme_label}_DONE"
                    if done_label_name in cached_data.ad_group_labels.get(ag_resource, set()):
                        logger.info(f"Skipping ad group {inp.ad_group_id} - already has {done_label_name} label")
                        skipped_ags.append(inp)
                        continue

                result = self._build_operations_for_ad_group(
                    inp,
                    ag_resource,
                    cached_data,
                    labels
                )

                if result:
                    # Track inputs that successfully built operations
                    processed_inputs.append(inp)
                    ad_operations.append(result["ad_data"])
                    label_operations_ads.extend(result["ad_labels"])
                    label_operations_ad_groups.extend(result["ag_labels"])
                    if result["old_ad"]:
                        old_ads_to_label.append(result["old_ad"])
                else:
                    # Track inputs that failed pre-checks (no existing ad or no final URL)
                    failed_inputs.append(inp)

            if skipped_ags:
                logger.info(f"Skipped {len(skipped_ags)} ad groups that already have theme-specific DONE labels")

            logger.info(
                f"Customer {customer_id}: Prepared {len(ad_operations)} ads, "
                f"{len(label_operations_ads)} ad labels, "
                f"{len(label_operations_ad_groups)} ad group labels"
            )

            # Step 4: Execute all mutations in batches (3-4 API calls total)
            if self.config.dry_run:
                logger.info(f"DRY RUN: Would create {len(ad_operations)} ads")
                return [
                    ProcessingResult(
                        customer_id=inp.customer_id,
                        ad_group_id=inp.ad_group_id,
                        success=True,
                        operations_count=1
                    )
                    for inp in inputs
                ]

            # Create new ads
            creation_result = await create_rsa_batch(
                self.client,
                customer_id,
                ad_operations
            )

            new_ad_resources = creation_result["resources"]
            creation_failures = creation_result["failures"]

            # Build ad_group_resource -> error map for failed creations
            failure_map = {}
            for failure in creation_failures:
                failure_map[failure["ad_group_resource"]] = failure["error"]

            # Label old ads
            if old_ads_to_label:
                await label_ads_batch(
                    self.client,
                    customer_id,
                    [(ad, labels["THEMA_ORIGINAL"]) for ad in old_ads_to_label]
                )

            # Label new ads with their respective theme labels
            if new_ad_resources:
                # Build label operations with correct theme labels
                new_label_ops = []
                for i, ad_res in enumerate(new_ad_resources):
                    # Get the corresponding input to know which theme label to use
                    if i < len(processed_inputs):
                        inp = processed_inputs[i]
                        theme_label_name = get_theme_label(inp.theme_name)
                        if theme_label_name in labels:
                            new_label_ops.append((ad_res, labels[theme_label_name]))
                        # new_label_ops.append((ad_res, labels["THEMA_AD"]))  # Disabled to reduce API operations

                await label_ads_batch(self.client, customer_id, new_label_ops)

            # Label ad groups (only successful ones)
            if label_operations_ad_groups and new_ad_resources:
                await label_ad_groups_batch(
                    self.client,
                    customer_id,
                    label_operations_ad_groups
                )

            # Build results
            results = []

            # Add results for processed ad groups (match with ad_operations)
            for i, inp in enumerate(processed_inputs):
                ad_group_res = ad_operations[i]["ad_group_resource"]

                # Check if this ad group had a creation failure
                if ad_group_res in failure_map:
                    results.append(
                        ProcessingResult(
                            customer_id=customer_id,
                            ad_group_id=inp.ad_group_id,
                            success=False,
                            error=f"Ad creation failed: {failure_map[ad_group_res]}",
                            operations_count=0
                        )
                    )
                elif i < len(new_ad_resources):
                    # Successfully created
                    results.append(
                        ProcessingResult(
                            customer_id=customer_id,
                            ad_group_id=inp.ad_group_id,
                            success=True,
                            new_ad_resource=new_ad_resources[i],
                            operations_count=1
                        )
                    )
                else:
                    # Shouldn't happen, but handle gracefully
                    results.append(
                        ProcessingResult(
                            customer_id=customer_id,
                            ad_group_id=inp.ad_group_id,
                            success=False,
                            error="Ad creation failed (no resource returned, no error info)",
                            operations_count=0
                        )
                    )

            # Add results for skipped ad groups (mark as success since they were already processed)
            for inp in skipped_ags:
                theme_label = get_theme_label(inp.theme_name)
                done_label_name = f"{theme_label}_DONE"
                results.append(
                    ProcessingResult(
                        customer_id=customer_id,
                        ad_group_id=inp.ad_group_id,
                        success=True,
                        new_ad_resource=None,
                        error=f"Already processed (has {done_label_name} label)",
                        operations_count=0
                    )
                )

            # Add results for failed ad groups (no existing ad or no final URL)
            for inp in failed_inputs:
                results.append(
                    ProcessingResult(
                        customer_id=customer_id,
                        ad_group_id=inp.ad_group_id,
                        success=False,
                        error="No existing ad found or no final URL available",
                        operations_count=0
                    )
                )

            return results

        except Exception as e:
            logger.error(f"Failed to process customer {customer_id}: {e}", exc_info=True)
            return [
                ProcessingResult(
                    customer_id=customer_id,
                    ad_group_id=inp.ad_group_id,
                    success=False,
                    error=str(e)
                )
                for inp in inputs
            ]

    async def _remove_conflicting_theme_ads(
        self,
        customer_id: str,
        inputs: List[AdGroupInput],
        ad_group_resources: List[str],
        cached_data,
        labels: Dict[str, str]
    ):
        """Remove old theme ads to make room for new ones.

        Google Ads has a limit of 3 RSAs per ad group. When adding a new theme ad:
        1. If ad group has 3 RSAs, remove 1 to make room
        2. Priority for removal:
           a. Paused theme ads (any theme)
           b. Active theme ads from OTHER themes (not the target theme)
        """
        from operations.labels import remove_ads_batch, get_ad_group_label_resources, remove_ad_group_labels_batch

        # Build map of ad_group_resource -> target theme label
        ag_target_theme = {}
        for inp, ag_resource in zip(inputs, ad_group_resources):
            theme_label = get_theme_label(inp.theme_name)
            ag_target_theme[ag_resource] = theme_label

        # Get all theme labels for filtering
        from themes import get_all_theme_labels
        all_theme_labels = set(get_all_theme_labels())

        logger.info("Checking RSA counts and theme ads for cleanup...")

        # Query all RSAs with labels and status
        def _get_rsa_details():
            """Get RSA details including status and labels."""
            ga_service = self.client.get_service("GoogleAdsService")
            ag_rsa_details = {}  # ag_resource -> list of {ad_resource, status, labels}

            # Query RSAs in batches
            for i in range(0, len(ad_group_resources), self.batch_size):
                batch = ad_group_resources[i:i + self.batch_size]
                resources_str = ", ".join(f"'{r}'" for r in batch)

                query = f"""
                    SELECT
                        ad_group_ad.ad_group,
                        ad_group_ad.resource_name,
                        ad_group_ad.status,
                        ad_group_ad_label.label
                    FROM ad_group_ad
                    LEFT JOIN ad_group_ad_label ON ad_group_ad.resource_name = ad_group_ad_label.ad_group_ad
                    WHERE ad_group_ad.ad_group IN ({resources_str})
                        AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                        AND ad_group_ad.status != REMOVED
                """

                try:
                    response = ga_service.search(customer_id=customer_id, query=query)

                    for row in response:
                        ag_res = row.ad_group_ad.ad_group
                        ad_res = row.ad_group_ad.resource_name
                        status = str(row.ad_group_ad.status)

                        # Get label if present
                        label_res = None
                        if hasattr(row, 'ad_group_ad_label') and row.ad_group_ad_label:
                            label_res = row.ad_group_ad_label.label

                        # Find if this ad has a theme label
                        theme_label_name = None
                        if label_res and label_res in labels.values():
                            # Reverse lookup label name
                            for label_name, label_resource in labels.items():
                                if label_resource == label_res and label_name in all_theme_labels:
                                    theme_label_name = label_name
                                    break

                        if ag_res not in ag_rsa_details:
                            ag_rsa_details[ag_res] = []

                        ag_rsa_details[ag_res].append({
                            'ad_resource': ad_res,
                            'status': status,
                            'theme_label': theme_label_name
                        })

                except Exception as e:
                    logger.warning(f"Failed to get RSA details: {e}")

            return ag_rsa_details

        # Get RSA details
        loop = asyncio.get_event_loop()
        ag_rsa_details = await loop.run_in_executor(None, _get_rsa_details)

        # Process each ad group
        ads_to_remove = []
        done_labels_to_remove = {}  # ag_resource -> set of done_label_names

        for ag_resource in ad_group_resources:
            rsa_list = ag_rsa_details.get(ag_resource, [])
            current_count = len(rsa_list)
            target_theme = ag_target_theme.get(ag_resource)

            if current_count < 3:
                continue  # No cleanup needed, we have room

            logger.info(f"  Ad group {ag_resource}: {current_count} RSAs (at limit)")

            # Need to remove 1 RSA to make room for the new theme ad
            # Priority: 1) Paused theme ads, 2) Active theme ads from OTHER themes

            # Sort RSAs by removal priority
            paused_theme_ads = []
            active_other_theme_ads = []

            for rsa in rsa_list:
                if rsa['theme_label']:  # This is a theme ad
                    if rsa['status'] == 'PAUSED':
                        paused_theme_ads.append(rsa)
                    elif rsa['theme_label'] != target_theme:
                        active_other_theme_ads.append(rsa)

            # Pick one ad to remove (highest priority available)
            ad_to_remove = None
            if paused_theme_ads:
                ad_to_remove = paused_theme_ads[0]
                logger.info(f"    Will remove paused {ad_to_remove['theme_label']} ad")
            elif active_other_theme_ads:
                ad_to_remove = active_other_theme_ads[0]
                logger.info(f"    Will remove active {ad_to_remove['theme_label']} ad (different theme)")

            if ad_to_remove:
                ads_to_remove.append(ad_to_remove['ad_resource'])

                # Mark the DONE label for removal
                removed_theme = ad_to_remove['theme_label']
                done_label = f"{removed_theme}_DONE"
                if ag_resource not in done_labels_to_remove:
                    done_labels_to_remove[ag_resource] = set()
                done_labels_to_remove[ag_resource].add(done_label)

        # Execute removals
        if ads_to_remove:
            logger.info(f"Removing {len(ads_to_remove)} theme ads to make room for new ads")
            try:
                removed_count = await remove_ads_batch(self.client, customer_id, ads_to_remove)
                logger.info(f"  Successfully removed {removed_count} ads")
            except Exception as e:
                logger.warning(f"  Failed to remove ads: {e}")

        # Remove DONE labels
        for ag_resource, done_labels in done_labels_to_remove.items():
            for done_label in done_labels:
                try:
                    label_resources = await get_ad_group_label_resources(
                        self.client, customer_id, [ag_resource], done_label
                    )
                    if label_resources:
                        await remove_ad_group_labels_batch(self.client, customer_id, label_resources)
                        logger.info(f"  Removed {done_label} label from {ag_resource}")

                        # Update cached data
                        if ag_resource in cached_data.ad_group_labels:
                            cached_data.ad_group_labels[ag_resource].discard(done_label)

                except Exception as e:
                    logger.warning(f"  Failed to remove {done_label} label: {e}")

    def _build_operations_for_ad_group(
        self,
        inp: AdGroupInput,
        ad_group_resource: str,
        cached_data,
        labels: Dict[str, str]
    ) -> dict:
        """Build all operations for a single ad group."""

        # Get existing ad from cache
        existing_ad = cached_data.existing_ads.get(ad_group_resource)

        if not existing_ad:
            logger.debug(f"No existing ad for ad group {inp.ad_group_id}")
            return None

        if not existing_ad.final_urls:
            logger.debug(f"No final URL for ad group {inp.ad_group_id}")
            return None

        final_url = existing_ad.final_urls[0]
        base_headlines_3 = existing_ad.headlines[:3]
        base_desc_1 = existing_ad.descriptions[0] if existing_ad.descriptions else ""

        # Generate themed content using the input's theme
        extra_headlines, extra_descriptions, path1 = generate_themed_content(
            inp.theme_name,
            base_headlines_3,
            base_desc_1
        )

        # Build ad data
        ad_data = build_ad_data(
            ad_group_resource=ad_group_resource,
            final_url=final_url,
            base_headlines=base_headlines_3,
            base_description=base_desc_1,
            extra_headlines=extra_headlines,
            extra_descriptions=extra_descriptions,
            path1=path1,
            path2=existing_ad.path2 or existing_ad.path1 or ""
        )

        # Build label operations
        ad_labels = []  # Will be filled after ad creation

        # Get theme-specific DONE label
        theme_label = get_theme_label(inp.theme_name)
        done_label_name = f"{theme_label}_DONE"

        ag_labels = [
            (ad_group_resource, labels[done_label_name])
        ]

        return {
            "ad_data": ad_data,
            "ad_labels": ad_labels,
            "ag_labels": ag_labels,
            "old_ad": existing_ad.resource_name
        }


async def main():
    """Main entry point."""

    try:
        # Load configuration
        config = load_config_from_env()
        logger.info("Configuration loaded successfully")

        # Load input data
        inputs = load_data(config.input_file)
        logger.info(f"Loaded {len(inputs)} ad groups from {config.input_file}")

        if not inputs:
            logger.warning("No ad groups to process")
            return

        # Process
        processor = ThemaAdsProcessor(config)
        results = await processor.process_all(inputs)

        # Summary
        success_count = sum(1 for r in results if r.success)
        failed_count = len(results) - success_count

        logger.info("=" * 60)
        logger.info(f"SUMMARY: {success_count} successful, {failed_count} failed")
        logger.info("=" * 60)

        if failed_count > 0:
            logger.warning("Failed ad groups:")
            for r in results:
                if not r.success:
                    logger.warning(f"  - {r.customer_id} / {r.ad_group_id}: {r.error}")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
