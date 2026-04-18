"""
Thema Ads Service - Integration with FastAPI and state persistence
"""
import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
from backend.thema_ads_db import get_db_connection

# Configure logging
logger = logging.getLogger(__name__)

# Add thema_ads_optimized to path
THEMA_ADS_PATH = Path(__file__).parent.parent / "thema_ads_optimized"
sys.path.insert(0, str(THEMA_ADS_PATH))

import random
import time


async def mutate_with_retry(ad_group_ad_service, customer_id: str, operations: list,
                            max_retries: int = 3, base_delay: float = 1.0) -> tuple:
    """
    Execute mutate_ad_group_ads with retry logic for concurrent modification errors.

    Args:
        ad_group_ad_service: Google Ads AdGroupAdService
        customer_id: Customer ID
        operations: List of operations to execute
        max_retries: Maximum number of retries (default 3)
        base_delay: Base delay in seconds for exponential backoff (default 1.0)

    Returns:
        Tuple of (success: bool, result_or_error)
    """
    for attempt in range(max_retries + 1):
        try:
            result = ad_group_ad_service.mutate_ad_group_ads(
                customer_id=customer_id,
                operations=operations
            )
            return (True, result)
        except Exception as e:
            error_str = str(e)
            # Check if it's a concurrent modification error
            if 'CONCURRENT_MODIFICATION' in error_str:
                if attempt < max_retries:
                    # Exponential backoff with jitter
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"[{customer_id}] Concurrent modification, retry {attempt + 1}/{max_retries} after {delay:.1f}s")
                    await asyncio.sleep(delay)
                    continue
            # Either not a concurrent modification error, or we've exhausted retries
            return (False, e)
    return (False, Exception("Max retries exceeded"))


class ThemaAdsService:
    """Service for managing Thema Ads processing with state persistence."""

    def __init__(self):
        self.current_job_id = None
        self.is_running = False

    def get_customer_ids(self) -> List[str]:
        """Load customer IDs from the account ids file."""
        account_ids_file = Path(__file__).parent.parent / "thema_ads_optimized" / "account ids"
        if not account_ids_file.exists():
            logger.error(f"Account IDs file not found at {account_ids_file}")
            return []

        with open(account_ids_file, 'r') as f:
            customer_ids = [line.strip() for line in f if line.strip()]

        logger.info(f"Loaded {len(customer_ids)} customer IDs from account ids file")
        return customer_ids

    def _fetch_campaign_info_with_client(self, client, customer_id: str, ad_group_id: str) -> Dict:
        """Fetch campaign information from Google Ads API using existing client."""
        try:
            # Query ad group to get campaign info
            ga_service = client.get_service("GoogleAdsService")
            query = f"""
                SELECT
                    ad_group.id,
                    ad_group.name,
                    campaign.id,
                    campaign.name
                FROM ad_group
                WHERE ad_group.id = {ad_group_id}
                LIMIT 1
            """

            response = ga_service.search(customer_id=customer_id, query=query)

            for row in response:
                return {
                    'campaign_id': str(row.campaign.id),
                    'campaign_name': row.campaign.name
                }

            raise ValueError(f"Ad group {ad_group_id} not found for customer {customer_id}")

        except Exception as e:
            logger.error(f"Failed to fetch campaign info: {e}")
            raise

    def create_job(self, input_data: List[Dict], batch_size: int = 7500, is_repair_job: bool = False) -> int:
        """Create a new processing job and store input data using batch inserts."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            # Determine theme from input data (use first item's theme or default to singles_day)
            theme_name = input_data[0].get('theme_name', 'singles_day') if input_data else 'singles_day'

            # Create job with batch_size, repair flag, and theme
            cur.execute("""
                INSERT INTO thema_ads_jobs (status, total_ad_groups, batch_size, is_repair_job, theme_name)
                VALUES ('pending', %s, %s, %s, %s)
                RETURNING id
            """, (len(input_data), batch_size, is_repair_job, theme_name))

            job_id = cur.fetchone()['id']

            # Batch insert input data (much faster than individual inserts)
            if input_data:
                input_values = [
                    (job_id, item['customer_id'], item.get('campaign_id'),
                     item.get('campaign_name'), item['ad_group_id'], item.get('ad_group_name'),
                     item.get('theme_name', 'singles_day'))
                    for item in input_data
                ]

                cur.executemany("""
                    INSERT INTO thema_ads_input_data (job_id, customer_id, campaign_id, campaign_name, ad_group_id, ad_group_name, theme_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, input_values)

                # Batch insert job items
                job_item_values = [
                    (job_id, item['customer_id'], item.get('campaign_id'),
                     item.get('campaign_name'), item['ad_group_id'], item.get('ad_group_name'),
                     item.get('theme_name', 'singles_day'), 'pending')
                    for item in input_data
                ]

                cur.executemany("""
                    INSERT INTO thema_ads_job_items (job_id, customer_id, campaign_id, campaign_name, ad_group_id, ad_group_name, theme_name, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, job_item_values)

            conn.commit()
            logger.info(f"Created job {job_id} with {len(input_data)} ad groups using batch inserts")
            return job_id

        finally:
            cur.close()
            conn.close()

    def get_job_status(self, job_id: int) -> Dict:
        """Get current status of a job."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            # Get job info
            cur.execute("""
                SELECT * FROM thema_ads_jobs WHERE id = %s
            """, (job_id,))

            job = cur.fetchone()
            if not job:
                return None

            # Get item statistics
            cur.execute("""
                SELECT
                    status,
                    COUNT(*) as count
                FROM thema_ads_job_items
                WHERE job_id = %s
                GROUP BY status
            """, (job_id,))

            items_by_status = {row['status']: row['count'] for row in cur.fetchall()}

            # Get recent failures
            cur.execute("""
                SELECT customer_id, ad_group_id, error_message
                FROM thema_ads_job_items
                WHERE job_id = %s AND status = 'failed'
                ORDER BY processed_at DESC
                LIMIT 10
            """, (job_id,))

            recent_failures = cur.fetchall()

            # Map database columns to API field names
            job_dict = dict(job)
            return {
                'id': job_dict['id'],
                'status': job_dict['status'],
                'total_items': job_dict.get('total_ad_groups', 0),
                'successful_items': items_by_status.get('successful', 0),
                'failed_items': items_by_status.get('failed', 0),
                'skipped_items': items_by_status.get('skipped', 0),
                'pending_items': items_by_status.get('pending', 0),
                'started_at': job_dict.get('started_at'),
                'completed_at': job_dict.get('completed_at'),
                'created_at': job_dict.get('created_at'),
                'updated_at': job_dict.get('updated_at'),
                'error_message': job_dict.get('error_message'),
                'batch_size': job_dict.get('batch_size', 7500),
                'is_repair_job': job_dict.get('is_repair_job', False),
                'items_by_status': items_by_status,
                'recent_failures': recent_failures
            }

        finally:
            cur.close()
            conn.close()

    def get_pending_items(self, job_id: int) -> List[Dict]:
        """Get all pending items for a job (for resume)."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            cur.execute("""
                SELECT customer_id, campaign_id, campaign_name, ad_group_id, ad_group_name, theme_name
                FROM thema_ads_job_items
                WHERE job_id = %s AND status = 'pending'
            """, (job_id,))

            return cur.fetchall()

        finally:
            cur.close()
            conn.close()

    def update_job_status(self, job_id: int, status: str, **kwargs):
        """Update job status."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            updates = ['status = %s', 'updated_at = CURRENT_TIMESTAMP']
            values = [status]

            if 'error_message' in kwargs:
                updates.append('error_message = %s')
                values.append(kwargs['error_message'])

            if status == 'running' and 'started_at' not in kwargs:
                updates.append('started_at = CURRENT_TIMESTAMP')

            if status in ('completed', 'failed'):
                updates.append('completed_at = CURRENT_TIMESTAMP')

            values.append(job_id)

            cur.execute(f"""
                UPDATE thema_ads_jobs
                SET {', '.join(updates)}
                WHERE id = %s
            """, values)

            conn.commit()

        finally:
            cur.close()
            conn.close()

    def update_item_status(self, job_id: int, customer_id: str, ad_group_id: str,
                          status: str, new_ad_resource: Optional[str] = None,
                          error_message: Optional[str] = None):
        """Update individual item status. DEPRECATED - Use batch_update_items() for better performance."""
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            try:
                cur.execute("""
                    UPDATE thema_ads_job_items
                    SET status = %s,
                        new_ad_resource = %s,
                        error_message = %s,
                        processed_at = CURRENT_TIMESTAMP
                    WHERE job_id = %s AND customer_id = %s AND ad_group_id = %s
                """, (status, new_ad_resource, error_message, job_id, customer_id, ad_group_id))

                # Update job statistics
                cur.execute("""
                    UPDATE thema_ads_jobs
                    SET processed_ad_groups = (
                            SELECT COUNT(*) FROM thema_ads_job_items
                            WHERE job_id = %s AND status IN ('successful', 'failed', 'skipped')
                        ),
                        successful_ad_groups = (
                            SELECT COUNT(*) FROM thema_ads_job_items
                            WHERE job_id = %s AND status = 'successful'
                        ),
                        failed_ad_groups = (
                            SELECT COUNT(*) FROM thema_ads_job_items
                            WHERE job_id = %s AND status = 'failed'
                        ),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (job_id, job_id, job_id, job_id))

                conn.commit()
                logger.info(f"✓ DB UPDATE: Job {job_id}, Ad Group {ad_group_id}: {status}")

            finally:
                cur.close()
                conn.close()
        except Exception as e:
            logger.error(f"Failed to update item status for job {job_id}, ad_group {ad_group_id}: {e}")
            raise

    def batch_update_items(self, job_id: int, updates: List[tuple]):
        """
        Batch update item statuses for 10-20x performance improvement.

        Args:
            job_id: Job ID
            updates: List of tuples (customer_id, ad_group_id, status, new_ad_resource, error_message)
        """
        if not updates:
            return

        try:
            conn = get_db_connection()
            cur = conn.cursor()

            try:
                # Batch update all items at once using executemany
                cur.executemany("""
                    UPDATE thema_ads_job_items
                    SET status = %s,
                        new_ad_resource = %s,
                        error_message = %s,
                        processed_at = CURRENT_TIMESTAMP
                    WHERE job_id = %s AND customer_id = %s AND ad_group_id = %s
                """, [(u[2], u[3], u[4], job_id, u[0], u[1]) for u in updates])

                # Update job statistics ONCE per batch instead of per item
                cur.execute("""
                    UPDATE thema_ads_jobs
                    SET processed_ad_groups = (
                            SELECT COUNT(*) FROM thema_ads_job_items
                            WHERE job_id = %s AND status IN ('successful', 'failed', 'skipped')
                        ),
                        successful_ad_groups = (
                            SELECT COUNT(*) FROM thema_ads_job_items
                            WHERE job_id = %s AND status = 'successful'
                        ),
                        failed_ad_groups = (
                            SELECT COUNT(*) FROM thema_ads_job_items
                            WHERE job_id = %s AND status = 'failed'
                        ),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (job_id, job_id, job_id, job_id))

                conn.commit()
                logger.info(f"✓ BATCH DB UPDATE: Job {job_id}, {len(updates)} items updated")

            finally:
                cur.close()
                conn.close()
        except Exception as e:
            logger.error(f"Failed to batch update items for job {job_id}: {e}")
            raise

    async def process_job(self, job_id: int):
        """Process a job with state persistence."""
        try:
            import os
            from dotenv import load_dotenv

            # Load .env file from thema_ads_optimized directory
            env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
            if env_path.exists():
                logger.info(f"Loading environment from: {env_path}")
                load_dotenv(env_path)
            else:
                logger.warning(f"Environment file not found at: {env_path}")

            from config import load_config_from_env
            from google_ads_client import initialize_client
            from models import AdGroupInput

            # Load config
            config = load_config_from_env()

            # Get job details including batch_size and repair flag
            job_details = self.get_job_status(job_id)
            batch_size = job_details.get('batch_size', 7500)
            is_repair_job = job_details.get('is_repair_job', False)
            logger.info(f"Job {job_id} will use batch_size: {batch_size}, is_repair_job: {is_repair_job}")

            # Get pending items
            pending_items = self.get_pending_items(job_id)

            if not pending_items:
                logger.info(f"No pending items for job {job_id}")
                self.update_job_status(job_id, 'completed')
                return

            # Initialize client for potential campaign info fetching
            client = initialize_client(config.google_ads)

            # Convert to AdGroupInput objects, fetching campaign info if missing
            inputs = []
            for item in pending_items:
                campaign_id = item['campaign_id']
                campaign_name = item['campaign_name']

                # Fetch campaign info if not in database
                if not campaign_id or not campaign_name:
                    logger.info(f"Fetching campaign info for ad group {item['ad_group_id']}")
                    campaign_info = self._fetch_campaign_info_with_client(
                        client,
                        item['customer_id'],
                        item['ad_group_id']
                    )
                    campaign_id = campaign_info['campaign_id']
                    campaign_name = campaign_info['campaign_name']

                inputs.append(AdGroupInput(
                    customer_id=item['customer_id'],
                    campaign_name=campaign_name,
                    campaign_id=campaign_id,
                    ad_group_id=item['ad_group_id'],
                    ad_group_name=item.get('ad_group_name'),
                    theme_name=item.get('theme_name', 'singles_day')
                ))

            # Update job status
            self.update_job_status(job_id, 'running')
            self.current_job_id = job_id
            self.is_running = True

            logger.info(f"Starting job {job_id} with {len(inputs)} items, batch_size={batch_size}")

            # Import and initialize processor
            from main_optimized import ThemaAdsProcessor
            processor = ThemaAdsProcessor(config, batch_size=batch_size, skip_sd_done_check=is_repair_job)

            # Process with custom callback
            results = await self._process_with_tracking(processor, inputs, job_id)

            # Update final status
            job_status = self.get_job_status(job_id)
            if job_status['failed_items'] == 0:
                self.update_job_status(job_id, 'completed')
            else:
                self.update_job_status(
                    job_id,
                    'completed_with_errors',
                    error_message=f"{job_status['failed_items']} item(s) failed"
                )

            self.is_running = False
            self.current_job_id = None

            logger.info(f"Job {job_id} completed")

            # Check if auto-queue is enabled and start next job
            await self._start_next_job_if_queue_enabled()

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)
            self.update_job_status(job_id, 'failed', error_message=str(e))
            self.is_running = False
            self.current_job_id = None

            # Even if job failed, try to start next job if auto-queue enabled
            await self._start_next_job_if_queue_enabled()

    async def _process_with_tracking(self, processor, inputs, job_id):
        """Process inputs with progress tracking."""
        from collections import defaultdict

        # Group by customer
        by_customer = defaultdict(list)
        for inp in inputs:
            by_customer[inp.customer_id].append(inp)

        # Process customers
        semaphore = asyncio.Semaphore(processor.config.performance.max_concurrent_customers)

        async def process_with_tracking(customer_id, customer_inputs):
            async with semaphore:
                # Honor pause: `pause_job()` sets is_running=False. Without this
                # gate, paused jobs keep chewing through the remaining queued
                # customers until the whole batch finishes.
                if not self.is_running:
                    logger.info(f"⏸️  Skipping customer {customer_id} — job paused")
                    return []
                try:
                    logger.info(f"🔵 START processing customer {customer_id} with {len(customer_inputs)} inputs")
                    results = await processor.process_customer(customer_id, customer_inputs)
                    logger.info(f"🟢 FINISHED processor.process_customer for {customer_id}, got {len(results)} results")

                    # Update database with results using batch updates (10-20x faster)
                    logger.info(f"Processing {len(results)} results for customer {customer_id}")
                    update_buffer = []
                    BATCH_SIZE = 1000

                    for result, inp in zip(results, customer_inputs):
                        # Determine status based on result
                        if result.success and result.error and "Already processed" in result.error:
                            # Ad group already has SD_DONE label
                            status = 'skipped'
                        elif not result.success and result.error and "No existing ad" in result.error:
                            # Ad group has no existing ads to work with (not a failure, just can't process)
                            status = 'skipped'
                        elif result.success:
                            status = 'successful'
                        else:
                            status = 'failed'

                        # Buffer update instead of executing immediately
                        update_buffer.append((
                            customer_id,
                            inp.ad_group_id,
                            status,
                            result.new_ad_resource if result.success else None,
                            result.error
                        ))

                        # Flush buffer when it reaches BATCH_SIZE
                        if len(update_buffer) >= BATCH_SIZE:
                            logger.info(f"Flushing batch of {len(update_buffer)} DB updates for customer {customer_id}")
                            self.batch_update_items(job_id, update_buffer)
                            update_buffer = []

                    # Flush remaining updates
                    if update_buffer:
                        logger.info(f"Flushing final batch of {len(update_buffer)} DB updates for customer {customer_id}")
                        self.batch_update_items(job_id, update_buffer)

                    return results
                except Exception as e:
                    logger.error(f"🔴 ERROR in process_with_tracking for customer {customer_id}: {e}", exc_info=True)
                    raise

        tasks = [
            process_with_tracking(cid, inputs_list)
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

        return all_results

    def pause_job(self, job_id: int):
        """Pause a running job."""
        self.is_running = False
        self.update_job_status(job_id, 'paused')
        logger.info(f"Job {job_id} paused")

    def resume_job(self, job_id: int):
        """Resume a paused job. Guarded so double-clicks can't spawn two
        concurrent process_job tasks racing on the same job."""
        if self.is_running and self.current_job_id == job_id:
            logger.warning(f"Job {job_id} is already running — ignoring resume")
            return
        status = self.get_job_status(job_id).get('status')
        if status not in ('paused', 'failed'):
            raise ValueError(f"Cannot resume job {job_id} from status '{status}'")
        self.update_job_status(job_id, 'running')
        asyncio.create_task(self.process_job(job_id))
        logger.info(f"Job {job_id} resumed")

    def list_jobs(self, limit: int = 20) -> List[Dict]:
        """List all jobs."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            # Pre-aggregate item counts per job_id so the outer query doesn't
            # need a GROUP BY (thema_ads_jobs.id has no PRIMARY KEY, so Postgres
            # can't infer functional dependency of j.* on j.id).
            cur.execute("""
                SELECT
                    j.*,
                    COALESCE(c.successful_count, 0) AS successful_count,
                    COALESCE(c.failed_count,     0) AS failed_count,
                    COALESCE(c.skipped_count,    0) AS skipped_count,
                    COALESCE(c.pending_count,    0) AS pending_count
                FROM thema_ads_jobs j
                LEFT JOIN (
                    SELECT
                        job_id,
                        SUM(CASE WHEN status = 'successful' THEN 1 ELSE 0 END) AS successful_count,
                        SUM(CASE WHEN status = 'failed'     THEN 1 ELSE 0 END) AS failed_count,
                        SUM(CASE WHEN status = 'skipped'    THEN 1 ELSE 0 END) AS skipped_count,
                        SUM(CASE WHEN status = 'pending'    THEN 1 ELSE 0 END) AS pending_count
                    FROM thema_ads_job_items
                    GROUP BY job_id
                ) c ON c.job_id = j.id
                ORDER BY j.created_at DESC
                LIMIT %s
            """, (limit,))

            jobs = cur.fetchall()

            # Map database columns to API field names
            return [{
                'id': job['id'],
                'status': job['status'],
                'total_items': job.get('total_ad_groups', 0),
                'successful_items': job.get('successful_count', 0),
                'failed_items': job.get('failed_count', 0),
                'skipped_items': job.get('skipped_count', 0),
                'pending_items': job.get('pending_count', 0),
                'started_at': job.get('started_at'),
                'completed_at': job.get('completed_at'),
                'created_at': job.get('created_at'),
                'batch_size': job.get('batch_size', 7500),
                'theme_name': job.get('theme_name')
            } for job in jobs]

        finally:
            cur.close()
            conn.close()

    def delete_job(self, job_id: int):
        """Delete a job and all associated data."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            # Delete job (will cascade to job_items and input_data)
            cur.execute("""
                DELETE FROM thema_ads_jobs
                WHERE id = %s
            """, (job_id,))

            conn.commit()
            logger.info(f"Deleted job {job_id}")

        finally:
            cur.close()
            conn.close()

    async def remove_checkup_labels(
        self,
        client,
        customer_ids: List[str]
    ) -> Dict:
        """
        Remove THEMES_CHECK_DONE labels from all ad groups.
        This allows doing a clean audit run without skipping any ad groups.

        Args:
            client: Google Ads API client
            customer_ids: List of customer IDs to process

        Returns:
            Dict with removal results
        """
        logger.info(f"Removing THEMES_CHECK_DONE labels from {len(customer_ids)} customers")

        ga_service = client.get_service("GoogleAdsService")
        ad_group_label_service = client.get_service("AdGroupLabelService")

        stats = {
            'customers_processed': 0,
            'labels_removed': 0,
            'errors': 0
        }

        for customer_id in customer_ids:
            try:
                # Find THEMES_CHECK_DONE label
                label_query = """
                    SELECT label.resource_name
                    FROM label
                    WHERE label.name = 'THEMES_CHECK_DONE'
                    LIMIT 1
                """

                label_response = ga_service.search(customer_id=customer_id, query=label_query)
                audit_label_resource = None

                for row in label_response:
                    audit_label_resource = row.label.resource_name
                    break

                if not audit_label_resource:
                    logger.info(f"Customer {customer_id}: No THEMES_CHECK_DONE label found")
                    continue

                # Find all ad groups with this label
                ag_label_query = f"""
                    SELECT ad_group_label.resource_name
                    FROM ad_group_label
                    WHERE ad_group_label.label = '{audit_label_resource}'
                """

                ag_label_response = ga_service.search(customer_id=customer_id, query=ag_label_query)
                operations = []

                for row in ag_label_response:
                    operation = client.get_type('AdGroupLabelOperation')
                    operation.remove = row.ad_group_label.resource_name
                    operations.append(operation)

                if operations:
                    # Remove in batches of 5000
                    for i in range(0, len(operations), 5000):
                        batch = operations[i:i+5000]
                        response = ad_group_label_service.mutate_ad_group_labels(
                            customer_id=customer_id,
                            operations=batch
                        )
                        stats['labels_removed'] += len(response.results)

                    logger.info(f"Customer {customer_id}: Removed {len(operations)} THEMES_CHECK_DONE labels")

                stats['customers_processed'] += 1

            except Exception as e:
                logger.error(f"Customer {customer_id}: Error removing labels: {e}")
                stats['errors'] += 1
                continue

        logger.info(f"Removal complete: {stats}")

        return {
            'status': 'completed',
            'stats': stats
        }

    async def checkup_ad_groups(
        self,
        client,
        customer_ids: List[str],
        limit: Optional[int] = None,
        batch_size: int = 5000,
        job_chunk_size: int = 50000,
        background_tasks=None,
        skip_audited: bool = True
    ) -> Dict:
        """
        OPTIMIZED: Audit theme DONE labels and verify themed ads exist.

        Queries Google Ads directly for ad groups with THEME_*_DONE labels,
        verifies they have the corresponding themed ad, and removes invalid DONE labels.
        Adds THEMES_CHECK_DONE tracking label to validated ad groups.

        Performance optimizations:
        - Queries all themes at once (4x faster)
        - Filters to HS/ campaigns only (2-3x faster)
        - Better chunking for large queries (1.5x faster)
        - Customer pre-filtering
        - Skips already-audited ad groups with THEMES_CHECK_DONE label

        Args:
            client: Google Ads API client
            customer_ids: List of customer IDs to check
            limit: Optional limit on number of ad groups to check (not implemented for bulk processing)
            batch_size: Batch size for API queries
            job_chunk_size: Maximum items per repair job
            background_tasks: FastAPI background tasks
            skip_audited: If True, skip ad groups with THEMES_CHECK_DONE label

        Returns:
            Dict with audit results and created job IDs
        """
        logger.info(f"Starting optimized multi-theme audit: skip_audited={skip_audited}, batch_size={batch_size}")

        # Import theme utilities
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent / "thema_ads_optimized"))
        from themes import SUPPORTED_THEMES

        ga_service = client.get_service("GoogleAdsService")
        ad_group_label_service = client.get_service("AdGroupLabelService")

        # Theme mapping - theme_name -> (theme_label, done_label)
        THEMES = {
            'black_friday': ('THEME_BF', 'THEME_BF_DONE'),
            'cyber_monday': ('THEME_CM', 'THEME_CM_DONE'),
            'sinterklaas': ('THEME_SK', 'THEME_SK_DONE'),
            'kerstmis': ('THEME_KM', 'THEME_KM_DONE'),
        }

        # Chunk sizes
        AD_GROUP_CHUNK_SIZE = 500
        AD_CHUNK_SIZE = 1000

        stats = {
            'customers_processed': 0,
            'ad_groups_checked': 0,
            'ad_groups_with_done_label': 0,
            'ad_groups_skipped_already_audited': 0,
            'ad_groups_missing_theme_ads': 0,
            'done_labels_removed': 0,
            'audit_labels_added': 0,
            'repair_jobs_created': 0,
            'themes_found': {}  # Count per theme
        }

        repair_items = []  # Ad groups needing repair (with theme info)

        # Pre-filter: Find which customers have DONE labels
        logger.info("Pre-filtering customers with DONE labels...")
        customers_with_labels = {}
        done_label_names = [THEMES[theme][1] for theme in THEMES.keys()]

        for customer_id in customer_ids:
            try:
                # Query which DONE labels exist for this customer
                labels_str = "', '".join(done_label_names)
                query = f"""
                    SELECT label.name, label.resource_name
                    FROM label
                    WHERE label.name IN ('{labels_str}')
                """

                response = ga_service.search(customer_id=customer_id, query=query)
                customer_themes = []

                for row in response:
                    label_name = row.label.name
                    # Find which theme this label belongs to
                    for theme_name, (_, done_label) in THEMES.items():
                        if done_label == label_name:
                            customer_themes.append(theme_name)
                            break

                if customer_themes:
                    customers_with_labels[customer_id] = customer_themes
                    logger.info(f"Customer {customer_id}: Found {len(customer_themes)} theme(s) with DONE labels")

            except Exception as e:
                logger.warning(f"Customer {customer_id}: Error pre-filtering - {e}")
                continue

        if not customers_with_labels:
            logger.info("No customers with DONE labels found")
            return {
                'status': 'completed',
                'stats': stats,
                'repair_job_ids': [],
                'message': 'No customers with DONE labels found'
            }

        logger.info(f"Found {len(customers_with_labels)} customers with DONE labels (out of {len(customer_ids)})")

        # Process each customer
        for customer_id, themes_to_audit in customers_with_labels.items():
            try:
                logger.info(f"[{customer_id}] Auditing {len(themes_to_audit)} themes in bulk...")
                stats['customers_processed'] += 1

                # Step 1: Get all DONE labels, theme labels, audit tracking label, and failure label
                all_labels_to_find = ['THEMES_CHECK_DONE', 'THEMES_CHECKUP_FAILED']  # Audit tracking label + failure label
                for theme in themes_to_audit:
                    theme_label, done_label = THEMES[theme]
                    all_labels_to_find.extend([theme_label, done_label])

                labels_str = "', '".join(all_labels_to_find)
                labels_query = f"""
                    SELECT label.name, label.resource_name
                    FROM label
                    WHERE label.name IN ('{labels_str}')
                """

                labels_response = ga_service.search(customer_id=customer_id, query=labels_query)

                # Map label names to resources
                label_resources = {}
                for row in labels_response:
                    label_resources[row.label.name] = row.label.resource_name

                # Create audit tracking label if it doesn't exist
                audit_label_name = 'THEMES_CHECK_DONE'
                if audit_label_name not in label_resources:
                    try:
                        label_service = client.get_service('LabelService')
                        label_operation = client.get_type('LabelOperation')
                        label = label_operation.create
                        label.name = audit_label_name
                        # Note: Google Ads API labels don't support description field

                        response = label_service.mutate_labels(
                            customer_id=customer_id,
                            operations=[label_operation]
                        )
                        label_resources[audit_label_name] = response.results[0].resource_name
                        logger.info(f"[{customer_id}] Created {audit_label_name} label")
                    except Exception as e:
                        logger.warning(f"[{customer_id}] Warning: Could not create {audit_label_name} label: {e}")

                audit_label_resource = label_resources.get(audit_label_name)
                checkup_failed_label_resource = label_resources.get('THEMES_CHECKUP_FAILED')

                # Build theme-specific mappings
                theme_mappings = {}
                for theme in themes_to_audit:
                    theme_label, done_label = THEMES[theme]
                    if done_label not in label_resources:
                        logger.info(f"[{customer_id}] Theme {theme}: No {done_label} label found - skipping")
                        continue

                    theme_mappings[theme] = {
                        'theme_label': theme_label,
                        'done_label': done_label,
                        'theme_label_resource': label_resources.get(theme_label),
                        'done_label_resource': label_resources[done_label]
                    }

                if not theme_mappings:
                    logger.info(f"[{customer_id}] No DONE labels found - skipping")
                    continue

                logger.info(f"[{customer_id}] Found {len(theme_mappings)} theme(s) with DONE labels")

                # Step 1.5: Find ad groups with THEMES_CHECK_DONE label to skip
                ad_groups_already_audited = set()
                if skip_audited and audit_label_resource:
                    try:
                        audited_query = f"""
                            SELECT ad_group.resource_name
                            FROM ad_group_label
                            WHERE ad_group_label.label = '{audit_label_resource}'
                            AND campaign.name LIKE 'HS/%'
                        """
                        audited_response = ga_service.search(customer_id=customer_id, query=audited_query)
                        for row in audited_response:
                            ad_groups_already_audited.add(row.ad_group.resource_name)

                        if ad_groups_already_audited:
                            logger.info(f"[{customer_id}] Found {len(ad_groups_already_audited)} ad groups already audited (will skip)")
                    except Exception as e:
                        logger.warning(f"[{customer_id}] Warning: Could not query already-audited ad groups: {e}")

                # Step 1.6: Find ad groups with THEMES_CHECKUP_FAILED label to skip
                ad_groups_checkup_failed = set()
                if checkup_failed_label_resource:
                    try:
                        failed_query = f"""
                            SELECT ad_group.resource_name
                            FROM ad_group_label
                            WHERE ad_group_label.label = '{checkup_failed_label_resource}'
                            AND campaign.name LIKE 'HS/%'
                        """
                        failed_response = ga_service.search(customer_id=customer_id, query=failed_query)
                        for row in failed_response:
                            ad_groups_checkup_failed.add(row.ad_group.resource_name)

                        if ad_groups_checkup_failed:
                            logger.info(f"[{customer_id}] Found {len(ad_groups_checkup_failed)} ad groups with checkup-failed label (will skip)")
                    except Exception as e:
                        logger.warning(f"[{customer_id}] Warning: Could not query checkup-failed ad groups: {e}")

                # Step 2: Get all ad groups with ANY of the DONE labels (HS/ campaigns only)
                done_resources = [tm['done_label_resource'] for tm in theme_mappings.values()]
                done_resources_str = "', '".join(done_resources)

                ag_query = f"""
                    SELECT
                        ad_group.id,
                        ad_group.name,
                        ad_group.resource_name,
                        ad_group_label.label,
                        ad_group_label.resource_name,
                        campaign.id,
                        campaign.name
                    FROM ad_group_label
                    WHERE ad_group_label.label IN ('{done_resources_str}')
                    AND campaign.name LIKE 'HS/%'
                """

                ag_response = ga_service.search(customer_id=customer_id, query=ag_query)

                # Group ad groups by theme
                ad_groups_by_theme = {theme: [] for theme in theme_mappings.keys()}

                for row in ag_response:
                    label_resource = row.ad_group_label.label

                    # Find which theme this belongs to
                    for theme, mapping in theme_mappings.items():
                        if label_resource == mapping['done_label_resource']:
                            ad_groups_by_theme[theme].append({
                                'id': str(row.ad_group.id),
                                'name': row.ad_group.name,
                                'resource': row.ad_group.resource_name,
                                'label_resource': row.ad_group_label.resource_name,
                                'campaign_id': str(row.campaign.id),
                                'campaign_name': row.campaign.name
                            })
                            break

                total_ag_count = sum(len(ags) for ags in ad_groups_by_theme.values())
                stats['ad_groups_with_done_label'] += total_ag_count
                logger.info(f"[{customer_id}] Found {total_ag_count} ad groups with DONE labels in HS/ campaigns")

                if total_ag_count == 0:
                    continue

                # Step 3: Process each theme (simplified audit)
                for theme, ad_groups_list in ad_groups_by_theme.items():
                    if not ad_groups_list:
                        continue

                    # Filter out already-audited ad groups
                    original_count = len(ad_groups_list)
                    if skip_audited and ad_groups_already_audited:
                        ad_groups_list = [ag for ag in ad_groups_list
                                          if ag['resource'] not in ad_groups_already_audited]
                        skipped_count = original_count - len(ad_groups_list)
                        if skipped_count > 0:
                            stats['ad_groups_skipped_already_audited'] += skipped_count
                            logger.info(f"[{customer_id}] Theme {theme}: Skipped {skipped_count} already-audited ad groups")

                    # Filter out checkup-failed ad groups (always skip these)
                    if ad_groups_checkup_failed:
                        before_filter = len(ad_groups_list)
                        ad_groups_list = [ag for ag in ad_groups_list
                                          if ag['resource'] not in ad_groups_checkup_failed]
                        skipped_failed = before_filter - len(ad_groups_list)
                        if skipped_failed > 0:
                            stats['ad_groups_skipped_already_audited'] += skipped_failed  # Reuse counter
                            logger.info(f"[{customer_id}] Theme {theme}: Skipped {skipped_failed} checkup-failed ad groups")

                    if not ad_groups_list:
                        logger.info(f"[{customer_id}] Theme {theme}: All ad groups filtered out - skipping")
                        continue

                    theme_label = theme_mappings[theme]['theme_label']
                    done_label = theme_mappings[theme]['done_label']

                    logger.info(f"[{customer_id}] Theme {theme}: Processing {len(ad_groups_list)} ad groups...")

                    operations = []
                    audit_operations = []

                    # Step 3.1: Batch query all ads for these ad groups to check for theme labels
                    theme_label_resource = theme_mappings[theme].get('theme_label_resource')

                    # Build map of ad group resource -> has theme ad
                    ag_has_theme_ad = {}

                    if theme_label_resource:
                        # Query ads with the theme label in batches
                        BATCH_SIZE = 500
                        for batch_start in range(0, len(ad_groups_list), BATCH_SIZE):
                            batch = ad_groups_list[batch_start:batch_start + BATCH_SIZE]
                            ag_resources_str = "', '".join([ag['resource'] for ag in batch])

                            ads_query = f"""
                                SELECT
                                    ad_group_ad.ad_group,
                                    ad_group_ad_label.label
                                FROM ad_group_ad_label
                                WHERE ad_group_ad_label.label = '{theme_label_resource}'
                                AND ad_group_ad.ad_group IN ('{ag_resources_str}')
                                AND ad_group_ad.status != REMOVED
                            """

                            try:
                                ads_response = ga_service.search(customer_id=customer_id, query=ads_query)
                                for row in ads_response:
                                    ag_resource = row.ad_group_ad.ad_group
                                    ag_has_theme_ad[ag_resource] = True
                            except Exception as e:
                                logger.warning(f"[{customer_id}] Theme {theme}: Failed to query ads batch: {e}")

                        logger.info(f"[{customer_id}] Theme {theme}: Found {len(ag_has_theme_ad)} ad groups with theme ads")
                    else:
                        logger.warning(f"[{customer_id}] Theme {theme}: No theme label resource found, assuming all need repair")

                    # Step 3.2: Check each ad group and mark for repair if theme ad is missing
                    for ag in ad_groups_list:
                        stats['ad_groups_checked'] += 1

                        # Check if this ad group has a theme ad
                        has_theme_ad = ag_has_theme_ad.get(ag['resource'], False)

                        if not has_theme_ad:
                            # Missing theme ad - mark for repair
                            stats['ad_groups_missing_theme_ads'] += 1
                            repair_items.append({
                                'customer_id': customer_id,
                                'campaign_id': ag['campaign_id'],
                                'campaign_name': ag['campaign_name'],
                                'ad_group_id': ag['id'],
                                'ad_group_name': ag['name'],
                                'theme_name': theme
                            })

                            # Remove DONE label since the theme ad is missing
                            operation = client.get_type('AdGroupLabelOperation')
                            operation.remove = ag['label_resource']
                            operations.append(operation)
                        else:
                            # Has theme ad - add THEMES_CHECK_DONE label to mark as validated
                            if audit_label_resource:
                                audit_op = client.get_type('AdGroupLabelOperation')
                                audit_op.create.ad_group = ag['resource']
                                audit_op.create.label = audit_label_resource
                                audit_operations.append(audit_op)
                                stats['audit_labels_added'] += 1

                    # Execute removals
                    if operations:
                        try:
                            response = ad_group_label_service.mutate_ad_group_labels(
                                customer_id=customer_id,
                                operations=operations[:5000]  # Limit to 5000
                            )
                            stats['done_labels_removed'] += len(response.results)
                            logger.info(f"[{customer_id}] Theme {theme}: Removed {len(response.results)} {done_label} labels")
                        except Exception as e:
                            logger.warning(f"[{customer_id}] Theme {theme}: Error removing labels: {e}")

                    # Execute audit label additions for validated ad groups
                    if audit_operations:
                        try:
                            # Process in batches of 5000
                            AUDIT_BATCH_SIZE = 5000
                            for batch_start in range(0, len(audit_operations), AUDIT_BATCH_SIZE):
                                batch = audit_operations[batch_start:batch_start + AUDIT_BATCH_SIZE]
                                response = ad_group_label_service.mutate_ad_group_labels(
                                    customer_id=customer_id,
                                    operations=batch
                                )
                                logger.info(f"[{customer_id}] Theme {theme}: Added {len(response.results)} THEMES_CHECK_DONE labels")
                        except Exception as e:
                            logger.warning(f"[{customer_id}] Theme {theme}: Error adding audit labels: {e}")

                logger.info(f"[{customer_id}]: Completed audit")

            except Exception as e:
                logger.error(f"Customer {customer_id}: Unexpected error: {e}", exc_info=True)
                continue

        # Create repair jobs if needed
        job_ids = []
        if repair_items:
            logger.info(f"Creating repair jobs for {len(repair_items)} ad groups")

            # Split into jobs based on job_chunk_size
            num_chunks = (len(repair_items) + job_chunk_size - 1) // job_chunk_size

            for chunk_idx in range(num_chunks):
                start_idx = chunk_idx * job_chunk_size
                end_idx = min(start_idx + job_chunk_size, len(repair_items))
                chunk_data = repair_items[start_idx:end_idx]

                # Create repair job with is_repair_job flag
                job_id = self.create_job(chunk_data, batch_size=batch_size, is_repair_job=True)
                job_ids.append(job_id)
                stats['repair_jobs_created'] += 1
                logger.info(f"Created repair job {job_id} with {len(chunk_data)} items")

                # Automatically start the job
                if background_tasks:
                    background_tasks.add_task(self.process_job, job_id)

        logger.info(f"Checkup completed: {stats}")

        return {
            'status': 'completed',
            'stats': stats,
            'repair_job_ids': job_ids
        }

    def get_next_pending_job(self) -> Optional[int]:
        """Get the oldest pending job (FIFO)."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            cur.execute("""
                SELECT id FROM thema_ads_jobs
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
            """)

            result = cur.fetchone()
            return result['id'] if result else None

        finally:
            cur.close()
            conn.close()

    def _get_customer_label_cache(self, client, customer_id: str) -> Dict[str, str]:
        """
        Fetch all labels for a customer and return a resource->name mapping.
        This eliminates N+1 query problems by caching labels.

        Returns:
            Dict mapping label resource names to label names
        """
        ga_service = client.get_service("GoogleAdsService")
        label_cache = {}

        try:
            # Single query to fetch all labels for this customer
            labels_query = """
                SELECT
                    label.resource_name,
                    label.name
                FROM label
            """

            label_response = ga_service.search(customer_id=customer_id, query=labels_query)
            for row in label_response:
                label_cache[row.label.resource_name] = row.label.name

            logger.info(f"Cached {len(label_cache)} labels for customer {customer_id}")

        except Exception as e:
            logger.warning(f"Could not cache labels for customer {customer_id}: {e}")

        return label_cache

    def _validate_missing_ad_groups(
        self,
        client,
        missing_by_theme: Dict[str, List[Dict]],
        selected_themes: List[str],
        batch_size: int = 5000
    ) -> Dict[str, List[Dict]]:
        """
        Validate missing ad groups using prefetch-style label checking.
        This ensures consistency with how job processing checks labels.

        Returns: Filtered dict with only ad groups that truly lack DONE labels
        """
        from themes import get_theme_label

        ga_service = client.get_service("GoogleAdsService")

        # Group ad groups by customer for efficient batching
        customer_ag_map = {}  # customer_id -> {theme -> [ag_data]}
        for theme, ag_list in missing_by_theme.items():
            for ag_data in ag_list:
                customer_id = ag_data['customer_id']
                if customer_id not in customer_ag_map:
                    customer_ag_map[customer_id] = {}
                if theme not in customer_ag_map[customer_id]:
                    customer_ag_map[customer_id][theme] = []
                customer_ag_map[customer_id][theme].append(ag_data)

        validated_missing = {theme: [] for theme in selected_themes}

        # Process each customer
        for customer_id, themes_ag_map in customer_ag_map.items():
            try:
                # Get label resources for all DONE labels (prefetch-style)
                label_resources_map = {}  # label_name -> resource_name
                for theme in selected_themes:
                    theme_label = get_theme_label(theme)
                    done_label_name = f"{theme_label}_DONE"

                    label_query = f"""
                        SELECT label.resource_name, label.name
                        FROM label
                        WHERE label.name = '{done_label_name}'
                    """
                    try:
                        label_search = ga_service.search(customer_id=customer_id, query=label_query)
                        for row in label_search:
                            label_resources_map[done_label_name] = row.label.resource_name
                            break
                    except Exception:
                        pass

                if not label_resources_map:
                    # No DONE labels exist for this customer - all are valid (but only for selected themes)
                    logger.info(f"Validation: No DONE label resources found for customer {customer_id}")
                    for theme, ag_list in themes_ag_map.items():
                        if theme in selected_themes:  # Only add themes that were requested
                            validated_missing[theme].extend(ag_list)
                            logger.info(f"  Accepting {len(ag_list)} ad groups for theme '{theme}' (no labels exist)")
                        else:
                            logger.info(f"  Skipping {len(ag_list)} ad groups for theme '{theme}' (not in selected themes)")
                    continue

                # Debug: log found label resources
                logger.debug(f"Validation: Customer {customer_id} label resources: {label_resources_map}")

                # Reverse map: resource -> name
                resource_to_name = {v: k for k, v in label_resources_map.items()}

                # Collect all ad group resources for this customer
                all_ag_resources = []
                ag_resource_to_data = {}  # ad_group_resource -> (theme, ag_data)
                for theme, ag_list in themes_ag_map.items():
                    for ag_data in ag_list:
                        ag_resource = f"customers/{customer_id}/adGroups/{ag_data['ad_group_id']}"
                        all_ag_resources.append(ag_resource)
                        ag_resource_to_data[ag_resource] = (theme, ag_data)

                # Query ad_group_label in batches (prefetch-style)
                ag_done_labels = {}  # ag_resource -> set of DONE label names
                for i in range(0, len(all_ag_resources), batch_size):
                    batch = all_ag_resources[i:i + batch_size]
                    resources_str = ", ".join(f"'{r}'" for r in batch)

                    query = f"""
                        SELECT
                            ad_group_label.ad_group,
                            ad_group_label.label
                        FROM ad_group_label
                        WHERE ad_group_label.ad_group IN ({resources_str})
                    """

                    try:
                        response = ga_service.search(customer_id=customer_id, query=query)
                        for row in response:
                            label_resource = row.ad_group_label.label
                            ag_resource = row.ad_group_label.ad_group

                            # Check if this is one of the DONE labels we care about
                            if label_resource in resource_to_name:
                                label_name = resource_to_name[label_resource]
                                if ag_resource not in ag_done_labels:
                                    ag_done_labels[ag_resource] = set()
                                ag_done_labels[ag_resource].add(label_name)
                    except Exception as e:
                        logger.warning(f"Validation query failed for customer {customer_id}: {e}")

                # Filter: only keep ad groups that DON'T have their theme's DONE label (and only for selected themes)
                for ag_resource, (theme, ag_data) in ag_resource_to_data.items():
                    # Skip themes that weren't requested
                    if theme not in selected_themes:
                        continue

                    theme_label = get_theme_label(theme)
                    done_label_name = f"{theme_label}_DONE"

                    ag_labels = ag_done_labels.get(ag_resource, set())
                    if done_label_name not in ag_labels:
                        # Confirmed missing - add to validated list
                        validated_missing[theme].append(ag_data)
                    else:
                        # Has DONE label - skip it
                        logger.debug(f"Filtered out ad group {ag_data['ad_group_id']} - already has {done_label_name}")

            except Exception as e:
                logger.error(f"Validation failed for customer {customer_id}: {e}", exc_info=True)
                # On error, include ad groups from this customer (fail-safe, but only for selected themes)
                for theme, ag_list in themes_ag_map.items():
                    if theme in selected_themes:
                        validated_missing[theme].extend(ag_list)

        return validated_missing

    async def discover_all_missing_themes(
        self,
        client,
        customer_filter: str = "Beslist.nl -",
        selected_themes: Optional[List[str]] = None,
        limit: Optional[int] = None,
        batch_size: int = 5000,
        job_chunk_size: int = 50000,
        background_tasks=None
    ) -> Dict:
        """
        Discover all ad groups and identify which themes are missing.
        Creates jobs to add missing theme ads to each ad group.

        Uses batch queries to minimize API calls and improve performance.

        Args:
            client: Google Ads API client
            customer_filter: Customer name prefix filter (default: "Beslist.nl -")
            selected_themes: List of themes to process (None = all themes)
            limit: Optional limit on number of ad groups to check
            batch_size: Batch size for API queries
            job_chunk_size: Maximum items per job
            background_tasks: FastAPI background tasks

        Returns:
            Dict with discovery results and created job IDs per theme
        """
        import time
        start_time = time.time()
        logger.info(f"Starting all-themes discovery: filter='{customer_filter}', themes={selected_themes}, limit={limit}")

        # Import theme utilities
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent / "thema_ads_optimized"))
        from themes import get_theme_label, SUPPORTED_THEMES

        # Default to all themes if not specified
        if selected_themes is None:
            selected_themes = list(SUPPORTED_THEMES.keys())

        logger.info(f"Processing themes: {selected_themes}")

        ga_service = client.get_service("GoogleAdsService")

        stats = {
            'customers_found': 0,
            'customers_processed': 0,
            'ad_groups_analyzed': 0,
            'ad_groups_with_missing_themes': 0,
            'jobs_created_by_theme': {},
            'missing_by_theme': {theme: 0 for theme in selected_themes}
        }

        # Track missing themes per ad group
        missing_by_theme = {theme: [] for theme in selected_themes}

        # Step 1: Find customers matching filter
        try:
            # Query for customers in MCC account
            customer_query = f"""
                SELECT
                    customer_client.id,
                    customer_client.descriptive_name
                FROM customer_client
                WHERE customer_client.descriptive_name LIKE '{customer_filter}%'
                AND customer_client.status = 'ENABLED'
            """

            # Query from MCC account (get login customer ID)
            from google.ads.googleads.client import GoogleAdsClient
            login_customer_id = client.login_customer_id

            customer_response = ga_service.search(customer_id=login_customer_id, query=customer_query)
            customer_ids = []

            for row in customer_response:
                customer_ids.append(str(row.customer_client.id))
                logger.info(f"Found customer: {row.customer_client.descriptive_name} ({row.customer_client.id})")

            stats['customers_found'] = len(customer_ids)

            if not customer_ids:
                logger.warning(f"No customers found matching filter: '{customer_filter}'")
                return {
                    'status': 'completed',
                    'stats': stats,
                    'job_ids_by_theme': {},
                    'message': f"No customers found matching filter: '{customer_filter}'"
                }

        except Exception as e:
            logger.error(f"Failed to query customers: {e}", exc_info=True)
            # Fall back to using provided customer IDs if available
            return {
                'status': 'error',
                'stats': stats,
                'message': f"Failed to query customers: {str(e)}"
            }

        # Step 2: Process each customer sequentially
        for customer_id in customer_ids:
            if limit and stats['ad_groups_analyzed'] >= limit:
                logger.info(f"Reached limit of {limit} ad groups analyzed")
                break

            try:
                logger.info(f"Processing customer {customer_id}")
                stats['customers_processed'] += 1

                # Cache all labels for this customer (eliminates N+1 queries)
                label_cache = self._get_customer_label_cache(client, customer_id)

                # Query ad groups in HS/ campaigns with active RSAs
                ad_groups_query = """
                    SELECT
                        ad_group_ad.ad_group,
                        ad_group.id,
                        ad_group.name,
                        campaign.id,
                        campaign.name
                    FROM ad_group_ad
                    WHERE campaign.name LIKE 'HS/%'
                    AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                    AND ad_group_ad.status != REMOVED
                    AND ad_group.status = 'ENABLED'
                    AND campaign.status = 'ENABLED'
                """

                ag_response = ga_service.search(customer_id=customer_id, query=ad_groups_query)

                # Collect unique ad groups
                ad_group_map = {}
                for row in ag_response:
                    ag_resource = row.ad_group_ad.ad_group
                    ag_id = str(row.ad_group.id)
                    if ag_id not in ad_group_map:
                        ad_group_map[ag_id] = {
                            'customer_id': customer_id,
                            'campaign_id': str(row.campaign.id),
                            'campaign_name': row.campaign.name,
                            'ad_group_id': ag_id,
                            'ad_group_name': row.ad_group.name,
                            'ad_group_resource': ag_resource
                        }

                logger.info(f"  Found {len(ad_group_map)} ad groups in HS/ campaigns")

                if not ad_group_map:
                    continue

                # Limit ad groups if specified
                if limit:
                    remaining = limit - stats['ad_groups_analyzed']
                    ad_group_list = list(ad_group_map.items())[:remaining]
                else:
                    ad_group_list = list(ad_group_map.items())

                logger.info(f"  Batch processing {len(ad_group_list)} ad groups")

                # Create resource -> ag_id lookup for fast mapping
                resource_to_ag_id = {ag_info['ad_group_resource']: ag_id for ag_id, ag_info in ad_group_list}

                # Step 3: BATCH fetch ad group labels (instead of one-by-one)
                BATCH_SIZE = 5000
                ag_labels_map = {}  # ag_id -> set of label names

                ad_group_resources = [ag_info['ad_group_resource'] for ag_id, ag_info in ad_group_list]

                for batch_start in range(0, len(ad_group_resources), BATCH_SIZE):
                    batch = ad_group_resources[batch_start:batch_start + BATCH_SIZE]
                    resources_str = ", ".join(f"'{r}'" for r in batch)

                    batch_labels_query = f"""
                        SELECT ad_group_label.ad_group, ad_group_label.label
                        FROM ad_group_label
                        WHERE ad_group_label.ad_group IN ({resources_str})
                    """

                    try:
                        label_response = ga_service.search(customer_id=customer_id, query=batch_labels_query)
                        for row in label_response:
                            ag_resource = row.ad_group_label.ad_group
                            label_resource = row.ad_group_label.label
                            label_name = label_cache.get(label_resource)

                            if label_name:
                                ag_id = resource_to_ag_id.get(ag_resource)
                                if ag_id:
                                    if ag_id not in ag_labels_map:
                                        ag_labels_map[ag_id] = set()
                                    ag_labels_map[ag_id].add(label_name)
                    except Exception as e:
                        logger.warning(f"  Batch label query failed: {e}")

                logger.info(f"  Fetched labels for {len(ag_labels_map)} ad groups in {(len(ad_group_resources) + BATCH_SIZE - 1) // BATCH_SIZE} batches")

                # Step 4: BATCH fetch ads for all ad groups
                all_ads_map = {}  # ag_id -> list of (ad_id, ad_resource)

                for batch_start in range(0, len(ad_group_resources), BATCH_SIZE):
                    batch = ad_group_resources[batch_start:batch_start + BATCH_SIZE]
                    resources_str = ", ".join(f"'{r}'" for r in batch)

                    batch_ads_query = f"""
                        SELECT
                            ad_group_ad.ad_group,
                            ad_group_ad.ad.id,
                            ad_group_ad.resource_name
                        FROM ad_group_ad
                        WHERE ad_group_ad.ad_group IN ({resources_str})
                        AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                        AND ad_group_ad.status != REMOVED
                    """

                    try:
                        ads_response = ga_service.search(customer_id=customer_id, query=batch_ads_query)
                        for row in ads_response:
                            ag_resource = row.ad_group_ad.ad_group
                            ad_id = str(row.ad_group_ad.ad.id)
                            ad_resource = row.ad_group_ad.resource_name

                            ag_id = resource_to_ag_id.get(ag_resource)
                            if ag_id:
                                if ag_id not in all_ads_map:
                                    all_ads_map[ag_id] = []
                                all_ads_map[ag_id].append((ad_id, ad_resource))
                    except Exception as e:
                        logger.warning(f"  Batch ads query failed: {e}")

                logger.info(f"  Fetched ads for {len(all_ads_map)} ad groups")

                # Step 5: BATCH fetch ad labels for all ads
                all_ad_labels_map = {}  # ad_id -> set of label names
                all_ad_resources = []
                ad_resource_to_id = {}  # ad_resource -> ad_id lookup
                for ads_list in all_ads_map.values():
                    for ad_id, ad_resource in ads_list:
                        all_ad_resources.append((ad_id, ad_resource))
                        ad_resource_to_id[ad_resource] = ad_id

                for batch_start in range(0, len(all_ad_resources), BATCH_SIZE):
                    batch = all_ad_resources[batch_start:batch_start + BATCH_SIZE]
                    resources_str = ", ".join(f"'{ad_resource}'" for ad_id, ad_resource in batch)

                    if not resources_str:
                        continue

                    batch_ad_labels_query = f"""
                        SELECT ad_group_ad_label.ad_group_ad, ad_group_ad_label.label
                        FROM ad_group_ad_label
                        WHERE ad_group_ad_label.ad_group_ad IN ({resources_str})
                    """

                    try:
                        ad_label_response = ga_service.search(customer_id=customer_id, query=batch_ad_labels_query)
                        for row in ad_label_response:
                            ad_resource = row.ad_group_ad_label.ad_group_ad
                            label_resource = row.ad_group_ad_label.label
                            label_name = label_cache.get(label_resource)

                            if label_name:
                                ad_id = ad_resource_to_id.get(ad_resource)
                                if ad_id:
                                    if ad_id not in all_ad_labels_map:
                                        all_ad_labels_map[ad_id] = set()
                                    all_ad_labels_map[ad_id].add(label_name)
                    except Exception as e:
                        logger.warning(f"  Batch ad labels query failed: {e}")

                logger.info(f"  Fetched labels for {len(all_ad_labels_map)} ads")

                # Step 6: Process results in memory (fast)
                for ag_id, ag_info in ad_group_list:
                    if limit and stats['ad_groups_analyzed'] >= limit:
                        break

                    stats['ad_groups_analyzed'] += 1

                    ag_labels = ag_labels_map.get(ag_id, set())
                    ads_list = all_ads_map.get(ag_id, [])

                    # Build ad labels map for this ad group
                    ad_labels_for_ag = {}
                    for ad_id, ad_resource in ads_list:
                        ad_labels_for_ag[ad_id] = all_ad_labels_map.get(ad_id, set())

                    # Determine missing themes for this ad group
                    ag_missing_themes = []
                    for theme in selected_themes:
                        theme_label = get_theme_label(theme)
                        done_label = f"{theme_label}_DONE"

                        # Check if theme is missing
                        has_done_label = done_label in ag_labels
                        has_theme_ad = any(theme_label in labels for labels in ad_labels_for_ag.values())

                        if not has_done_label and not has_theme_ad:
                            # Theme is missing
                            ag_missing_themes.append(theme)
                            stats['missing_by_theme'][theme] += 1

                            # Add to job list for this theme
                            missing_by_theme[theme].append({
                                'customer_id': ag_info['customer_id'],
                                'campaign_id': ag_info['campaign_id'],
                                'campaign_name': ag_info['campaign_name'],
                                'ad_group_id': ag_info['ad_group_id'],
                                'ad_group_name': ag_info['ad_group_name'],
                                'theme_name': theme
                            })

                    if ag_missing_themes:
                        stats['ad_groups_with_missing_themes'] += 1
                        logger.info(f"    Ad group {ag_id}: Missing themes: {ag_missing_themes}")

            except Exception as e:
                logger.error(f"Customer {customer_id}: Error processing: {e}", exc_info=True)
                continue

        # Step 2.5: Validate missing ad groups using prefetch-style logic
        # This ensures we use the same label-checking approach that job processing will use
        logger.info("Validating missing ad groups before creating jobs...")
        validated_missing_by_theme = self._validate_missing_ad_groups(
            client, missing_by_theme, selected_themes, batch_size
        )

        # Update stats with validation results
        for theme in selected_themes:
            original_count = len(missing_by_theme.get(theme, []))
            validated_count = len(validated_missing_by_theme.get(theme, []))
            if original_count != validated_count:
                removed = original_count - validated_count
                logger.info(f"  Theme '{theme}': Filtered out {removed} ad groups that already have DONE labels")
                stats['missing_by_theme'][theme] = validated_count

        # Step 3: Create jobs per theme (only for requested themes)
        job_ids_by_theme = {}
        for theme, ad_groups_list in validated_missing_by_theme.items():
            # Skip themes that weren't requested
            if theme not in selected_themes:
                logger.info(f"Skipping theme '{theme}' - not in selected themes")
                continue

            if not ad_groups_list:
                logger.info(f"No missing ad groups for theme '{theme}'")
                continue

            logger.info(f"Creating jobs for theme '{theme}' with {len(ad_groups_list)} ad groups")

            # Split into jobs based on job_chunk_size
            num_chunks = (len(ad_groups_list) + job_chunk_size - 1) // job_chunk_size
            job_ids_by_theme[theme] = []

            for chunk_idx in range(num_chunks):
                start_idx = chunk_idx * job_chunk_size
                end_idx = min(start_idx + job_chunk_size, len(ad_groups_list))
                chunk_data = ad_groups_list[start_idx:end_idx]

                # Add theme_name to each item in chunk_data so create_job can use it
                for item in chunk_data:
                    item['theme_name'] = theme

                # Create job
                job_id = self.create_job(chunk_data, batch_size=batch_size, is_repair_job=False)
                job_ids_by_theme[theme].append(job_id)
                logger.info(f"Created job {job_id} for theme '{theme}' with {len(chunk_data)} items")

                # Automatically start the first job
                if chunk_idx == 0 and background_tasks:
                    background_tasks.add_task(self.process_job, job_id)

            stats['jobs_created_by_theme'][theme] = len(job_ids_by_theme[theme])

        # Performance metrics
        total_elapsed = time.time() - start_time

        logger.info(f"All-themes discovery completed: {stats}")
        logger.info(f"Execution time: {total_elapsed:.1f}s")

        return {
            'status': 'completed',
            'stats': stats,
            'job_ids_by_theme': job_ids_by_theme
        }

    async def activate_ads_per_plan(
        self,
        client,
        customer_ids: Optional[List[str]] = None,
        batch_size: int = 5000,
        reset_labels: bool = False
    ) -> Dict:
        """
        Activate the correct theme ad per customer based on activation plan.
        Pauses all ads first, then activates the correct theme ad.

        Args:
            client: Google Ads API client
            customer_ids: Optional list of customer IDs to process (None = all in plan)
            batch_size: Batch size for API queries
            reset_labels: If True, reprocess ad groups with ACTIVATION_DONE label

        Returns:
            Dict with statistics and missing ads list
        """
        from backend.thema_ads_db import get_activation_plan, add_activation_missing_ad, clear_activation_missing_ads
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent / "thema_ads_optimized"))
        from themes import get_theme_label

        logger.info(f"Starting ad activation: customer_ids={customer_ids}, reset_labels={reset_labels}")

        # Clear old missing ads records
        clear_activation_missing_ads()

        # Get activation plan
        plan = get_activation_plan(customer_ids)
        if not plan:
            return {
                'status': 'error',
                'message': 'No activation plan found. Please upload an activation plan first.',
                'stats': {}
            }

        logger.info(f"Loaded activation plan for {len(plan)} customers")

        ga_service = client.get_service("GoogleAdsService")
        ad_group_ad_service = client.get_service("AdGroupAdService")
        ad_group_label_service = client.get_service("AdGroupLabelService")
        label_service = client.get_service("LabelService")

        stats = {
            'customers_processed': 0,
            'ad_groups_checked': 0,
            'ad_groups_activated': 0,
            'ad_groups_already_correct': 0,
            'ad_groups_missing_theme_ad': 0,
            'ad_groups_skipped_done_label': 0
        }

        # Ensure ACTIVATION_DONE label exists for all customers
        activation_done_labels = {}  # customer_id -> label_resource
        for customer_id in plan.keys():
            try:
                # Check if label exists
                label_query = """
                    SELECT label.resource_name
                    FROM label
                    WHERE label.name = 'ACTIVATION_DONE'
                    LIMIT 1
                """
                label_response = ga_service.search(customer_id=customer_id, query=label_query)
                label_found = False
                for row in label_response:
                    activation_done_labels[customer_id] = row.label.resource_name
                    label_found = True
                    break

                # Create label if not found
                if not label_found:
                    label_operation = client.get_type("LabelOperation")
                    label = label_operation.create
                    label.name = "ACTIVATION_DONE"
                    label.description = "Ad group processed by activation function"

                    response = label_service.mutate_labels(
                        customer_id=customer_id,
                        operations=[label_operation]
                    )
                    activation_done_labels[customer_id] = response.results[0].resource_name
                    logger.info(f"Created ACTIVATION_DONE label for customer {customer_id}")

            except Exception as e:
                logger.warning(f"Could not create ACTIVATION_DONE label for customer {customer_id}: {e}")

        # Process each customer
        for customer_id, required_theme in plan.items():
            try:
                logger.info(f"Processing customer {customer_id} - required theme: {required_theme}")
                stats['customers_processed'] += 1

                theme_label_name = get_theme_label(required_theme)

                # Query ad groups in HS/ campaigns
                ad_groups_query = """
                    SELECT
                        ad_group.id,
                        ad_group.name,
                        ad_group.resource_name,
                        campaign.id,
                        campaign.name
                    FROM ad_group
                    WHERE campaign.name LIKE 'HS/%'
                    AND ad_group.status = 'ENABLED'
                    AND campaign.status = 'ENABLED'
                """

                ag_response = ga_service.search(customer_id=customer_id, query=ad_groups_query)
                ad_groups_list = []
                for row in ag_response:
                    ad_groups_list.append({
                        'id': str(row.ad_group.id),
                        'name': row.ad_group.name,
                        'resource': row.ad_group.resource_name,
                        'campaign_id': str(row.campaign.id),
                        'campaign_name': row.campaign.name
                    })

                logger.info(f"  Found {len(ad_groups_list)} ad groups")

                # Batch query all ad group labels at once (Optimization 2)
                ad_groups_with_done_label = set()
                if not reset_labels and ad_groups_list:
                    try:
                        ag_resources = "', '".join([ag['resource'] for ag in ad_groups_list])
                        batch_labels_query = f"""
                            SELECT ad_group_label.ad_group, ad_group_label.label
                            FROM ad_group_label
                            WHERE ad_group_label.ad_group IN ('{ag_resources}')
                        """
                        batch_labels_response = ga_service.search(customer_id=customer_id, query=batch_labels_query)

                        done_label_resource = activation_done_labels.get(customer_id)
                        for label_row in batch_labels_response:
                            if done_label_resource and label_row.ad_group_label.label == done_label_resource:
                                ad_groups_with_done_label.add(label_row.ad_group_label.ad_group)

                        logger.info(f"  Found {len(ad_groups_with_done_label)} ad groups with ACTIVATION_DONE label")
                    except Exception as e:
                        logger.warning(f"  Could not batch query ad group labels: {e}")

                # Batch query all ads and their labels across all ad groups (Optimization 2 continued)
                ad_groups_ads_map = {}  # ad_group_resource -> list of ads
                if ad_groups_list:
                    try:
                        # Filter out ad groups with ACTIVATION_DONE label
                        ag_resources_to_query = [ag['resource'] for ag in ad_groups_list
                                                if not (not reset_labels and ag['resource'] in ad_groups_with_done_label)]

                        if ag_resources_to_query:
                            ag_resources_str = "', '".join(ag_resources_to_query)

                            # Query all ads with their labels using JOIN
                            batch_ads_query = f"""
                                SELECT
                                    ad_group_ad.ad_group,
                                    ad_group_ad.ad.id,
                                    ad_group_ad.ad.name,
                                    ad_group_ad.status,
                                    ad_group_ad.resource_name,
                                    label.name
                                FROM ad_group_ad
                                LEFT JOIN ad_group_ad_label ON ad_group_ad.resource_name = ad_group_ad_label.ad_group_ad
                                LEFT JOIN label ON ad_group_ad_label.label = label.resource_name
                                WHERE ad_group_ad.ad_group IN ('{ag_resources_str}')
                                AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                                AND ad_group_ad.status != REMOVED
                            """

                            batch_ads_response = ga_service.search(customer_id=customer_id, query=batch_ads_query)

                            # Organize ads by ad group
                            ads_temp_map = {}  # ad_resource -> ad dict
                            for ad_row in batch_ads_response:
                                ad_group_res = ad_row.ad_group_ad.ad_group
                                ad_res = ad_row.ad_group_ad.resource_name

                                # Create ad entry if not exists
                                if ad_res not in ads_temp_map:
                                    ads_temp_map[ad_res] = {
                                        'id': str(ad_row.ad_group_ad.ad.id),
                                        'name': ad_row.ad_group_ad.ad.name,
                                        'status': ad_row.ad_group_ad.status.name,
                                        'resource': ad_res,
                                        'ad_group': ad_group_res,
                                        'labels': set()
                                    }

                                # Add label if present
                                if hasattr(ad_row, 'label') and hasattr(ad_row.label, 'name'):
                                    ads_temp_map[ad_res]['labels'].add(ad_row.label.name)

                            # Group ads by ad group
                            for ad in ads_temp_map.values():
                                ad_group_res = ad['ad_group']
                                if ad_group_res not in ad_groups_ads_map:
                                    ad_groups_ads_map[ad_group_res] = []
                                ad_groups_ads_map[ad_group_res].append(ad)

                            logger.info(f"  Batch queried ads for {len(ad_groups_ads_map)} ad groups")
                    except Exception as e:
                        logger.warning(f"  Could not batch query ads: {e}")

                # Process each ad group
                for ag in ad_groups_list:
                    stats['ad_groups_checked'] += 1
                    ag_resource = ag['resource']

                    # Check for ACTIVATION_DONE label (skip if present unless reset_labels)
                    if not reset_labels and ag_resource in ad_groups_with_done_label:
                        stats['ad_groups_skipped_done_label'] += 1
                        continue

                    # Get ads from pre-fetched data, or fall back to individual query
                    ads_list = ad_groups_ads_map.get(ag_resource)

                    if ads_list is None:
                        # Fallback: query ads individually if batch query failed
                        ads_query = f"""
                            SELECT
                                ad_group_ad.ad.id,
                                ad_group_ad.ad.name,
                                ad_group_ad.status,
                                ad_group_ad.resource_name
                            FROM ad_group_ad
                            WHERE ad_group_ad.ad_group = '{ag_resource}'
                            AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                            AND ad_group_ad.status != REMOVED
                        """

                        ads_list = []
                        try:
                            ads_response = ga_service.search(customer_id=customer_id, query=ads_query)
                            for ad_row in ads_response:
                                ads_list.append({
                                    'id': str(ad_row.ad_group_ad.ad.id),
                                    'name': ad_row.ad_group_ad.ad.name,
                                    'status': ad_row.ad_group_ad.status.name,
                                    'resource': ad_row.ad_group_ad.resource_name,
                                    'labels': set()
                                })

                            # Query labels for all ads in one query using JOIN
                            if ads_list:
                                ad_resources = "', '".join([ad['resource'] for ad in ads_list])
                                ads_labels_query = f"""
                                    SELECT
                                        ad_group_ad_label.ad_group_ad,
                                        label.name
                                    FROM ad_group_ad_label
                                    LEFT JOIN label ON ad_group_ad_label.label = label.resource_name
                                    WHERE ad_group_ad_label.ad_group_ad IN ('{ad_resources}')
                                """
                                try:
                                    ads_labels_response = ga_service.search(customer_id=customer_id, query=ads_labels_query)
                                    # Build a map of ad_resource -> labels
                                    ad_labels_map = {}
                                    for label_row in ads_labels_response:
                                        ad_res = label_row.ad_group_ad_label.ad_group_ad
                                        label_name = label_row.label.name
                                        if ad_res not in ad_labels_map:
                                            ad_labels_map[ad_res] = set()
                                        ad_labels_map[ad_res].add(label_name)

                                    # Assign labels to ads
                                    for ad in ads_list:
                                        if ad['resource'] in ad_labels_map:
                                            ad['labels'] = ad_labels_map[ad['resource']]
                                except Exception as e:
                                    logger.warning(f"    Could not query ad labels in batch: {e}")

                        except Exception as e:
                            logger.warning(f"    Could not query ads for ad group {ag['id']}: {e}")
                            continue

                    if not ads_list:
                        continue

                    # Find the theme ad for required theme
                    theme_ad = None
                    for ad in ads_list:
                        if theme_label_name in ad['labels']:
                            theme_ad = ad
                            break

                    if not theme_ad:
                        # Missing required theme ad - track and skip
                        add_activation_missing_ad(
                            customer_id=customer_id,
                            campaign_id=ag['campaign_id'],
                            campaign_name=ag['campaign_name'],
                            ad_group_id=ag['id'],
                            ad_group_name=ag['name'],
                            required_theme=required_theme
                        )
                        stats['ad_groups_missing_theme_ad'] += 1
                        logger.info(f"    Ad group {ag['id']}: Missing {required_theme} ad - tracked")
                        continue

                    # Check if correct ad is already the only active ad
                    active_ads = [ad for ad in ads_list if ad['status'] == 'ENABLED']
                    if len(active_ads) == 1 and active_ads[0]['id'] == theme_ad['id']:
                        stats['ad_groups_already_correct'] += 1
                        logger.info(f"    Ad group {ag['id']}: Already correct - skipped")
                        continue

                    # Activate: pause all ads, then enable the correct one
                    try:
                        operations = []

                        # Step 1: Pause all enabled ads
                        for ad in ads_list:
                            if ad['status'] == 'ENABLED':
                                operation = client.get_type("AdGroupAdOperation")
                                ad_group_ad = operation.update
                                ad_group_ad.resource_name = ad['resource']
                                ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED
                                operation.update_mask.paths.append('status')
                                operations.append(operation)

                        # Execute pause operations
                        if operations:
                            ad_group_ad_service.mutate_ad_group_ads(
                                customer_id=customer_id,
                                operations=operations
                            )
                            logger.info(f"    Ad group {ag['id']}: Paused {len(operations)} ads")

                        # Step 2: Enable the correct theme ad
                        enable_operation = client.get_type("AdGroupAdOperation")
                        ad_group_ad = enable_operation.update
                        ad_group_ad.resource_name = theme_ad['resource']
                        ad_group_ad.status = client.enums.AdGroupAdStatusEnum.ENABLED
                        enable_operation.update_mask.paths.append('status')

                        ad_group_ad_service.mutate_ad_group_ads(
                            customer_id=customer_id,
                            operations=[enable_operation]
                        )
                        logger.info(f"    Ad group {ag['id']}: Activated {required_theme} ad")

                        # Step 3: Add ACTIVATION_DONE label
                        if customer_id in activation_done_labels:
                            label_operation = client.get_type("AdGroupLabelOperation")
                            ad_group_label = label_operation.create
                            ad_group_label.ad_group = ag_resource
                            ad_group_label.label = activation_done_labels[customer_id]

                            try:
                                ad_group_label_service.mutate_ad_group_labels(
                                    customer_id=customer_id,
                                    operations=[label_operation]
                                )
                            except Exception as e:
                                # Ignore if label already exists
                                if "ALREADY_EXISTS" not in str(e):
                                    logger.warning(f"    Could not add ACTIVATION_DONE label: {e}")

                        stats['ad_groups_activated'] += 1

                    except Exception as e:
                        logger.error(f"    Failed to activate ad in ad group {ag['id']}: {e}")
                        continue

                logger.info(f"Customer {customer_id}: Completed")

            except Exception as e:
                logger.error(f"Customer {customer_id}: Error: {e}", exc_info=True)
                continue

        logger.info(f"Ad activation completed: {stats}")

        return {
            'status': 'completed',
            'stats': stats
        }

    async def activate_ads_per_plan_optimized(
        self,
        client,
        customer_ids: Optional[List[str]] = None,
        parallel_workers: int = 5,
        reset_labels: bool = False
    ) -> Dict:
        """
        OPTIMIZED: Activate the correct theme ad per customer based on activation plan.

        Key optimizations:
        1. Parallel customer processing (5-10x faster than sequential)
        2. Batch ALL mutations per customer:
           - Collect all pause/enable operations across all ad groups
           - Execute in single mutate_ad_group_ads() call
           - Batch label operations across all ad groups
        3. Uses EXACT same proven query patterns as activate_ads_per_plan()

        Args:
            client: Google Ads API client
            customer_ids: Optional list of customer IDs to process (None = all in plan)
            parallel_workers: Number of customers to process in parallel (default: 5)
            reset_labels: If True, reprocess ad groups with ACTIVATION_DONE label

        Returns:
            Dict with statistics
        """
        from backend.thema_ads_db import get_activation_plan, add_activation_missing_ad, clear_activation_missing_ads
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent / "thema_ads_optimized"))
        from themes import get_theme_label

        logger.info(f"Starting OPTIMIZED ad activation: customers={customer_ids}, parallel={parallel_workers}, reset={reset_labels}")

        # Clear old missing ads records
        clear_activation_missing_ads()

        # Get activation plan
        plan = get_activation_plan(customer_ids)
        if not plan:
            return {
                'status': 'error',
                'message': 'No activation plan found. Please upload an activation plan first.',
                'stats': {}
            }

        logger.info(f"Loaded activation plan for {len(plan)} customers")

        # Shared stats (thread-safe with asyncio)
        stats = {
            'customers_processed': 0,
            'customers_failed': 0,
            'ad_groups_checked': 0,
            'ad_groups_activated': 0,
            'ad_groups_already_correct': 0,
            'ad_groups_missing_theme_ad': 0,
            'ad_groups_skipped_done_label': 0,
            'theme_ads_enabled': 0,
            'original_ads_paused': 0,
            'errors': []
        }
        stats_lock = asyncio.Lock()

        async def process_customer(customer_id: str, required_theme: str):
            """
            Process a single customer using AD-FIRST approach.

            Instead of querying all ad groups and filtering, we directly query:
            1. All ads with the required theme label (THEME_BF, etc.)
            2. All ads with THEMA_ORIGINAL label in those same ad groups
            3. Build batch operations to enable theme ads and pause original ads

            This is 10-100x faster than the ad-group-first approach.
            """
            try:
                logger.info(f"[{customer_id}] Processing (AD-FIRST) - required theme: {required_theme}")

                ga_service = client.get_service("GoogleAdsService")
                ad_group_ad_service = client.get_service("AdGroupAdService")

                theme_label_name = get_theme_label(required_theme)

                # Step 1: Direct query for ALL ads with the theme label in HS/ campaigns
                ad_groups_query = """
                    SELECT
                        ad_group.id,
                        ad_group.name,
                        ad_group.resource_name,
                        campaign.id,
                        campaign.name
                    FROM ad_group
                    WHERE campaign.name LIKE 'HS/%'
                    AND ad_group.status = 'ENABLED'
                    AND campaign.status = 'ENABLED'
                """

                ad_groups_list = []
                try:
                    response = ga_service.search(customer_id=customer_id, query=ad_groups_query)
                    for row in response:
                        ad_groups_list.append({
                            'id': str(row.ad_group.id),
                            'name': row.ad_group.name,
                            'resource': row.ad_group.resource_name,
                            'campaign_id': str(row.campaign.id),
                            'campaign_name': row.campaign.name
                        })
                    logger.info(f"[{customer_id}] Found {len(ad_groups_list)} ad groups")
                except Exception as e:
                    logger.error(f"[{customer_id}] Failed to query ad groups: {e}")
                    async with stats_lock:
                        stats['customers_failed'] += 1
                        stats['errors'].append(f"{customer_id}: Failed to query ad groups - {e}")
                    return

                if not ad_groups_list:
                    logger.info(f"[{customer_id}] No ad groups found")
                    async with stats_lock:
                        stats['customers_processed'] += 1
                    return

                # Step 2: Query ACTIVATION_DONE labels to filter ad groups (if not reset_labels)
                ad_groups_with_done_label = set()
                if not reset_labels:
                    try:
                        ag_resources = "', '".join([ag['resource'] for ag in ad_groups_list])
                        batch_labels_query = f"""
                            SELECT ad_group_label.ad_group, label.name
                            FROM ad_group_label
                            LEFT JOIN label ON ad_group_label.label = label.resource_name
                            WHERE ad_group_label.ad_group IN ('{ag_resources}')
                            AND label.name = 'ACTIVATION_DONE'
                        """
                        label_response = ga_service.search(customer_id=customer_id, query=batch_labels_query)
                        for label_row in label_response:
                            ad_groups_with_done_label.add(label_row.ad_group_label.ad_group)

                        logger.info(f"[{customer_id}] Found {len(ad_groups_with_done_label)} ad groups with ACTIVATION_DONE label")
                    except Exception as e:
                        logger.warning(f"[{customer_id}] Could not query ACTIVATION_DONE labels: {e}")

                # Filter out ad groups with ACTIVATION_DONE label (unless reset_labels=True)
                ag_resources_to_query = [ag['resource'] for ag in ad_groups_list
                                        if reset_labels or ag['resource'] not in ad_groups_with_done_label]

                if not ag_resources_to_query:
                    logger.info(f"[{customer_id}] All ad groups already processed (ACTIVATION_DONE)")
                    async with stats_lock:
                        stats['customers_processed'] += 1
                        stats['ad_groups_skipped_done_label'] += len(ad_groups_list)
                    return

                # Step 3: Batch query all ads (WITHOUT labels - two-step approach)
                # LEFT JOIN is unreliable in GAQL, so fetch ads first, then labels separately
                ag_resources_str = "', '".join(ag_resources_to_query)
                batch_ads_query = f"""
                    SELECT
                        ad_group_ad.ad_group,
                        ad_group_ad.ad.id,
                        ad_group_ad.status,
                        ad_group_ad.resource_name
                    FROM ad_group_ad
                    WHERE ad_group_ad.ad_group IN ('{ag_resources_str}')
                    AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                    AND ad_group_ad.status != REMOVED
                """

                # Organize ads by ad group
                ads_by_ag = {}  # ad_group_resource -> {ads: []}
                ad_resources = []  # Track all ad resources for label query
                try:
                    response = ga_service.search(customer_id=customer_id, query=batch_ads_query)
                    for row in response:
                        ag_res = row.ad_group_ad.ad_group
                        if ag_res not in ads_by_ag:
                            ads_by_ag[ag_res] = {'ads': []}

                        ad_res = row.ad_group_ad.resource_name
                        ad_info = {
                            'resource': ad_res,
                            'status': row.ad_group_ad.status.name,
                            'labels': set()
                        }
                        ads_by_ag[ag_res]['ads'].append(ad_info)
                        ad_resources.append(ad_res)

                    logger.info(f"[{customer_id}] Found {len(ad_resources)} ads in {len(ads_by_ag)} ad groups")
                except Exception as e:
                    logger.error(f"[{customer_id}] Failed to batch query ads: {e}")
                    async with stats_lock:
                        stats['customers_failed'] += 1
                        stats['errors'].append(f"{customer_id}: Failed to batch query ads - {e}")
                    return

                # Step 3b: Batch query labels for all ads (in chunks to avoid FILTER_HAS_TOO_MANY_VALUES)
                if ad_resources:
                    try:
                        chunk_size = 5000
                        for i in range(0, len(ad_resources), chunk_size):
                            chunk = ad_resources[i:i+chunk_size]
                            ad_res_str = "', '".join(chunk)
                            labels_query = f"""
                                SELECT
                                    ad_group_ad_label.ad_group_ad,
                                    label.name
                                FROM ad_group_ad_label
                                LEFT JOIN label ON ad_group_ad_label.label = label.resource_name
                                WHERE ad_group_ad_label.ad_group_ad IN ('{ad_res_str}')
                            """

                            label_response = ga_service.search(customer_id=customer_id, query=labels_query)
                            for label_row in label_response:
                                ad_res = label_row.ad_group_ad_label.ad_group_ad
                                label_name = label_row.label.name if hasattr(label_row, 'label') and hasattr(label_row.label, 'name') else None

                                # Find the ad and add the label
                                for ag_data in ads_by_ag.values():
                                    for ad in ag_data['ads']:
                                        if ad['resource'] == ad_res and label_name:
                                            ad['labels'].add(label_name)
                                            break

                        logger.info(f"[{customer_id}] Fetched labels for all ads")
                    except Exception as e:
                        logger.warning(f"[{customer_id}] Could not fetch labels (continuing without): {e}")

                # Step 4: Process each ad group (only those without ACTIVATION_DONE label)
                enable_operations = []
                pause_operations = []

                # Track skipped ad groups
                skipped_count = len(ad_groups_list) - len(ag_resources_to_query)
                if skipped_count > 0:
                    async with stats_lock:
                        stats['ad_groups_skipped_done_label'] += skipped_count

                for ag in ad_groups_list:
                    ag_res = ag['resource']
                    ag_id = ag['id']

                    # Skip if has ACTIVATION_DONE label (unless reset_labels)
                    if not reset_labels and ag_res in ad_groups_with_done_label:
                        continue

                    ag_ads = ads_by_ag.get(ag_res, {'ads': [], 'labels': set()})

                    async with stats_lock:
                        stats['ad_groups_checked'] += 1

                    # Find theme ad and THEMA_ORIGINAL ads
                    theme_ad = None
                    original_ads = []
                    for ad in ag_ads['ads']:
                        if theme_label_name in ad['labels']:
                            theme_ad = ad
                        if 'THEMA_ORIGINAL' in ad['labels']:
                            original_ads.append(ad)

                    if theme_ad:
                        needs_activation = False

                        # Enable theme ad if paused
                        if theme_ad['status'] == 'PAUSED':
                            operation = client.get_type("AdGroupAdOperation")
                            ad_group_ad = operation.update
                            ad_group_ad.resource_name = theme_ad['resource']
                            ad_group_ad.status = client.enums.AdGroupAdStatusEnum.ENABLED
                            operation.update_mask.paths.append('status')
                            enable_operations.append(operation)
                            needs_activation = True

                        # Pause all THEMA_ORIGINAL ads
                        for orig_ad in original_ads:
                            if orig_ad['status'] == 'ENABLED':
                                operation = client.get_type("AdGroupAdOperation")
                                ad_group_ad = operation.update
                                ad_group_ad.resource_name = orig_ad['resource']
                                ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED
                                operation.update_mask.paths.append('status')
                                pause_operations.append(operation)
                                needs_activation = True

                        async with stats_lock:
                            if needs_activation:
                                stats['ad_groups_activated'] += 1
                            else:
                                stats['ad_groups_already_correct'] += 1
                    else:
                        # Missing theme ad
                        add_activation_missing_ad(
                            customer_id=customer_id,
                            campaign_id=ag['campaign_id'],
                            campaign_name=ag['campaign_name'],
                            ad_group_id=ag_id,
                            ad_group_name=ag['name'],
                            required_theme=required_theme
                        )
                        async with stats_lock:
                            stats['ad_groups_missing_theme_ad'] += 1

                # Step 4: Execute batch mutations
                # IMPORTANT: Pause originals FIRST to free up RSA slots, then enable theme ads
                if pause_operations:
                    try:
                        chunk_size = 5000
                        for i in range(0, len(pause_operations), chunk_size):
                            chunk = pause_operations[i:i+chunk_size]
                            ad_group_ad_service.mutate_ad_group_ads(
                                customer_id=customer_id,
                                operations=chunk
                            )
                        logger.info(f"[{customer_id}] Paused {len(pause_operations)} THEMA_ORIGINAL ads")
                        async with stats_lock:
                            stats['original_ads_paused'] += len(pause_operations)
                    except Exception as e:
                        logger.error(f"[{customer_id}] Failed to pause THEMA_ORIGINAL ads: {e}")
                        async with stats_lock:
                            stats['errors'].append(f"{customer_id}: Failed to pause THEMA_ORIGINAL ads - {e}")

                # Now enable theme ads (after pausing originals to free up slots)
                if enable_operations:
                    try:
                        chunk_size = 5000
                        for i in range(0, len(enable_operations), chunk_size):
                            chunk = enable_operations[i:i+chunk_size]
                            ad_group_ad_service.mutate_ad_group_ads(
                                customer_id=customer_id,
                                operations=chunk
                            )
                        logger.info(f"[{customer_id}] Enabled {len(enable_operations)} theme ads")
                        async with stats_lock:
                            stats['theme_ads_enabled'] += len(enable_operations)
                    except Exception as e:
                        logger.error(f"[{customer_id}] Failed to enable theme ads: {e}")
                        async with stats_lock:
                            stats['errors'].append(f"{customer_id}: Failed to enable theme ads - {e}")

                async with stats_lock:
                    stats['customers_processed'] += 1

                logger.info(f"[{customer_id}] Completed successfully")

            except Exception as e:
                logger.error(f"[{customer_id}] Error: {e}", exc_info=True)
                async with stats_lock:
                    stats['customers_failed'] += 1
                    stats['errors'].append(f"{customer_id}: {str(e)}")

        # Process customers in parallel
        tasks = []
        for customer_id, required_theme in plan.items():
            task = process_customer(customer_id, required_theme)
            tasks.append(task)

        # Run in batches of parallel_workers
        for i in range(0, len(tasks), parallel_workers):
            batch = tasks[i:i+parallel_workers]
            await asyncio.gather(*batch, return_exceptions=True)

        logger.info(f"OPTIMIZED ad activation completed: {stats}")

        return {
            'status': 'completed',
            'stats': stats
        }

    async def activate_ads_per_plan_v2(
        self,
        client,
        customer_ids: Optional[List[str]] = None,
        parallel_workers: int = 5,
        reset_labels: bool = False
    ) -> Dict:
        """
        V2: Ultra-fast AD-FIRST activation approach.

        Key innovation: Directly query ads by label instead of querying all ad groups first.
        This is 10-100x faster because we only query the exact ads we need.

        Performance comparison:
        - Ad-group-first: Query 10,000 ad groups → filter → query ads → query labels (slow)
        - Ad-first (V2): Query ~1,000 theme ads → query ~2,000 original ads → done (fast!)

        Args:
            client: Google Ads API client
            customer_ids: Optional list of customer IDs (None = all in plan)
            parallel_workers: Number of customers to process in parallel (default: 5)
            reset_labels: If True, reprocess ad groups with ACTIVATION_DONE label

        Returns:
            Dict with status and statistics
        """
        from backend.thema_ads_db import get_activation_plan, add_activation_missing_ad, clear_activation_missing_ads
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent / "thema_ads_optimized"))
        from themes import get_theme_label

        logger.info(f"Starting V2 (AD-FIRST) activation: customers={customer_ids}, parallel={parallel_workers}")

        # Clear old missing ads records
        clear_activation_missing_ads()

        # Get activation plan
        plan = get_activation_plan(customer_ids)
        if not plan:
            return {
                'status': 'error',
                'message': 'No activation plan found. Please upload an activation plan first.',
                'stats': {}
            }

        logger.info(f"Loaded activation plan for {len(plan)} customers")

        # Shared stats (thread-safe with asyncio)
        stats = {
            'customers_processed': 0,
            'customers_failed': 0,
            'ad_groups_checked': 0,
            'ad_groups_activated': 0,
            'ad_groups_already_correct': 0,
            'ad_groups_skipped_done_label': 0,
            'ad_groups_missing_theme_ad': 0,
            'theme_ads_enabled': 0,
            'original_ads_paused': 0,
            'errors': []
        }
        stats_lock = asyncio.Lock()

        async def process_customer_v2(customer_id: str, required_theme: str):
            """Process a single customer using AD-FIRST approach."""
            try:
                logger.info(f"[{customer_id}] V2 Processing - theme: {required_theme}")

                ga_service = client.get_service("GoogleAdsService")
                ad_group_ad_service = client.get_service("AdGroupAdService")

                theme_label_name = get_theme_label(required_theme)

                # Step 0: Get the label ID for the theme label
                # Handle case where multiple labels have the same name - pick the one with most ads
                label_query = f"""
                    SELECT label.id, label.resource_name
                    FROM label
                    WHERE label.name = '{theme_label_name}'
                """

                theme_label_id = None
                try:
                    response = ga_service.search(customer_id=customer_id, query=label_query)
                    candidate_labels = [row.label.id for row in response]

                    if len(candidate_labels) == 1:
                        theme_label_id = candidate_labels[0]
                    elif len(candidate_labels) > 1:
                        # Multiple labels with same name - pick the one with most ads
                        logger.warning(f"[{customer_id}] Found {len(candidate_labels)} labels named {theme_label_name}, picking best one")
                        best_count = -1
                        for label_id in candidate_labels:
                            count_query = f"""
                                SELECT ad_group_ad.ad.id
                                FROM ad_group_ad_label
                                WHERE label.id = {label_id}
                                AND ad_group_ad.status != REMOVED
                            """
                            count_response = ga_service.search(customer_id=customer_id, query=count_query)
                            count = sum(1 for _ in count_response)
                            if count > best_count:
                                best_count = count
                                theme_label_id = label_id
                        logger.info(f"[{customer_id}] Selected label ID {theme_label_id} with {best_count} ads")
                except Exception as e:
                    logger.error(f"[{customer_id}] Failed to find label {theme_label_name}: {e}")
                    async with stats_lock:
                        stats['customers_failed'] += 1
                        stats['errors'].append(f"{customer_id}: Label {theme_label_name} not found - {e}")
                    return

                if not theme_label_id:
                    logger.info(f"[{customer_id}] Label {theme_label_name} does not exist")
                    async with stats_lock:
                        stats['customers_processed'] += 1
                    return

                logger.info(f"[{customer_id}] Found label {theme_label_name} with ID: {theme_label_id}")

                # Step 1: Direct query for ALL theme ads by querying FROM ad_group_ad_label
                # This is the KEY optimization - query FROM the label relationship!
                theme_ads_query = f"""
                    SELECT
                        ad_group_ad.ad_group,
                        ad_group_ad.resource_name,
                        ad_group_ad.status,
                        campaign.name
                    FROM ad_group_ad_label
                    WHERE campaign.name LIKE 'HS/%'
                    AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                    AND ad_group_ad.status != REMOVED
                    AND label.id = {theme_label_id}
                """

                # Organize theme ads by ad group
                theme_ads_by_ag = {}  # ad_group_resource -> list of ad dicts
                ad_groups_with_theme = set()

                logger.debug(f"[{customer_id}] Theme ads query: {theme_ads_query}")

                try:
                    response = ga_service.search(customer_id=customer_id, query=theme_ads_query)
                    for row in response:
                        ag_res = row.ad_group_ad.ad_group
                        ad_res = row.ad_group_ad.resource_name
                        ad_status = row.ad_group_ad.status.name

                        # Support multiple theme ads per ad group (store as list)
                        if ag_res not in theme_ads_by_ag:
                            theme_ads_by_ag[ag_res] = []

                        theme_ads_by_ag[ag_res].append({
                            'resource': ad_res,
                            'status': ad_status
                        })
                        ad_groups_with_theme.add(ag_res)

                    total_theme_ads = sum(len(ads) for ads in theme_ads_by_ag.values())
                    logger.info(f"[{customer_id}] Found {total_theme_ads} theme ads in {len(ad_groups_with_theme)} ad groups")
                except Exception as e:
                    logger.error(f"[{customer_id}] Failed to query theme ads: {e}")
                    async with stats_lock:
                        stats['customers_failed'] += 1
                        stats['errors'].append(f"{customer_id}: Failed to query theme ads - {e}")
                    return

                if not theme_ads_by_ag:
                    logger.info(f"[{customer_id}] No theme ads found")
                    async with stats_lock:
                        stats['customers_processed'] += 1
                    return

                # OPTIMIZATION: Only query RSAs for ad groups where theme ad is PAUSED
                # If theme ad is already ENABLED, we don't need to pause other RSAs
                ad_groups_needing_rsa_query = set()
                ad_groups_already_active = set()
                for ag_res, theme_ads in theme_ads_by_ag.items():
                    has_paused_theme_ad = any(ad['status'] == 'PAUSED' for ad in theme_ads)
                    if has_paused_theme_ad:
                        ad_groups_needing_rsa_query.add(ag_res)
                    else:
                        ad_groups_already_active.add(ag_res)

                logger.info(f"[{customer_id}] {len(ad_groups_needing_rsa_query)} ad groups need activation, {len(ad_groups_already_active)} already have ENABLED theme ad")

                # Step 2: Query ALL RSAs in those ad groups (not just theme-labeled ones)
                # This ensures we pause ALL other RSAs before enabling the theme ad
                # IMPORTANT: Batch the query to avoid FILTER_HAS_TOO_MANY_VALUES error
                other_ads_by_ag = {}  # ad_group_resource -> {ad_resource -> ad_info}
                ad_groups_list = list(ad_groups_needing_rsa_query)  # OPTIMIZED: only query ad groups that need activation
                BATCH_SIZE = 1000  # Google Ads API limit for IN clause values

                # Build a set of target theme ad resources to exclude from pausing
                target_theme_ad_resources = set()
                for ag_res, theme_ads in theme_ads_by_ag.items():
                    for theme_ad in theme_ads:
                        target_theme_ad_resources.add(theme_ad['resource'])

                if ad_groups_list:
                    logger.info(f"[{customer_id}] Querying ALL RSAs (to pause) in {len(ad_groups_list)} ad groups (batched)...")
                else:
                    logger.info(f"[{customer_id}] No ad groups need RSA query (all theme ads already ENABLED)")

                for batch_idx in range(0, len(ad_groups_list), BATCH_SIZE):
                    batch = ad_groups_list[batch_idx:batch_idx + BATCH_SIZE]
                    ag_resources_str = "', '".join(batch)

                    # Query ALL RSAs in these ad groups (not just theme-labeled ones)
                    all_rsas_query = f"""
                        SELECT
                            ad_group_ad.ad_group,
                            ad_group_ad.resource_name,
                            ad_group_ad.status
                        FROM ad_group_ad
                        WHERE ad_group_ad.ad_group IN ('{ag_resources_str}')
                        AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
                        AND ad_group_ad.status != REMOVED
                    """

                    try:
                        response = ga_service.search(customer_id=customer_id, query=all_rsas_query)
                        for row in response:
                            ag_res = row.ad_group_ad.ad_group
                            ad_res = row.ad_group_ad.resource_name
                            ad_status = row.ad_group_ad.status.name

                            # Skip the target theme ads (we'll enable those, not pause)
                            if ad_res in target_theme_ad_resources:
                                continue

                            # Store unique ads by resource_name
                            if ag_res not in other_ads_by_ag:
                                other_ads_by_ag[ag_res] = {}

                            # Only store if not already present (prevent duplicate operations)
                            if ad_res not in other_ads_by_ag[ag_res]:
                                other_ads_by_ag[ag_res][ad_res] = {
                                    'resource': ad_res,
                                    'status': ad_status
                                }

                    except Exception as e:
                        logger.warning(f"[{customer_id}] Batch {batch_idx//BATCH_SIZE + 1}: Could not query RSAs: {e}")

                logger.info(f"[{customer_id}] Found {sum(len(ads) for ads in other_ads_by_ag.values())} other RSAs to pause (excluding target theme ads)")

                # Step 3: Process ad groups in batches (pause→enable immediately per batch)
                # This minimizes time gap between pausing originals and enabling theme ads
                ad_groups_list = list(theme_ads_by_ag.items())
                batch_size = 200  # OPTIMIZED: Process 200 ad groups at a time (was 100, max 2000 is too risky)

                total_paused = 0
                total_enabled = 0
                ad_groups_needing_activation = set()  # Track ad groups that need changes

                for batch_idx in range(0, len(ad_groups_list), batch_size):
                    batch = ad_groups_list[batch_idx:batch_idx+batch_size]

                    pause_operations = []
                    enable_operations = []

                    # Build operations for this batch of ad groups
                    for ag_res, theme_ads in batch:
                        needs_changes = False

                        # Pause ALL other RSAs in this ad group FIRST (including unlabeled ads)
                        if ag_res in other_ads_by_ag:
                            # Iterate over dict values (deduplicated ad info)
                            for other_ad in other_ads_by_ag[ag_res].values():
                                if other_ad['status'] == 'ENABLED':
                                    operation = client.get_type("AdGroupAdOperation")
                                    ad_group_ad = operation.update
                                    ad_group_ad.resource_name = other_ad['resource']
                                    ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED
                                    operation.update_mask.paths.append('status')
                                    pause_operations.append(operation)
                                    needs_changes = True

                        # Enable ONLY ONE theme ad (the first PAUSED one) in this ad group
                        # CRITICAL FIX: Only enable one to avoid exceeding 3-ad RSA limit
                        for theme_ad in theme_ads:
                            if theme_ad['status'] == 'PAUSED':
                                operation = client.get_type("AdGroupAdOperation")
                                ad_group_ad = operation.update
                                ad_group_ad.resource_name = theme_ad['resource']
                                ad_group_ad.status = client.enums.AdGroupAdStatusEnum.ENABLED
                                operation.update_mask.paths.append('status')
                                enable_operations.append(operation)
                                needs_changes = True
                                break  # CRITICAL: Only enable ONE theme ad per ad group

                        # Track this ad group if it needed changes
                        if needs_changes:
                            ad_groups_needing_activation.add(ag_res)

                    # Step 4: Execute pause operations for this batch (with retry)
                    if pause_operations:
                        success, result = await mutate_with_retry(
                            ad_group_ad_service, customer_id, pause_operations,
                            max_retries=3, base_delay=1.0
                        )
                        if success:
                            total_paused += len(pause_operations)
                        else:
                            logger.error(f"[{customer_id}] Batch {batch_idx//batch_size + 1}: Failed to pause other RSAs after retries: {result}")
                            async with stats_lock:
                                stats['errors'].append(f"{customer_id}: Batch {batch_idx//batch_size + 1}: Failed to pause - {result}")

                    # Step 5: Immediately enable theme ads for this batch (with retry)
                    if enable_operations:
                        success, result = await mutate_with_retry(
                            ad_group_ad_service, customer_id, enable_operations,
                            max_retries=3, base_delay=1.0
                        )
                        if success:
                            total_enabled += len(enable_operations)
                        else:
                            logger.error(f"[{customer_id}] Batch {batch_idx//batch_size + 1}: Failed to enable theme ads after retries: {result}")
                            async with stats_lock:
                                stats['errors'].append(f"{customer_id}: Batch {batch_idx//batch_size + 1}: Failed to enable - {result}")

                # Calculate ad groups that were already correct (includes those with theme ad already ENABLED)
                # ad_groups_already_active = skipped RSA query (theme ad already ENABLED)
                # ad_groups in theme_ads_by_ag but not in ad_groups_needing_activation = no changes needed
                ad_groups_already_correct = len(ad_groups_already_active) + (len(theme_ads_by_ag) - len(ad_groups_already_active) - len(ad_groups_needing_activation))

                logger.info(f"[{customer_id}] Paused {total_paused} other RSAs, Enabled {total_enabled} {theme_label_name} ads")
                logger.info(f"[{customer_id}] Activated: {len(ad_groups_needing_activation)}, Already correct: {ad_groups_already_correct} (incl {len(ad_groups_already_active)} pre-active)")
                async with stats_lock:
                    stats['original_ads_paused'] += total_paused
                    stats['theme_ads_enabled'] += total_enabled
                    stats['ad_groups_activated'] += len(ad_groups_needing_activation)
                    stats['ad_groups_already_correct'] += ad_groups_already_correct
                    stats['ad_groups_checked'] += len(theme_ads_by_ag)
                    stats['customers_processed'] += 1

                logger.info(f"[{customer_id}] V2 Completed successfully")

            except Exception as e:
                logger.error(f"[{customer_id}] V2 Error: {e}", exc_info=True)
                async with stats_lock:
                    stats['customers_failed'] += 1
                    stats['errors'].append(f"{customer_id}: {str(e)}")

        # Process customers in parallel
        tasks = []
        for customer_id, required_theme in plan.items():
            task = process_customer_v2(customer_id, required_theme)
            tasks.append(task)

        # Run in batches of parallel_workers
        for i in range(0, len(tasks), parallel_workers):
            batch = tasks[i:i+parallel_workers]
            await asyncio.gather(*batch, return_exceptions=True)

        logger.info(f"V2 (AD-FIRST) activation completed: {stats}")

        return {
            'status': 'completed',
            'stats': stats
        }

    async def remove_duplicates_all_customers(
        self,
        client,
        customer_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
        dry_run: bool = True,
        reset_labels: bool = False
    ) -> Dict:
        """
        Remove duplicate ads across customers, keeping ads with theme labels.

        Finds ads with identical content (headlines + descriptions) and removes
        duplicates, prioritizing ads with theme labels.

        Args:
            client: Google Ads client
            customer_ids: List of customer IDs (default: all Beslist.nl)
            limit: Limit ad groups per customer (for testing)
            dry_run: If True, only report what would be done
            reset_labels: If True, recheck ad groups with THEME_DUPLICATES_CHECK label

        Returns:
            Dict with stats: customers_processed, ad_groups_checked, duplicates_found, ads_removed
        """
        from collections import defaultdict

        logger.info(f"Starting duplicate removal (dry_run={dry_run}, reset_labels={reset_labels})")
        logger.info(f"Customer IDs to process: {customer_ids}")

        # Get customer IDs
        if customer_ids is None:
            customer_ids = self.get_customer_ids()

        stats = {
            'customers_processed': 0,
            'ad_groups_checked': 0,
            'ad_groups_with_duplicates': 0,
            'duplicate_sets_found': 0,
            'ads_removed': 0
        }

        theme_labels = {'THEME_BF', 'THEME_CM', 'THEME_SK', 'THEME_KM', 'THEME_SD'}
        checked_label_name = 'THEME_DUPLICATES_CHECK'

        # Services
        ga_service = client.get_service("GoogleAdsService")
        ad_service = client.get_service("AdGroupAdService")

        for customer_id in customer_ids:
            try:
                logger.info(f"[{customer_id}] Processing customer...")

                # Get or create THEME_DUPLICATES_CHECK label
                checked_label_resource = None
                if not dry_run:
                    label_query = f"SELECT label.resource_name FROM label WHERE label.name = '{checked_label_name}'"
                    try:
                        response = ga_service.search(customer_id=customer_id, query=label_query)
                        for row in response:
                            checked_label_resource = row.label.resource_name
                            break
                    except:
                        pass

                    if not checked_label_resource:
                        # Create label
                        label_service = client.get_service("LabelService")
                        label_operation = client.get_type("LabelOperation")
                        label = label_operation.create
                        label.name = checked_label_name

                        try:
                            response = label_service.mutate_labels(
                                customer_id=customer_id,
                                operations=[label_operation]
                            )
                            checked_label_resource = response.results[0].resource_name
                            logger.info(f"[{customer_id}] Created {checked_label_name} label")
                        except:
                            pass

                # Get ad groups
                ag_query = """
                    SELECT
                        ad_group.id,
                        ad_group.name,
                        campaign.id,
                        campaign.name
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

                logger.info(f"[{customer_id}] Checking {len(ad_groups)} ad groups")

                if not ad_groups:
                    continue

                # Batch fetch all ads
                all_ads_by_ag = {}
                all_ad_ids = []
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
                            all_ad_ids.append((ag_id, ad_id))

                    except Exception as e:
                        logger.error(f"[{customer_id}] Error fetching ads batch {i//batch_size + 1}: {e}")
                        continue

                logger.info(f"[{customer_id}] Fetched {len(all_ad_ids)} ads from {len(all_ads_by_ag)} ad groups")

                # Batch fetch labels
                logger.info(f"[{customer_id}] Batch fetching labels...")
                ad_labels_dict = {}

                label_batch_size = 5000
                for i in range(0, len(all_ad_ids), label_batch_size):
                    batch = all_ad_ids[i:i + label_batch_size]

                    ad_resources = [f"'customers/{customer_id}/adGroupAds/{ag_id}~{ad_id}'"
                                   for ag_id, ad_id in batch]
                    in_clause = ", ".join(ad_resources)

                    query1 = f"""
                        SELECT
                            ad_group_ad_label.ad_group_ad,
                            ad_group_ad_label.label
                        FROM ad_group_ad_label
                        WHERE ad_group_ad_label.ad_group_ad IN ({in_clause})
                    """

                    try:
                        ad_to_label_resources = {}
                        all_label_resources = set()

                        response = ga_service.search(customer_id=customer_id, query=query1)
                        for row in response:
                            ad_resource = row.ad_group_ad_label.ad_group_ad
                            key = ad_resource.split('/')[-1]
                            label_resource = row.ad_group_ad_label.label

                            if key not in ad_to_label_resources:
                                ad_to_label_resources[key] = []
                            ad_to_label_resources[key].append(label_resource)
                            all_label_resources.add(label_resource)

                        # Fetch label names
                        label_resource_to_name = {}
                        if all_label_resources:
                            label_resources_list = [f"'{lr}'" for lr in all_label_resources]
                            label_in_clause = ", ".join(label_resources_list)

                            query2 = f"""
                                SELECT label.resource_name, label.name
                                FROM label
                                WHERE label.resource_name IN ({label_in_clause})
                            """

                            label_response = ga_service.search(customer_id=customer_id, query=query2)
                            for row in label_response:
                                label_resource_to_name[row.label.resource_name] = row.label.name

                        # Map to dict
                        for ad_key, label_resources in ad_to_label_resources.items():
                            if ad_key not in ad_labels_dict:
                                ad_labels_dict[ad_key] = set()
                            for label_resource in label_resources:
                                label_name = label_resource_to_name.get(label_resource)
                                if label_name:
                                    ad_labels_dict[ad_key].add(label_name)

                    except Exception as e:
                        logger.warning(f"[{customer_id}] Failed to fetch labels batch: {e}")

                # Find and process duplicates
                processed_ad_groups = []

                for ag_id, ag_data in all_ads_by_ag.items():
                    stats['ad_groups_checked'] += 1
                    ag_name = ag_data['name']
                    content_groups = defaultdict(list)

                    for ad_data in ag_data['ads']:
                        ad_id = ad_data['ad_id']
                        label_key = f"{ag_id}~{ad_id}"
                        labels = ad_labels_dict.get(label_key, set())

                        # Create content signature
                        h_sorted = tuple(sorted(ad_data['headlines']))
                        d_sorted = tuple(sorted(ad_data['descriptions']))
                        signature = f"{h_sorted}||{d_sorted}"

                        content_groups[signature].append({
                            'ad_id': ad_id,
                            'resource_name': ad_data['resource_name'],
                            'status': ad_data['status'],
                            'labels': labels
                        })

                    # Find duplicate groups
                    duplicate_groups = [ads for ads in content_groups.values() if len(ads) > 1]

                    if duplicate_groups:
                        stats['ad_groups_with_duplicates'] += 1
                        stats['duplicate_sets_found'] += len(duplicate_groups)
                        logger.info(f"[{customer_id}] Ad group {ag_id} ({ag_name}): {len(duplicate_groups)} duplicate set(s)")

                        # Process each duplicate group
                        for duplicate_group in duplicate_groups:
                            # Score ads
                            scored_ads = []
                            for ad in duplicate_group:
                                theme_label_count = len([l for l in ad['labels'] if l in theme_labels])
                                has_any_theme = any(l in theme_labels for l in ad['labels'])
                                is_enabled = ad['status'] == 'ENABLED'

                                score = (theme_label_count * 100) + (has_any_theme * 10) + (is_enabled * 1)
                                scored_ads.append((score, ad))

                            scored_ads.sort(reverse=True, key=lambda x: x[0])

                            to_keep = scored_ads[0][1]
                            to_remove = [ad for score, ad in scored_ads[1:]]

                            logger.info(f"[{customer_id}]   KEEP: Ad {to_keep['ad_id']} (Labels: {to_keep['labels']})")

                            for ad in to_remove:
                                logger.info(f"[{customer_id}]   REMOVE: Ad {ad['ad_id']} (Labels: {ad['labels']})")

                                if not dry_run:
                                    try:
                                        ad_group_ad_operation = client.get_type("AdGroupAdOperation")
                                        ad_group_ad_operation.remove = ad['resource_name']

                                        ad_service.mutate_ad_group_ads(
                                            customer_id=customer_id,
                                            operations=[ad_group_ad_operation]
                                        )
                                        stats['ads_removed'] += 1
                                    except Exception as e:
                                        logger.error(f"[{customer_id}]   Failed to remove ad {ad['ad_id']}: {e}")
                                else:
                                    stats['ads_removed'] += 1

                        processed_ad_groups.append(ag_id)

                # Add labels to processed ad groups
                if not dry_run and checked_label_resource and processed_ad_groups:
                    logger.info(f"[{customer_id}] Labeling {len(processed_ad_groups)} ad groups...")
                    ag_label_service = client.get_service("AdGroupLabelService")

                    for ag_id in processed_ad_groups:
                        try:
                            ag_label_operation = client.get_type("AdGroupLabelOperation")
                            ag_label = ag_label_operation.create
                            ag_label.ad_group = f"customers/{customer_id}/adGroups/{ag_id}"
                            ag_label.label = checked_label_resource

                            ag_label_service.mutate_ad_group_labels(
                                customer_id=customer_id,
                                operations=[ag_label_operation]
                            )
                        except Exception as e:
                            if "ENTITY_ALREADY_EXISTS" not in str(e):
                                logger.warning(f"[{customer_id}] Failed to label ad group {ag_id}: {e}")

                stats['customers_processed'] += 1
                logger.info(f"[{customer_id}] Completed")

            except Exception as e:
                logger.error(f"[{customer_id}] Error: {e}", exc_info=True)
                continue

        logger.info(f"Duplicate removal completed: {stats}")

        return {
            'status': 'completed',
            'stats': stats,
            'dry_run': dry_run
        }

    async def _start_next_job_if_queue_enabled(self):
        """Check if auto-queue is enabled and start the next pending job."""
        from backend.thema_ads_db import get_auto_queue_enabled

        # Wait 30 seconds before checking for next job
        logger.info("Waiting 30 seconds before checking for next job...")
        await asyncio.sleep(30)

        # Check if auto-queue is enabled
        queue_enabled = get_auto_queue_enabled()
        if not queue_enabled:
            logger.info("Auto-queue is disabled, not starting next job")
            return

        # Get next pending job
        next_job_id = self.get_next_pending_job()
        if next_job_id is None:
            logger.info("No pending jobs in queue")
            return

        logger.info(f"Auto-queue: Starting next pending job {next_job_id}")
        # Create task in background to avoid blocking and ensure proper async context
        asyncio.create_task(self.process_job(next_job_id))


# Global service instance
thema_ads_service = ThemaAdsService()
