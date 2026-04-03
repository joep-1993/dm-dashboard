from fastapi import APIRouter, HTTPException, UploadFile, File, BackgroundTasks, Form, Query
from fastapi.responses import StreamingResponse, FileResponse
from typing import List, Optional
import csv
import io
import re
import logging
from datetime import datetime
from pathlib import Path
from backend.thema_ads_service import thema_ads_service
import sys

# Add thema_ads_optimized to path for theme imports
THEMA_ADS_PATH = Path(__file__).parent.parent / "thema_ads_optimized"
sys.path.insert(0, str(THEMA_ADS_PATH))

# Import theme module
from themes import is_valid_theme, get_all_theme_labels, SUPPORTED_THEMES

# Import openpyxl for Excel file parsing
try:
    import openpyxl
except ImportError:
    openpyxl = None

# Create router with prefix
router = APIRouter(prefix="/api/thema-ads", tags=["thema-ads"])

# Startup function to be called from main app
async def cleanup_stale_jobs():
    """Clean up stale 'running' jobs on startup (jobs interrupted by container restart)."""
    logger = logging.getLogger(__name__)

    try:
        logger.info("Checking for stale running jobs...")
        jobs = thema_ads_service.list_jobs(limit=100)

        stale_count = 0
        for job in jobs:
            if job['status'] == 'running':
                # Mark as failed since it was interrupted
                logger.warning(f"Found stale running job {job['id']}, marking as failed")
                thema_ads_service.update_job_status(
                    job['id'],
                    'failed',
                    error_message='Job interrupted by container restart'
                )
                stale_count += 1

        if stale_count > 0:
            logger.info(f"Cleaned up {stale_count} stale running jobs")
        else:
            logger.info("No stale running jobs found")

    except Exception as e:
        logger.error(f"Error cleaning up stale jobs: {e}")

@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "thema_ads"}


def convert_scientific_notation(value: str) -> str:
    """Convert scientific notation to regular number string.
    Handles both period and comma decimal separators (e.g., 1.76256E+11 or 1,76256E+11).
    """
    if not value:
        return value

    value = value.strip()

    # Check if it's in scientific notation (e.g., 1.76256E+11 or 1,76256E+11)
    if 'E' in value.upper():
        try:
            # Replace comma with period for locales that use comma as decimal separator
            value_normalized = value.replace(',', '.')
            # Convert to float, then to int, then to string (removes scientific notation)
            return str(int(float(value_normalized)))
        except (ValueError, OverflowError):
            # If conversion fails, return original value
            return value

    return value


@router.post("/discover")
async def discover_ad_groups(
    background_tasks: BackgroundTasks = None,
    limit: int = None,
    batch_size: int = 5000,
    job_chunk_size: int = 50000,
    theme: str = Form("singles_day")
):
    """
    Auto-discover ad groups from Google Ads MCC account.
    Finds all accounts, campaigns starting with 'HS/',
    and ad groups without the theme's DONE label (e.g., THEME_BF_DONE for Black Friday).

    Args:
        limit: Optional limit on number of ad groups to discover
        batch_size: Batch size for API queries (default: 5000)
        job_chunk_size: Maximum items per job (splits large discoveries into multiple jobs, default: 50000)
        theme: Theme to apply (default: singles_day)
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Discover parameters: limit={limit}, batch_size={batch_size}, job_chunk_size={job_chunk_size}, theme={theme}")

    # Validate theme
    if not is_valid_theme(theme):
        supported_themes = ', '.join(SUPPORTED_THEMES.keys())
        raise HTTPException(
            status_code=400,
            detail=f"Invalid theme '{theme}'. Supported themes: {supported_themes}"
        )

    try:
        from pathlib import Path
        from dotenv import load_dotenv

        # Load environment variables
        env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

        from config import load_config_from_env
        from google_ads_client import initialize_client
        from themes import get_theme_label

        config = load_config_from_env()
        client = initialize_client(config.google_ads)

        # Load customer IDs from file
        account_ids_file = Path(__file__).parent.parent / "thema_ads_optimized" / "account ids"
        if not account_ids_file.exists():
            raise HTTPException(status_code=500, detail="Account IDs file not found")

        with open(account_ids_file, 'r') as f:
            customer_ids = [line.strip() for line in f if line.strip()]

        logger.info(f"Loaded {len(customer_ids)} customer IDs from account ids file")

        # Get all customer accounts
        ga_service = client.get_service("GoogleAdsService")

        # Build customer list with IDs from file
        beslist_customers = [{'id': cid} for cid in customer_ids]

        logger.info(f"Using {len(beslist_customers)} customer accounts from file")

        # Query ads directly with campaign filter for faster discovery
        input_data = []
        ad_group_map = {}  # Deduplicate: ad_group_resource -> {customer_id, campaign_id, campaign_name, ad_group_id}

        for customer in beslist_customers:
            customer_id = customer['id']
            logger.info(f"Processing customer {customer_id}")

            try:
                # Direct ad query with campaign.name filter (much faster than nested queries)
                ad_query = """
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

                ad_response = ga_service.search(customer_id=customer_id, query=ad_query)

                # Deduplicate by ad_group (multiple ads per ad group)
                for row in ad_response:
                    ag_resource = row.ad_group_ad.ad_group

                    # Only store first occurrence of each ad group
                    if ag_resource not in ad_group_map:
                        ad_group_map[ag_resource] = {
                            'customer_id': customer_id,
                            'campaign_id': str(row.campaign.id),
                            'campaign_name': row.campaign.name,
                            'ad_group_id': str(row.ad_group.id),
                            'ad_group_resource': ag_resource
                        }

                logger.info(f"  Found {len([ag for ag in ad_group_map.values() if ag['customer_id'] == customer_id])} unique ad groups")

            except Exception as e:
                logger.warning(f"Error processing customer {customer_id}: {e}")
                continue

        # Get all unique ad group resources across all customers
        ad_group_resources = list(ad_group_map.keys())
        logger.info(f"Total unique ad groups across all customers: {len(ad_group_resources)}")

        if not ad_group_resources:
            return {
                "status": "no_ad_groups_found",
                "message": "No ad groups found matching the criteria",
                "total_items": 0,
                "customers_found": len(beslist_customers)
            }

        # Get theme-specific DONE label name
        theme_label = get_theme_label(theme)
        done_label_name = f"{theme_label}_DONE"
        logger.info(f"Filtering out ad groups with label: {done_label_name}")

        # Batch check theme DONE labels (group by customer for API efficiency)
        ag_with_done_label = set()

        for customer in beslist_customers:
            customer_id = customer['id']

            # Get ad groups for this customer
            customer_ag_resources = [
                ag_resource for ag_resource, ag_data in ad_group_map.items()
                if ag_data['customer_id'] == customer_id
            ]

            if not customer_ag_resources:
                continue

            # Get theme DONE label resource
            done_label_query = f"""
                SELECT label.resource_name
                FROM label
                WHERE label.name = '{done_label_name}'
                LIMIT 1
            """
            try:
                label_response = ga_service.search(customer_id=customer_id, query=done_label_query)
                done_label_resource = None
                for row in label_response:
                    done_label_resource = row.label.resource_name
                    break

                if done_label_resource:
                    # Batch query in chunks using configured batch_size
                    for i in range(0, len(customer_ag_resources), batch_size):
                        batch = customer_ag_resources[i:i + batch_size]
                        resources_str = ", ".join(f"'{r}'" for r in batch)

                        label_check_query = f"""
                            SELECT ad_group_label.ad_group
                            FROM ad_group_label
                            WHERE ad_group_label.ad_group IN ({resources_str})
                            AND ad_group_label.label = '{done_label_resource}'
                        """

                        label_response = ga_service.search(customer_id=customer_id, query=label_check_query)
                        for row in label_response:
                            ag_with_done_label.add(row.ad_group_label.ad_group)

            except Exception as e:
                logger.warning(f"  Could not check {done_label_name} labels for customer {customer_id}: {e}")

        logger.info(f"Found {len(ag_with_done_label)} ad groups with {done_label_name} label")

        # Also check for ATTEMPTED labels (all themes) to exclude permanently failed items
        attempted_label_name = f"{theme_label}_ATTEMPTED"
        ag_with_attempted_label = set()

        for customer in beslist_customers:
            customer_id = customer['id']

            customer_ag_resources = [
                ag_resource for ag_resource, ag_data in ad_group_map.items()
                if ag_data['customer_id'] == customer_id
            ]

            if not customer_ag_resources:
                continue

            # Get ATTEMPTED label resource
            attempted_label_query = f"""
                SELECT label.resource_name
                FROM label
                WHERE label.name = '{attempted_label_name}'
                LIMIT 1
            """
            try:
                label_response = ga_service.search(customer_id=customer_id, query=attempted_label_query)
                attempted_label_resource = None
                for row in label_response:
                    attempted_label_resource = row.label.resource_name
                    break

                if attempted_label_resource:
                    # Batch query in chunks
                    for i in range(0, len(customer_ag_resources), batch_size):
                        batch = customer_ag_resources[i:i + batch_size]
                        resources_str = ", ".join(f"'{r}'" for r in batch)

                        label_check_query = f"""
                            SELECT ad_group_label.ad_group
                            FROM ad_group_label
                            WHERE ad_group_label.ad_group IN ({resources_str})
                            AND ad_group_label.label = '{attempted_label_resource}'
                        """

                        label_response = ga_service.search(customer_id=customer_id, query=label_check_query)
                        for row in label_response:
                            ag_with_attempted_label.add(row.ad_group_label.ad_group)

            except Exception as e:
                logger.warning(f"  Could not check {attempted_label_name} labels for customer {customer_id}: {e}")

        logger.info(f"Found {len(ag_with_attempted_label)} ad groups with {attempted_label_name} label (excluded)")

        # Build input data from ad groups without DONE or ATTEMPTED labels
        for ag_resource, ag_data in ad_group_map.items():
            if ag_resource not in ag_with_done_label and ag_resource not in ag_with_attempted_label:
                input_data.append({
                    'customer_id': ag_data['customer_id'],
                    'campaign_id': ag_data['campaign_id'],
                    'campaign_name': ag_data['campaign_name'],
                    'ad_group_id': ag_data['ad_group_id'],
                    'theme_name': theme
                })

                # Check limit
                if limit and len(input_data) >= limit:
                    logger.info(f"Reached limit of {limit} ad groups")
                    break

        logger.info(f"Discovered {len(input_data)} ad groups to process")

        if not input_data:
            return {
                "status": "no_ad_groups_found",
                "message": "No ad groups found matching the criteria",
                "total_items": 0
            }

        # Split into multiple jobs if needed
        from backend.thema_ads_service import thema_ads_service
        job_ids = []
        total_items = len(input_data)

        # Calculate number of chunks needed
        num_chunks = (total_items + job_chunk_size - 1) // job_chunk_size

        if num_chunks > 1:
            logger.info(f"Splitting {total_items} ad groups into {num_chunks} jobs of max {job_chunk_size} items each")

        for chunk_idx in range(num_chunks):
            start_idx = chunk_idx * job_chunk_size
            end_idx = min(start_idx + job_chunk_size, total_items)
            chunk_data = input_data[start_idx:end_idx]

            # Create job for this chunk
            job_id = thema_ads_service.create_job(chunk_data, batch_size=batch_size)
            job_ids.append(job_id)
            logger.info(f"Created job {job_id} with {len(chunk_data)} items (chunk {chunk_idx + 1}/{num_chunks})")

            # Automatically start the job
            if background_tasks:
                background_tasks.add_task(thema_ads_service.process_job, job_id)

        return {
            "job_ids": job_ids,
            "total_items": total_items,
            "jobs_created": len(job_ids),
            "items_per_job": job_chunk_size,
            "status": "processing",
            "customers_found": len(beslist_customers),
            "ad_groups_discovered": total_items
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Discovery failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
    batch_size: int = Form(7500),
    theme: str = Form("singles_day")
):
    """
    Upload CSV file with customer_id and optionally ad_group_id/theme columns.

    CSV Modes:
    1. customer_id + ad_group_id: Process specific ad groups
    2. customer_id only: Auto-discover all ad groups in customer account
       - Filters for campaigns starting with 'HS/'
       - Excludes ad groups with theme's DONE label

    Theme Options:
    - Include 'theme' column in CSV: Different theme per row
    - No 'theme' column: Uses form theme parameter for all rows

    Args:
        file: CSV file to upload
        batch_size: Batch size for API queries (default: 7500)
        theme: Default theme to apply (default: singles_day). Overridden by 'theme' column if present.
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Upload parameters: batch_size={batch_size}, theme={theme}")

    # Validate theme
    if not is_valid_theme(theme):
        supported_themes = ', '.join(SUPPORTED_THEMES.keys())
        raise HTTPException(
            status_code=400,
            detail=f"Invalid theme '{theme}'. Supported themes: {supported_themes}"
        )

    try:
        logger.info(f"Receiving file upload: {file.filename}")
        contents = await file.read()
        logger.info(f"File size: {len(contents)} bytes")

        # Try multiple encodings to decode the file
        decoded = None
        encodings = ['utf-8', 'utf-8-sig', 'windows-1252', 'iso-8859-1', 'latin1']
        for encoding in encodings:
            try:
                decoded = contents.decode(encoding)
                logger.info(f"Successfully decoded file using encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue

        if decoded is None:
            raise HTTPException(
                status_code=400,
                detail="Unable to decode file. Please ensure it's a valid CSV file saved with UTF-8 or Windows-1252 encoding."
            )

        # Auto-detect delimiter (comma or semicolon)
        sample = decoded[:1024]  # Check first 1KB
        delimiter = ';' if ';' in sample.split('\n')[0] else ','
        logger.info(f"Using delimiter: '{delimiter}'")

        csv_reader = csv.DictReader(io.StringIO(decoded), delimiter=delimiter)

        # Parse CSV data
        input_data = []
        customers_to_discover = {}  # customer_id -> theme mapping for auto-discovery
        headers_seen = None
        has_theme_column = False

        for row_num, row in enumerate(csv_reader):
            if headers_seen is None:
                headers_seen = list(row.keys())
                has_theme_column = 'theme' in headers_seen
                logger.info(f"CSV headers found: {headers_seen}")
                if has_theme_column:
                    logger.info("Theme column detected - per-row themes enabled")

            if 'customer_id' not in row:
                continue

            # Get customer_id
            customer_id = convert_scientific_notation(row['customer_id'])
            customer_id = customer_id.strip().replace('-', '')

            if not customer_id:
                continue

            # Determine theme for this row
            row_theme = theme  # Default from form parameter
            if has_theme_column and 'theme' in row and row['theme'].strip():
                row_theme = row['theme'].strip().lower()
                # Validate theme
                if not is_valid_theme(row_theme):
                    logger.warning(f"Invalid theme '{row_theme}' in row {row_num + 2}, using default '{theme}'")
                    row_theme = theme

            # Check if ad_group_id is provided
            has_ad_group_id = 'ad_group_id' in row and row['ad_group_id'].strip()

            if has_ad_group_id:
                # Mode 1: Specific ad group provided
                ad_group_id = convert_scientific_notation(row['ad_group_id'])
                ad_group_id = ad_group_id.strip()

                item = {
                    'customer_id': customer_id,
                    'ad_group_id': ad_group_id
                }

                # Add optional campaign info if provided
                if 'campaign_id' in row and row['campaign_id'].strip():
                    campaign_id = convert_scientific_notation(row['campaign_id'])
                    item['campaign_id'] = campaign_id.strip()
                if 'campaign_name' in row and row['campaign_name'].strip():
                    item['campaign_name'] = row['campaign_name'].strip()

                # Add optional ad_group_name if provided
                if 'ad_group_name' in row and row['ad_group_name'].strip():
                    item['ad_group_name'] = row['ad_group_name'].strip()

                # Add theme for this row
                item['theme_name'] = row_theme

                input_data.append(item)
            else:
                # Mode 2: Only customer_id provided - need auto-discovery
                # Store customer_id with its theme
                if customer_id not in customers_to_discover:
                    customers_to_discover[customer_id] = row_theme
                    logger.info(f"Customer {customer_id} marked for auto-discovery with theme '{row_theme}'")

        logger.info(f"Parsed {len(input_data)} specific ad groups and {len(customers_to_discover)} customers for auto-discovery")

        # Auto-discover ad groups for customers without ad_group_id
        if customers_to_discover:
            logger.info(f"Starting auto-discovery for {len(customers_to_discover)} customers...")

            try:
                from pathlib import Path
                from dotenv import load_dotenv

                # Load environment variables
                env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
                if env_path.exists():
                    load_dotenv(env_path)
                else:
                    raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

                from config import load_config_from_env
                from google_ads_client import initialize_client
                from themes import get_theme_label

                config = load_config_from_env()
                client = initialize_client(config.google_ads)
                ga_service = client.get_service("GoogleAdsService")

                # Discover ad groups for each customer with their specific theme
                for customer_id, customer_theme in customers_to_discover.items():
                    logger.info(f"Discovering ad groups for customer {customer_id} with theme '{customer_theme}'")

                    # Get the done-label name for this customer's theme
                    theme_label = get_theme_label(customer_theme)
                    done_label_name = f"{theme_label}_DONE"
                    logger.info(f"  Filtering out ad groups with label: {done_label_name}")

                    try:
                        # Query for ad groups in HS/ campaigns
                        ad_query = """
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

                        ad_response = ga_service.search(customer_id=customer_id, query=ad_query)

                        # Collect unique ad groups
                        ad_group_map = {}
                        for row in ad_response:
                            ag_resource = row.ad_group_ad.ad_group
                            if ag_resource not in ad_group_map:
                                ad_group_map[ag_resource] = {
                                    'customer_id': customer_id,
                                    'campaign_id': str(row.campaign.id),
                                    'campaign_name': row.campaign.name,
                                    'ad_group_id': str(row.ad_group.id),
                                    'ad_group_name': row.ad_group.name,
                                    'ad_group_resource': ag_resource
                                }

                        logger.info(f"  Found {len(ad_group_map)} ad groups in HS/ campaigns")

                        if not ad_group_map:
                            continue

                        # Check for done-label
                        ag_with_done_label = set()

                        # Get done-label resource
                        done_label_query = f"""
                            SELECT label.resource_name
                            FROM label
                            WHERE label.name = '{done_label_name}'
                            LIMIT 1
                        """

                        done_label_resource = None
                        try:
                            label_response = ga_service.search(customer_id=customer_id, query=done_label_query)
                            for row in label_response:
                                done_label_resource = row.label.resource_name
                                break
                        except Exception as e:
                            logger.warning(f"  Could not find {done_label_name} label: {e}")

                        if done_label_resource:
                            # Query ad groups with done-label in batches
                            ad_group_resources = list(ad_group_map.keys())
                            for i in range(0, len(ad_group_resources), batch_size):
                                batch = ad_group_resources[i:i + batch_size]
                                resources_str = ", ".join(f"'{r}'" for r in batch)

                                label_check_query = f"""
                                    SELECT ad_group_label.ad_group
                                    FROM ad_group_label
                                    WHERE ad_group_label.ad_group IN ({resources_str})
                                    AND ad_group_label.label = '{done_label_resource}'
                                """

                                label_response = ga_service.search(customer_id=customer_id, query=label_check_query)
                                for row in label_response:
                                    ag_with_done_label.add(row.ad_group_label.ad_group)

                            logger.info(f"  {len(ag_with_done_label)} ad groups already have {done_label_name} label")

                        # Add ad groups without done-label to input_data
                        for ag_resource, ag_data in ad_group_map.items():
                            if ag_resource not in ag_with_done_label:
                                input_data.append({
                                    'customer_id': ag_data['customer_id'],
                                    'campaign_id': ag_data['campaign_id'],
                                    'campaign_name': ag_data['campaign_name'],
                                    'ad_group_id': ag_data['ad_group_id'],
                                    'ad_group_name': ag_data['ad_group_name'],
                                    'theme_name': customer_theme
                                })

                        discovered_count = len(ad_group_map) - len(ag_with_done_label)
                        logger.info(f"  Discovered {discovered_count} ad groups to process")

                    except Exception as e:
                        logger.warning(f"Error discovering ad groups for customer {customer_id}: {e}")
                        continue

                logger.info(f"Auto-discovery complete. Total ad groups to process: {len(input_data)}")

            except Exception as e:
                logger.error(f"Auto-discovery failed: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Auto-discovery failed: {str(e)}")

        if not input_data:
            error_msg = f"No ad groups found to process. CSV must contain 'customer_id' column (with optional 'ad_group_id'). Found headers: {headers_seen}"
            logger.error(error_msg)
            raise HTTPException(
                status_code=400,
                detail=error_msg
            )

        # Create job with batch_size
        logger.info("Creating job in database...")
        job_id = thema_ads_service.create_job(input_data, batch_size=batch_size)
        logger.info(f"Job created with ID: {job_id}, batch_size: {batch_size}")

        # Automatically start the job
        if background_tasks:
            background_tasks.add_task(thema_ads_service.process_job, job_id)
            logger.info(f"Job {job_id} queued for automatic processing")

        return {
            "job_id": job_id,
            "total_items": len(input_data),
            "status": "processing"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload-excel")
async def upload_excel(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
    batch_size: int = Form(7500),
    is_activation_plan: bool = Form(False),
    reset_activation_labels: bool = Form(False)
):
    """
    Upload Excel file with customer_id, ad_group_id, and theme columns.
    Creates a new job and automatically starts processing.

    Args:
        file: Excel file (.xlsx) to upload
        batch_size: Batch size for API queries (default: 7500)
        is_activation_plan: If True, stores as activation plan instead of creating jobs
        reset_activation_labels: If True (with is_activation_plan), resets ACTIVATION_DONE labels
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Excel upload parameters: batch_size={batch_size}")

    if not openpyxl:
        raise HTTPException(
            status_code=500,
            detail="Excel support not available. Please install openpyxl: pip install openpyxl"
        )

    try:
        logger.info(f"Receiving Excel file upload: {file.filename}")

        # Validate file extension
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400,
                detail="File must be an Excel file (.xlsx or .xls)"
            )

        # Read file contents
        contents = await file.read()
        logger.info(f"File size: {len(contents)} bytes")

        # Load Excel workbook from bytes
        workbook = openpyxl.load_workbook(io.BytesIO(contents), read_only=True, data_only=True)
        sheet = workbook.active
        logger.info(f"Loaded Excel sheet: {sheet.title}")

        # Read header row
        headers = []
        for cell in sheet[1]:
            # Skip None/empty cells and normalize headers
            if cell.value:
                header = str(cell.value).strip().lower()
                headers.append(header)
            else:
                headers.append(None)

        # Filter out None values for validation
        valid_headers = [h for h in headers if h is not None]
        logger.info(f"Excel headers found: {headers}")
        logger.info(f"Valid headers (normalized): {valid_headers}")

        # Validate required columns (only customer_id and theme are required)
        required_columns = ['customer_id', 'theme']
        missing_columns = [col for col in required_columns if col not in valid_headers]
        if missing_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Excel file must contain columns: {', '.join(required_columns)}. Missing: {', '.join(missing_columns)}. Found headers: {', '.join(valid_headers)}"
            )

        # Find column indices
        customer_id_idx = headers.index('customer_id')
        ad_group_id_idx = headers.index('ad_group_id') if 'ad_group_id' in headers else None
        theme_idx = headers.index('theme')

        # Optional columns
        campaign_id_idx = headers.index('campaign_id') if 'campaign_id' in headers else None
        campaign_name_idx = headers.index('campaign_name') if 'campaign_name' in headers else None
        ad_group_name_idx = headers.index('ad_group_name') if 'ad_group_name' in headers else None

        # Parse data rows
        input_data = []
        customers_to_discover = {}  # customer_id -> theme mapping for auto-discovery
        invalid_themes = set()

        # Calculate required indices for length check
        required_indices = [customer_id_idx, theme_idx]
        if ad_group_id_idx is not None:
            required_indices.append(ad_group_id_idx)
        max_required_idx = max(required_indices)

        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            if not row or len(row) <= max_required_idx:
                continue

            customer_id = row[customer_id_idx]
            theme = row[theme_idx]

            # Skip empty rows (customer_id and theme are required)
            if not customer_id or not theme:
                continue

            # Convert to string and clean
            customer_id = str(customer_id).strip().replace('-', '')
            theme = str(theme).strip().lower()

            # Convert scientific notation if needed
            customer_id = convert_scientific_notation(customer_id)

            # Validate theme
            if not is_valid_theme(theme):
                invalid_themes.add(theme)
                continue

            # Check if ad_group_id is provided
            has_ad_group_id = ad_group_id_idx is not None and row[ad_group_id_idx]

            if has_ad_group_id:
                # Mode 1: Specific ad group provided
                ad_group_id = str(row[ad_group_id_idx]).strip()
                ad_group_id = convert_scientific_notation(ad_group_id)

                item = {
                    'customer_id': customer_id,
                    'ad_group_id': ad_group_id,
                    'theme_name': theme
                }

                # Add optional columns
                if campaign_id_idx is not None and len(row) > campaign_id_idx and row[campaign_id_idx]:
                    campaign_id = convert_scientific_notation(str(row[campaign_id_idx]).strip())
                    item['campaign_id'] = campaign_id
                if campaign_name_idx is not None and len(row) > campaign_name_idx and row[campaign_name_idx]:
                    item['campaign_name'] = str(row[campaign_name_idx]).strip()
                if ad_group_name_idx is not None and len(row) > ad_group_name_idx and row[ad_group_name_idx]:
                    item['ad_group_name'] = str(row[ad_group_name_idx]).strip()

                input_data.append(item)
            else:
                # Mode 2: Only customer_id and theme provided - need auto-discovery
                if customer_id not in customers_to_discover:
                    customers_to_discover[customer_id] = theme
                    logger.info(f"Customer {customer_id} marked for auto-discovery with theme '{theme}'")

        workbook.close()
        logger.info(f"Parsed {len(input_data)} specific ad groups and {len(customers_to_discover)} customers for auto-discovery")

        # Check if this is an activation plan upload
        if is_activation_plan:
            # Store as activation plan (customer_id -> theme mapping)
            from backend.thema_ads_db import store_activation_plan

            if customers_to_discover:
                # This is a pure activation plan (customer + theme only)
                plan_data = customers_to_discover
            elif input_data:
                # Convert input_data to plan format (use customer+theme, ignore ad_group_id)
                plan_data = {}
                for item in input_data:
                    customer_id = item['customer_id']
                    theme = item.get('theme_name', 'singles_day')
                    plan_data[customer_id] = theme
            else:
                raise HTTPException(
                    status_code=400,
                    detail="No valid data found for activation plan"
                )

            num_customers = store_activation_plan(plan_data, reset_labels=reset_activation_labels)

            logger.info(f"Stored activation plan for {num_customers} customers")

            return {
                "status": "success",
                "message": "Activation plan uploaded successfully",
                "customers_in_plan": num_customers,
                "reset_labels": reset_activation_labels
            }

        # Auto-discover ad groups for customers without ad_group_id
        if customers_to_discover:
            logger.info(f"Starting auto-discovery for {len(customers_to_discover)} customers...")

            try:
                from pathlib import Path
                from dotenv import load_dotenv

                # Load environment variables
                env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
                if env_path.exists():
                    load_dotenv(env_path)
                else:
                    raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

                from config import load_config_from_env
                from google_ads_client import initialize_client
                from themes import get_theme_label

                config = load_config_from_env()
                client = initialize_client(config.google_ads)
                ga_service = client.get_service("GoogleAdsService")

                # Discover ad groups for each customer with their specific theme
                for customer_id, customer_theme in customers_to_discover.items():
                    logger.info(f"Discovering ad groups for customer {customer_id} with theme '{customer_theme}'")

                    # Get the done-label name for this customer's theme
                    theme_label = get_theme_label(customer_theme)
                    done_label_name = f"{theme_label}_DONE"
                    logger.info(f"  Filtering out ad groups with label: {done_label_name}")

                    try:
                        # Query for ad groups in HS/ campaigns
                        ad_query = """
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

                        ad_response = ga_service.search(customer_id=customer_id, query=ad_query)

                        # Collect unique ad groups
                        ad_group_map = {}
                        for row in ad_response:
                            ag_resource = row.ad_group_ad.ad_group
                            if ag_resource not in ad_group_map:
                                ad_group_map[ag_resource] = {
                                    'customer_id': customer_id,
                                    'campaign_id': str(row.campaign.id),
                                    'campaign_name': row.campaign.name,
                                    'ad_group_id': str(row.ad_group.id),
                                    'ad_group_name': row.ad_group.name,
                                    'ad_group_resource': ag_resource
                                }

                        logger.info(f"  Found {len(ad_group_map)} ad groups in HS/ campaigns")

                        if not ad_group_map:
                            continue

                        # Check for done-label
                        ag_with_done_label = set()

                        # Get done-label resource
                        done_label_query = f"""
                            SELECT label.resource_name
                            FROM label
                            WHERE label.name = '{done_label_name}'
                            LIMIT 1
                        """

                        done_label_resource = None
                        try:
                            label_response = ga_service.search(customer_id=customer_id, query=done_label_query)
                            for row in label_response:
                                done_label_resource = row.label.resource_name
                                break
                        except Exception as e:
                            logger.warning(f"  Could not find {done_label_name} label: {e}")

                        if done_label_resource:
                            # Query ad groups with done-label in batches
                            ad_group_resources = list(ad_group_map.keys())
                            for i in range(0, len(ad_group_resources), batch_size):
                                batch = ad_group_resources[i:i + batch_size]
                                resources_str = ", ".join(f"'{r}'" for r in batch)

                                label_check_query = f"""
                                    SELECT ad_group_label.ad_group
                                    FROM ad_group_label
                                    WHERE ad_group_label.ad_group IN ({resources_str})
                                    AND ad_group_label.label = '{done_label_resource}'
                                """

                                label_response = ga_service.search(customer_id=customer_id, query=label_check_query)
                                for row in label_response:
                                    ag_with_done_label.add(row.ad_group_label.ad_group)

                            logger.info(f"  {len(ag_with_done_label)} ad groups already have {done_label_name} label")

                        # Add ad groups without done-label to input_data
                        for ag_resource, ag_data in ad_group_map.items():
                            if ag_resource not in ag_with_done_label:
                                input_data.append({
                                    'customer_id': ag_data['customer_id'],
                                    'campaign_id': ag_data['campaign_id'],
                                    'campaign_name': ag_data['campaign_name'],
                                    'ad_group_id': ag_data['ad_group_id'],
                                    'ad_group_name': ag_data['ad_group_name'],
                                    'theme_name': customer_theme
                                })

                        discovered_count = len(ad_group_map) - len(ag_with_done_label)
                        logger.info(f"  Discovered {discovered_count} ad groups to process")

                    except Exception as e:
                        logger.warning(f"Error discovering ad groups for customer {customer_id}: {e}")
                        continue

                logger.info(f"Auto-discovery complete. Total ad groups to process: {len(input_data)}")

            except Exception as e:
                logger.error(f"Auto-discovery failed: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Auto-discovery failed: {str(e)}")

        if invalid_themes:
            supported_themes = ', '.join(SUPPORTED_THEMES.keys())
            logger.warning(f"Skipped rows with invalid themes: {invalid_themes}")
            raise HTTPException(
                status_code=400,
                detail=f"Invalid theme(s) found: {', '.join(invalid_themes)}. Supported themes: {supported_themes}"
            )

        if not input_data:
            raise HTTPException(
                status_code=400,
                detail="No valid data rows found in Excel file"
            )

        # Split input data by theme first, then into 50K chunks per theme
        from collections import defaultdict
        by_theme = defaultdict(list)
        for item in input_data:
            theme = item.get('theme_name', 'singles_day')
            by_theme[theme].append(item)

        logger.info(f"Found {len(by_theme)} themes in uploaded data:")
        for theme, items in by_theme.items():
            logger.info(f"  - {theme}: {len(items)} ad groups")

        # Create jobs (split by theme, then by 50K chunks)
        JOB_CHUNK_SIZE = 50000
        job_ids = []
        total_jobs = 0

        for theme, theme_items in by_theme.items():
            num_chunks = (len(theme_items) + JOB_CHUNK_SIZE - 1) // JOB_CHUNK_SIZE

            if num_chunks > 1:
                logger.info(f"Theme '{theme}': Splitting {len(theme_items)} ad groups into {num_chunks} jobs")

            for chunk_idx in range(num_chunks):
                start_idx = chunk_idx * JOB_CHUNK_SIZE
                end_idx = min(start_idx + JOB_CHUNK_SIZE, len(theme_items))
                chunk_data = theme_items[start_idx:end_idx]

                # Create job for this chunk
                job_id = thema_ads_service.create_job(chunk_data, batch_size=batch_size)
                job_ids.append(job_id)
                total_jobs += 1

                logger.info(f"Created job {job_id}: theme='{theme}', items={len(chunk_data)} (chunk {chunk_idx + 1}/{num_chunks})")

                # Automatically start the job
                if background_tasks:
                    background_tasks.add_task(thema_ads_service.process_job, job_id)

        logger.info(f"Created {total_jobs} jobs total for {len(input_data)} ad groups across {len(by_theme)} themes")

        return {
            "job_ids": job_ids,
            "jobs_created": total_jobs,
            "total_items": len(input_data),
            "themes": list(by_theme.keys()),
            "items_per_theme": {theme: len(items) for theme, items in by_theme.items()},
            "status": "processing"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Excel upload failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/start")
async def start_job(job_id: int, background_tasks: BackgroundTasks):
    """Start processing a job in the background."""
    try:
        job = thema_ads_service.get_job_status(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job['status'] == 'running':
            raise HTTPException(status_code=400, detail="Job is already running")

        # Run job in background
        background_tasks.add_task(thema_ads_service.process_job, job_id)

        return {"status": "started", "job_id": job_id}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/pause")
async def pause_job(job_id: int):
    """Pause a running job."""
    try:
        job = thema_ads_service.get_job_status(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        thema_ads_service.pause_job(job_id)

        return {"status": "paused", "job_id": job_id}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/resume")
async def resume_job(job_id: int, background_tasks: BackgroundTasks):
    """Resume a paused job."""
    try:
        job = thema_ads_service.get_job_status(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job['status'] not in ('paused', 'failed'):
            raise HTTPException(
                status_code=400,
                detail=f"Cannot resume job with status '{job['status']}'"
            )

        # Run job in background
        background_tasks.add_task(thema_ads_service.process_job, job_id)

        return {"status": "resumed", "job_id": job_id}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: int):
    """Get detailed status of a specific job."""
    try:
        job = thema_ads_service.get_job_status(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        return job

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs")
async def list_jobs(limit: int = 20):
    """List all jobs."""
    try:
        jobs = thema_ads_service.list_jobs(limit)
        return {"jobs": jobs}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/jobs/{job_id}")
async def delete_job(job_id: int):
    """Delete a job and all its associated data."""
    try:
        job = thema_ads_service.get_job_status(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        # Don't allow deleting running jobs
        if job['status'] == 'running':
            raise HTTPException(
                status_code=400,
                detail="Cannot delete a running job. Please pause it first."
            )

        thema_ads_service.delete_job(job_id)

        return {"status": "deleted", "job_id": job_id}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/queue/status")
async def get_queue_status():
    """Get the current auto-queue status."""
    try:
        from backend.thema_ads_db import get_auto_queue_enabled
        enabled = get_auto_queue_enabled()
        return {"auto_queue_enabled": enabled}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/queue/enable")
async def enable_queue():
    """Enable automatic job queue."""
    try:
        from backend.thema_ads_db import set_auto_queue_enabled
        set_auto_queue_enabled(True)
        return {"status": "enabled", "auto_queue_enabled": True}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/queue/disable")
async def disable_queue():
    """Disable automatic job queue."""
    try:
        from backend.thema_ads_db import set_auto_queue_enabled
        set_auto_queue_enabled(False)
        return {"status": "disabled", "auto_queue_enabled": False}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}/failed-items-csv")
async def download_failed_items(job_id: int):
    """Download CSV of failed and skipped items for a job."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get failed and skipped items
        cur.execute("""
            SELECT customer_id, campaign_id, campaign_name, ad_group_id, ad_group_name, status, error_message
            FROM thema_ads_job_items
            WHERE job_id = %s AND status IN ('failed', 'skipped')
            ORDER BY status, customer_id, ad_group_id
        """, (job_id,))

        items = cur.fetchall()
        cur.close()
        conn.close()

        if not items:
            raise HTTPException(status_code=404, detail="No failed or skipped items found for this job")

        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(['customer_id', 'campaign_id', 'campaign_name', 'ad_group_id', 'ad_group_name', 'status', 'reason', 'error_message'])

        # Write data
        for item in items:
            # Format reason based on status and error message
            if item['status'] == 'skipped':
                if item['error_message'] and 'Already processed' in item['error_message']:
                    # Extract the actual theme label from error message (e.g., "has THEME_KM_DONE label")
                    match = re.search(r'has (THEME_\w+_DONE) label', item['error_message'])
                    if match:
                        theme_label = match.group(1)
                        reason = f"Ad group has '{theme_label}' label (already processed)"
                    else:
                        # Fallback if pattern doesn't match
                        reason = item['error_message']
                elif item['error_message'] and 'No existing ad' in item['error_message']:
                    reason = "Ad group has 0 ads"
                else:
                    reason = item['error_message'] or 'Skipped'
            else:
                reason = item['error_message'] or 'Unknown error'

            writer.writerow([
                item['customer_id'],
                item['campaign_id'] or '',
                item['campaign_name'] or '',
                item['ad_group_id'],
                item['ad_group_name'] or '',
                item['status'],
                reason,
                item['error_message'] or ''
            ])

        # Prepare response
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=job_{job_id}_failed_and_skipped_items.csv"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}/successful-items-csv")
async def download_successful_items(job_id: int):
    """Download CSV of successfully processed items for a job."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get successful items
        cur.execute("""
            SELECT customer_id, campaign_id, campaign_name, ad_group_id, ad_group_name, new_ad_resource
            FROM thema_ads_job_items
            WHERE job_id = %s AND status = 'successful'
            ORDER BY customer_id, ad_group_id
        """, (job_id,))

        items = cur.fetchall()
        cur.close()
        conn.close()

        if not items:
            raise HTTPException(status_code=404, detail="No successful items found for this job")

        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(['customer_id', 'campaign_id', 'campaign_name', 'ad_group_id', 'ad_group_name', 'new_ad_resource'])

        # Write data
        for item in items:
            writer.writerow([
                item['customer_id'],
                item['campaign_id'] or '',
                item['campaign_name'] or '',
                item['ad_group_id'],
                item['ad_group_name'] or '',
                item['new_ad_resource'] or ''
            ])

        # Prepare response
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=job_{job_id}_successful_items.csv"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}/plan")
async def get_job_plan(job_id: int):
    """Get the uploaded plan (input data) for a job, showing theme distribution."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get all input data with themes
        cur.execute("""
            SELECT
                customer_id,
                campaign_id,
                campaign_name,
                ad_group_id,
                ad_group_name,
                theme_name
            FROM thema_ads_input_data
            WHERE job_id = %s
            ORDER BY customer_id, theme_name, ad_group_id
        """, (job_id,))

        items = cur.fetchall()

        if not items:
            cur.close()
            conn.close()
            raise HTTPException(status_code=404, detail="No plan found for this job")

        # Calculate theme statistics
        from collections import defaultdict
        theme_counts = defaultdict(int)
        customer_theme_counts = defaultdict(lambda: defaultdict(int))

        for item in items:
            theme = item['theme_name'] or 'singles_day'
            customer_id = item['customer_id']
            theme_counts[theme] += 1
            customer_theme_counts[customer_id][theme] += 1

        # Get job info
        cur.execute("""
            SELECT id, created_at, status, total_ad_groups
            FROM thema_ads_jobs
            WHERE id = %s
        """, (job_id,))

        job = cur.fetchone()
        cur.close()
        conn.close()

        # Convert to list of dicts for response
        plan_items = [dict(item) for item in items]

        return {
            "job_id": job_id,
            "created_at": job['created_at'],
            "status": job['status'],
            "total_ad_groups": job['total_ad_groups'],
            "theme_distribution": dict(theme_counts),
            "customer_theme_distribution": {
                cid: dict(themes) for cid, themes in customer_theme_counts.items()
            },
            "plan_items": plan_items
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}/plan-csv")
async def download_job_plan(job_id: int):
    """Download the uploaded plan (input data) for a job as CSV."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get all input data
        cur.execute("""
            SELECT
                customer_id,
                campaign_id,
                campaign_name,
                ad_group_id,
                ad_group_name,
                theme_name
            FROM thema_ads_input_data
            WHERE job_id = %s
            ORDER BY customer_id, theme_name, ad_group_id
        """, (job_id,))

        items = cur.fetchall()
        cur.close()
        conn.close()

        if not items:
            raise HTTPException(status_code=404, detail="No plan found for this job")

        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(['customer_id', 'campaign_id', 'campaign_name', 'ad_group_id', 'ad_group_name', 'theme'])

        # Write data
        for item in items:
            writer.writerow([
                item['customer_id'],
                item['campaign_id'] or '',
                item['campaign_name'] or '',
                item['ad_group_id'],
                item['ad_group_name'] or '',
                item['theme_name'] or 'singles_day'
            ])

        # Prepare response
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=job_{job_id}_plan.csv"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/checkup")
async def checkup_ad_groups(
    background_tasks: BackgroundTasks = None,
    limit: int = None,
    batch_size: int = 5000,
    job_chunk_size: int = 50000,
    skip_audited: bool = True
):
    """
    OPTIMIZED Check-up: Audit ad groups with THEME_*_DONE labels, verify themed ads exist,
    and remove invalid DONE labels. Creates repair jobs for missing ads.

    Performance optimizations:
    - Queries all themes at once (4x faster)
    - Filters to HS/ campaigns only (2-3x faster)
    - Better chunking for large queries (1.5x faster)
    - Skips already-audited ad groups with THEMES_CHECK_DONE label

    Args:
        limit: Optional limit on number of ad groups to check
        batch_size: Batch size for API queries (default: 5000)
        job_chunk_size: Maximum items per repair job (default: 50000)
        skip_audited: Skip ad groups with THEMES_CHECK_DONE label (default: True)
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Checkup parameters: limit={limit}, batch_size={batch_size}, job_chunk_size={job_chunk_size}")

    try:
        from pathlib import Path
        from dotenv import load_dotenv

        # Load environment variables
        env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

        from config import load_config_from_env
        from google_ads_client import initialize_client

        config = load_config_from_env()
        client = initialize_client(config.google_ads)

        # Load customer IDs from file
        account_ids_file = Path(__file__).parent.parent / "thema_ads_optimized" / "account ids"
        if not account_ids_file.exists():
            raise HTTPException(status_code=500, detail="Account IDs file not found")

        with open(account_ids_file, 'r') as f:
            customer_ids = [line.strip() for line in f if line.strip()]

        logger.info(f"Loaded {len(customer_ids)} customer IDs from account ids file")

        # Run checkup
        result = await thema_ads_service.checkup_ad_groups(
            client=client,
            customer_ids=customer_ids,
            limit=limit,
            batch_size=batch_size,
            job_chunk_size=job_chunk_size,
            background_tasks=background_tasks,
            skip_audited=skip_audited
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Checkup failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/remove-checkup-labels")
async def remove_checkup_labels():
    """
    Remove THEMES_CHECK_DONE labels from all ad groups.
    This allows doing a clean audit run without skipping any ad groups.

    Use this when you want to re-audit all ad groups from scratch.
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info("Starting removal of THEMES_CHECK_DONE labels")

    try:
        from pathlib import Path
        from dotenv import load_dotenv

        # Load environment variables
        env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

        from config import load_config_from_env
        from google_ads_client import initialize_client

        config = load_config_from_env()
        client = initialize_client(config.google_ads)

        # Load customer IDs from file
        account_ids_file = Path(__file__).parent.parent / "thema_ads_optimized" / "account ids"
        if not account_ids_file.exists():
            raise HTTPException(status_code=500, detail="Account IDs file not found")

        with open(account_ids_file, 'r') as f:
            customer_ids = [line.strip() for line in f if line.strip()]

        logger.info(f"Loaded {len(customer_ids)} customer IDs from account ids file")

        # Run label removal
        result = await thema_ads_service.remove_checkup_labels(
            client=client,
            customer_ids=customer_ids
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Label removal failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cleanup-thema-original")
async def cleanup_thema_original_labels(dry_run: bool = False):
    """
    Remove THEMA_ORIGINAL labels from ads that also have theme labels.

    Theme ads should ONLY have theme labels (THEME_BF, THEME_CM, etc), not THEMA_ORIGINAL.
    This function finds and removes incorrect THEMA_ORIGINAL labels from theme ads.

    Args:
        dry_run: If True, only report what would be changed without making changes (default: False)
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Starting THEMA_ORIGINAL cleanup (dry_run={dry_run})")

    try:
        from pathlib import Path
        from dotenv import load_dotenv
        import subprocess

        # Load environment variables
        env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
        if not env_path.exists():
            raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

        # Run the cleanup script
        script_path = Path(__file__).parent.parent / "thema_ads_optimized" / "cleanup_thema_original_labels.py"
        if not script_path.exists():
            raise HTTPException(status_code=500, detail="Cleanup script not found")

        # Build command
        cmd = ["python3", str(script_path)]
        if not dry_run:
            cmd.append("--execute")

        logger.info(f"Running cleanup script: {' '.join(cmd)}")

        # Run script and capture output
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )

        if result.returncode != 0:
            logger.error(f"Cleanup script failed: {result.stderr}")
            raise HTTPException(status_code=500, detail=f"Cleanup failed: {result.stderr}")

        # Parse output for summary
        output_lines = result.stderr.split('\n')

        # Extract summary statistics
        total_checked = 0
        total_fixed = 0
        total_failed = 0

        for line in output_lines:
            if "Total ads with conflicting labels:" in line:
                total_checked = int(line.split(":")[-1].strip())
            elif "Successfully fixed:" in line:
                total_fixed = int(line.split(":")[-1].strip())
            elif "Failed:" in line and "INFO" in line:
                total_failed = int(line.split(":")[-1].strip())

        logger.info(f"Cleanup complete: {total_fixed} ads fixed, {total_failed} failed")

        return {
            "success": True,
            "dry_run": dry_run,
            "total_checked": total_checked,
            "total_fixed": total_fixed,
            "total_failed": total_failed,
            "message": f"{'Would fix' if dry_run else 'Fixed'} {total_fixed} ads with conflicting labels"
        }

    except subprocess.TimeoutExpired:
        logger.error("Cleanup script timed out")
        raise HTTPException(status_code=500, detail="Cleanup operation timed out")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Cleanup failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run-all-themes")
async def run_all_themes(
    background_tasks: BackgroundTasks = None,
    customer_filter: str = "Beslist.nl -",
    themes: List[str] = Query(None),
    limit: int = None,
    batch_size: int = 5000,
    job_chunk_size: int = 50000
):
    """
    Run All Themes: Discover all ad groups and add missing theme ads.

    Uses batch queries for improved performance.

    This function:
    - Finds all customers matching the filter
    - Discovers all ad groups in HS/ campaigns with batch queries
    - Checks which themes are missing (no DONE label AND no theme-labeled ads)
    - Creates jobs per theme to add missing theme ads

    Args:
        customer_filter: Customer name prefix (default: "Beslist.nl -")
        themes: List of theme names to process (None = all themes)
        limit: Optional limit on number of ad groups to check
        batch_size: Batch size for API queries (default: 5000)
        job_chunk_size: Maximum items per job (default: 50000)
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Run All Themes: filter='{customer_filter}', themes={themes}, limit={limit}")

    try:
        from pathlib import Path
        from dotenv import load_dotenv

        # Load environment variables
        env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

        from config import load_config_from_env
        from google_ads_client import initialize_client

        config = load_config_from_env()
        client = initialize_client(config.google_ads)

        # Run all-themes discovery
        result = await thema_ads_service.discover_all_missing_themes(
            client=client,
            customer_filter=customer_filter,
            selected_themes=themes,
            limit=limit,
            batch_size=batch_size,
            job_chunk_size=job_chunk_size,
            background_tasks=background_tasks
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Run All Themes failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/activate-ads")
async def activate_ads(
    customer_ids: List[str] = None,
    reset_labels: bool = False,
    batch_size: int = 5000
):
    """
    Activate the correct theme ad per customer based on activation plan.
    Pauses all ads first, then activates the correct theme ad.

    Args:
        customer_ids: Optional list of customer IDs to process (None = all in plan)
        reset_labels: If True, reprocess ad groups with ACTIVATION_DONE label
        batch_size: Batch size for API queries (default: 5000)
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Activate ads parameters: customer_ids={customer_ids}, reset_labels={reset_labels}")

    try:
        from pathlib import Path
        from dotenv import load_dotenv

        # Load environment variables
        env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

        from config import load_config_from_env
        from google_ads_client import initialize_client

        config = load_config_from_env()
        client = initialize_client(config.google_ads)

        # Run activation
        result = await thema_ads_service.activate_ads_per_plan(
            client=client,
            customer_ids=customer_ids,
            batch_size=batch_size,
            reset_labels=reset_labels
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ad activation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/activate-optimized")
async def activate_ads_optimized(
    customer_ids: List[str] = None,
    reset_labels: bool = False,
    parallel_workers: int = 5
):
    """
    OPTIMIZED: Activate the correct theme ad per customer based on activation plan.

    Performance optimizations:
    - Process customers in parallel (5-10x faster)
    - Bulk query all ads with theme label in ONE query per customer
    - Bulk query all ads with THEMA_ORIGINAL label in ONE query per customer
    - Batch status updates (enable theme ads, pause original ads)

    Args:
        customer_ids: Optional list of customer IDs to process (None = all in plan)
        reset_labels: If True, reprocess ad groups with ACTIVATION_DONE label
        parallel_workers: Number of customers to process in parallel (default: 5)
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"OPTIMIZED Activate ads parameters: customer_ids={customer_ids}, parallel={parallel_workers}, reset={reset_labels}")

    try:
        from pathlib import Path
        from dotenv import load_dotenv

        # Load environment variables
        env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

        from config import load_config_from_env
        from google_ads_client import initialize_client

        config = load_config_from_env()
        client = initialize_client(config.google_ads)

        # Run optimized activation
        result = await thema_ads_service.activate_ads_per_plan_optimized(
            client=client,
            customer_ids=customer_ids,
            parallel_workers=parallel_workers,
            reset_labels=reset_labels
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"OPTIMIZED ad activation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/activate-v2")
async def activate_ads_v2_endpoint(
    background_tasks: BackgroundTasks,
    customer_ids: List[str] = Query(None),
    reset_labels: bool = False,
    parallel_workers: int = 5
):
    """
    V2: Ultra-fast AD-FIRST activation approach - runs as background task.

    Returns immediately with accepted status. Check logs for progress.

    Args:
        customer_ids: Optional list of customer IDs to process (None = all in plan)
        reset_labels: If True, reprocess ad groups with ACTIVATION_DONE label
        parallel_workers: Number of customers to process in parallel (default: 5)
    """
    import logging
    logger = logging.getLogger(__name__)

    async def run_activation():
        """Background task to run activation"""
        try:
            print("[ACTIVATE-V2] Background task started", flush=True)
            from pathlib import Path
            from dotenv import load_dotenv

            # Load environment
            env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
            if env_path.exists():
                load_dotenv(env_path)

            from config import load_config_from_env
            from google_ads_client import initialize_client

            print("[ACTIVATE-V2] Initializing client in background task...", flush=True)
            logger.info("[ACTIVATE-V2] Initializing client in background task...")
            config = load_config_from_env()
            client = initialize_client(config.google_ads)

            logger.info("[ACTIVATE-V2] Starting activation...")
            result = await thema_ads_service.activate_ads_per_plan_v2(
                client=client,
                customer_ids=customer_ids,
                parallel_workers=parallel_workers,
                reset_labels=reset_labels
            )

            logger.info(f"[ACTIVATE-V2] Completed: {result}")
        except Exception as e:
            print(f"[ACTIVATE-V2] Failed: {e}", flush=True)
            logger.error(f"[ACTIVATE-V2] Failed: {e}", exc_info=True)
            import traceback
            traceback.print_exc()

    # Add to background tasks - use asyncio.create_task for async functions
    import asyncio
    asyncio.create_task(run_activation())

    logger.info(f"[ACTIVATE-V2] Activation queued for {customer_ids or 'all customers'} with parallel_workers={parallel_workers}")

    return {
        "status": "accepted",
        "message": "Activation started in background. Check logs for progress.",
        "customer_ids": customer_ids,
        "parallel_workers": parallel_workers
    }


@router.post("/remove-duplicates")
async def remove_duplicates_endpoint(
    customer_ids: List[str] = Query(None),
    limit: int = None,
    dry_run: bool = True,
    reset_labels: bool = False
):
    """
    Remove duplicate ads from ad groups in HS/ campaigns.

    Finds ads with identical content (headlines + descriptions) and removes
    duplicates, keeping ads with theme labels.

    Args:
        customer_ids: Optional list of customer IDs (None = all Beslist.nl)
        limit: Optional limit of ad groups per customer (for testing)
        dry_run: If True, only report what would be done (default: True)
        reset_labels: If True, recheck ad groups with THEME_DUPLICATES_CHECK label
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Remove duplicates parameters: customer_ids={customer_ids}, limit={limit}, dry_run={dry_run}, reset={reset_labels}")

    try:
        from pathlib import Path
        from dotenv import load_dotenv

        # Load environment variables
        env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

        from config import load_config_from_env
        from google_ads_client import initialize_client

        config = load_config_from_env()
        client = initialize_client(config.google_ads)

        # Run duplicate removal
        result = await thema_ads_service.remove_duplicates_all_customers(
            client=client,
            customer_ids=customer_ids,
            limit=limit,
            dry_run=dry_run,
            reset_labels=reset_labels
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Duplicate removal failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/activation-plan")
async def get_activation_plan_api(customer_ids: List[str] = None):
    """Get the current activation plan."""
    try:
        from backend.thema_ads_db import get_activation_plan
        plan = get_activation_plan(customer_ids)
        return {"plan": plan, "customer_count": len(plan)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/activation-missing-ads")
async def get_activation_missing_ads_api():
    """Get list of ad groups missing required theme ads."""
    try:
        from backend.thema_ads_db import get_activation_missing_ads
        missing_ads = get_activation_missing_ads()
        return {"missing_ads": missing_ads, "count": len(missing_ads)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/activation-missing-ads/export")
async def export_activation_missing_ads():
    """Export missing ads as CSV file."""
    try:
        from backend.thema_ads_db import get_activation_missing_ads
        import io
        import csv

        missing_ads = get_activation_missing_ads()

        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(['customer_id', 'campaign_id', 'campaign_name', 'ad_group_id', 'ad_group_name', 'required_theme', 'detected_at'])

        # Write data
        for row in missing_ads:
            writer.writerow([
                row['customer_id'],
                row['campaign_id'],
                row['campaign_name'],
                row['ad_group_id'],
                row['ad_group_name'],
                row['required_theme'],
                row['detected_at']
            ])

        # Prepare response
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=activation_missing_ads.csv"}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/themes")
async def list_themes():
    """Get list of supported themes."""
    try:
        themes = []
        for theme_name, theme_info in SUPPORTED_THEMES.items():
            themes.append({
                "name": theme_name,
                "label": theme_info["label"],
                "display_name": theme_info["display_name"],
                "countdown_date": theme_info["countdown_date"]
            })
        return {"themes": themes}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/label-failed")
async def label_failed_ad_groups(
    background_tasks: BackgroundTasks = None,
    theme: str = Form(...),
    job_ids: str = Form(...)
):
    """
    Label permanently failed ad groups with THEME_XX_ATTEMPTED label.
    This prevents them from appearing in future discoveries.

    Args:
        theme: Theme name (e.g., 'black_friday')
        job_ids: Comma-separated job IDs (e.g., '232,233,234')
    """
    import logging
    logger = logging.getLogger(__name__)

    try:
        job_id_list = [int(x.strip()) for x in job_ids.split(',')]
        logger.info(f"Labeling failed ad groups for theme={theme}, jobs={job_id_list}")

        # Validate theme
        if not is_valid_theme(theme):
            supported_themes = ', '.join(SUPPORTED_THEMES.keys())
            raise HTTPException(
                status_code=400,
                detail=f"Invalid theme '{theme}'. Supported themes: {supported_themes}"
            )

        # Get failed ad groups from database
        from backend.thema_ads_db import get_db_connection
        import psycopg2.extras
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT DISTINCT customer_id, ad_group_id
            FROM thema_ads_job_items
            WHERE job_id = ANY(%s)
            AND status = 'failed'
            AND (
                error_message LIKE '%%no resource returned%%'
                OR error_message LIKE '%%PROHIBITED_SYMBOLS%%'
                OR error_message LIKE '%%DESTINATION_NOT_WORKING%%'
                OR error_message LIKE '%%POLICY_FINDING%%'
            )
            ORDER BY customer_id, ad_group_id
        """, (job_id_list,))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            return {
                "status": "no_failures_found",
                "message": "No permanently failed ad groups found in specified jobs",
                "total_labeled": 0
            }

        # Group by customer
        from collections import defaultdict
        by_customer = defaultdict(list)
        for row in rows:
            by_customer[row['customer_id']].append(row['ad_group_id'])

        total_ad_groups = len(rows)
        logger.info(f"Found {total_ad_groups} failed ad groups across {len(by_customer)} customers")

        # Load Google Ads client
        from pathlib import Path
        from dotenv import load_dotenv

        env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
        if not env_path.exists():
            raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

        load_dotenv(env_path)

        from config import load_config_from_env
        from google_ads_client import initialize_client
        from themes import get_theme_label

        config = load_config_from_env()
        client = initialize_client(config.google_ads)

        # Get theme label
        theme_label = get_theme_label(theme)
        attempted_label_name = f"{theme_label}_ATTEMPTED"

        logger.info(f"Applying label: {attempted_label_name}")

        # Label ad groups for each customer
        ga_service = client.get_service("GoogleAdsService")
        label_service = client.get_service("LabelService")
        ad_group_label_service = client.get_service("AdGroupLabelService")

        total_labeled = 0

        for customer_id, ad_group_ids in by_customer.items():
            try:
                logger.info(f"Processing customer {customer_id}: {len(ad_group_ids)} ad groups")

                # Ensure label exists
                label_query = f"SELECT label.resource_name FROM label WHERE label.name = '{attempted_label_name}' LIMIT 1"
                label_resource = None

                try:
                    label_response = ga_service.search(customer_id=customer_id, query=label_query)
                    for row in label_response:
                        label_resource = row.label.resource_name
                        break
                except:
                    pass

                if not label_resource:
                    # Create label
                    logger.info(f"  Creating label '{attempted_label_name}'")
                    label_operation = client.get_type("LabelOperation")
                    label = label_operation.create
                    label.name = attempted_label_name

                    response = label_service.mutate_labels(
                        customer_id=customer_id,
                        operations=[label_operation]
                    )
                    label_resource = response.results[0].resource_name

                # Apply label to ad groups in batches
                operations = []
                for ag_id in ad_group_ids:
                    operation = client.get_type("AdGroupLabelOperation")
                    ad_group_label = operation.create
                    ad_group_label.ad_group = ga_service.ad_group_path(customer_id, ag_id)
                    ad_group_label.label = label_resource
                    operations.append(operation)

                # Batch in chunks of 5000
                BATCH_SIZE = 5000
                for i in range(0, len(operations), BATCH_SIZE):
                    batch = operations[i:i + BATCH_SIZE]
                    try:
                        response = ad_group_label_service.mutate_ad_group_labels(
                            customer_id=customer_id,
                            operations=batch
                        )
                        total_labeled += len(response.results)
                        logger.info(f"  Labeled {len(response.results)} ad groups")
                    except Exception as e:
                        logger.error(f"  Error labeling batch: {e}")

            except Exception as e:
                logger.error(f"Error processing customer {customer_id}: {e}")
                continue

        return {
            "status": "completed",
            "theme": theme,
            "label_applied": attempted_label_name,
            "total_ad_groups_found": total_ad_groups,
            "total_labeled": total_labeled,
            "customers_processed": len(by_customer)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to label ad groups: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/label-checkup-failed")
async def label_checkup_failed_ad_groups(
    background_tasks: BackgroundTasks = None,
    job_ids: str = Form(...)
):
    """
    Label ad groups from repair jobs with THEMES_CHECKUP_FAILED label.
    This prevents them from being processed in future checkup runs.

    These are ad groups that:
    - Had DONE labels but no corresponding themed ads
    - Are now in repair jobs that are failing

    Args:
        job_ids: Comma-separated job IDs (e.g., '479,480')
    """
    import logging
    logger = logging.getLogger(__name__)

    try:
        job_id_list = [int(x.strip()) for x in job_ids.split(',')]
        logger.info(f"Labeling checkup-failed ad groups for jobs={job_id_list}")

        # Get ALL ad groups from these repair jobs (not just failed ones)
        from backend.thema_ads_db import get_db_connection
        import psycopg2.extras
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT DISTINCT customer_id, ad_group_id
            FROM thema_ads_job_items
            WHERE job_id = ANY(%s)
            ORDER BY customer_id, ad_group_id
        """, (job_id_list,))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            return {
                "status": "no_items_found",
                "message": "No ad groups found in specified jobs",
                "total_labeled": 0
            }

        # Group by customer
        from collections import defaultdict
        by_customer = defaultdict(list)
        for row in rows:
            by_customer[row['customer_id']].append(row['ad_group_id'])

        total_ad_groups = len(rows)
        logger.info(f"Found {total_ad_groups} ad groups across {len(by_customer)} customers")

        # Load Google Ads client
        from pathlib import Path
        from dotenv import load_dotenv

        env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
        if not env_path.exists():
            raise HTTPException(status_code=500, detail="Google Ads credentials not configured")

        load_dotenv(env_path)

        from config import load_config_from_env
        from google_ads_client import initialize_client

        config = load_config_from_env()
        client = initialize_client(config.google_ads)

        # Use generic checkup failure label
        checkup_failed_label = "THEMES_CHECKUP_FAILED"

        logger.info(f"Applying label: {checkup_failed_label}")

        # Label ad groups for each customer
        ga_service = client.get_service("GoogleAdsService")
        label_service = client.get_service("LabelService")
        ad_group_label_service = client.get_service("AdGroupLabelService")

        total_labeled = 0

        for customer_id, ad_group_ids in by_customer.items():
            try:
                logger.info(f"Processing customer {customer_id}: {len(ad_group_ids)} ad groups")

                # Ensure label exists
                label_query = f"SELECT label.resource_name FROM label WHERE label.name = '{checkup_failed_label}' LIMIT 1"
                label_resource = None

                try:
                    label_response = ga_service.search(customer_id=customer_id, query=label_query)
                    for row in label_response:
                        label_resource = row.label.resource_name
                        break
                except:
                    pass

                if not label_resource:
                    # Create label
                    logger.info(f"  Creating label '{checkup_failed_label}'")
                    label_operation = client.get_type("LabelOperation")
                    label = label_operation.create
                    label.name = checkup_failed_label

                    response = label_service.mutate_labels(
                        customer_id=customer_id,
                        operations=[label_operation]
                    )
                    label_resource = response.results[0].resource_name

                # Apply label to ad groups in batches
                operations = []
                for ag_id in ad_group_ids:
                    operation = client.get_type("AdGroupLabelOperation")
                    ad_group_label = operation.create
                    ad_group_label.ad_group = ga_service.ad_group_path(customer_id, ag_id)
                    ad_group_label.label = label_resource
                    operations.append(operation)

                # Batch in chunks of 5000
                BATCH_SIZE = 5000
                for i in range(0, len(operations), BATCH_SIZE):
                    batch = operations[i:i + BATCH_SIZE]
                    try:
                        response = ad_group_label_service.mutate_ad_group_labels(
                            customer_id=customer_id,
                            operations=batch,
                            partial_failure=True  # Continue even if some fail (e.g., already labeled)
                        )
                        total_labeled += len(response.results)
                        logger.info(f"  Labeled {len(response.results)} ad groups")
                    except Exception as e:
                        logger.error(f"  Error labeling batch: {e}")

            except Exception as e:
                logger.error(f"Error processing customer {customer_id}: {e}")
                continue

        return {
            "status": "completed",
            "label_applied": checkup_failed_label,
            "total_ad_groups_found": total_ad_groups,
            "total_labeled": total_labeled,
            "customers_processed": len(by_customer)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to label checkup-failed ad groups: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
