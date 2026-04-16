"""
DMA+ Service — Web wrapper for campaign_processor.py operations.

Provides background execution, progress tracking, change history, and
NL/BE country switching for the 5 core DMA operations:
  1. process_inclusion_sheet_v2
  2. process_exclusion_sheet_v2
  3. validate_cl1_targeting_for_campaigns
  4. validate_ads_for_campaigns
  5. validate_listing_trees_for_campaigns
"""

import io
import os
import sys
import time
import uuid
import logging
import threading
import traceback
from collections import deque
from datetime import datetime
from typing import Optional

import openpyxl
from google.ads.googleads.client import GoogleAdsClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Google Ads client (shared with dma_bidding_service pattern)
# ---------------------------------------------------------------------------
MCC_CUSTOMER_ID = "3011145605"

COUNTRY_CONFIG = {
    "NL": {"customer_id": "3800751597", "merchant_center_id": 140784594, "exclude_dataedis": True},
    "BE": {"customer_id": "9920951707", "merchant_center_id": 140784810, "exclude_dataedis": False},
}


def _get_client() -> GoogleAdsClient:
    """Initialize Google Ads client from environment, checking both naming conventions."""
    config = {
        "developer_token": os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN")
                           or os.environ.get("GOOGLE_DEVELOPER_TOKEN", ""),
        "refresh_token": os.environ.get("GOOGLE_ADS_REFRESH_TOKEN")
                         or os.environ.get("GOOGLE_REFRESH_TOKEN", ""),
        "client_id": os.environ.get("GOOGLE_CLIENT_ID", ""),
        "client_secret": os.environ.get("GOOGLE_CLIENT_SECRET", ""),
        "login_customer_id": os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
                             or os.environ.get("GOOGLE_LOGIN_CUSTOMER_ID", MCC_CUSTOMER_ID),
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config)


# ---------------------------------------------------------------------------
# Task store (in-memory, background tasks)
# ---------------------------------------------------------------------------
_tasks: dict = {}
_history: deque = deque(maxlen=200)  # change history, capped


def _get_task(task_id: str) -> Optional[dict]:
    return _tasks.get(task_id)


def _set_task(task_id: str, data: dict):
    existing = _tasks.get(task_id)
    if existing and existing.get("cancel") and "cancel" not in data:
        data["cancel"] = True
    _tasks[task_id] = data


# ---------------------------------------------------------------------------
# Patch campaign_processor globals for web use
# ---------------------------------------------------------------------------
def _patch_campaign_processor(country: str):
    """
    Set campaign_processor module globals to match the selected country.
    Must be called BEFORE invoking any processor function.
    """
    from backend import campaign_processor as cp

    cfg = COUNTRY_CONFIG[country]
    cp.COUNTRY = country
    cp.CUSTOMER_ID = cfg["customer_id"]
    cp.MERCHANT_CENTER_ID = cfg["merchant_center_id"]
    cp.EXCLUDE_DATAEDIS = cfg["exclude_dataedis"]


# ---------------------------------------------------------------------------
# Build workbook from shop name input (for quick include/exclude)
# ---------------------------------------------------------------------------
def _build_inclusion_workbook(shop_name: str, maincat: str, maincat_id: str,
                              cl1: str, budget: float) -> openpyxl.Workbook:
    """Create a minimal workbook matching the inclusion sheet layout."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "toevoegen"
    # Header: campaign_name, ad_group_name, Shop ID, maincat, maincat_id, cl1, budget, result, error
    ws.append(["campaign_name", "ad_group_name", "Shop ID", "maincat", "maincat_id",
               "custom label 1", "budget", "result", "error message"])
    # Build campaign name: PLA/{maincat}_{cl1}
    campaign_name = f"PLA/{maincat}_{cl1}"
    ws.append([campaign_name, shop_name, "", maincat, maincat_id, cl1, budget, None, None])
    return wb


def _build_exclusion_workbook(shop_name: str, maincat: str, maincat_id: str,
                              cl1: str) -> openpyxl.Workbook:
    """Create a minimal workbook matching the exclusion sheet layout."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "uitsluiten"
    ws.append(["shop_name", "Shop ID", "maincat", "maincat_id", "custom label 1", "result", "error message"])
    ws.append([shop_name, "", maincat, maincat_id, cl1, None, None])
    # Add cat_ids sheet (needed by exclusion processor)
    ws_cat = wb.create_sheet("cat_ids")
    ws_cat.append(["maincat", "maincat_id", "deepest_cat", "cat_id"])
    return wb


# ---------------------------------------------------------------------------
# Extract results from workbook after processing
# ---------------------------------------------------------------------------
def _extract_results(wb: openpyxl.Workbook, sheet_name: str, result_col: int, error_col: int) -> list:
    """Read back results from the processed workbook."""
    results = []
    if sheet_name not in wb.sheetnames:
        return results
    ws = wb[sheet_name]
    for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=False), start=2):
        if row_idx == 1:
            continue
        cells = list(row)
        if not cells or not cells[0].value:
            continue
        result_val = cells[result_col].value if result_col < len(cells) else None
        error_val = cells[error_col].value if error_col < len(cells) else None
        row_data = [c.value for c in cells]
        results.append({
            "row": row_idx,
            "data": row_data,
            "success": result_val is True or str(result_val).upper() == "TRUE",
            "error": str(error_val) if error_val else None,
        })
    return results


# ---------------------------------------------------------------------------
# Core operation runners (run in background thread)
# ---------------------------------------------------------------------------
def _run_operation(task_id: str, operation: str, country: str,
                   wb: Optional[openpyxl.Workbook] = None,
                   wb_bytes: Optional[bytes] = None,
                   campaign_pattern: str = None,
                   dry_run: bool = False,
                   fix: bool = False):
    """Background thread target for all 5 operations."""
    try:
        _set_task(task_id, {
            "status": "initializing",
            "operation": operation,
            "country": country,
            "progress": 0,
            "message": "Initializing Google Ads client...",
            "started_at": datetime.now().isoformat(),
        })

        # Patch campaign_processor for country
        _patch_campaign_processor(country)
        from backend import campaign_processor as cp

        # Initialize client
        client = _get_client()
        customer_id = COUNTRY_CONFIG[country]["customer_id"]

        _set_task(task_id, {
            **_get_task(task_id),
            "status": "running",
            "progress": 10,
            "message": f"Running {operation}...",
        })

        result_data = None

        # ---- INCLUSION ----
        if operation == "inclusion":
            if wb_bytes:
                wb = openpyxl.load_workbook(io.BytesIO(wb_bytes), data_only=True)
            if not wb:
                raise ValueError("No workbook provided for inclusion")

            # Redirect stdout to capture progress
            old_stdout = sys.stdout
            captured = io.StringIO()
            sys.stdout = captured
            try:
                cp.process_inclusion_sheet_v2(client, wb, customer_id)
            finally:
                sys.stdout = old_stdout

            log_output = captured.getvalue()
            results = _extract_results(wb, "toevoegen", 7, 8)  # col H=result, I=error
            result_data = {
                "rows_processed": len(results),
                "successes": sum(1 for r in results if r["success"]),
                "failures": sum(1 for r in results if not r["success"] and r["error"]),
                "details": results,
                "log": log_output[-5000:],  # last 5k chars
            }

        # ---- EXCLUSION ----
        elif operation == "exclusion":
            if wb_bytes:
                wb = openpyxl.load_workbook(io.BytesIO(wb_bytes), data_only=True)
            if not wb:
                raise ValueError("No workbook provided for exclusion")

            old_stdout = sys.stdout
            captured = io.StringIO()
            sys.stdout = captured
            try:
                cp.process_exclusion_sheet_v2(client, wb, customer_id)
            finally:
                sys.stdout = old_stdout

            log_output = captured.getvalue()
            results = _extract_results(wb, "uitsluiten", 5, 6)  # col F=result, G=error
            result_data = {
                "rows_processed": len(results),
                "successes": sum(1 for r in results if r["success"]),
                "failures": sum(1 for r in results if not r["success"] and r["error"]),
                "details": results,
                "log": log_output[-5000:],
            }

        # ---- VALIDATE CL1 ----
        elif operation == "validate_cl1":
            old_stdout = sys.stdout
            captured = io.StringIO()
            sys.stdout = captured
            try:
                result_data = cp.validate_cl1_targeting_for_campaigns(
                    client, customer_id,
                    campaign_name_pattern=campaign_pattern,
                    dry_run=dry_run,
                )
            finally:
                sys.stdout = old_stdout
            if result_data:
                result_data["log"] = captured.getvalue()[-5000:]

        # ---- VALIDATE ADS ----
        elif operation == "validate_ads":
            old_stdout = sys.stdout
            captured = io.StringIO()
            sys.stdout = captured
            try:
                result_data = cp.validate_ads_for_campaigns(
                    client, customer_id,
                    campaign_name_pattern=campaign_pattern,
                    fix=fix,
                )
            finally:
                sys.stdout = old_stdout
            if result_data:
                result_data["log"] = captured.getvalue()[-5000:]

        # ---- VALIDATE LISTING TREES ----
        elif operation == "validate_trees":
            old_stdout = sys.stdout
            captured = io.StringIO()
            sys.stdout = captured
            try:
                excel_path = None
                if wb_bytes:
                    # Save to temp file for cat_ids reading
                    import tempfile
                    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
                    tmp.write(wb_bytes)
                    tmp.close()
                    excel_path = tmp.name

                result_data = cp.validate_listing_trees_for_campaigns(
                    client, customer_id,
                    campaign_name_pattern=campaign_pattern or "PLA/%",
                    dry_run=dry_run,
                    excel_path=excel_path,
                )
            finally:
                sys.stdout = old_stdout
            if result_data:
                result_data["log"] = captured.getvalue()[-5000:]

        else:
            raise ValueError(f"Unknown operation: {operation}")

        # Store result
        _set_task(task_id, {
            "status": "completed",
            "operation": operation,
            "country": country,
            "progress": 100,
            "message": "Completed",
            "started_at": _get_task(task_id).get("started_at"),
            "completed_at": datetime.now().isoformat(),
            "result": result_data,
        })

        # Add to history
        _history.appendleft({
            "task_id": task_id,
            "operation": operation,
            "country": country,
            "started_at": _get_task(task_id).get("started_at"),
            "completed_at": datetime.now().isoformat(),
            "status": "completed",
            "summary": _summarize_result(operation, result_data),
        })

    except Exception as e:
        logger.error(f"DMA+ task {task_id} failed: {e}", exc_info=True)
        _set_task(task_id, {
            "status": "failed",
            "operation": operation,
            "country": country,
            "progress": 0,
            "message": str(e),
            "error": traceback.format_exc()[-3000:],
            "started_at": _get_task(task_id).get("started_at", ""),
            "completed_at": datetime.now().isoformat(),
        })
        _history.appendleft({
            "task_id": task_id,
            "operation": operation,
            "country": country,
            "started_at": _get_task(task_id).get("started_at", ""),
            "completed_at": datetime.now().isoformat(),
            "status": "failed",
            "summary": f"Error: {str(e)[:200]}",
        })


def _summarize_result(operation: str, result: dict) -> str:
    if not result:
        return "No results"
    if operation in ("inclusion", "exclusion"):
        return f"{result.get('successes', 0)} ok, {result.get('failures', 0)} failed of {result.get('rows_processed', 0)} rows"
    elif operation == "validate_cl1":
        return f"{result.get('ok', 0)} ok, {result.get('fixed', 0)} fixed, {result.get('error', 0)} errors of {result.get('total', 0)}"
    elif operation == "validate_ads":
        return f"{result.get('with_ads', 0)} with ads, {result.get('missing_ads', 0)} missing, {result.get('fixed', 0)} fixed"
    elif operation == "validate_trees":
        return f"{result.get('ok', 0)} ok, {result.get('created', 0)} created, {result.get('error', 0)} errors of {result.get('total', 0)}"
    return str(result)[:200]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def start_operation(operation: str, country: str = "NL",
                    wb_bytes: bytes = None,
                    shop_name: str = None,
                    maincat: str = None,
                    maincat_id: str = None,
                    cl1: str = None,
                    budget: float = None,
                    campaign_pattern: str = None,
                    dry_run: bool = False,
                    fix: bool = False) -> str:
    """
    Start a DMA+ operation in the background.
    Returns task_id for progress polling.
    """
    task_id = str(uuid.uuid4())[:8]

    wb = None
    # If shop_name provided (quick input), build a workbook
    if shop_name and operation == "inclusion":
        wb = _build_inclusion_workbook(
            shop_name, maincat or "", maincat_id or "", cl1 or "a", budget or 10.0
        )
        wb_bytes = None  # use the wb object directly
    elif shop_name and operation == "exclusion":
        wb = _build_exclusion_workbook(
            shop_name, maincat or "", maincat_id or "", cl1 or "a"
        )
        wb_bytes = None

    thread = threading.Thread(
        target=_run_operation,
        args=(task_id, operation, country),
        kwargs={
            "wb": wb,
            "wb_bytes": wb_bytes,
            "campaign_pattern": campaign_pattern,
            "dry_run": dry_run,
            "fix": fix,
        },
        daemon=True,
    )
    thread.start()

    return task_id


def get_task_status(task_id: str) -> Optional[dict]:
    return _get_task(task_id)


def cancel_task(task_id: str) -> bool:
    task = _get_task(task_id)
    if task and task.get("status") == "running":
        task["cancel"] = True
        _set_task(task_id, task)
        return True
    return False


def get_history() -> list:
    return list(_history)


def clear_history():
    _history.clear()
