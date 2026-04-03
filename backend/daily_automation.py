"""
Daily automation script for DM Dashboard.
Runs: reset validations → validate links (parallel) → process URLs → publish to production.
Designed for Windows Task Scheduler, daily at 07:00.
"""
import sys
import os
import time
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from dotenv import load_dotenv

# Load .env from project root for DASHBOARD_PASSWORD
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "http://localhost:3003"
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
LOG_FILE = os.path.join(LOG_DIR, "daily_automation.log")

POLL_INTERVAL = 15            # seconds between status polls
VALIDATION_TIMEOUT = 7200     # 2 hours max for a validation step
PROCESS_TIMEOUT = 14400       # 4 hours max for processing loops
PUBLISH_TIMEOUT = 3600        # 1 hour max for publish

# Authenticated session (reused across all requests)
SESSION = requests.Session()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("automation")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=7, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger

# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------
def login():
    """Authenticate with the dashboard using the password from .env."""
    log = logging.getLogger("automation")
    password = os.getenv("DASHBOARD_PASSWORD", "")
    if not password:
        log.warning("No DASHBOARD_PASSWORD set, proceeding without auth")
        return
    resp = SESSION.post(f"{BASE_URL}/login", data={"password": password}, allow_redirects=False)
    if resp.status_code in (200, 302, 303, 307):
        log.info("Authenticated with dashboard")
    else:
        raise RuntimeError(f"Login failed with status {resp.status_code}")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def poll_task(status_url, timeout):
    """Poll a background task until completed/failed."""
    log = logging.getLogger("automation")
    start = time.time()
    while time.time() - start < timeout:
        resp = SESSION.get(status_url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status", "")

        if status == "completed":
            log.info(f"  Task completed")
            return data
        if status in ("error", "failed"):
            raise RuntimeError(f"Task failed: {data}")

        # Log progress
        rechecked = data.get("rechecked", data.get("validated", ""))
        total = data.get("total_to_recheck", data.get("total", ""))
        log.info(f"  Polling… status={status}  progress={rechecked}/{total}")
        time.sleep(POLL_INTERVAL)

    raise TimeoutError(f"Task timed out after {timeout}s")


def loop_until_done(url, timeout, params=None):
    """Call an endpoint repeatedly until no URLs left to process."""
    log = logging.getLogger("automation")
    start = time.time()
    iteration = 0
    total_processed = 0
    while time.time() - start < timeout:
        iteration += 1
        resp = SESSION.post(url, params=params, timeout=300)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == "complete" or data.get("message") == "No URLs to process":
            log.info(f"  Done after {iteration} iterations, total processed: {total_processed}")
            return data

        processed = data.get("processed", 0)
        total_processed += processed
        log.info(f"  Iteration {iteration}: processed={processed} (total so far: {total_processed})")
        time.sleep(2)

    raise TimeoutError(f"Processing loop timed out after {timeout}s")

# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------
def step_reset_faq_validation():
    log = logging.getLogger("automation")
    resp = SESSION.delete(f"{BASE_URL}/api/faq/validation-history/reset", timeout=30)
    resp.raise_for_status()
    data = resp.json()
    log.info(f"  FAQ validation reset — cleared: {data.get('cleared_count', data.get('deleted', 0))}")


def step_reset_kopteksten_validation():
    log = logging.getLogger("automation")
    resp = SESSION.delete(f"{BASE_URL}/api/validation-history/reset", timeout=30)
    resp.raise_for_status()
    data = resp.json()
    log.info(f"  Kopteksten validation reset — cleared: {data.get('cleared_count', 0)}")


def step_validate_faq_links():
    log = logging.getLogger("automation")
    resp = SESSION.post(
        f"{BASE_URL}/api/faq/validate-all-links",
        params={"parallel_workers": 3, "batch_size": 500},
        timeout=30,
    )
    resp.raise_for_status()
    task_id = resp.json().get("task_id")
    log.info(f"  FAQ validate-all started, task_id={task_id}")
    poll_task(f"{BASE_URL}/api/faq/validate-all-links/status/{task_id}", VALIDATION_TIMEOUT)


def step_validate_kopteksten_links():
    log = logging.getLogger("automation")
    resp = SESSION.post(
        f"{BASE_URL}/api/recheck-skipped-urls",
        params={"parallel_workers": 3, "batch_size": 50},
        timeout=30,
    )
    resp.raise_for_status()
    task_id = resp.json().get("task_id")
    log.info(f"  Kopteksten recheck started, task_id={task_id}")
    poll_task(f"{BASE_URL}/api/recheck-skipped-urls/status/{task_id}", VALIDATION_TIMEOUT)


def step_validate_parallel():
    """Run FAQ and Kopteksten validation in parallel."""
    log = logging.getLogger("automation")
    log.info("  Starting FAQ + Kopteksten validation in parallel")
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(step_validate_faq_links): "Validate FAQ links",
            executor.submit(step_validate_kopteksten_links): "Validate Kopteksten links",
        }
        for future in as_completed(futures):
            name = futures[future]
            future.result()  # raises if failed
            log.info(f"  ✓ {name} completed")


def step_process_faq_urls():
    loop_until_done(
        f"{BASE_URL}/api/faq/process-urls",
        PROCESS_TIMEOUT,
        params={"batch_size": 200, "parallel_workers": 20, "num_faqs": 6},
    )


def step_process_kopteksten_urls():
    loop_until_done(
        f"{BASE_URL}/api/process-urls",
        PROCESS_TIMEOUT,
        params={"batch_size": 2, "parallel_workers": 1},
    )


def step_publish_production():
    log = logging.getLogger("automation")
    resp = SESSION.post(
        f"{BASE_URL}/api/content-publish",
        params={"environment": "production", "content_type": "all"},
        timeout=30,
    )
    resp.raise_for_status()
    task_id = resp.json().get("task_id")
    log.info(f"  Publish started, task_id={task_id}")
    result = poll_task(f"{BASE_URL}/api/content-publish/status/{task_id}", PUBLISH_TIMEOUT)

    pub_result = result.get("result", {})
    if pub_result.get("success"):
        log.info(f"  Published {pub_result.get('total_urls', 0)} URLs to production")
    else:
        raise RuntimeError(f"Publish did not succeed: {pub_result}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log = setup_logging()
    log.info("=" * 60)
    log.info(f"Daily automation started at {datetime.now():%Y-%m-%d %H:%M:%S}")
    log.info("=" * 60)

    login()

    steps = [
        ("Reset FAQ validation",          step_reset_faq_validation),
        ("Reset Kopteksten validation",    step_reset_kopteksten_validation),
        ("Validate links (parallel)",      step_validate_parallel),
        ("Process FAQ URLs",               step_process_faq_urls),
        ("Process Kopteksten URLs",        step_process_kopteksten_urls),
        ("Publish to Production",          step_publish_production),
    ]

    for step_name, step_func in steps:
        log.info(f"--- Starting: {step_name} ---")
        try:
            step_func()
            log.info(f"--- Completed: {step_name} ---")
        except Exception as e:
            log.error(f"--- FAILED: {step_name} --- Error: {e}", exc_info=True)
            sys.exit(1)

    log.info("=" * 60)
    log.info("Daily automation completed successfully")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
