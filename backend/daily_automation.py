"""
Daily automation script for DM Tools / DM Dashboard.

Flow:
  1. Cancel stale tasks from previous runs
  2. Reset FAQ + Kopteksten validation
  3. Validate FAQ + Kopteksten links (parallel)
  4. Recheck skipped URLs (makes URLs with new products eligible again)
  5. Regenerate FAQ + Kopteksten content (parallel)
  6. Publish all content to production

Can be triggered manually, via cron (Linux), or Windows Task Scheduler.

Environment:
  BASE_URL                 — dashboard URL (default http://localhost:8003)
  DASHBOARD_PASSWORD       — if set, logs in before running automation
  DISABLE_SSL_VERIFY=true  — skip cert verification (for self-signed HTTPS)
  SLACK_BOT_TOKEN / SLACK_USER_ID — for completion/failure notifications
"""
import sys
import os
import time
import socket
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Safety net: no socket operation should ever block longer than 2 minutes.
# This catches edge cases where the requests timeout parameter doesn't fire
# (e.g. SSL handshake hang on Windows).
socket.setdefaulttimeout(120)

# Load .env from project root so this script can be run standalone (e.g. Task Scheduler)
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass  # dotenv is optional; running inside the app already has env loaded

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = os.getenv("BASE_URL", "http://localhost:8003")
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
LOG_FILE = os.path.join(LOG_DIR, "daily_automation.log")

POLL_INTERVAL = 15            # seconds between status polls
VALIDATION_TIMEOUT = float("inf")  # no timeout — validation runs until done
PROCESS_TIMEOUT = 28800       # 8 hours max for processing loops
PROCESS_MAX_RETRIES = 3       # retry timed-out processing steps up to N times
PUBLISH_TIMEOUT = 3600        # 1 hour max for publish
POLL_MAX_ERRORS = 10          # consecutive poll failures before aborting a task
POLL_MAX_RESTARTS = 3         # max task restarts after detected server restart
CONNECT_TIMEOUT = 10          # seconds to wait for TCP connect
READ_TIMEOUT = 60             # seconds to wait for HTTP response body
LONG_READ_TIMEOUT = 300       # seconds for slow endpoints (process-urls)

# Reusable session — SSL verify can be disabled for self-signed certs
SESSION = requests.Session()
if os.getenv("DISABLE_SSL_VERIFY", "").lower() == "true":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    SESSION.verify = False

# Retry adapter: automatically retry on connection errors and 502/503/504
_retry = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
SESSION.mount("http://", HTTPAdapter(max_retries=_retry))
SESSION.mount("https://", HTTPAdapter(max_retries=_retry))


def login_if_configured():
    """Authenticate with the dashboard using DASHBOARD_PASSWORD if set."""
    log = logging.getLogger("automation")
    password = os.getenv("DASHBOARD_PASSWORD", "")
    if not password:
        return  # no auth configured — local/dev mode
    resp = SESSION.post(f"{BASE_URL}/login", data={"password": password}, allow_redirects=False)
    if resp.status_code in (200, 302, 303, 307):
        log.info("Authenticated with dashboard")
    else:
        raise RuntimeError(f"Login failed with status {resp.status_code}")

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
# Helpers
# ---------------------------------------------------------------------------
def cancel_running_tasks():
    """Cancel any stale validation tasks left over from previous runs.

    The server-side validation endpoints run as daemon threads.  If a previous
    automation run timed out, those threads keep running and will compete with
    new tasks for ES / DB resources.  We cancel them before starting fresh.
    """
    log = logging.getLogger("automation")

    cancel_endpoints = [
        "/api/faq/validate-all-links/cancel",
        "/api/validate-all-links/cancel",
        "/api/recheck-skipped-urls/cancel",
    ]

    for endpoint in cancel_endpoints:
        try:
            resp = SESSION.post(f"{BASE_URL}{endpoint}/all", timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            if resp.status_code == 200:
                data = resp.json()
                cancelled = data.get("cancelled", 0)
                if cancelled:
                    log.info(f"  Cancelled {cancelled} stale task(s) via {endpoint}")
        except Exception:
            pass  # best-effort


def _reauth_on_401(resp):
    """Re-authenticate when the server returns 401 (e.g. after a restart)."""
    if resp.status_code != 401:
        return False
    log = logging.getLogger("automation")
    log.info("  Got 401 — re-authenticating…")
    try:
        login_if_configured()
        return True
    except Exception as e:
        log.warning(f"  Re-authentication failed: {e}")
        return False


def poll_task(status_url, timeout, restart_fn=None):
    """Poll a background task until completed/failed.

    Tolerates up to POLL_MAX_ERRORS consecutive connection/timeout failures
    before aborting — a single flaky request no longer kills the whole run.

    If *restart_fn* is provided and a server restart is detected (404 after
    at least one successful poll), the function calls restart_fn() to start a
    new task and continues polling with the returned status URL.
    """
    log = logging.getLogger("automation")
    start = time.time()
    consecutive_errors = 0
    had_success = False           # True once at least one poll returned OK
    restarts = 0
    while time.time() - start < timeout:
        try:
            resp = SESSION.get(status_url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            if resp.status_code == 401 and _reauth_on_401(resp):
                resp = SESSION.get(status_url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

            # Detect server restart: 404 after we previously polled OK
            if (resp.status_code == 404 and had_success
                    and restart_fn and restarts < POLL_MAX_RESTARTS):
                restarts += 1
                log.warning(
                    f"  Server restart detected (404 after successful poll)"
                    f" — restarting task ({restarts}/{POLL_MAX_RESTARTS})"
                )
                status_url = restart_fn()
                consecutive_errors = 0
                had_success = False
                time.sleep(POLL_INTERVAL)
                continue

            resp.raise_for_status()
            data = resp.json()
            consecutive_errors = 0  # reset on success
            had_success = True
        except Exception as e:
            consecutive_errors += 1
            log.warning(f"  Poll error ({consecutive_errors}/{POLL_MAX_ERRORS}): {e}")
            if consecutive_errors >= POLL_MAX_ERRORS:
                raise RuntimeError(
                    f"Task polling failed after {POLL_MAX_ERRORS} consecutive errors, last: {e}"
                )
            time.sleep(POLL_INTERVAL)
            continue

        status = data.get("status", "")

        if status == "completed":
            log.info(f"  Task completed")
            return data
        if status in ("error", "failed"):
            raise RuntimeError(f"Task failed: {data}")

        # Log progress — support both validation response shapes
        validated = data.get("validated", data.get("rechecked", ""))
        total = data.get("total_to_validate", data.get("total_to_recheck", data.get("total", "")))
        log.info(f"  Polling… status={status}  progress={validated}/{total}")
        time.sleep(POLL_INTERVAL)

    raise TimeoutError(f"Task timed out after {timeout}s")


def loop_until_done(url, timeout, params=None):
    """Call an endpoint repeatedly until no URLs left to process.

    Returns a dict with 'total_processed' and 'timed_out' flag.
    Does NOT raise on timeout — partial progress is still useful.
    Tolerates up to POLL_MAX_ERRORS consecutive connection failures.
    """
    log = logging.getLogger("automation")
    start = time.time()
    iteration = 0
    total_processed = 0
    consecutive_errors = 0
    while time.time() - start < timeout:
        iteration += 1
        try:
            resp = SESSION.post(url, params=params, timeout=(CONNECT_TIMEOUT, LONG_READ_TIMEOUT))
            if resp.status_code == 401 and _reauth_on_401(resp):
                resp = SESSION.post(url, params=params, timeout=(CONNECT_TIMEOUT, LONG_READ_TIMEOUT))
            resp.raise_for_status()
            data = resp.json()
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            log.warning(f"  Loop error ({consecutive_errors}/{POLL_MAX_ERRORS}): {e}")
            if consecutive_errors >= POLL_MAX_ERRORS:
                raise RuntimeError(
                    f"Processing loop failed after {POLL_MAX_ERRORS} consecutive errors, last: {e}"
                )
            time.sleep(POLL_INTERVAL)
            continue

        if data.get("status") == "complete" or data.get("message") == "No URLs to process":
            log.info(f"  Done after {iteration} iterations, total processed: {total_processed}")
            return {"total_processed": total_processed, "timed_out": False}

        processed = data.get("processed", 0)
        total_processed += processed
        log.info(f"  Iteration {iteration}: processed={processed} (total so far: {total_processed})")
        time.sleep(2)

    log.warning(f"  Timeout after {timeout}s — processed {total_processed} URLs before timeout")
    return {"total_processed": total_processed, "timed_out": True}

# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------
def step_reset_faq_validation():
    log = logging.getLogger("automation")
    resp = SESSION.delete(f"{BASE_URL}/api/faq/validation-history/reset", timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    resp.raise_for_status()
    data = resp.json()
    log.info(f"  FAQ validation reset — cleared: {data.get('cleared_count', data.get('deleted', 0))}")


def step_reset_kopteksten_validation():
    log = logging.getLogger("automation")
    resp = SESSION.delete(f"{BASE_URL}/api/validation-history/reset", timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    resp.raise_for_status()
    data = resp.json()
    log.info(f"  Kopteksten validation reset — cleared: {data.get('cleared_count', 0)}")


def step_validate_faq_links():
    log = logging.getLogger("automation")

    def _start():
        resp = SESSION.post(
            f"{BASE_URL}/api/faq/validate-all-links",
            params={"parallel_workers": 20, "batch_size": 500},
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )
        if resp.status_code == 401 and _reauth_on_401(resp):
            resp = SESSION.post(
                f"{BASE_URL}/api/faq/validate-all-links",
                params={"parallel_workers": 20, "batch_size": 500},
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
        resp.raise_for_status()
        task_id = resp.json().get("task_id")
        log.info(f"  FAQ validate-all started, task_id={task_id}")
        return f"{BASE_URL}/api/faq/validate-all-links/status/{task_id}"

    status_url = _start()
    poll_task(status_url, VALIDATION_TIMEOUT, restart_fn=_start)


def step_validate_kopteksten_links():
    log = logging.getLogger("automation")

    def _start():
        resp = SESSION.post(
            f"{BASE_URL}/api/validate-all-links",
            params={"parallel_workers": 20, "batch_size": 500},
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )
        if resp.status_code == 401 and _reauth_on_401(resp):
            resp = SESSION.post(
                f"{BASE_URL}/api/validate-all-links",
                params={"parallel_workers": 20, "batch_size": 500},
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
        resp.raise_for_status()
        task_id = resp.json().get("task_id")
        log.info(f"  Kopteksten validate-all started, task_id={task_id}")
        return f"{BASE_URL}/api/validate-all-links/status/{task_id}"

    status_url = _start()
    poll_task(status_url, VALIDATION_TIMEOUT, restart_fn=_start)


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
            log.info(f"  {name} completed")


def step_recheck_skipped_urls():
    """Recheck URLs that were previously skipped (no products).

    URLs that now have products are removed from the skip-list and added
    back to the werkvoorraad for both FAQ and Kopteksten regeneration.
    """
    log = logging.getLogger("automation")
    SESSION.delete(f"{BASE_URL}/api/recheck-skipped-urls/reset", timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

    def _start():
        resp = SESSION.post(
            f"{BASE_URL}/api/recheck-skipped-urls",
            params={"parallel_workers": 20, "batch_size": 50},
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )
        if resp.status_code == 401 and _reauth_on_401(resp):
            resp = SESSION.post(
                f"{BASE_URL}/api/recheck-skipped-urls",
                params={"parallel_workers": 20, "batch_size": 50},
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
        resp.raise_for_status()
        task_id = resp.json().get("task_id")
        log.info(f"  Recheck skipped URLs started, task_id={task_id}")
        return f"{BASE_URL}/api/recheck-skipped-urls/status/{task_id}"

    status_url = _start()
    poll_task(status_url, VALIDATION_TIMEOUT, restart_fn=_start)


PROCESS_WORKERS = 20          # parallel workers for content generation


def step_process_faq_urls():
    return loop_until_done(
        f"{BASE_URL}/api/faq/process-urls",
        PROCESS_TIMEOUT,
        params={"batch_size": 200, "parallel_workers": PROCESS_WORKERS, "num_faqs": 6},
    )


def step_process_kopteksten_urls():
    return loop_until_done(
        f"{BASE_URL}/api/process-urls",
        PROCESS_TIMEOUT,
        params={"batch_size": 200, "parallel_workers": PROCESS_WORKERS},
    )


def step_process_parallel():
    """Regenerate FAQ and Kopteksten content in parallel.

    Returns a summary dict. Does NOT raise on timeout — partial progress
    is preserved in the DB and will be published in the next step.
    """
    log = logging.getLogger("automation")
    log.info("  Starting FAQ + Kopteksten processing in parallel")

    results = {}
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(step_process_faq_urls): "FAQ",
            executor.submit(step_process_kopteksten_urls): "Kopteksten",
        }
        for future in as_completed(futures):
            name = futures[future]
            result = future.result()  # raises on network/server errors (not timeout)
            results[name] = result
            if result["timed_out"]:
                log.warning(f"  ⚠ {name} timed out after processing {result['total_processed']} URLs")
            else:
                log.info(f"  ✓ {name} completed — {result['total_processed']} URLs processed")

    for attempt in range(1, PROCESS_MAX_RETRIES + 1):
        timed_out_names = [n for n, r in results.items() if r["timed_out"]]
        if not timed_out_names:
            break
        log.info(f"  Retry {attempt}/{PROCESS_MAX_RETRIES} for: {', '.join(timed_out_names)}")
        with ThreadPoolExecutor(max_workers=2) as executor:
            retry_futures = {}
            if results.get("FAQ", {}).get("timed_out"):
                retry_futures[executor.submit(step_process_faq_urls)] = "FAQ"
            if results.get("Kopteksten", {}).get("timed_out"):
                retry_futures[executor.submit(step_process_kopteksten_urls)] = "Kopteksten"
            for future in as_completed(retry_futures):
                name = retry_futures[future]
                result = future.result()
                extra = result["total_processed"]
                results[name]["total_processed"] += extra
                results[name]["timed_out"] = result["timed_out"]
                if result["timed_out"]:
                    log.warning(f"  ⚠ {name} retry {attempt} timed out (+{extra} URLs)")
                else:
                    log.info(f"  ✓ {name} retry {attempt} completed (+{extra} URLs)")

    return results


def step_publish_production():
    log = logging.getLogger("automation")
    resp = SESSION.post(
        f"{BASE_URL}/api/content-publish",
        params={"environment": "production", "content_type": "all"},
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    )
    resp.raise_for_status()
    task_id = resp.json().get("task_id")
    log.info(f"  Publish started, task_id={task_id}")
    result = poll_task(f"{BASE_URL}/api/content-publish/status/{task_id}", PUBLISH_TIMEOUT)

    pub_result = result.get("result", {})
    if pub_result.get("success"):
        log.info(f"  Published {pub_result.get('total_urls', 0)} URLs to production")
        return pub_result
    else:
        raise RuntimeError(f"Publish did not succeed: {pub_result}")

# ---------------------------------------------------------------------------
# Slack notification
# ---------------------------------------------------------------------------
def send_slack_message(text):
    """Send a DM to the configured Slack user via Bot Token."""
    log = logging.getLogger("automation")
    token = os.getenv("SLACK_BOT_TOKEN", "")
    user_id = os.getenv("SLACK_USER_ID", "")
    if not token or not user_id:
        log.warning("SLACK_BOT_TOKEN or SLACK_USER_ID not set, skipping Slack notification")
        return

    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"channel": user_id, "text": text},
            timeout=15,
        )
        data = resp.json()
        if data.get("ok"):
            log.info("Slack notification sent")
        else:
            log.warning(f"Slack API error: {data.get('error')}")
    except Exception as e:
        log.warning(f"Failed to send Slack notification: {e}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log = setup_logging()
    start_time = datetime.now()
    log.info("=" * 60)
    log.info(f"Daily automation started at {start_time:%Y-%m-%d %H:%M:%S}")
    log.info(f"Target: {BASE_URL}")
    log.info("=" * 60)

    # Authenticate if the dashboard is password-protected
    login_if_configured()

    # Cancel any stale validation tasks from previous failed runs
    log.info("--- Cancelling stale tasks ---")
    cancel_running_tasks()

    # Steps before content generation — these are critical (abort on failure)
    prep_steps = [
        ("Reset FAQ validation",              step_reset_faq_validation),
        ("Reset Kopteksten validation",       step_reset_kopteksten_validation),
        ("Validate links (parallel)",         step_validate_parallel),
        ("Recheck skipped URLs",              step_recheck_skipped_urls),
    ]

    completed_steps = []
    process_results = None
    publish_result = None

    # --- Preparation steps (critical) ---
    for step_name, step_func in prep_steps:
        log.info(f"--- Starting: {step_name} ---")
        try:
            step_func()
            completed_steps.append(step_name)
            log.info(f"--- Completed: {step_name} ---")
        except Exception as e:
            log.error(f"--- FAILED: {step_name} --- Error: {e}", exc_info=True)
            duration = datetime.now() - start_time
            send_slack_message(
                f":x: *DM Tools - Daily Automation Failed*\n"
                f"Failed at: *{step_name}*\n"
                f"Error: {e}\n"
                f"Duration: {str(duration).split('.')[0]}\n"
                f"Completed steps: {', '.join(completed_steps) or 'None'}"
            )
            sys.exit(1)

    # --- Content generation (non-fatal on timeout — always continue to publish) ---
    log.info("--- Starting: Regenerate content (parallel) ---")
    try:
        process_results = step_process_parallel()
        completed_steps.append("Regenerate content (parallel)")

        any_timed_out = any(r["timed_out"] for r in process_results.values())
        if any_timed_out:
            timed_out_names = [n for n, r in process_results.items() if r["timed_out"]]
            log.warning(f"--- Partial: Regenerate content --- {', '.join(timed_out_names)} timed out after retry, continuing to publish")
        else:
            log.info("--- Completed: Regenerate content (parallel) ---")
    except Exception as e:
        log.error(f"--- FAILED: Regenerate content --- Error: {e}", exc_info=True)
        log.info("Continuing to publish — already-generated content is still in the DB")

    # --- Publish (always runs) ---
    log.info("--- Starting: Publish to Production ---")
    try:
        publish_result = step_publish_production()
        completed_steps.append("Publish to Production")
        log.info("--- Completed: Publish to Production ---")
    except Exception as e:
        log.error(f"--- FAILED: Publish to Production --- Error: {e}", exc_info=True)
        duration = datetime.now() - start_time
        send_slack_message(
            f":x: *DM Dashboard - Daily Automation Failed*\n"
            f"Failed at: *Publish to Production*\n"
            f"Error: {e}\n"
            f"Duration: {str(duration).split('.')[0]}\n"
            f"Completed steps: {', '.join(completed_steps) or 'None'}"
        )
        sys.exit(1)

    # --- Final Slack notification ---
    duration = datetime.now() - start_time
    total_urls = publish_result.get("total_urls", 0) if publish_result else "?"
    payload_mb = publish_result.get("payload_size_mb", "?") if publish_result else "?"

    # Build process summary
    process_summary = ""
    if process_results:
        parts = []
        for name, r in process_results.items():
            status = "timed out" if r["timed_out"] else "done"
            parts.append(f"{name}: {r['total_processed']} URLs ({status})")
        process_summary = f"\nGeneration: {', '.join(parts)}"

    any_timed_out = process_results and any(r["timed_out"] for r in process_results.values())
    icon = ":warning:" if any_timed_out else ":white_check_mark:"
    label = "Partial" if any_timed_out else "Complete"

    send_slack_message(
        f"{icon} *DM Tools - Daily Automation {label}*\n"
        f"Published *{total_urls}* URLs to production ({payload_mb} MB)"
        f"{process_summary}\n"
        f"Duration: {str(duration).split('.')[0]}"
    )

    log.info("=" * 60)
    log.info("Daily automation completed successfully")
    log.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except BaseException as e:
        # Catch-all so unexpected failures (e.g. login ConnectionError before the
        # per-step try/except blocks) still produce a Slack notification instead
        # of a silent exit(1) that only shows up as Task Scheduler "Last Result: 1".
        logging.getLogger("automation").error(f"Unhandled exception: {e}", exc_info=True)
        try:
            send_slack_message(
                f":x: *DM Dashboard - Daily Automation Crashed*\n"
                f"Unhandled error: {type(e).__name__}: {e}\n"
                f"Check logs/daily_automation.log for traceback."
            )
        except Exception:
            pass
        sys.exit(1)
