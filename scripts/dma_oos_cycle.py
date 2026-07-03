"""DMA Exclusions – automated OOS cycle.

Runs for both NL and BE:
  1. Scan OOS products live in DMA
  2. Exclude all headline-match candidates that aren't already excluded
  3. Re-enable exclusions whose product has recovered (back in stock)

Sends a Slack summary via SLACK_BOT_TOKEN / SLACK_USER_ID (same as
daily_automation.py). Designed to be called by Windows Task Scheduler
every 6 hours.
"""
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import requests

# Load .env from project root (same as the dashboard itself)
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

_base_url = os.getenv("BASE_URL", "https://localhost:3003").rstrip("/")
BASE = f"{_base_url}/api/dma-exclusions"
VERIFY_SSL = os.getenv("DISABLE_SSL_VERIFY", "").lower() != "true"
MARKETS = ["NL", "BE"]
TIMEOUT_SCAN = 600      # scan can be slow (GA + OOS monitor)
TIMEOUT_EXCLUDE = 1800  # bulk exclude is sequential per item (~3s each)
TIMEOUT_REENABLE = 300

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dma_oos_cycle")


def _get(path: str, params: dict | None = None, timeout: int = 120) -> dict:
    r = requests.get(f"{BASE}{path}", params=params, timeout=timeout,
                     verify=VERIFY_SSL)
    r.raise_for_status()
    return r.json()


def _post(path: str, params: dict | None = None,
          json_body: dict | None = None, timeout: int = 120) -> dict:
    r = requests.post(f"{BASE}{path}", params=params, json=json_body,
                      timeout=timeout, verify=VERIFY_SSL)
    r.raise_for_status()
    return r.json()


def run_market(market: str) -> dict:
    """Full OOS cycle for one market. Returns a summary dict."""
    summary: dict = {"market": market, "ts": datetime.now().isoformat()}

    # ── 1. Scan ──────────────────────────────────────────────────────────
    log.info("[%s] Scanning OOS products…", market)
    scan = _get("/oos/scan", {"market": market}, timeout=TIMEOUT_SCAN)
    candidates = scan.get("candidates", [])
    log.info("[%s] Scan done – %d live in DMA, %d headline matches",
             market, scan["live_in_dma"],
             scan["headline_counts"]["match"])
    summary["scan"] = {
        "oos_total": scan["oos_total"],
        "live_in_dma": scan["live_in_dma"],
        "headline_matches": scan["headline_counts"]["match"],
    }

    # ── 2. Exclude headline matches that aren't already excluded ─────────
    to_exclude = [c["item_id"] for c in candidates
                  if c.get("headline_match") and not c.get("already_excluded")]
    log.info("[%s] Excluding %d OOS items…", market, len(to_exclude))
    if to_exclude:
        exc = _post("/oos/exclude",
                     json_body={"market": market, "item_ids": to_exclude},
                     timeout=TIMEOUT_EXCLUDE)
        summary["exclude"] = {
            "sent": len(to_exclude),
            "processed": exc["processed"],
            "skipped": exc["skipped"],
        }
        log.info("[%s] Excluded %d (skipped %d)",
                 market, exc["processed"] - exc["skipped"], exc["skipped"])
    else:
        summary["exclude"] = {"sent": 0, "processed": 0, "skipped": 0}
        log.info("[%s] Nothing to exclude", market)

    # ── 3. Re-enable recovered ───────────────────────────────────────────
    log.info("[%s] Re-enabling recovered exclusions…", market)
    reen = _post("/oos/reenable", {"market": market}, timeout=TIMEOUT_REENABLE)
    summary["reenable"] = {"recovered": reen["recovered"]}
    log.info("[%s] Re-enabled %d recovered exclusions", market, reen["recovered"])

    return summary


def _send_slack(text: str) -> None:
    """Send a DM via Slack Bot Token (same pattern as daily_automation.py)."""
    token = os.getenv("SLACK_BOT_TOKEN", "")
    user_id = os.getenv("SLACK_USER_ID", "")
    if not token or not user_id:
        log.warning("SLACK_BOT_TOKEN or SLACK_USER_ID not set, skipping Slack notification")
        return
    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}",
                     "Content-Type": "application/json"},
            json={"channel": user_id, "text": text},
            timeout=15,
        )
        data = resp.json()
        if data.get("ok"):
            log.info("Slack notification sent")
        else:
            log.warning("Slack API error: %s", data.get("error"))
    except Exception as e:
        log.warning("Failed to send Slack notification: %s", e)


def _format_slack(results: list[dict], duration: str, ok: bool) -> str:
    """Build a readable Slack summary message."""
    icon = ":white_check_mark:" if ok else ":x:"
    status = "Complete" if ok else "Failed"
    lines = [f"{icon} *DMA OOS Cycle — {status}*  ({duration})"]

    for r in results:
        mkt = r["market"]
        if r.get("error"):
            lines.append(f"\n:x: *{mkt}* — error (check logs)")
            continue

        scan = r["scan"]
        exc = r["exclude"]
        reen = r["reenable"]
        excluded_count = exc["processed"] - exc["skipped"]

        lines.append(f"\n*{mkt}*")
        lines.append(f"  Scan: {scan['live_in_dma']} live in DMA "
                     f"({scan['headline_matches']} headline matches, "
                     f"{scan['oos_total']} OOS total)")
        if exc["sent"] > 0:
            lines.append(f"  Excluded: {excluded_count} new"
                         + (f" ({exc['skipped']} skipped)" if exc["skipped"] else ""))
        else:
            lines.append("  Excluded: 0 (nothing new)")
        lines.append(f"  Re-enabled: {reen['recovered']} recovered")

    return "\n".join(lines)


def main() -> None:
    log.info("═══ DMA OOS cycle started ═══")
    start = datetime.now()
    results = []
    ok = True
    for mkt in MARKETS:
        try:
            results.append(run_market(mkt))
        except Exception:
            log.exception("Market %s FAILED", mkt)
            results.append({"market": mkt, "error": True})
            ok = False

    duration = str(datetime.now() - start).split(".")[0]
    log.info("═══ DMA OOS cycle finished ═══")
    log.info("Summary:\n%s", json.dumps(results, indent=2))

    _send_slack(_format_slack(results, duration, ok))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
