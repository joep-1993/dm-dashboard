"""
Revert DMA bidding changes from two live runs (12:29 and 12:43 on 2026-04-07).
Uses the dashboard API (already running service) to apply changes.
"""
import json
import os
import requests
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://localhost:3003"
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

SESSION = requests.Session()
SESSION.verify = False


def login():
    password = os.getenv("DASHBOARD_PASSWORD", "")
    resp = SESSION.post(f"{BASE_URL}/login", data={"password": password}, allow_redirects=False)
    if resp.status_code in (200, 302, 303, 307):
        print("Authenticated")
    else:
        raise RuntimeError(f"Login failed: {resp.status_code}")


def main():
    login()

    # Load both run results
    base = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(base, "tmp_run_1229.json")) as f:
        run1 = json.load(f)
    with open(os.path.join(base, "tmp_run_1243.json")) as f:
        run2 = json.load(f)

    # Build revert map: campaign_name -> original_level
    # Run 12:29 has the TRUE originals
    revert_map = {}
    for key, changes in run1["changes"].items():
        if key == "stuck_l1":
            continue
        for c in changes:
            revert_map[c["campaign_name"]] = c["current_level"]

    print(f"Run 12:29: {len(revert_map)} campaigns to revert")

    # Add run 12:43 (only if not already tracked from run 12:29)
    run2_count = 0
    for key, changes in run2["changes"].items():
        if key == "stuck_l1":
            continue
        for c in changes:
            if c["campaign_name"] not in revert_map:
                revert_map[c["campaign_name"]] = c["current_level"]
                run2_count += 1

    print(f"Run 12:43: {run2_count} additional campaigns to revert")
    print(f"Total: {len(revert_map)} campaigns to revert\n")

    # Group by target level
    by_level = {}
    for name, lvl in revert_map.items():
        by_level.setdefault(lvl, []).append(name)

    for lvl in sorted(by_level):
        print(f"  -> Level {lvl}: {len(by_level[lvl])} campaigns")

    # Use the DMA bidding run endpoint with include_campaigns for each level
    # We run separate passes - one per target level - using include filter
    # But the run endpoint applies rules, not manual overrides...
    # Instead, we need a direct revert endpoint or individual campaign changes.
    # Let's call a custom revert via the API.

    # Build the revert payload and POST it
    revert_list = [{"campaign_name": name, "target_level": lvl} for name, lvl in revert_map.items()]

    print(f"\nSending revert request for {len(revert_list)} campaigns...")
    resp = SESSION.post(
        f"{BASE_URL}/api/dma-bidding/revert",
        json=revert_list,
        timeout=300,
    )
    if resp.ok:
        result = resp.json()
        print(f"Success: {result.get('success', 0)}, Failed: {result.get('failed', 0)}")
    else:
        print(f"Error: {resp.status_code} - {resp.text}")


if __name__ == "__main__":
    main()
