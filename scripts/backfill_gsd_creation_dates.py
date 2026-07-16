"""
One-off backfill of GSD campaign creation dates from a spreadsheet.

The Campaigns-created "Date" column reads pa.jvs_gsd_campaign_created, keyed by
(shop_id, country). This seeds that table from an Excel export that has the real
per-shop creation dates (columns: datum, shop_id, shop_name, Country).

Match key is (shop_id, country); the EARLIEST date per key is kept. Insert is
ON CONFLICT DO NOTHING, so run this on the fresh/empty table FIRST (before any
create-time logging) to make the spreadsheet dates authoritative.

Usage (from the dm-tools project root, with the project venv):
    ./venv/bin/python scripts/backfill_gsd_creation_dates.py [--dry-run] \
        [--path /mnt/c/Users/JoepvanSchagen/Downloads/claude/gsd_campaigns_creation_dates.xlsx]
"""
import argparse
import os
import sys

import pandas as pd

# Make `backend` importable when run from the project root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.gsd_campaigns_service import (  # noqa: E402
    ensure_campaign_created_table,
    upsert_created_dates,
)

DEFAULT_PATH = "/mnt/c/Users/JoepvanSchagen/Downloads/claude/gsd_campaigns_creation_dates.xlsx"


def build_rows(path: str):
    """Read the Excel and collapse to one (shop_id, country, earliest_date, shop_name) row per shop."""
    df = pd.read_excel(path)
    # Tolerate minor header variations.
    cols = {c.lower().strip(): c for c in df.columns}
    date_col = cols.get("datum") or cols.get("date") or cols.get("created_date")
    shop_id_col = cols.get("shop_id") or cols.get("shop id")
    country_col = cols.get("country") or cols.get("land")
    name_col = cols.get("shop_name") or cols.get("shopname") or cols.get("shop name")
    if not (date_col and shop_id_col and country_col):
        raise SystemExit(f"Missing required columns. Found: {list(df.columns)}")

    by_shop = {}  # (shop_id, country) -> (shop_id, country, date_str, shop_name)
    skipped = 0
    for _, r in df.iterrows():
        try:
            shop_id = int(r[shop_id_col])
        except (TypeError, ValueError):
            skipped += 1
            continue
        country = str(r[country_col]).strip().upper()
        if not country or country == "NAN":
            skipped += 1
            continue
        d = pd.to_datetime(r[date_col], errors="coerce")
        if pd.isna(d):
            skipped += 1
            continue
        date_str = d.strftime("%Y-%m-%d")
        shop_name = str(r[name_col]).strip() if name_col and not pd.isna(r[name_col]) else None
        key = (shop_id, country)
        if key not in by_shop or date_str < by_shop[key][2]:
            by_shop[key] = (shop_id, country, date_str, shop_name)
    return list(by_shop.values()), skipped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default=DEFAULT_PATH)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    rows, skipped = build_rows(args.path)
    print(f"Parsed {len(rows)} unique (shop_id, country) rows ({skipped} skipped).")
    for r in rows[:10]:
        print("   ", r)
    if len(rows) > 10:
        print(f"    ... and {len(rows) - 10} more")

    if args.dry_run:
        print("Dry run — nothing written.")
        return

    ensure_campaign_created_table()
    res = upsert_created_dates(rows)
    print(f"Inserted {res['inserted']} new rows (existing (shop, country) rows left untouched).")
    if res.get("error"):
        print("ERROR:", res["error"])
        sys.exit(1)


if __name__ == "__main__":
    main()
