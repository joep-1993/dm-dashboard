"""List facet URL slugs that appear in pa.urls but have no pa.facet_position_rules row.

Read-only. Prints a CSV-ish report to stdout. Use this to audit which facets are
"floating" (no global order, no type-facet decision) so they can be added to the
rules table — either manually or by extending facet_order.xlsx and re-importing.

Usage (from dm-tools repo root, with .env present):
    python scripts/list_unrulled_facets.py
    python scripts/list_unrulled_facets.py --top 50
    python scripts/list_unrulled_facets.py --csv unrulled.csv
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import psycopg2


_URL_FACET_SLUG_RE = re.compile(r"(?:^|~)([a-z][a-z0-9_]*)~[^~]+")


def _dsn() -> str:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        sys.exit("DATABASE_URL not set — load .env from the dm-tools repo root.")
    return dsn


def collect_url_slugs(cur) -> dict:
    """Return {slug: url_count} for every facet slug found in pa.urls."""
    counts: dict = {}
    cur.execute("SELECT url FROM pa.urls WHERE url LIKE '/products/%/c/%'")
    for (url,) in cur.fetchall():
        m = re.search(r"/c/([^/?#]+)", url)
        if not m:
            continue
        for slug in _URL_FACET_SLUG_RE.findall(m.group(1)):
            counts[slug] = counts.get(slug, 0) + 1
    return counts


def rule_slugs(cur) -> set:
    cur.execute("SELECT facet_slug FROM pa.facet_position_rules")
    return {r[0].lower() for r in cur.fetchall()}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--top", type=int, default=0, help="limit output to top N by url_count")
    p.add_argument("--csv", help="write CSV here in addition to stdout")
    args = p.parse_args()

    conn = psycopg2.connect(dsn=_dsn())
    cur = conn.cursor()
    url_counts = collect_url_slugs(cur)
    have = rule_slugs(cur)
    missing = sorted(
        ((slug, n) for slug, n in url_counts.items() if slug not in have),
        key=lambda t: (-t[1], t[0]),
    )
    total_missing = len(missing)
    if args.top:
        missing = missing[: args.top]

    print(f"# Slugs in URLs but NOT in pa.facet_position_rules: {total_missing}"
          + (f" (showing top {len(missing)})" if args.top else ""))
    print("slug,url_count")
    for slug, n in missing:
        print(f"{slug},{n}")

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["slug", "url_count"])
            w.writerows(missing)
        print(f"\nWrote {len(missing)} rows to {args.csv}", file=sys.stderr)


if __name__ == "__main__":
    main()
