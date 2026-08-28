#!/usr/bin/env python3
"""Daily entry point for Facet Watch — pull yesterday's taxonomy audit events.

Register in the dashboard's Task Scheduler (or Windows schtasks directly) as a
DAILY task. The Taxonomy API's own downstream jobs land early (FacetCoverage
~04:05, the Redshift taxonomy tables ~02:03), but this reads the audit log, which
is seconds-fresh — so the time of day only decides which events land in which
run, not whether they are seen at all.

Idempotent: pa.facet_watch_events is keyed on the audit log's own id, and the
default window reaches one day back past the newest stored event. Running it twice,
or running it after a missed day, is safe and cannot double-count.

Usage:
    venv/bin/python scripts/facet_watch_ingest.py                    # daily default
    venv/bin/python scripts/facet_watch_ingest.py --from 2026-08-01  # backfill
    venv/bin/python scripts/facet_watch_ingest.py --seed-values      # refresh the
                                                                     # value->facet cache
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend import facet_watch_service as fw  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="date_from", default=None, help="YYYY-MM-DD")
    ap.add_argument("--to", dest="date_to", default=None, help="YYYY-MM-DD")
    ap.add_argument("--seed-values", action="store_true",
                    help="refresh value->facet from the full /api/Facets/values dump "
                         "(555k rows, ~146 MB, ~60 s) before ingesting")
    ap.add_argument("--skip-maincats", action="store_true")
    args = ap.parse_args()

    fw.init_tables()
    if not args.skip_maincats:
        print("[1/3] main categories:", fw.refresh_maincats())
    if args.seed_values:
        print("[2/3] seeding value->facet map (this is the 146 MB call) ...")
        print("      ", fw.seed_value_facet_map(
            progress=lambda d, t: print(f"        {d:,}/{t:,}", flush=True)))
    else:
        print("[2/3] value->facet seed skipped (pass --seed-values to refresh)")

    print(f"[3/3] ingest {args.date_from or '(default: 1 day before newest stored)'}"
          f" .. {args.date_to or 'now'}")
    res = fw.ingest(args.date_from, args.date_to)
    print("      ", res)

    st = fw.get_status()
    print(f"\nstore: {st['events']:,} events "
          f"({st['resolved']:,} attributed, {st['unattributed']:,} not) · "
          f"{st['oldest']} .. {st['newest']}")
    print(f"caches: {st['value_facet_cache']:,} values · "
          f"{st['facet_maincat_cache']:,} facets · {st['main_categories']} main categories")
    return 0 if res.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())
