#!/usr/bin/env python3
"""Create blueprints for uncovered (cat, facet-combo) gaps that actually get traffic.

Input is seo_titles_gap_traffic_365d.csv from seo_titles_gap_traffic.py. The default
cut is >= 6 SEO visits/year, which over the 2025-07-30..2026-07-30 window is 1.789 of
the 40.156 buildable gaps but 21% of their traffic — 4,5% of the combos for a fifth of
the visits. Building all 40k would grow the blueprint set ~53% to chase 73k visits/yr
and EUR 5.419 total, which is not defensible; not a single uncovered combo reaches one
visit per WEEK.

Rows land as status='built', never pushed: they show up in the SEO-titles tool for
review, and Publish stays a deliberate click.

Each row is stored WITH its sample_url as source_url, so the Facets column in Built
titles links to a live example — unlike the synthesised top-5 combos, which have no
source URL by construction.

Guards: combos already covered are skipped (re-checked live, not trusted from the CSV),
and impossible_reason() is re-evaluated against pa.facet_dependencies so a dependency
added since the CSV was written still blocks the build.

Usage:
    venv/bin/python scripts/analysis/seo_titles_build_gap_combos.py            # dry run
    venv/bin/python scripts/analysis/seo_titles_build_gap_combos.py --apply
    venv/bin/python scripts/analysis/seo_titles_build_gap_combos.py --min-visits 12 --apply
"""
import argparse
import csv
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backend.database import get_db_connection, return_db_connection  # noqa: E402
from backend.seo_titles_service import (  # noqa: E402
    build_blueprint, canon_key, impossible_reason, load_existing_combos,
    load_facet_deps, load_rules, _upsert_blueprint,
)

SRC = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
       "seo_titles_gap_traffic_365d.csv")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=SRC)
    ap.add_argument("--min-visits", type=int, default=6)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    rows = [r for r in csv.DictReader(open(args.csv, encoding="utf-8"))
            if r["verdict"] == "buildable" and int(r["seo_visits"]) >= args.min_visits]
    rows.sort(key=lambda r: -int(r["seo_visits"]))
    print(f"[1/3] candidates at >= {args.min_visits} visits/yr: {len(rows):,} "
          f"({sum(int(r['seo_visits']) for r in rows):,} visits)")

    rules = load_rules()
    deps = load_facet_deps()
    existing = load_existing_combos(force=True)
    print(f"      rules {len(rules):,} · dependencies {len(deps):,} · "
          f"already covered {len(existing):,}")

    todo, skipped = [], Counter()
    for r in rows:
        cat_id = int(r["cat_id"])
        types = [t for t in r["canon_key"].split("~") if t]
        if not types:
            skipped["empty key"] += 1
            continue
        if (cat_id, canon_key("~".join(types))) in existing:
            skipped["covered since the csv was written"] += 1
            continue
        bad = impossible_reason(types, deps)
        if bad:
            skipped[f"impossible ({bad})"] += 1
            continue
        todo.append((cat_id, r["cat_name"], types, r))

    print(f"[2/3] buildable now: {len(todo):,}")
    for k, v in skipped.most_common(6):
        print(f"      skipped {v:5}  {k}")

    print("\n      sample of what would be created:")
    for cat_id, cat_name, types, r in todo[:8]:
        bp = build_blueprint(cat_id, cat_name or "", types, rules)
        print(f"        {int(r['seo_visits']):4}v  {cat_id} {(cat_name or '')[:20]:20} "
              f"h1={bp['h1_title'][:60]}")

    if not args.apply:
        print("\n[3/3] dry run — pass --apply to create them as status='built'")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    made = 0
    try:
        for cat_id, cat_name, types, r in todo:
            bp = build_blueprint(cat_id, cat_name or "", types, rules)
            # source_url = a real example URL for this combo, so Built titles can link
            # it. visits/revenue come from the same 365-day measurement.
            _upsert_blueprint(cur, bp, r.get("sample_url") or None,
                              int(r["seo_visits"]), float(r["seo_revenue"] or 0))
            made += 1
            if made % 250 == 0:
                conn.commit()
                print(f"      {made:,}/{len(todo):,}")
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)
    print(f"[3/3] created/refreshed {made:,} blueprints as status='built'")
    print("\n      undo (nothing else has this exact combination of source_url + visits):")
    print("        DELETE FROM pa.seo_titles_blueprints")
    print(f"        WHERE status='built' AND visits >= {args.min_visits}")
    print("          AND source_url LIKE 'https://www.beslist.nl/%' ;   -- review first")


if __name__ == "__main__":
    main()
