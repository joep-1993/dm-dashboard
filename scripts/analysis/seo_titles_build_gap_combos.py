#!/usr/bin/env python3
"""Create blueprints for uncovered (cat, facet-combo) gaps that actually get traffic.

Input is the CSV from a gap run — by default seo_titles_gap_from_query.csv
(seo_titles_gap_from_query.py: SEO traffic 2025-01-01.., faceted /c/ pages with a
" - " page_heading, visits > 3, `winkel` combos excluded). It also reads the older
seo_titles_gap_traffic.py output; both carry the same columns.

--min-visits defaults to 0: the gap CSV has already applied its own traffic floor, so
a second cut here would silently drop rows the caller thinks it is building. Pass a
value when working from an unfiltered gap list (the 2026-07-30 365-day run had 40.156
buildable gaps, of which >= 6 visits/yr kept 1.789 combos carrying 21% of the traffic;
building all 40k to chase 73k visits/yr and EUR 5.419 was not defensible).

Rows land as status='built', never pushed: they show up in the SEO-titles tool for
review, and Publish stays a deliberate click.

Each row is stored WITH its sample_url as source_url, so the Facets column in Built
titles links to a live example — unlike the synthesised top-5 combos, which have no
source URL by construction.

Guards, all re-evaluated live rather than trusted from the CSV: combos already held
locally (pa.seo_titles_blueprints) are skipped, so are combos the LIVE /page-titles
store already has (store_has_combos — authoritative; --skip-store trades that check
for speed), and impossible_reason() is re-run against pa.facet_dependencies so a
dependency added since the CSV was written still blocks the build.

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
    build_blueprint, canon_key, impossible_reason, load_facet_deps,
    load_local_combos, load_rules, store_has_combos, _upsert_blueprint,
)

SRC = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
       "seo_titles_gap_from_query.csv")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=SRC)
    ap.add_argument("--min-visits", type=int, default=0)
    ap.add_argument("--skip-store", action="store_true",
                    help="do not re-ask the live /page-titles store")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    # utf-8-sig: the gap CSV is written with a BOM for Excel, which would otherwise
    # land in the first header name and break the r["cat_id"] lookup.
    rows = [r for r in csv.DictReader(open(args.csv, encoding="utf-8-sig"))
            if r["verdict"] == "buildable" and int(r["seo_visits"]) >= args.min_visits]
    rows.sort(key=lambda r: -int(r["seo_visits"]))
    print(f"[1/3] candidates at >= {args.min_visits} visits: {len(rows):,} "
          f"({sum(int(r['seo_visits']) for r in rows):,} visits) from {args.csv}")

    rules = load_rules()
    deps = load_facet_deps()
    existing = load_local_combos(force=True)
    print(f"      rules {len(rules):,} · dependencies {len(deps):,} · "
          f"held locally {len(existing):,}")
    if not args.skip_store:
        cand = [(int(r["cat_id"]), canon_key(r["canon_key"])) for r in rows]
        in_store = store_has_combos(cand)
        print(f"      live store already holds {len(in_store):,} of the {len(cand):,}")
        existing = existing | in_store

    todo, skipped = [], Counter()
    for r in rows:
        cat_id = int(r["cat_id"])
        types = [t for t in r["canon_key"].split("~") if t]
        if not types:
            skipped["empty key"] += 1
            continue
        if (cat_id, canon_key("~".join(types))) in existing:
            skipped["already covered (local or live store)"] += 1
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
