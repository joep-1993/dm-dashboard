#!/usr/bin/env python3
"""Rebuild the LEGACY blueprints whose phrase changes because of a position pin.

Context (2026-07-31). `position` (pre_noun / end) became live in the blueprint builder, and
the tool's own corpus was re-pushed the same day. The legacy tblPageTitles rows in
pa.page_titles_existing are excluded from the tool's builds by dedup, so they kept the old
order — e.g. `!!thema_speelgoed!! !!sub_category_lower!!` where the pin says the theme
trails the noun. Joep asked for exactly these rows to be regenerated.

WHAT THIS DOES, AND WHAT IT MEANS
  * selects legacy NL rows that contain a position-pinned facet AND whose rebuilt phrase
    differs from the live one;
  * SKIPS rows whose live phrase contains hand-written words ("Ontwormen
    !!dier_dierenbenodigdheden!!", "… supplementen"). Regenerating deletes editorial text,
    which is a content decision, not a mechanical fix. They are written to a separate CSV.
  * inserts the rebuilt rows into pa.seo_titles_blueprints and publishes them, which
    ADOPTS the combo into the /page-titles store — from then on this tool owns it, not the
    MySQL export. That is the only way to change what is live, since /page-titles is
    POST-upsert on (cat_id, key).

Usage:
    venv/bin/python scripts/analysis/seo_titles_adopt_pin_rows.py                   # dry run
    venv/bin/python scripts/analysis/seo_titles_adopt_pin_rows.py --apply --env staging
    venv/bin/python scripts/analysis/seo_titles_adopt_pin_rows.py --apply --env production
"""
import argparse
import csv
import os
import re
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backend.database import get_db_connection, return_db_connection  # noqa: E402
from backend.seo_titles_service import (  # noqa: E402
    build_blueprint, load_rules, publish_built, _rule, _upsert_blueprint,
)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BACKUP = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
          f"seo_titles_pin_adopt_backup_{STAMP}.csv")
EDITORIAL = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
             f"seo_titles_pin_skipped_editorial_{STAMP}.csv")
PH = re.compile(r'!![a-z0-9_]+!!', re.I)


def pinned_slugs(rules):
    return {slug for slug, r in rules.items() if _rule(rules, slug)[2]}


def literals(phrase):
    """Words that are not placeholders — hand-authored text a rebuild would delete."""
    return [w for w in PH.sub(' ', phrase or '').split() if w]


def collect():
    rules = load_rules()
    pins = pinned_slugs(rules)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT cat_id, key, cat_name, h1_title, title, description
                       FROM pa.page_titles_existing WHERE country_code = 'NL'""")
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)

    todo, editorial = [], []
    for r in rows:
        types = [t for t in (r["key"] or "").split("~") if t]
        if not types or not (set(types) & pins):
            continue
        bp = build_blueprint(r["cat_id"], r["cat_name"] or "", types, rules)
        # KEEP THE LEGACY KEY VERBATIM. build_blueprint() re-sorts the facet tokens, and
        # /page-titles upserts on (cat_id, key) as a STRING — so pushing a re-sorted key
        # writes a SECOND record and leaves the live one untouched. 616 legacy NL rows have
        # key <> canon_key; one of them was in this batch and silently failed to publish.
        bp["key"] = r["key"]
        live = (r["h1_title"] or "").replace('!!sub_category_lower!!', '!!sub_category!!')
        if live.strip() == bp["h1_title"].strip():
            continue                     # pin does not change this row
        (editorial if literals(r["h1_title"]) else todo).append((r, bp))
    return todo, editorial


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--env", default="production", choices=["production", "staging", "dev"])
    args = ap.parse_args()

    todo, editorial = collect()
    print(f"pin-affected legacy rows to rebuild : {len(todo):,}")
    print(f"skipped, live phrase is hand-written: {len(editorial):,}  (see CSV)")
    for r, bp in todo[:6]:
        print(f"  cat {r['cat_id']} {r['key'][:44]}")
        print(f"     live: {r['h1_title']}")
        print(f"     new : {bp['h1_title']}")
    if not todo:
        return
    if not args.apply:
        print("\ndry run — pass --apply to insert + publish")
        return

    with open(BACKUP, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter=";")
        w.writerow(["cat_id", "key", "live_h1", "live_title", "live_description",
                    "new_h1", "new_title"])
        for r, bp in todo:
            w.writerow([r["cat_id"], r["key"], r["h1_title"], r["title"],
                        r["description"], bp["h1_title"], bp["title"]])
    with open(EDITORIAL, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter=";")
        w.writerow(["cat_id", "key", "live_h1", "would_become"])
        for r, bp in editorial:
            w.writerow([r["cat_id"], r["key"], r["h1_title"], bp["h1_title"]])
    print(f"\nbackup   -> {BACKUP}")
    print(f"skipped  -> {EDITORIAL}")

    conn = get_db_connection()
    cur = conn.cursor()
    n = 0
    try:
        for r, bp in todo:
            # source_url None: these come from the legacy export, not from a live URL.
            _upsert_blueprint(cur, bp, None, None, None)
            n += 1
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)
    print(f"inserted/refreshed in pa.seo_titles_blueprints: {n:,}")

    combos = [{"cat_id": r["cat_id"], "key": r["key"]} for r, _ in todo]
    print(f"publishing {len(combos):,} combos to {args.env} …")
    res = publish_built(env=args.env, push_unique_titles=False, combos=combos)
    print("result:", {k: v for k, v in res.items() if k != "batch_results"})


if __name__ == "__main__":
    main()
