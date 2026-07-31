#!/usr/bin/env python3
"""Publish colour combos that are covered ONLY under a different key order.

The problem (found 2026-07-31 via https://www.beslist.nl/products/mode_accessoires/
mode_accessoires_457570/c/kleur_mode_accessoires~457466~~kleurtint_bruin~7742283):
a blueprint exists for the facet SET, but its key string lists the facets in another
order — e.g. legacy `kleurtint_bruin~kleur_mode_accessoires` while every URL says
`kleur_mode_accessoires~kleurtint_bruin` (236 urls in that order, 0 in the other).
/page-titles upserts on (cat_id, key) as a STRING, so that record can never resolve for
those URLs. Priced over 365 days: 1.186 such colour combos carry 4.346 SEO visits/yr,
concentrated in kleur_mode_accessoires + kleurtint_* in the mode-accessoires tree.

Fix: publish the same combo under the key order the URLs actually use.

TWO THINGS TO KNOW
  * This ADDS a record; /page-titles has no delete verb, so the old wrong-order record
    stays behind, inert (no URL uses that string).
  * The phrase is COPIED VERBATIM from the covering row — only the key changes. It is NOT
    rebuilt: the current builder renders every facet in the URL, which for these combos
    means the generic colour AND the hue ("Blauw Lichtblauw Strandtassen"), while the
    covering phrases and the live rendered titles use the hue alone ("Lichtblauwe
    Strandtassen", "Bordeaux rode Petten"). Copying also preserves hand-written editorial
    text, so nothing has to be skipped for it.

Input is the CSV from seo_titles_colour_gap_traffic.py. Read-only until --apply.

Usage:
    venv/bin/python scripts/analysis/seo_titles_push_canon_only_keys.py --min-visits 1
    venv/bin/python scripts/analysis/seo_titles_push_canon_only_keys.py --min-visits 1 --apply
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
    build_blueprint, canon_key, load_rules, publish_built, _upsert_blueprint,
)

SRC = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
       "seo_titles_colour_gap_traffic.csv")
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
DONE = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
        f"seo_titles_canon_only_pushed_{STAMP}.csv")
SKIPPED = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
           f"seo_titles_canon_only_skipped_editorial_{STAMP}.csv")
PH = re.compile(r'!![a-z0-9_]+!!', re.I)


def literals(phrase):
    return [w for w in PH.sub(' ', phrase or '').split() if w]


def existing_phrases():
    """{(cat_id, canon_key): (source, key, row)} for the row that already covers the set."""
    out = {}
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # legacy first, tool second: if both exist the tool's is the live-managed one.
        for src, tbl in (("legacy", "pa.page_titles_existing"),
                         ("tool", "pa.seo_titles_blueprints")):
            cur.execute(f"SELECT cat_id, key, title, h1_title, description FROM {tbl}")
            for r in cur.fetchall():
                out[(r["cat_id"], canon_key(r["key"]))] = (src, r["key"], dict(r))
    finally:
        cur.close()
        return_db_connection(conn)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-visits", type=int, default=1)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--env", default="production", choices=["production", "staging", "dev"])
    ap.add_argument("--src", default=SRC)
    args = ap.parse_args()

    rows = list(csv.DictReader(open(args.src, encoding="utf-8"), delimiter=";"))
    cand = [r for r in rows if r["coverage"] == "canon-only"
            and int(r["visits"]) >= args.min_visits]
    print(f"canon-only colour combos with >= {args.min_visits} visits/yr: {len(cand):,}")

    have = existing_phrases()
    todo, missing, lowered = [], [], []
    for r in cand:
        cat_id, key = int(r["cat_id"]), r["key_url_order"]
        cov = have.get((cat_id, canon_key(key)))
        if not cov:
            missing.append(r)                 # coverage vanished since the pricing run
            continue
        src, old_key, old = cov
        # !!sub_category_lower!! -> !!sub_category!!. The legacy vocabulary is used by
        # 116.132 rows in the MySQL export, but this tool has never pushed it to
        # /page-titles and we cannot verify from here that that renderer knows it — a
        # placeholder it does not resolve would be worse than a capital letter. The one
        # rendered example we could read is capitalised anyway ("Bruine - Kaki Sjaals").
        def fix(v):
            return (v or "").replace("!!sub_category_lower!!", "!!sub_category!!")
        bp = {
            "cat_id": cat_id, "key": key, "cat_name": r["cat_name"],
            "title": fix(old["title"]), "h1_title": fix(old["h1_title"]),
            "description": fix(old["description"]), "country_code": "NL",
        }
        if "sub_category_lower" in (old["h1_title"] or ""):
            lowered.append(r)
        todo.append((r, bp, src, old_key))

    print(f"  to publish                       : {len(todo):,}")
    print(f"  coverage disappeared since pricing: {len(missing):,}")
    print(f"  phrases whose !!sub_category_lower!! was normalised: {len(lowered):,}")
    print(f"  visits/yr covered by the publish : "
          f"{sum(int(r['visits']) for r, *_ in todo):,}")
    for r, bp, src, old_key in todo[:6]:
        print(f"  cat {r['cat_id']} {r['cat_name'][:20]:20} {int(r['visits']):>5} visits")
        print(f"     {src} key : {old_key}")
        print(f"     new  key : {bp['key']}   (phrase copied unchanged)")
        print(f"        h1     : {bp['h1_title']}")
    if not args.apply:
        print("\ndry run — pass --apply to insert + publish")
        return

    with open(DONE, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter=";")
        w.writerow(["cat_id", "cat_name", "visits_yr", "old_source", "old_key",
                    "new_key", "h1_copied", "sample_url"])
        for r, bp, src, old_key in todo:
            w.writerow([r["cat_id"], r["cat_name"], r["visits"], src, old_key,
                        bp["key"], bp["h1_title"], r["sample_url"]])
    print(f"\npushed-list -> {DONE}")

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for _, bp, *_ in todo:
            _upsert_blueprint(cur, bp, None, None, None)
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)
    print(f"inserted/refreshed in pa.seo_titles_blueprints: {len(todo):,}")

    combos = [{"cat_id": int(r["cat_id"]), "key": bp["key"]} for r, bp, *_ in todo]
    print(f"publishing {len(combos):,} combos to {args.env} …")
    res = publish_built(env=args.env, push_unique_titles=False, combos=combos)
    print("result:", {k: v for k, v in res.items() if k != "batch_results"})


if __name__ == "__main__":
    main()
