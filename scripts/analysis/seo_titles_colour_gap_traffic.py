#!/usr/bin/env python3
"""Price the COLOUR blueprint gap in SEO visits, so a band can be picked instead of "all".

Joep, 2026-07-31: ~28k colour-bearing (category, facet-combo) pairs appear in real URLs
with no blueprint. Row counts do not justify a build — the earlier gap work showed 4,5% of
combos carrying a fifth of the traffic — so this attaches 365 days of real SEO visits to
each uncovered colour combo and bands them.

TWO NOTIONS OF "COVERED", reported separately, because they differ by 1.823 combos:
  * canon  — a blueprint exists for the same facet SET (what the generator's dedup means);
  * exact  — a blueprint exists under the key string in the URL's own facet ORDER.
/page-titles upserts on (cat_id, key) as a string, and 616 legacy rows have
key <> canon_key, so a canon-only match may be coverage that never resolves for that URL.

Traffic comes from fetch_top_urls() — the SAME query the generator uses (SEO channel,
is_real_visit=1, faceted /c/, /r/ and /l/ excluded) — so the numbers are what a build
would have seen. NOTE its date_to defaults to a hardcoded 20260608: always pass both.

Read-only. Usage:
    venv/bin/python scripts/analysis/seo_titles_colour_gap_traffic.py \
        --date-from 20250801 --date-to 20260731
"""
import argparse
import csv
import os
import re
import sys
from collections import Counter, defaultdict
from urllib.parse import unquote

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backend.seo_titles_service import (  # noqa: E402
    IGNORE_FACETS, canon_key, fetch_top_urls, impossible_reason, load_facet_deps,
    _resolve_cat,
)
from backend.database import get_db_connection, return_db_connection  # noqa: E402

OUT = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
       "seo_titles_colour_gap_traffic.csv")

# kleur / kleur_* / kleurtint / kleurtint_<hue> / kleurcombinaties_*. Deliberately NOT
# haarkleur, goudkleur, lichtkleur, steenkleur_ring, kleurresolutie_printer, t_kleurboek —
# those match "kleur" as a substring but are not colour-choice facets.
COLOUR = re.compile(r'^(kleur|kleurtint|kleurcombinaties?)(_|$)')
BANDS = [(6, 10**9, ">=6 visits/yr"), (3, 5, "3-5"), (1, 2, "1-2"), (0, 0, "0")]


def facets_in_order(url):
    """(leaf, [facet slugs in the order the URL lists them]) — order matters here."""
    if '/c/' not in url:
        return None, None
    path, fstr = url.split('/c/', 1)
    segs = [s for s in path.split('/') if s]
    leaf = segs[-1] if segs else ''
    out = []
    for pair in fstr.split('~~'):
        bits = pair.split('~')
        if len(bits) >= 2 and bits[0] and bits[1]:
            t = unquote(bits[0])
            if t not in IGNORE_FACETS and t not in out:
                out.append(t)
    return leaf, out


def coverage():
    """(exact {(cat_id, key)}, canon {(cat_id, canon_key)}) over both blueprint stores."""
    exact, canon = set(), set()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for tbl in ("pa.seo_titles_blueprints", "pa.page_titles_existing"):
            cur.execute(f"SELECT cat_id, key FROM {tbl}")
            for r in cur.fetchall():
                exact.add((r["cat_id"], (r["key"] or "").lower()))
                canon.add((r["cat_id"], canon_key(r["key"])))
    finally:
        cur.close()
        return_db_connection(conn)
    return exact, canon


def band(v):
    for lo, hi, name in BANDS:
        if lo <= v <= hi:
            return name
    return "0"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top-n", type=int, default=400000)
    ap.add_argument("--date-from", required=True, help="YYYYMMDD")
    ap.add_argument("--date-to", required=True, help="YYYYMMDD")
    ap.add_argument("--csv", default=OUT)
    args = ap.parse_args()

    print(f"[1/4] Redshift: top {args.top_n:,} SEO-visited faceted /c/ urls "
          f"{args.date_from}..{args.date_to}")
    rows = fetch_top_urls(args.top_n, args.date_from, args.date_to)
    print(f"      {len(rows):,} urls, {sum(int(r['visits'] or 0) for r in rows):,} visits")

    exact, canon = coverage()
    deps = load_facet_deps()
    from backend.url_validator_service import _cache as taxonomy_cache
    print(f"[2/4] coverage {len(exact):,} exact / {len(canon):,} canon · deps {len(deps):,}")

    print("[3/4] mapping urls -> colour combos")
    combos = defaultdict(lambda: {"visits": 0, "revenue": 0.0, "urls": 0,
                                  "cat_name": "", "sample": ""})
    leaf_cache, skipped = {}, Counter()
    for r in rows:
        url = (r["url"] or "").lower()
        leaf, order = facets_in_order(url)
        if not order:
            skipped["not faceted"] += 1
            continue
        if not any(COLOUR.match(t) for t in order):
            skipped["no colour facet"] += 1
            continue
        if leaf not in leaf_cache:
            leaf_cache[leaf] = _resolve_cat(taxonomy_cache, leaf)
        cat = leaf_cache[leaf]
        if not cat:
            skipped["leaf not in taxonomy"] += 1
            continue
        key = "~".join(order)
        rec = combos[(cat["cat_id"], key)]
        rec["visits"] += int(r["visits"] or 0)
        rec["revenue"] += float(r["revenue"] or 0)
        rec["urls"] += 1
        rec["cat_name"] = cat.get("cat_name") or ""
        if not rec["sample"]:
            rec["sample"] = r["url"]
    print(f"      colour combos with traffic: {len(combos):,}   skipped: {dict(skipped)}")

    print("[4/4] banding")
    out_rows, agg = [], defaultdict(lambda: {"combos": 0, "visits": 0, "revenue": 0.0})
    for (cat_id, key), rec in combos.items():
        types = key.split("~")
        why = impossible_reason(types, deps)
        cov = ("exact" if (cat_id, key) in exact
               else "canon-only" if (cat_id, canon_key(key)) in canon
               else "none")
        b = band(rec["visits"])
        if cov != "exact" and not why:
            agg[(cov, b)]["combos"] += 1
            agg[(cov, b)]["visits"] += rec["visits"]
            agg[(cov, b)]["revenue"] += rec["revenue"]
            out_rows.append([cat_id, rec["cat_name"], key, cov, b, rec["visits"],
                             round(rec["revenue"], 2), rec["urls"], rec["sample"],
                             len(types)])
    covered_v = sum(r["visits"] for (c, k), r in combos.items() if (c, k) in exact)
    print(f"\ntraffic on colour combos that ARE covered exactly: {covered_v:,} visits")
    print("\nUNCOVERED colour combos — build candidates")
    print(f"{'coverage':<12}{'band':<16}{'combos':>9}{'visits':>12}{'revenue':>12}")
    for cov in ("none", "canon-only"):
        for _, _, b in BANDS:
            a = agg.get((cov, b))
            if not a:
                continue
            print(f"{cov:<12}{b:<16}{a['combos']:>9,}{a['visits']:>12,}"
                  f"{a['revenue']:>12,.0f}")
    tot = {"combos": sum(a["combos"] for a in agg.values()),
           "visits": sum(a["visits"] for a in agg.values())}
    print(f"{'TOTAL':<28}{tot['combos']:>9,}{tot['visits']:>12,}")
    print(f"\n(combos with NO traffic in the window are absent here by construction — "
          f"fetch_top_urls only returns visited urls)")

    out_rows.sort(key=lambda r: -r[5])
    with open(args.csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter=";")
        w.writerow(["cat_id", "cat_name", "key_url_order", "coverage", "band", "visits",
                    "revenue", "urls", "sample_url", "facet_depth"])
        w.writerows(out_rows)
    print(f"csv -> {args.csv}  ({len(out_rows):,} rows)")
    print("\ntop 15 uncovered colour combos by visits:")
    for r in out_rows[:15]:
        print(f"  {r[5]:>6,} visits  cat {r[0]} {r[1][:22]:22} {r[3]:<10} {r[2][:58]}")


if __name__ == "__main__":
    main()
