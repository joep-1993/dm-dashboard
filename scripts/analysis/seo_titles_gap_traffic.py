#!/usr/bin/env python3
"""How much SEO traffic do the uncovered (cat, facet-combo) gaps actually get?

unique_titles_vs_seo_titles_gap.py answers "which combos have no blueprint"; it counts
URLs, which says nothing about whether anyone visits them. This attaches real SEO
visits + revenue per combo so the build/skip call is made on traffic, not on row counts.

Traffic comes from fetch_top_urls(), the SAME Redshift query the generator uses to
decide what to build (SEO channel, is_real_visit=1, faceted /c/, /r/ excluded), so a
combo's number here is what the generator would have seen. Its default window starts
2025-01-01; pass --date-from to narrow it.

The url -> (cat_id, combo) mapping reuses parse_url / canon_key / _resolve_cat, and
"already covered" is load_existing_combos(), exactly as the generator means it.

Output: per uncovered combo, visits + revenue + url count, ranked by visits, split by
facet depth so the long tail is visible. Read-only.

Usage:
    venv/bin/python scripts/analysis/seo_titles_gap_traffic.py [--top-n 400000]
                    [--date-from 20260501] [--csv OUT.csv]
"""
import argparse
import csv
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backend.seo_titles_service import (  # noqa: E402
    canon_key, fetch_top_urls, impossible_reason, load_existing_combos,
    load_facet_deps, parse_url, _resolve_cat,
)

OUT = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
       "seo_titles_gap_traffic.csv")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top-n", type=int, default=400000,
                    help="how many top-visited faceted /c/ urls to pull (default 400k)")
    ap.add_argument("--date-from", default=None, help="YYYYMMDD")
    ap.add_argument("--date-to", default=None, help="YYYYMMDD")
    ap.add_argument("--csv", default=OUT)
    args = ap.parse_args()

    print(f"[1/4] Redshift: top {args.top_n:,} SEO-visited faceted /c/ urls "
          f"({args.date_from or 'default'}..{args.date_to or 'default'})")
    rows = fetch_top_urls(args.top_n, args.date_from, args.date_to)
    tot_v = sum(int(r["visits"] or 0) for r in rows)
    print(f"      {len(rows):,} urls, {tot_v:,} visits")

    existing = load_existing_combos(force=True)
    deps = load_facet_deps()
    from backend.url_validator_service import _cache as taxonomy_cache
    print(f"[2/4] covered combos {len(existing):,} · dependencies {len(deps):,}")

    print("[3/4] mapping urls -> combos")
    gaps = defaultdict(lambda: {"visits": 0, "revenue": 0.0, "urls": 0,
                                "cat_name": "", "sample": ""})
    covered_v = gap_v = skipped = 0
    for r in rows:
        url = (r["url"] or "").lower()
        v = int(r["visits"] or 0)
        rev = float(r["revenue"] or 0.0)
        p = parse_url(url)
        if not p:
            skipped += 1
            continue
        leaf, types = p
        if not types:
            skipped += 1
            continue
        cat = _resolve_cat(taxonomy_cache, leaf)
        if not cat:
            skipped += 1
            continue
        ck = canon_key("~".join(sorted(types)))
        if (cat["cat_id"], ck) in existing:
            covered_v += v
            continue
        g = gaps[(cat["cat_id"], ck)]
        g["visits"] += v
        g["revenue"] += rev
        g["urls"] += 1
        g["cat_name"] = cat.get("cat_name", "")
        if not g["sample"]:
            g["sample"] = r["url"]
        gap_v += v

    print(f"      covered {covered_v:,} visits · uncovered {gap_v:,} visits "
          f"({100.0 * gap_v / max(covered_v + gap_v, 1):.1f}% of matched traffic) · "
          f"{skipped:,} urls unmapped")

    items = []
    for (cat_id, ck), g in gaps.items():
        depth = len([t for t in ck.split("~") if t])
        reason = impossible_reason([t for t in ck.split("~") if t], deps)
        items.append((cat_id, ck, depth, g, reason))
    items.sort(key=lambda x: -x[3]["visits"])
    buildable = [i for i in items if not i[4]]

    print(f"\n[4/4] uncovered combos: {len(items):,} "
          f"({len(buildable):,} buildable)")
    print("\n  top uncovered combos by SEO visits:")
    for cat_id, ck, depth, g, reason in buildable[:20]:
        print(f"    {g['visits']:7,} visits  EUR {g['revenue']:8,.0f}  d{depth}  "
              f"{g['cat_name'][:20]:20} {ck[:52]}")

    print("\n  by facet depth (buildable only):")
    dv, dc = Counter(), Counter()
    for cat_id, ck, depth, g, reason in buildable:
        dv[depth] += g["visits"]
        dc[depth] += 1
    for d in sorted(dv):
        share = 100.0 * dv[d] / max(sum(dv.values()), 1)
        print(f"    depth {d}: {dc[d]:6,} combos  {dv[d]:9,} visits  ({share:4.1f}%)  "
              f"{dv[d] / max(dc[d], 1):6.1f} visits/combo")

    print("\n  top categories by uncovered visits (buildable):")
    cats = Counter()
    for cat_id, ck, depth, g, reason in buildable:
        cats[(cat_id, g["cat_name"])] += g["visits"]
    for (cid, cname), n in cats.most_common(12):
        print(f"    {n:8,} visits  {cid} {cname}")

    with open(args.csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["cat_id", "cat_name", "canon_key", "depth", "seo_visits",
                    "seo_revenue", "urls", "verdict", "impossible_reason", "sample_url"])
        for cat_id, ck, depth, g, reason in items:
            w.writerow([cat_id, g["cat_name"], ck, depth, g["visits"],
                        round(g["revenue"], 2), g["urls"],
                        "impossible" if reason else "buildable", reason or "", g["sample"]])
    print(f"\nwrote {args.csv}")


if __name__ == "__main__":
    main()
