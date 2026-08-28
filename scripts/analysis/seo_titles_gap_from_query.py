#!/usr/bin/env python3
"""Which (cat, facet-combo) pages with real SEO traffic still have NO title blueprint?

Runs Joep's traffic query verbatim (SEO channel, is_real_visit=1, faceted /c/ pages
with a " - " page_heading, visits > 3, 2025-01-01..2026-08-28), maps each URL to the
(cat_id, canon_key) combo the blueprint generator would derive from it, and subtracts
everything already covered.

`winkel` combos are dropped: every `winkel` facet is globally disabled, so those pages
are not a blueprint target (see seo_prio_service notes on the 2026-03-16 bulk seed).

"Covered" is asked in the same two steps the generator uses:
  (a) pa.seo_titles_blueprints  — what this tool already holds locally
  (b) GET /page-titles/{cat_id}/record?key= — the LIVE store, authoritative, memoised
A combo whose GET cannot be answered counts as covered (store_has_combos is
deliberately conservative), so the gap list under-reports rather than over-reports.

Redshift rows are cached in the scratchpad so a re-run costs nothing; --refresh re-queries.
Read-only: no writes anywhere except the CSV and the row cache.

Usage:
    venv/bin/python scripts/analysis/seo_titles_gap_from_query.py
                    [--refresh] [--min-visits 3] [--date-from 20250101] [--date-to 20260828]
                    [--skip-store] [--csv OUT.csv]
"""
import argparse
import csv
import os
import pickle
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backend.database import get_redshift_connection, return_redshift_connection  # noqa: E402
from backend.seo_titles_service import (  # noqa: E402
    canon_key, impossible_reason, load_facet_deps, load_local_combos, parse_url,
    store_has_combos, _resolve_cat,
)

CACHE = ("/tmp/claude-1001/-home-joepvanschagen/"
         "6054cccb-5c4b-4705-b04b-8723d17394d4/scratchpad/seo_gap_rows.pkl")
OUT = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
       "seo_titles_gap_from_query.csv")

# Joep's query, verbatim apart from the parameterised window / min-visits.
SQL = """
    SELECT dv.main_cat_name,
           dv.deepest_subcat_name,
           SPLIT_PART(dv.url, '?', 1) AS url,
           dv.page_heading,
           count(*) AS visits,
           sum(fcv.cpc_revenue) + sum(fcv.ww_revenue) AS revenue
    FROM datamart.fct_visits fcv
    JOIN datamart.dim_visit dv ON fcv.dim_visit_key = dv.dim_visit_key
    JOIN datamart.dim_date dat ON fcv.dim_date_key = dat.dim_date_key
    JOIN chan_deriv.ref_channel_derivation_stats chan
         ON dv.aff_id = chan.aff_id AND dv.channel_id = chan.channel_id
    WHERE dv.is_real_visit = 1
      AND chan.marketing_channel = 'SEO'
      AND fcv.dim_date_key BETWEEN %s AND %s
      AND dv.url LIKE '%%beslist.nl%%'
      AND dv.url LIKE '%%/c/%%'
      AND dv.page_heading LIKE '%% - %%'
      AND dv.url NOT LIKE '%%/r/%%'
      AND dv.url NOT LIKE '%%/l/%%'
      AND dv.url NOT LIKE '%%/page_%%'
      AND dv.url NOT LIKE '%%?page=%%'
      AND dv.url NOT LIKE '%%#%%'
    GROUP BY 1, 2, 3, 4
    HAVING count(*) > %s
    ORDER BY 5 DESC
"""


def fetch_rows(date_from, date_to, min_visits, refresh):
    if not refresh and os.path.exists(CACHE):
        rows = pickle.load(open(CACHE, "rb"))
        print(f"[1/4] cache: {len(rows):,} rows ({CACHE})")
        return rows
    print(f"[1/4] Redshift: {date_from}..{date_to}, visits > {min_visits} ...")
    conn = get_redshift_connection()
    cur = conn.cursor()
    try:
        cur.execute(SQL, (date_from, date_to, min_visits))
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_redshift_connection(conn)
    os.makedirs(os.path.dirname(CACHE), exist_ok=True)
    pickle.dump(rows, open(CACHE, "wb"))
    print(f"      {len(rows):,} rows")
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date-from", default="20250101")
    ap.add_argument("--date-to", default="20260828")
    ap.add_argument("--min-visits", type=int, default=3)
    ap.add_argument("--refresh", action="store_true", help="re-query Redshift")
    ap.add_argument("--skip-store", action="store_true",
                    help="only check pa.seo_titles_blueprints, not the live store")
    ap.add_argument("--csv", default=OUT)
    args = ap.parse_args()

    rows = fetch_rows(args.date_from, args.date_to, args.min_visits, args.refresh)
    tot_v = sum(int(r["visits"] or 0) for r in rows)
    print(f"      {tot_v:,} visits total")

    print("[2/4] mapping urls -> (cat_id, combo)")
    from backend.url_validator_service import _cache as taxonomy_cache
    deps = load_facet_deps()

    combos = defaultdict(lambda: {"visits": 0, "revenue": 0.0, "urls": 0,
                                  "cat_name": "", "main_cat": "", "sample": "",
                                  "heading": ""})
    unparsed = no_facet = no_cat = 0
    winkel_v = winkel_c = 0
    for r in rows:
        url = (r["url"] or "").lower()
        v = int(r["visits"] or 0)
        rev = float(r["revenue"] or 0.0)
        p = parse_url(url)
        if not p:
            unparsed += v
            continue
        leaf, types = p
        if not types:
            no_facet += v
            continue
        if "winkel" in types:          # globally-disabled facet, not a target
            winkel_v += v
            winkel_c += 1
            continue
        cat = _resolve_cat(taxonomy_cache, leaf)
        if not cat:
            no_cat += v
            continue
        ck = canon_key("~".join(sorted(types)))
        g = combos[(cat["cat_id"], ck)]
        g["visits"] += v
        g["revenue"] += rev
        g["urls"] += 1
        g["cat_name"] = cat.get("cat_name", "")
        g["main_cat"] = r.get("main_cat_name") or ""
        if not g["sample"]:
            g["sample"] = r["url"]
            g["heading"] = r.get("page_heading") or ""

    print(f"      {len(combos):,} distinct combos in traffic")
    print(f"      dropped: winkel {winkel_v:,} visits ({winkel_c:,} urls) · "
          f"no facet {no_facet:,} · unknown cat {no_cat:,} · unparsed {unparsed:,}")

    print("[3/4] coverage")
    local = load_local_combos(force=True)
    keys = list(combos.keys())
    covered = {k for k in keys if k in local}
    print(f"      pa.seo_titles_blueprints holds {len(local):,} combos "
          f"-> {len(covered):,} of ours")
    if not args.skip_store:
        todo = [k for k in keys if k not in covered]
        print(f"      asking the live store about {len(todo):,} combos ...")
        in_store = store_has_combos(
            todo, progress=lambda d, t: print(f"        {d:,}/{t:,}", flush=True))
        covered |= set(in_store)
        print(f"      live store holds {len(in_store):,} of them")
    else:
        print("      (--skip-store: live store NOT consulted, gap is an upper bound)")

    gaps = [(cid, ck, combos[(cid, ck)]) for (cid, ck) in keys
            if (cid, ck) not in covered]
    items = []
    for cid, ck, g in gaps:
        types = [t for t in ck.split("~") if t]
        items.append((cid, ck, len(types), g, impossible_reason(types, deps)))
    items.sort(key=lambda x: -x[3]["visits"])
    buildable = [i for i in items if not i[4]]

    gap_v = sum(i[3]["visits"] for i in items)
    bld_v = sum(i[3]["visits"] for i in buildable)
    cov_v = sum(combos[k]["visits"] for k in covered)
    print(f"\n[4/4] UNCOVERED: {len(items):,} combos, {gap_v:,} visits "
          f"({100.0 * gap_v / max(gap_v + cov_v, 1):.1f}% of mapped traffic)")
    print(f"      of which buildable now: {len(buildable):,} combos, {bld_v:,} visits")

    print("\n  top 30 uncovered buildable combos by SEO visits:")
    for cid, ck, depth, g, _ in buildable[:30]:
        print(f"    {g['visits']:7,} v  EUR {g['revenue']:9,.0f}  d{depth}  "
              f"{cid:>9} {g['cat_name'][:24]:24} {ck[:44]}")

    print("\n  buildable, by facet depth:")
    dv, dc = Counter(), Counter()
    for cid, ck, depth, g, _ in buildable:
        dv[depth] += g["visits"]
        dc[depth] += 1
    for d in sorted(dv):
        print(f"    depth {d}: {dc[d]:6,} combos  {dv[d]:9,} visits  "
              f"({100.0 * dv[d] / max(sum(dv.values()), 1):4.1f}%)  "
              f"{dv[d] / max(dc[d], 1):6.1f} v/combo")

    print("\n  top main categories by uncovered buildable visits:")
    mc = Counter()
    for cid, ck, depth, g, _ in buildable:
        mc[g["main_cat"] or "?"] += g["visits"]
    for name, n in mc.most_common(12):
        print(f"    {n:9,} visits  {name}")

    if items and items[0][4] is not None or any(i[4] for i in items):
        print("\n  blocked (dependent facet without its parent) — top 10:")
        for cid, ck, depth, g, reason in [i for i in items if i[4]][:10]:
            print(f"    {g['visits']:7,} v  {ck[:44]:44} {reason}")

    with open(args.csv, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["cat_id", "main_cat_name", "cat_name", "canon_key", "depth",
                    "seo_visits", "seo_revenue", "urls", "verdict",
                    "blocked_reason", "sample_url", "sample_page_heading"])
        for cid, ck, depth, g, reason in items:
            w.writerow([cid, g["main_cat"], g["cat_name"], ck, depth, g["visits"],
                        round(g["revenue"], 2), g["urls"],
                        "blocked" if reason else "buildable", reason or "",
                        g["sample"], g["heading"]])
    print(f"\nwrote {args.csv}")


if __name__ == "__main__":
    main()
