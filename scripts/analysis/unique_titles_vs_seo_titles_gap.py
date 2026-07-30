#!/usr/bin/env python3
"""Which (cat_id, facet-combo) pairs have a Unique Title but NO SEO-title blueprint?

Unique Titles works per URL; SEO titles work per (category, facet combo). So a URL can
carry an AI unique title while the (cat, combo) it belongs to has no page-title
blueprint at all — the H1/title then falls back to whatever the site does by default.
This lists those gaps, biggest first.

It deliberately reuses the generation path's own helpers — parse_url, canon_key,
_resolve_cat, load_existing_combos — so "already covered" means exactly what the
generator means by it (pa.page_titles_existing UNION pa.seo_titles_blueprints, both
statuses), and a gap here is genuinely something a run would build.

Gaps are split by whether they are worth building:
  * BUILDABLE  — no dependency problem; a generation run would create these
  * IMPOSSIBLE — the combo names a dependent facet without its parent (type_parfum
                 without merk, kleurtint_* without kleur), so the URL is unreachable
                 and the generator now skips it on purpose. Reported separately so
                 the buildable number is not inflated by combos we refuse to build.

Read-only.

Usage:
    venv/bin/python scripts/analysis/unique_titles_vs_seo_titles_gap.py [--csv OUT.csv]
"""
import argparse
import csv
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backend.database import get_db_connection, return_db_connection  # noqa: E402
from backend.seo_titles_service import (  # noqa: E402
    canon_key, impossible_reason, load_existing_combos, load_facet_deps, parse_url,
    _resolve_cat,
)

OUT = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
       "unique_titles_without_seo_title.csv")


def unique_title_urls():
    """Every URL that has a non-empty AI unique title."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT u.url_id, u.url, c.title
            FROM pa.urls u
            JOIN pa.unique_titles_content c ON c.url_id = u.url_id
            WHERE COALESCE(c.title, '') <> ''
        """)
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=OUT)
    args = ap.parse_args()

    rows = unique_title_urls()
    print(f"[1/4] URLs with a unique title: {len(rows):,}")

    existing = load_existing_combos(force=True)
    deps = load_facet_deps()
    print(f"[2/4] combos already covered: {len(existing):,}   "
          f"facet dependencies: {len(deps):,}")

    from backend.url_validator_service import _cache as taxonomy_cache

    print("[3/4] resolving urls -> (cat_id, combo)")
    covered = no_parse = no_cat = no_facets = 0
    gaps = defaultdict(lambda: {"urls": 0, "cat_name": "", "sample": ""})
    for r in rows:
        url = (r["url"] or "").lower()
        p = parse_url(url)
        if not p:
            no_parse += 1
            continue
        leaf, types = p
        if not types:
            no_facets += 1
            continue
        cat = _resolve_cat(taxonomy_cache, leaf)
        if not cat:
            no_cat += 1
            continue
        ck = canon_key("~".join(sorted(types)))
        if (cat["cat_id"], ck) in existing:
            covered += 1
            continue
        g = gaps[(cat["cat_id"], ck)]
        g["urls"] += 1
        g["cat_name"] = cat.get("cat_name", "")
        if not g["sample"]:
            g["sample"] = r["url"]

    print(f"      covered {covered:,} · gaps {len(gaps):,} distinct combos · "
          f"skipped: {no_parse:,} not a faceted /c/ url, {no_facets:,} no facets, "
          f"{no_cat:,} category unresolved")

    buildable, impossible = [], []
    for (cat_id, ck), g in gaps.items():
        reason = impossible_reason([t for t in ck.split("~") if t], deps)
        (impossible if reason else buildable).append((cat_id, ck, g, reason))
    buildable.sort(key=lambda x: -x[2]["urls"])
    impossible.sort(key=lambda x: -x[2]["urls"])

    print(f"\n[4/4] BUILDABLE gaps: {len(buildable):,} combos "
          f"({sum(x[2]['urls'] for x in buildable):,} urls)")
    for cat_id, ck, g, _ in buildable[:20]:
        print(f"    {g['urls']:5} urls  {cat_id} {g['cat_name'][:24]:24} {ck[:60]}")
    print(f"\n      IMPOSSIBLE gaps (generator refuses these): {len(impossible):,} combos "
          f"({sum(x[2]['urls'] for x in impossible):,} urls)")
    for cat_id, ck, g, reason in impossible[:10]:
        print(f"    {g['urls']:5} urls  {cat_id} {g['cat_name'][:22]:22} {ck[:44]:44} [{reason}]")

    cats = Counter()
    for cat_id, ck, g, _ in buildable:
        cats[(cat_id, g["cat_name"])] += g["urls"]
    print("\n      top categories by buildable gap urls:")
    for (cid, cname), n in cats.most_common(10):
        print(f"    {n:5} urls  {cid} {cname}")

    with open(args.csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["cat_id", "cat_name", "canon_key", "urls_with_unique_title",
                    "verdict", "impossible_reason", "sample_url"])
        for cat_id, ck, g, reason in buildable + impossible:
            w.writerow([cat_id, g["cat_name"], ck, g["urls"],
                        "impossible" if reason else "buildable", reason or "",
                        g["sample"]])
    print(f"\nwrote {args.csv}")


if __name__ == "__main__":
    main()
