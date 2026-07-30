#!/usr/bin/env python3
"""Audit pa.seo_titles_blueprints for facet combos that CANNOT exist on the site.

The Taxonomy API models facet dependencies: a child facet is only selectable once a
specific parent facet value is chosen. `type_parfum` (facetId 3432, label
"Collectie") depends on `merk` (3027), so a /c/ URL carrying type_parfum without
merk is unreachable — and a page-title blueprint for that combo is dead weight.

Two API shapes expose this; this script uses the cheaper one:
  * GET /api/Categories/{id}          -> facets[].dependentMetadata
                                         {parentFacetId, parentFacetValueIds}
                                         per CATEGORY-facet. 3.215 categories appear
                                         in the blueprints, so that is 3.215 large
                                         responses.
  * GET /api/Facets/{id}/value-dependencies  -> rows whose parentFacetValue.facet
                                         gives the parent. 204 = no dependency.
                                         Keyed by FACET, so it is one call per
                                         distinct facet (2.226) and the answer is
                                         global rather than per category.
The facet-scoped view is what we want anyway: if a facet needs a parent at all, a
combo naming the child without the parent is impossible in every category.

NOTE ON `parentFacetId`: the dependency row's own `parentFacetId` is 0 in the live
data — read the parent from `parentFacetValue.facetId` instead. Trusting the
top-level field silently yields "no parent" for everything.

Usage:
    venv/bin/python scripts/analysis/seo_titles_dependency_audit.py [--limit-facets N]
                    [--csv OUT.csv] [--status built|pushed|all]

Read-only: reports, never edits blueprints.
"""
import argparse
import csv
import json
import os
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backend.database import get_db_connection, return_db_connection  # noqa: E402

API = "http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl"
HEADERS = {"X-User-Name": "SEO_JOEP"}
WORKERS = 12
TIMEOUT = 60


def facet_slug_map():
    """urlSlug -> facetId, from the full facet list (one call)."""
    r = requests.get(f"{API}/api/Facets", headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    by_slug, by_id = {}, {}
    for f in r.json():
        for lab in (f.get("labels") or []):
            slug = (lab.get("urlSlug") or "").strip()
            if slug:
                by_slug.setdefault(slug, f["id"])
                by_id.setdefault(f["id"], slug)
    return by_slug, by_id


def parent_of(facet_id):
    """Parent facetId for a child facet, or None. 204 == no dependency."""
    try:
        r = requests.get(f"{API}/api/Facets/{facet_id}/value-dependencies",
                         headers=HEADERS, timeout=TIMEOUT)
    except requests.RequestException:
        return "error"
    if r.status_code == 204:
        return None
    if r.status_code == 404 or not r.ok:
        return None
    try:
        rows = r.json() or []
    except ValueError:
        return None
    for row in rows:
        pv = row.get("parentFacetValue") or {}
        # The row's own parentFacetId is 0 in live data; the value's facetId is real.
        pid = pv.get("facetId") or row.get("parentFacetId")
        if pid:
            return pid
    return None


def load_blueprints(status):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if status == "all":
            cur.execute("SELECT cat_id, cat_name, key, status FROM pa.seo_titles_blueprints")
        else:
            cur.execute("SELECT cat_id, cat_name, key, status FROM pa.seo_titles_blueprints"
                        " WHERE status = %s", (status,))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--status", default="all", choices=["built", "pushed", "all"])
    ap.add_argument("--limit-facets", type=int, default=0,
                    help="only probe the N most-used facets (for a quick look)")
    ap.add_argument("--csv", default=None)
    ap.add_argument("--delete-built", action="store_true",
                    help="DELETE the impossible status='built' rows. Always dumps a "
                         "full-column backup CSV first. Never touches 'pushed' rows: "
                         "removing one would leave the blueprint LIVE on the site while "
                         "dropping it from the dedup log, so the combo would be rebuilt "
                         "and re-pushed later — strictly worse than leaving the record.")
    ap.add_argument("--backup", default="/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
                                        "seo_titles_deleted_impossible_backup.csv")
    args = ap.parse_args()

    rows = load_blueprints(args.status)
    print(f"[1/4] blueprints: {len(rows):,} rows (status={args.status})")

    used = Counter()
    for r in rows:
        for s in (r["key"] or "").split("~"):
            if s:
                used[s] += 1
    print(f"      distinct facet slugs in keys: {len(used):,}")

    by_slug, by_id = facet_slug_map()
    print(f"[2/4] taxonomy facets: {len(by_slug):,} slugs")

    slugs = [s for s, _ in used.most_common()]
    if args.limit_facets:
        slugs = slugs[:args.limit_facets]
    known = [(s, by_slug[s]) for s in slugs if s in by_slug]
    unknown = [s for s in slugs if s not in by_slug]
    print(f"[3/4] probing dependencies for {len(known):,} facets "
          f"({len(unknown):,} slugs not found in the taxonomy — reported, not probed)")

    parents = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for (slug, fid), pid in zip(known, ex.map(lambda kv: parent_of(kv[1]), known)):
            if pid and pid != "error":
                parents[slug] = by_id.get(pid, f"facet:{pid}")
    print(f"      dependent facets found: {len(parents):,}")
    for child, par in sorted(parents.items(), key=lambda kv: -used[kv[0]])[:15]:
        print(f"        {child:34} needs {par:20} (in {used[child]:,} blueprint rows)")

    print("[4/4] scanning combos")
    bad = []
    for r in rows:
        combo = [s for s in (r["key"] or "").split("~") if s]
        have = set(combo)
        missing = [(c, parents[c]) for c in combo if c in parents and parents[c] not in have]
        if missing:
            bad.append((r, missing))

    print()
    print(f"IMPOSSIBLE COMBOS: {len(bad):,} of {len(rows):,} blueprint rows "
          f"({100.0 * len(bad) / max(len(rows), 1):.2f}%)")
    by_child = Counter()
    by_cat = Counter()
    by_status = Counter()
    for r, missing in bad:
        by_status[r["status"]] += 1
        by_cat[(r["cat_id"], r["cat_name"])] += 1
        for child, par in missing:
            by_child[f"{child} without {par}"] += 1
    print("  by status:", dict(by_status))
    print("  top offending facet pairs:")
    for k, n in by_child.most_common(15):
        print(f"    {n:6,}  {k}")
    print("  top categories:")
    for (cid, cname), n in by_cat.most_common(10):
        print(f"    {n:6,}  {cid} {cname}")

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["cat_id", "cat_name", "status", "key", "missing_parents"])
            for r, missing in bad:
                w.writerow([r["cat_id"], r["cat_name"], r["status"], r["key"],
                            "; ".join(f"{c} needs {p}" for c, p in missing)])
        print(f"\nwrote {args.csv}")

    if args.delete_built:
        targets = [(r["cat_id"], r["key"]) for r, _ in bad if r["status"] == "built"]
        skipped = sum(1 for r, _ in bad if r["status"] != "built")
        print(f"\n--- delete: {len(targets):,} built rows "
              f"({skipped:,} non-built left alone) ---")
        if not targets:
            return
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Full-column backup BEFORE deleting, so the exact rows can be restored.
            cur.execute("""
                SELECT * FROM pa.seo_titles_blueprints
                WHERE status = 'built' AND (cat_id, key) IN %s
            """, (tuple(targets),))
            dump = [dict(r) for r in cur.fetchall()]
            with open(args.backup, "w", newline="", encoding="utf-8") as fh:
                if dump:
                    w = csv.DictWriter(fh, fieldnames=list(dump[0].keys()))
                    w.writeheader()
                    for d in dump:
                        w.writerow(d)
            print(f"    backup: {len(dump):,} rows -> {args.backup}")
            # Re-assert status='built' in the DELETE itself: between the SELECT above
            # and here a row could have been pushed, and a pushed row must survive.
            cur.execute("""
                DELETE FROM pa.seo_titles_blueprints
                WHERE status = 'built' AND (cat_id, key) IN %s
            """, (tuple(targets),))
            deleted = cur.rowcount
            conn.commit()
            print(f"    deleted: {deleted:,} rows")
        finally:
            cur.close()
            return_db_connection(conn)


if __name__ == "__main__":
    main()
