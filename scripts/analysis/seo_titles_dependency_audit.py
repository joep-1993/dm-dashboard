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
                                         Keyed by FACET ID, one call each.

A SLUG IS NOT A FACET. 551 of 7.910 slugs map to several facet ids — `kleurtint_bruin`
is six facets (4253, 4352, 4412, 4449, 5927, 5929), one per category family, and their
parents differ: `kleur` in most trees, `kleur_mode_accessoires` in mode. So every id
behind a slug is probed and the parents are UNIONED, and a combo counts as impossible
only when NONE of the acceptable parents is present. The earlier version kept the first
id per slug and therefore called live URLs impossible — e.g.
.../c/kleur_mode_accessoires~457466~~kleurtint_bruin~7742283 in Sjaals.

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
    """(slug -> {facetIds}), (facetId -> slug).

    A slug maps to a SET of ids: 551 of 7.910 slugs belong to more than one facet
    (`kleurtint_bruin` is six facets, one per category family, each with its own
    parent). Keeping only the first id — which this function used to do — makes the
    dependency map wrong for every other category and flags live URLs as impossible.
    """
    r = requests.get(f"{API}/api/Facets", headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    by_slug, by_id = {}, {}
    for f in r.json():
        for lab in (f.get("labels") or []):
            slug = (lab.get("urlSlug") or "").strip()
            if slug:
                by_slug.setdefault(slug, set()).add(f["id"])
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
    ap.add_argument("--refresh-cache", action="store_true",
                    help="write the child->parent map to pa.facet_dependencies, which "
                         "the generation path reads to skip impossible combos. Probes "
                         "EVERY taxonomy facet, not just the ones already in use, so a "
                         "facet appearing in tomorrow's URLs is covered too.")
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

    # For the cache we probe EVERY taxonomy facet, not only the ones already used:
    # the point is to catch a dependent facet the first time it shows up in a URL,
    # before any blueprint exists for it.
    slugs = sorted(by_slug) if args.refresh_cache else [s for s, _ in used.most_common()]
    if args.limit_facets:
        slugs = slugs[:args.limit_facets]
    unknown = [s for s in slugs if s not in by_slug]
    # Probe each facet ID ONCE and fan the answer back out to every slug that id
    # carries: `merk`, `brand` and `marke` are the same 29 facets, so probing per
    # (slug, id) pair would triple the work for no new information.
    unique_ids = sorted({fid for s in slugs if s in by_slug for fid in by_slug[s]})
    print(f"[3/4] probing {len(unique_ids):,} facet IDS behind "
          f"{len(slugs) - len(unknown):,} slugs "
          f"({len(unknown):,} slugs not in the taxonomy — reported, not probed)")

    id_parent = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for fid, pid in zip(unique_ids, ex.map(parent_of, unique_ids)):
            if pid and pid != "error":
                id_parent[fid] = pid
    # ENFORCE ONLY WHEN UNAMBIGUOUS. A slug is treated as dependent only if EVERY facet
    # id behind it has a dependency. `type` is 40+ facets: a few need merk or
    # populaire_serie, most need nothing — so in those categories `type` alone is a
    # perfectly reachable URL, and demanding any-of-that-union flagged 3.863 valid rows.
    # `kleurtint_bruin` is the opposite: all six ids are dependent (on kleur or
    # kleur_mode_accessoires), so requiring one of those two is right.
    parents, partial = {}, {}
    for slug in slugs:
        ids = by_slug.get(slug) or set()
        if not ids:
            continue
        found = {by_id.get(id_parent[f], f"facet:{id_parent[f]}") for f in ids if f in id_parent}
        if not found:
            continue
        dependent_ids = sum(1 for f in ids if f in id_parent)
        if dependent_ids == len(ids):
            parents[slug] = found
        else:
            # Some ids free-standing: cannot conclude from the slug alone. Reported so the
            # ambiguity is visible rather than silently ignored.
            partial[slug] = (dependent_ids, len(ids), found)
    if partial:
        print(f"      NOT enforced ({len(partial):,} slugs are dependent for only SOME of "
              f"their facet ids — free-standing elsewhere):")
        for slug, (d, n, f) in sorted(partial.items(), key=lambda kv: -used[kv[0]])[:8]:
            print(f"        {slug:28} {d}/{n} ids dependent  ({', '.join(sorted(f))[:48]})")
    print(f"      dependent facets found: {len(parents):,}")
    for child, par in sorted(parents.items(), key=lambda kv: -used[kv[0]])[:15]:
        print(f"        {child:34} needs {' or '.join(sorted(par)):40} (in {used[child]:,} blueprint rows)")

    if args.refresh_cache:
        from backend.seo_titles_service import FACET_DEPS_DDL, FACET_DEPS_MIGRATE
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(FACET_DEPS_DDL)
            cur.execute(FACET_DEPS_MIGRATE)
            # Full replace, not an upsert: a dependency REMOVED in the taxonomy must
            # disappear here too, or generation keeps skipping combos that are valid
            # again. Same transaction, so the table is never empty for a reader.
            cur.execute("DELETE FROM pa.facet_dependencies")
            cur.executemany(
                """INSERT INTO pa.facet_dependencies
                       (child_slug, parent_slug, child_id, parent_id, refreshed_at)
                   VALUES (%s, %s, NULL, NULL, now())""",
                [(child, parent) for child, ps in parents.items() for parent in sorted(ps)])
            conn.commit()
            print(f"      cache: pa.facet_dependencies replaced with "
                  f"{sum(len(v) for v in parents.values()):,} rows "
                  f"({len(parents):,} children)")
        finally:
            cur.close()
            return_db_connection(conn)

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
