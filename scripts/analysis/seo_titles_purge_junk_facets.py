#!/usr/bin/env python3
"""Find (and optionally delete) blueprints whose key contains a facet slug that is
not a real facet.

WHY THESE EXIST: `parse_url` used to accept a facet pair with a name but no value,
so real traffic URLs ending in "<name>~" — .../c/merkm~ , .../c/me~ ,
.../c/kleur_mode_accessoi~ — produced (cat_id, key) combos out of junk. The
resulting blueprint holds a placeholder like !!merkm!! that can never resolve. The
parser now requires a non-empty value (seo_titles_service.parse_url), so no new ones
appear; this cleans up what is already stored.

Two classes are reported separately, because only the first is junk:
  * UNKNOWN  — slug is not in the taxonomy AND has no position rule -> parse artifact
  * RETIRED  — slug is not in the taxonomy but DOES have a rule -> a facet that
               existed when the blueprint was built and has since been removed.
               Left alone: that is history, not corruption.

Only status='built' rows are deleted, for the same reason as the dependency audit:
deleting a 'pushed' row would leave the blueprint live on the site while dropping it
from the dedup log, so the combo would be rebuilt and re-pushed later.

Usage:
    venv/bin/python scripts/analysis/seo_titles_purge_junk_facets.py [--delete-built]
"""
import argparse
import csv
import os
import sys
from collections import Counter

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backend.database import get_db_connection, return_db_connection  # noqa: E402

API = "http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl"
HEADERS = {"X-User-Name": "SEO_JOEP"}
BACKUP = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
          "seo_titles_deleted_junk_facets_backup.csv")


def taxonomy_slugs():
    r = requests.get(f"{API}/api/Facets", headers=HEADERS, timeout=120)
    r.raise_for_status()
    out = set()
    for f in r.json():
        for lab in (f.get("labels") or []):
            s = (lab.get("urlSlug") or "").strip()
            if s:
                out.add(s)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--delete-built", action="store_true")
    args = ap.parse_args()

    tax = taxonomy_slugs()
    print(f"[1/3] taxonomy facet slugs: {len(tax):,}")

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT facet_slug FROM pa.facet_position_rules")
        ruled = {r["facet_slug"] for r in cur.fetchall()}
        cur.execute("SELECT cat_id, cat_name, key, status FROM pa.seo_titles_blueprints")
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)
    print(f"      rules: {len(ruled):,}   blueprints: {len(rows):,}")

    unknown, retired = [], []
    unk_slugs, ret_slugs = Counter(), Counter()
    for r in rows:
        bad_unknown, bad_retired = [], []
        for s in (r["key"] or "").split("~"):
            if not s or s in tax:
                continue
            (bad_retired if s in ruled else bad_unknown).append(s)
        if bad_unknown:
            unknown.append((r, bad_unknown))
            for s in bad_unknown:
                unk_slugs[s] += 1
        elif bad_retired:
            retired.append((r, bad_retired))
            for s in bad_retired:
                ret_slugs[s] += 1

    def summary(name, items, slugs):
        st = Counter(r["status"] for r, _ in items)
        print(f"\n{name}: {len(items):,} rows  {dict(st)}")
        for s, n in slugs.most_common(12):
            print(f"    {n:5}  {s}")

    print("[2/3] classifying")
    summary("UNKNOWN (parse artefacts — junk)", unknown, unk_slugs)
    summary("RETIRED (facet removed since build — left alone)", retired, ret_slugs)

    if not args.delete_built:
        print("\n[3/3] dry run — pass --delete-built to remove the UNKNOWN built rows")
        return

    targets = [(r["cat_id"], r["key"]) for r, _ in unknown if r["status"] == "built"]
    skipped = sum(1 for r, _ in unknown if r["status"] != "built")
    print(f"\n[3/3] deleting {len(targets):,} built rows ({skipped:,} non-built left alone)")
    if not targets:
        return
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT * FROM pa.seo_titles_blueprints
                       WHERE status='built' AND (cat_id, key) IN %s""", (tuple(targets),))
        dump = [dict(r) for r in cur.fetchall()]
        with open(BACKUP, "w", newline="", encoding="utf-8") as fh:
            if dump:
                w = csv.DictWriter(fh, fieldnames=list(dump[0].keys()))
                w.writeheader()
                for d in dump:
                    w.writerow(d)
        print(f"      backup: {len(dump):,} rows -> {BACKUP}")
        cur.execute("""DELETE FROM pa.seo_titles_blueprints
                       WHERE status='built' AND (cat_id, key) IN %s""", (tuple(targets),))
        print(f"      deleted: {cur.rowcount:,} rows")
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)


if __name__ == "__main__":
    main()
