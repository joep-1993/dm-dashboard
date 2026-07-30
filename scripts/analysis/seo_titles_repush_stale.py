#!/usr/bin/env python3
"""Re-push already-pushed blueprints whose phrase is stale under the current rules.

A facet-order edit only changes FUTURE builds. Rows that were pushed before the edit
keep their old title on the site, and `publish_built()` cannot help: it reads
status='built' only. /page-titles is POST-only (verified: `Allow: POST`), and it
upserts on (cat_id, key), so re-sending a record overwrites the live one.

Sequence per run:
  1. find pushed rows whose rebuilt blueprint differs from what is stored
  2. flip exactly those to status='built' (recorded in a backup CSV first)
  3. rewrite their title/h1/description to the rebuilt values
  4. publish_built(combos=...) -> POST to /page-titles, flip back to 'pushed'

If step 4 fails, the rows are left as 'built' with the NEW values — correct and
publishable, just not yet live. Nothing is lost; re-run to retry.

push_unique_titles is False on purpose: this re-pushes blueprints, not the per-URL
AI titles, so it changes exactly what the facet-order edit changed.

Usage:
    venv/bin/python scripts/analysis/seo_titles_repush_stale.py            # dry run
    venv/bin/python scripts/analysis/seo_titles_repush_stale.py --apply --env staging
    venv/bin/python scripts/analysis/seo_titles_repush_stale.py --apply --env production
"""
import argparse
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backend.database import get_db_connection, return_db_connection  # noqa: E402
from backend.seo_titles_service import build_blueprint, load_rules, publish_built  # noqa: E402

BACKUP = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
          "seo_titles_repush_backup.csv")


def stale_pushed():
    rules = load_rules()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT cat_id, cat_name, key, status, title, h1_title, description
                       FROM pa.seo_titles_blueprints WHERE status = 'pushed'""")
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)
    out = []
    for r in rows:
        types = [t for t in (r["key"] or "").split("~") if t]
        if not types:
            continue
        bp = build_blueprint(r["cat_id"], r["cat_name"] or "", types, rules)
        if bp["h1_title"] != r["h1_title"] or bp["title"] != r["title"]:
            out.append((r, bp))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--env", default="production", choices=["production", "staging", "dev"])
    args = ap.parse_args()

    stale = stale_pushed()
    print(f"stale pushed rows: {len(stale):,}")
    for r, bp in stale[:8]:
        print(f"  {(r['cat_name'] or '?')[:24]:24} {r['key'][:46]}")
        print(f"     old h1: {r['h1_title']}")
        print(f"     new h1: {bp['h1_title']}")
    if not stale:
        return
    if not args.apply:
        print("\ndry run — pass --apply to rewrite and re-push")
        return

    combos = [{"cat_id": r["cat_id"], "key": r["key"]} for r, _ in stale]
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        with open(BACKUP, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["cat_id", "key", "status", "title", "h1_title", "description"])
            for r, _ in stale:
                w.writerow([r["cat_id"], r["key"], r["status"], r["title"],
                            r["h1_title"], r["description"]])
        print(f"\nbackup -> {BACKUP}")
        n = 0
        for r, bp in stale:
            # Flip to 'built' AND write the rebuilt values in one statement, guarded on
            # status='pushed' so a row already flipped by something else is untouched.
            cur.execute("""UPDATE pa.seo_titles_blueprints
                           SET status='built', title=%s, h1_title=%s, description=%s
                           WHERE cat_id=%s AND key=%s AND status='pushed'""",
                        (bp["title"], bp["h1_title"], bp["description"],
                         r["cat_id"], r["key"]))
            n += cur.rowcount
        conn.commit()
        print(f"flipped to built + rewritten: {n:,}")
    finally:
        cur.close()
        return_db_connection(conn)

    print(f"publishing {len(combos):,} combos to {args.env} …")
    res = publish_built(env=args.env, push_unique_titles=False, combos=combos)
    print("result:", {k: v for k, v in res.items() if k != "batch_results"})

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT status, count(*) n FROM pa.seo_titles_blueprints
                       WHERE (cat_id, key) IN %s GROUP BY 1""",
                    (tuple((c["cat_id"], c["key"]) for c in combos),))
        print("status of the touched rows:", [(r["status"], r["n"]) for r in cur.fetchall()])
    finally:
        cur.close()
        return_db_connection(conn)


if __name__ == "__main__":
    main()
