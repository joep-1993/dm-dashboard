#!/usr/bin/env python3
"""
Backfill pa.page_titles_existing.cat_name from the Taxonomy category tree,
keyed by cat_id (reliable in every row, both export layouts). Adds the column
if missing. Resolves cat_id -> deepest category name from the taxonomy CSV
cache (no per-id API calls), falling back to the Taxonomy API for any cat_id
not present in the CSV.

Run: venv/bin/python scripts/backfill_page_titles_existing_catname.py
"""
import sys
sys.path.insert(0, "/home/joepvanschagen/projects/dm-dashboard")
from dotenv import load_dotenv
load_dotenv("/home/joepvanschagen/projects/dm-dashboard/.env", override=True)

from psycopg2.extras import execute_values
from backend.database import get_db_connection, return_db_connection
from backend.url_validator_service import _cache


def api_name(detail):
    """Extract the nl-NL category name from a /api/Categories/{id} detail dict."""
    if not isinstance(detail, dict):
        return None
    if detail.get("name"):
        return detail["name"]
    for l in detail.get("labels", []) or []:
        if l.get("locale") == "nl-NL":
            return l.get("name") or l.get("nameInColumn") or l.get("nameOnDetail")
    return None


def main():
    _cache._ensure_csv_loaded()
    id2name = {}
    for info in _cache._cat_by_slug.values():
        cid, nm = info.get("cat_id"), info.get("deepest_cat")
        if cid and nm:
            id2name.setdefault(cid, nm)
    for info in _cache._maincat_by_slug.values():
        cid, nm = info.get("id"), info.get("name")
        if cid and nm:
            id2name.setdefault(cid, nm)
    print(f"[cache] cat_id->name from CSV: {len(id2name)}", flush=True)

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE pa.page_titles_existing ADD COLUMN IF NOT EXISTS cat_name TEXT")
        conn.commit()
        cur.execute("SELECT DISTINCT cat_id FROM pa.page_titles_existing WHERE cat_id IS NOT NULL")
        cat_ids = [r["cat_id"] for r in cur.fetchall()]
        print(f"[db] distinct cat_ids in table: {len(cat_ids)}", flush=True)

        missing = [c for c in cat_ids if c not in id2name]
        print(f"[resolve] not in CSV, trying Taxonomy API: {len(missing)}", flush=True)
        api_ok = 0
        for c in missing:
            try:
                nm = api_name(_cache.get_category_detail(c))
                if nm:
                    id2name[c] = nm
                    api_ok += 1
            except Exception:
                pass
        print(f"[resolve] recovered via API: {api_ok}", flush=True)

        pairs = [(c, id2name[c]) for c in cat_ids if c in id2name]
        unresolved = [c for c in cat_ids if c not in id2name]
        print(f"[resolve] resolved={len(pairs)} unresolved={len(unresolved)}", flush=True)

        execute_values(cur, """
            UPDATE pa.page_titles_existing p
            SET cat_name = v.name
            FROM (VALUES %s) AS v(cat_id, name)
            WHERE p.cat_id = v.cat_id
        """, pairs, page_size=1000)
        conn.commit()

        cur.execute("SELECT count(*) AS n, count(cat_name) AS filled FROM pa.page_titles_existing")
        r = cur.fetchone()
        print(f"[done] rows={r['n']} cat_name filled={r['filled']} "
              f"({100.0*r['filled']/max(1,r['n']):.1f}%)", flush=True)
        if unresolved:
            print(f"[warn] unresolved cat_ids (sample): {unresolved[:15]}", flush=True)
    finally:
        cur.close()
        return_db_connection(conn)


if __name__ == "__main__":
    main()
