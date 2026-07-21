#!/usr/bin/env python3
"""
Backfill pa.page_titles_existing.browse_description from the website-configuration
/html-title-descriptions API (one category-level record per cat_id). Used to fill
the meta description for Existing-combo rows that have none (the "shifted" export
layout stores the H1 in the description column, so there is no real meta there).

Adds the column if missing. Fetches GET /html-title-descriptions/{cat_id} for each
distinct cat_id (threaded) and stores the record's browse_description.

Run: venv/bin/python scripts/backfill_page_titles_existing_description.py
"""
import os
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/home/joepvanschagen/projects/dm-dashboard")
from dotenv import load_dotenv
load_dotenv("/home/joepvanschagen/projects/dm-dashboard/.env", override=True)

import requests
from psycopg2.extras import execute_values
from backend.database import get_db_connection, return_db_connection

API = "https://website-configuration.api.beslist.nl/html-title-descriptions"
KEY = os.getenv("UNIQUE_TITLES_API_KEY", "")
HEADERS = {"X-Api-Key": KEY}
SESSION = requests.Session()


def fetch(cat_id):
    """Return (cat_id, browse_description|None)."""
    try:
        r = SESSION.get(f"{API}/{cat_id}", headers=HEADERS, timeout=30)
        if r.status_code != 200:
            return cat_id, None
        data = r.json()
        rec = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else None)
        if not rec:
            return cat_id, None
        return cat_id, (rec.get("browse_description") or None)
    except Exception:
        return cat_id, None


def main():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE pa.page_titles_existing ADD COLUMN IF NOT EXISTS browse_description TEXT")
        conn.commit()
        cur.execute("SELECT DISTINCT cat_id FROM pa.page_titles_existing WHERE cat_id IS NOT NULL")
        cat_ids = [r["cat_id"] for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)
    print(f"[db] distinct cat_ids: {len(cat_ids)}", flush=True)

    pairs = []
    done = 0
    with ThreadPoolExecutor(max_workers=16) as ex:
        for cid, desc in ex.map(fetch, cat_ids):
            done += 1
            if desc:
                pairs.append((cid, desc))
            if done % 500 == 0:
                print(f"  fetched {done}/{len(cat_ids)} (with desc: {len(pairs)})", flush=True)
    print(f"[api] cat_ids with browse_description: {len(pairs)}", flush=True)

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        execute_values(cur, """
            UPDATE pa.page_titles_existing p
            SET browse_description = v.descr
            FROM (VALUES %s) AS v(cat_id, descr)
            WHERE p.cat_id = v.cat_id
        """, pairs, page_size=1000)
        conn.commit()
        cur.execute("SELECT count(*) AS n, count(browse_description) AS filled FROM pa.page_titles_existing")
        r = cur.fetchone()
        print(f"[done] rows={r['n']} browse_description filled={r['filled']} "
              f"({100.0*r['filled']/max(1,r['n']):.1f}%)", flush=True)
    finally:
        cur.close()
        return_db_connection(conn)


if __name__ == "__main__":
    main()
