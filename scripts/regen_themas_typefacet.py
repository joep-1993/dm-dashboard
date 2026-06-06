#!/usr/bin/env python3
"""One-off: regenerate unique-titles H1s for the populaire_themas_mode + type-facet
candidate set, to clear the "dangling theme adjective" bug fixed in fd9273b
(type-facet noun routed to its own slot so a theme adjective like 'Zakelijke'
can no longer sort behind the product noun).

Targets ONLY the candidate URLs — does not sweep the existing pending queue.
Reuses process_single_url (v3 builder + AI polish + full-record write).
"""
import os, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import psycopg2, psycopg2.extras
from backend.ai_titles_service import process_single_url

DSN = ("postgresql://dbadmin:Q9fGRKtUdvdtxsiCM12HeFe0Nki0PvmjZRFLZ9ArmlWdMnDQXX8SdxKnPniqGmq6"
       "@10.1.32.9:5432/n8n-vector-db")
DOMAIN = "https://www.beslist.nl"
WORKERS = 8

def fetch_candidates():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT u.url, ct.h1_title AS old_h1
        FROM pa.unique_titles_content ct
        JOIN pa.urls u ON u.url_id = ct.url_id
        WHERE ct.h1_title IS NOT NULL
          AND u.url LIKE '%populaire_themas_mode%'
          AND (u.url LIKE '%~soort_%' OR u.url LIKE '%/soort_%'
               OR u.url LIKE '%~t_%' OR u.url LIKE '%/t_%')
        ORDER BY u.url
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def work(row):
    url = row['url']
    full = url if url.startswith('http') else DOMAIN + url
    old = (row['old_h1'] or '').strip()
    try:
        res = process_single_url(full)
        new = (res.get('h1_title') or '').strip()
        return (url, old, new, res.get('status'), None)
    except Exception as e:
        return (url, old, None, 'error', str(e))

def main():
    rows = fetch_candidates()
    total = len(rows)
    print(f"[REGEN] {total} candidate URLs", flush=True)
    done = changed = failed = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(work, r) for r in rows]
        for fut in as_completed(futs):
            url, old, new, status, err = fut.result()
            done += 1
            if status not in ('success',) or new is None:
                failed += 1
                print(f"[FAIL] {status} {err or ''} {url}", flush=True)
            elif new != old:
                changed += 1
                if changed <= 40:
                    print(f"[CHANGED] {old!r} -> {new!r}", flush=True)
            if done % 200 == 0:
                print(f"[PROGRESS] {done}/{total} changed={changed} failed={failed}", flush=True)
    print(f"[DONE] processed={done} changed={changed} failed={failed}", flush=True)

if __name__ == '__main__':
    main()
