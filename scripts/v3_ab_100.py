"""
v1 vs v3 A/B harness — 100 random AI-processed URLs.

Pulls stored v1 h1_title from pa.unique_titles_content as baseline, runs
generate_title_v3() fresh, writes side-by-side xlsx for manual scoring.

Run: python3 scripts/v3_ab_100.py [N]   (N defaults to 100)
"""
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import openpyxl
from backend.database import get_db_connection, return_db_connection
from backend.ai_titles_service import generate_title_v3

WORKERS = 10
SAMPLE_N = int(sys.argv[1]) if len(sys.argv) > 1 else 100


def sample_urls(n: int):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u.url, c.h1_title, c.title, c.description
            FROM pa.unique_titles_content c
            JOIN pa.urls u ON u.url_id = c.url_id
            WHERE c.h1_title IS NOT NULL AND c.h1_title <> ''
            ORDER BY random()
            LIMIT %s
            """,
            (n,),
        )
        return cur.fetchall()
    finally:
        return_db_connection(conn)


def run_one(row):
    # backend.database uses RealDictCursor — rows are dicts
    url = row["url"]
    v1_h1 = row["h1_title"]
    try:
        result = generate_title_v3(url, polish=False)
        if not result:
            return {"url": url, "v1_h1": v1_h1, "error": "v3 returned None"}
        return {
            "url": url,
            "v1_h1": v1_h1,
            "v3_h1": result.get("h1_title") or "",
            "error": "",
        }
    except Exception as e:
        return {"url": url, "v1_h1": v1_h1, "error": f"{type(e).__name__}: {e}"}


def main():
    print(f"Sampling {SAMPLE_N} URLs...")
    rows = sample_urls(SAMPLE_N)
    print(f"Got {len(rows)} rows. Running v3 with {WORKERS} workers...")

    started = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(run_one, r): r for r in rows}
        for i, fut in enumerate(as_completed(futures), 1):
            results.append(fut.result())
            if i % 10 == 0:
                print(f"  {i}/{len(rows)} ({time.time()-started:.1f}s)")

    # Sort to keep output deterministic-ish
    results.sort(key=lambda r: r["url"])

    out_path = os.path.expanduser(f"~/v1_vs_v3_{SAMPLE_N}_{date.today().isoformat()}.xlsx")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "v1_vs_v3"
    ws.append(["url", "v1_h1", "v3_h1", "error", "verdict"])
    for r in results:
        ws.append([
            r.get("url", ""),
            r.get("v1_h1", "") or "",
            r.get("v3_h1", "") or "",
            r.get("error", "") or "",
            "",
        ])
    widths = {"A": 60, "B": 50, "C": 50, "D": 30, "E": 12}
    for col, w in widths.items():
        ws.column_dimensions[col].width = w
    wb.save(out_path)

    errored = sum(1 for r in results if r.get("error"))
    differs = sum(1 for r in results
                  if not r.get("error") and (r.get("v1_h1") or "").strip() != (r.get("v3_h1") or "").strip())
    print(f"\nDone in {time.time()-started:.1f}s")
    print(f"  results: {len(results)}")
    print(f"  errored: {errored}")
    print(f"  v1 differs from v3: {differs} / {len(results)-errored}")
    print(f"  saved: {out_path}")


if __name__ == "__main__":
    main()
