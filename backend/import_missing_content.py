#!/usr/bin/env python3
"""
Import missing content from CSV file (LOCAL PostgreSQL only - no Redshift).
- Reads CSV with relative URLs
- Canonicalizes via pa.urls (creates url_id rows for new URLs)
- Inserts missing content into pa.kopteksten_content
- Marks pa.kopteksten_jobs.status = 'success'
"""

import csv
import os
from backend.database import get_db_connection, return_db_connection
from backend.url_catalog import canonicalize_url, bulk_upsert_urls

CSV_PATH = "/app/content_upload.csv"
BATCH_SIZE = 1000


def main():
    print("=" * 60)
    print("Import Missing Content from CSV (LOCAL DB ONLY)")
    print("=" * 60)

    # Read CSV file
    print(f"\n[1/5] Reading CSV file: {CSV_PATH}")
    csv_data = {}  # canonical_url -> content
    with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            url = row.get('url', '').strip()
            content = row.get('content_top', '').strip()
            if url and content:
                canon = canonicalize_url(url)
                if canon:
                    csv_data[canon] = content

    print(f"   Loaded {len(csv_data)} URLs with content from CSV")

    # Resolve url_ids in one shot (creating any missing rows in pa.urls)
    print(f"\n[2/5] Resolving url_ids in pa.urls...")
    conn = get_db_connection()
    cur = conn.cursor()
    url_id_map = bulk_upsert_urls(cur, list(csv_data.keys()))
    conn.commit()
    print(f"   {len(url_id_map)} url_ids resolved")

    # Find URLs that don't yet have content in pa.kopteksten_content
    print(f"\n[3/5] Checking existing kopteksten_content rows...")
    cur.execute("""
        SELECT url_id FROM pa.kopteksten_content
        WHERE url_id = ANY(%s)
    """, (list(url_id_map.values()),))
    existing_url_ids = {r['url_id'] for r in cur.fetchall()}

    missing_canon_urls = [c for c, uid in url_id_map.items() if uid not in existing_url_ids]
    already_exists = len(url_id_map) - len(missing_canon_urls)

    print(f"   URLs already in kopteksten_content: {already_exists}")
    print(f"   Missing (to import):                {len(missing_canon_urls)}")

    if not missing_canon_urls:
        print("\n   No new URLs to import.")
        return_db_connection(conn)
        return

    # Insert missing content
    print(f"\n[4/5] Inserting {len(missing_canon_urls)} rows into pa.kopteksten_content...")
    inserted = 0
    errors = 0
    for i, canon in enumerate(missing_canon_urls):
        try:
            cur.execute("""
                INSERT INTO pa.kopteksten_content (url_id, content, created_at, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (url_id) DO NOTHING
            """, (url_id_map[canon], csv_data[canon]))
            inserted += 1
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"   Error inserting: {e}")
            conn.rollback()
        if (i + 1) % BATCH_SIZE == 0:
            conn.commit()
            print(f"   Progress: {i + 1}/{len(missing_canon_urls)} ({inserted} inserted, {errors} errors)...")
    conn.commit()
    print(f"   Inserted {inserted} content rows ({errors} errors)")

    # Mark jobs as success
    print(f"\n[5/5] Marking pa.kopteksten_jobs as success for imported URLs...")
    tracked = 0
    track_errors = 0
    for i, canon in enumerate(missing_canon_urls):
        try:
            cur.execute("""
                INSERT INTO pa.kopteksten_jobs (url_id, status, created_at, updated_at)
                VALUES (%s, 'success', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (url_id) DO UPDATE SET
                    status = 'success',
                    last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
            """, (url_id_map[canon],))
            tracked += 1
        except Exception as e:
            track_errors += 1
            if track_errors <= 5:
                print(f"   Error tracking: {e}")
            conn.rollback()
        if (i + 1) % BATCH_SIZE == 0:
            conn.commit()
            print(f"   Progress: {i + 1}/{len(missing_canon_urls)} tracked...")
    conn.commit()
    print(f"   Updated {tracked} jobs ({track_errors} errors)")

    return_db_connection(conn)

    print("\n" + "=" * 60)
    print("IMPORT COMPLETE")
    print("=" * 60)
    print(f"   New content rows:  {inserted}")
    print(f"   Jobs updated:      {tracked}")
    print("=" * 60)


if __name__ == "__main__":
    main()
