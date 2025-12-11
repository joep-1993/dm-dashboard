#!/usr/bin/env python3
"""
Import missing content from CSV file (LOCAL PostgreSQL only - no Redshift).
- Reads CSV with relative URLs
- Converts to absolute URLs (https://www.beslist.nl prefix)
- Finds URLs not yet in pa.content_urls_joep
- Inserts missing content
- Updates tracking table (pa.jvs_seo_werkvoorraad_kopteksten_check)
"""

import csv
import os
from backend.database import get_db_connection, return_db_connection

BASE_URL = "https://www.beslist.nl"
CSV_PATH = "/app/content_upload.csv"
BATCH_SIZE = 1000


def main():
    print("=" * 60)
    print("Import Missing Content from CSV (LOCAL DB ONLY)")
    print("=" * 60)

    # Read CSV file
    print(f"\n[1/5] Reading CSV file: {CSV_PATH}")
    csv_data = {}  # url -> content
    with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            url = row.get('url', '').strip()
            content = row.get('content_top', '').strip()
            if url and content:
                # Convert relative URL to absolute
                if url.startswith('/'):
                    url = BASE_URL + url
                csv_data[url] = content

    print(f"   Loaded {len(csv_data)} URLs with content from CSV")

    # Get existing URLs from local PostgreSQL
    print(f"\n[2/5] Checking existing URLs in database...")
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT url FROM pa.content_urls_joep")
    existing_urls = {row['url'] for row in cur.fetchall()}
    print(f"   Found {len(existing_urls)} existing URLs in pa.content_urls_joep")

    # Find missing URLs
    csv_urls = set(csv_data.keys())
    missing_urls = csv_urls - existing_urls
    already_exists = csv_urls & existing_urls

    print(f"\n[3/5] Analysis:")
    print(f"   URLs in CSV:          {len(csv_urls)}")
    print(f"   Already in database:  {len(already_exists)}")
    print(f"   Missing (to import):  {len(missing_urls)}")

    if not missing_urls:
        print("\n   No new URLs to import. All URLs already exist in database.")
        return_db_connection(conn)
        return

    # Insert missing content into local PostgreSQL (simple INSERT, no ON CONFLICT)
    print(f"\n[4/5] Inserting {len(missing_urls)} URLs into pa.content_urls_joep...")
    inserted = 0
    errors = 0
    for i, url in enumerate(missing_urls):
        content = csv_data[url]
        try:
            cur.execute("""
                INSERT INTO pa.content_urls_joep (url, content)
                VALUES (%s, %s)
            """, (url, content))
            inserted += 1
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"   Error inserting: {e}")
            conn.rollback()  # Rollback and continue

        if (i + 1) % BATCH_SIZE == 0:
            conn.commit()
            print(f"   Progress: {i + 1}/{len(missing_urls)} URLs ({inserted} inserted, {errors} errors)...")

    conn.commit()
    print(f"   Inserted {inserted} URLs into pa.content_urls_joep ({errors} errors)")

    # Update tracking table
    print(f"\n[5/5] Updating tracking table (pa.jvs_seo_werkvoorraad_kopteksten_check)...")
    tracked = 0
    track_errors = 0
    for i, url in enumerate(missing_urls):
        try:
            cur.execute("""
                INSERT INTO pa.jvs_seo_werkvoorraad_kopteksten_check (url, status)
                VALUES (%s, 'success')
                ON CONFLICT (url) DO UPDATE SET status = 'success'
            """, (url,))
            tracked += 1
        except Exception as e:
            track_errors += 1
            if track_errors <= 5:
                print(f"   Error tracking: {e}")
            conn.rollback()

        if (i + 1) % BATCH_SIZE == 0:
            conn.commit()
            print(f"   Progress: {i + 1}/{len(missing_urls)} URLs tracked...")

    conn.commit()
    print(f"   Updated {tracked} URLs in tracking table ({track_errors} errors)")

    return_db_connection(conn)

    # Final summary
    print("\n" + "=" * 60)
    print("IMPORT COMPLETE")
    print("=" * 60)
    print(f"   New URLs imported:    {inserted}")
    print(f"   Tracking updated:     {tracked}")
    print("=" * 60)


if __name__ == "__main__":
    main()
