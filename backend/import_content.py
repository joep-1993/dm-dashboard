#!/usr/bin/env python3
"""
Import pre-generated content from CSV file into the database.
This script imports URLs and content, marking them as processed.
"""
import csv
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.database import get_db_connection
from backend.url_catalog import get_url_id

def import_content_from_csv(csv_path):
    """Import content from CSV file into the new schema:
      - pa.urls (catalog)
      - pa.kopteksten_content (the imported content)
      - pa.kopteksten_jobs (status='success')
    """

    conn = get_db_connection()
    cur = conn.cursor()

    added_count = 0
    skipped_count = 0
    error_count = 0

    print(f"Reading CSV file: {csv_path}")

    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=';')

            for row_num, row in enumerate(reader, start=2):
                try:
                    url = row.get('url', '').strip()
                    content_top = row.get('content_top', '').strip()

                    if not url or not content_top:
                        skipped_count += 1
                        continue

                    url_id = get_url_id(cur, url)
                    if url_id is None:
                        skipped_count += 1
                        continue

                    # Upsert content; only count as "added" if a new content row was created
                    cur.execute("""
                        INSERT INTO pa.kopteksten_content (url_id, content, created_at, updated_at)
                        VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (url_id) DO NOTHING
                    """, (url_id, content_top))

                    if cur.rowcount > 0:
                        cur.execute("""
                            INSERT INTO pa.kopteksten_jobs (url_id, status, created_at, updated_at)
                            VALUES (%s, 'success', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            ON CONFLICT (url_id) DO UPDATE SET
                                status = 'success',
                                last_error = NULL,
                                updated_at = CURRENT_TIMESTAMP
                        """, (url_id,))

                        added_count += 1
                        if added_count % 100 == 0:
                            print(f"Progress: {added_count} items imported...")
                            conn.commit()
                    else:
                        skipped_count += 1

                except Exception as e:
                    error_count += 1
                    print(f"Error on row {row_num}: {str(e)}")
                    continue

            conn.commit()

    except Exception as e:
        print(f"Fatal error: {str(e)}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

    print("\n=== Import Complete ===")
    print(f"Successfully imported: {added_count}")
    print(f"Skipped (duplicates/empty): {skipped_count}")
    print(f"Errors: {error_count}")
    print(f"Total rows processed: {added_count + skipped_count + error_count}")

    return True

if __name__ == "__main__":
    # Try different paths
    possible_paths = [
        "/app/content_upload.csv",
        "/mnt/c/Users/JoepvanSchagen/Downloads/content_upload_20251007.csv"
    ]

    csv_file = None
    for path in possible_paths:
        if os.path.exists(path):
            csv_file = path
            break

    if not csv_file:
        print(f"Error: File not found in any of these locations:")
        for path in possible_paths:
            print(f"  - {path}")
        sys.exit(1)

    print(f"Starting content import from: {csv_file}")
    success = import_content_from_csv(csv_file)

    if success:
        print("\nImport completed successfully!")
        sys.exit(0)
    else:
        print("\nImport failed!")
        sys.exit(1)
