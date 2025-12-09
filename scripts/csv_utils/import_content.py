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

def import_content_from_csv(csv_path):
    """Import content from CSV file into database"""

    conn = get_db_connection()
    cur = conn.cursor()

    added_count = 0
    skipped_count = 0
    error_count = 0

    print(f"Reading CSV file: {csv_path}")

    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            # Use semicolon as delimiter based on the CSV structure
            reader = csv.DictReader(f, delimiter=';')

            for row_num, row in enumerate(reader, start=2):  # start=2 because line 1 is header
                try:
                    url = row.get('url', '').strip()
                    content_top = row.get('content_top', '').strip()

                    # Skip if URL or content is empty
                    if not url or not content_top:
                        skipped_count += 1
                        continue

                    # Insert into pa.jvs_seo_werkvoorraad if not exists
                    cur.execute("""
                        INSERT INTO pa.jvs_seo_werkvoorraad (url, kopteksten)
                        VALUES (%s, 1)
                        ON CONFLICT (url) DO UPDATE SET kopteksten = 1
                    """, (url,))

                    # Insert into pa.content_urls_joep
                    cur.execute("""
                        INSERT INTO pa.content_urls_joep (url, content)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                    """, (url, content_top))

                    if cur.rowcount > 0:
                        # Insert into tracking table
                        cur.execute("""
                            INSERT INTO pa.jvs_seo_werkvoorraad_kopteksten_check (url, status)
                            VALUES (%s, 'success')
                            ON CONFLICT DO NOTHING
                        """, (url,))

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
    csv_file = "/mnt/c/Users/JoepvanSchagen/Downloads/content_upload_20251007.csv"

    if not os.path.exists(csv_file):
        print(f"Error: File not found: {csv_file}")
        sys.exit(1)

    print("Starting content import...")
    success = import_content_from_csv(csv_file)

    if success:
        print("\nImport completed successfully!")
        sys.exit(0)
    else:
        print("\nImport failed!")
        sys.exit(1)
