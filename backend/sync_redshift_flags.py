#!/usr/bin/env python3
"""
Sync Redshift kopteksten flags based on local content table.

This script ensures data consistency between:
- Local pa.kopteksten_content (URLs with generated content; new-schema table
  joined to pa.urls for the URL string)
- Redshift pa.jvs_seo_werkvoorraad_shopping_season (kopteksten flags)

It will:
1. Find all URLs in local pa.kopteksten_content
2. Update Redshift to set kopteksten=1 for those URLs
"""

import os
from backend.database import get_db_connection, get_output_connection, return_db_connection, return_output_connection

def sync_redshift_flags():
    """Sync Redshift kopteksten flags with local content table"""

    print("=" * 70)
    print("REDSHIFT KOPTEKSTEN FLAG SYNC")
    print("=" * 70)

    # Step 1: Get all URLs that have content locally
    print("\n[1/3] Fetching URLs with content from local database...")
    local_conn = get_db_connection()
    local_cur = local_conn.cursor()

    local_cur.execute("""
        SELECT u.url
        FROM pa.kopteksten_content c
        JOIN pa.urls u ON c.url_id = u.url_id
    """)
    urls_with_content = [row['url'] for row in local_cur.fetchall()]

    local_cur.close()
    return_db_connection(local_conn)

    print(f"      Found {len(urls_with_content):,} URLs with content locally")

    if not urls_with_content:
        print("\n✓ No URLs to sync")
        return

    # Step 2: Check which URLs need updating in Redshift
    print("\n[2/3] Checking Redshift flags...")
    output_conn = get_output_connection()
    output_cur = output_conn.cursor()

    # Check how many have kopteksten=0 (need updating)
    placeholders = ','.join(['%s'] * len(urls_with_content))
    output_cur.execute(f"""
        SELECT COUNT(*) as count
        FROM pa.jvs_seo_werkvoorraad_shopping_season
        WHERE url IN ({placeholders})
        AND kopteksten = 0
    """, urls_with_content)

    needs_update = output_cur.fetchone()['count']
    print(f"      {needs_update:,} URLs need kopteksten=1 update in Redshift")

    if needs_update == 0:
        print("\n✓ All URLs already synced!")
        output_cur.close()
        return_output_connection(output_conn)
        return

    # Step 3: Update Redshift flags
    print(f"\n[3/3] Updating Redshift flags for {needs_update:,} URLs...")
    print("      This may take a few minutes...")

    # Update in batches of 1000 for better performance
    batch_size = 1000
    updated = 0

    for i in range(0, len(urls_with_content), batch_size):
        batch = urls_with_content[i:i + batch_size]
        placeholders = ','.join(['%s'] * len(batch))

        output_cur.execute(f"""
            UPDATE pa.jvs_seo_werkvoorraad_shopping_season
            SET kopteksten = 1
            WHERE url IN ({placeholders})
            AND kopteksten = 0
        """, batch)

        updated += output_cur.rowcount

        if (i + batch_size) % 10000 == 0:
            print(f"      Progress: {updated:,} / {needs_update:,} URLs updated")
            output_conn.commit()

    output_conn.commit()
    output_cur.close()
    return_output_connection(output_conn)

    print(f"\n✓ Successfully updated {updated:,} URLs in Redshift")
    print(f"  All URLs with local content now have kopteksten=1")

    print("\n" + "=" * 70)
    print("SYNC COMPLETE")
    print("=" * 70)

if __name__ == "__main__":
    try:
        sync_redshift_flags()
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
