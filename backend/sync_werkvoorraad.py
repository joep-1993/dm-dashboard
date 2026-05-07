"""
Synchronize Redshift werkvoorraad with the local kopteksten content state.
- Marks Redshift pa.jvs_seo_werkvoorraad_shopping_season.kopteksten=1 for URLs
  with local kopteksten content
- Marks pa.kopteksten_jobs.status='success' for those URLs (idempotent)
"""

from backend.database import get_db_connection, get_output_connection
from backend.url_catalog import bulk_upsert_urls, canonicalize_url

def main():
    print("="*70)
    print("WERKVOORRAAD SYNCHRONIZATION SCRIPT")
    print("="*70)
    print("\nThis script will:")
    print("1. Read all URLs from local pa.kopteksten_content")
    print("2. Update Redshift pa.jvs_seo_werkvoorraad_shopping_season → kopteksten=1")
    print("3. Mark pa.kopteksten_jobs.status='success' (idempotent)")
    print("\nWARNING: Close all browser tabs and stop all processing before running!")
    print("="*70)

    input("\nPress Enter to continue or Ctrl+C to cancel...")

    try:
        local_conn = get_db_connection()
        local_cur = local_conn.cursor()

        print("\nStep 1: Reading local URLs with content...")
        local_cur.execute("""
            SELECT u.url
            FROM pa.kopteksten_content c
            JOIN pa.urls u ON c.url_id = u.url_id
        """)
        content_urls = [row['url'] for row in local_cur.fetchall()]
        print(f"  Found {len(content_urls)} URLs with content")

        # Step 2: update Redshift werkvoorraad
        print("\nStep 2: Updating Redshift werkvoorraad...")
        output_conn = get_output_connection()
        output_cur = output_conn.cursor()
        # Build a temporary sync table on Redshift; for simplicity here we batch
        # IN-clause updates. For very large lists you'd want a STAGE table.
        BATCH = 500
        updated_count = 0
        for i in range(0, len(content_urls), BATCH):
            chunk = content_urls[i:i+BATCH]
            placeholders = ','.join(['%s'] * len(chunk))
            output_cur.execute(f"""
                UPDATE pa.jvs_seo_werkvoorraad_shopping_season
                   SET kopteksten = 1
                 WHERE kopteksten = 0
                   AND url IN ({placeholders})
            """, chunk)
            updated_count += output_cur.rowcount
        output_conn.commit()
        output_cur.close()
        output_conn.close()
        print(f"  ✓ Updated {updated_count} Redshift rows")

        # Step 3: ensure local kopteksten_jobs has 'success' for those URLs
        print("\nStep 3: Marking pa.kopteksten_jobs.status='success'...")
        url_id_map = bulk_upsert_urls(local_cur, content_urls)
        marked = 0
        for url in content_urls:
            canon = canonicalize_url(url)
            uid = url_id_map.get(canon) if canon else None
            if uid is None:
                continue
            local_cur.execute("""
                INSERT INTO pa.kopteksten_jobs (url_id, status, created_at, updated_at)
                VALUES (%s, 'success', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (url_id) DO UPDATE SET
                    status = 'success',
                    last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
            """, (uid,))
            marked += 1
        local_conn.commit()
        local_cur.close()
        local_conn.close()
        print(f"  ✓ Processed {marked} job rows")

        print("\n✅ Synchronisation complete.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
