"""
Migration: Create shared url_validation_tracking table and move 'no_products_found' skips there.

This merges skipped URLs from both kopteksten_check and faq_tracking into one shared table,
so both features see the same "Skipped" count and don't re-scrape URLs the other already checked.

Run: docker exec dm_tools_app python -m backend.migrate_shared_validation
"""

import sys
from backend.database import get_db_connection, return_db_connection


def migrate():
    conn = get_db_connection()
    cur = conn.cursor()

    print("[MIGRATE] Creating pa.url_validation_tracking table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pa.url_validation_tracking (
            url VARCHAR(500) PRIMARY KEY,
            status VARCHAR(50) DEFAULT 'skipped',
            skip_reason VARCHAR(255),
            checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_url_validation_status ON pa.url_validation_tracking(status)")

    # Count existing rows
    cur.execute("SELECT COUNT(*) as c FROM pa.url_validation_tracking")
    existing = cur.fetchone()['c']
    if existing > 0:
        print(f"[MIGRATE] Table already has {existing} rows. Skipping data migration.")
        conn.commit()
        cur.close()
        return_db_connection(conn)
        return

    # Merge skipped URLs from both tables into shared table (union, keep most recent)
    print("[MIGRATE] Merging skipped URLs from kopteksten_check and faq_tracking...")
    cur.execute("""
        INSERT INTO pa.url_validation_tracking (url, status, skip_reason, checked_at)
        SELECT url, status, skip_reason, checked_at
        FROM (
            SELECT url, status, skip_reason, created_at as checked_at,
                   ROW_NUMBER() OVER (PARTITION BY url ORDER BY created_at DESC) as rn
            FROM (
                SELECT url, status, skip_reason, created_at
                FROM pa.jvs_seo_werkvoorraad_kopteksten_check
                WHERE status = 'skipped'
                UNION ALL
                SELECT url, status, skip_reason, created_at
                FROM pa.faq_tracking
                WHERE status = 'skipped'
            ) combined
        ) ranked
        WHERE rn = 1
        ON CONFLICT (url) DO NOTHING
    """)
    migrated = cur.rowcount
    print(f"[MIGRATE] Inserted {migrated} URLs into url_validation_tracking")

    # Remove skipped rows from kopteksten_check (keep success/failed/completed/pending)
    cur.execute("""
        DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check
        WHERE status = 'skipped'
    """)
    removed_kopteksten = cur.rowcount
    print(f"[MIGRATE] Removed {removed_kopteksten} skipped rows from kopteksten_check")

    # Remove skipped rows from faq_tracking (keep success/failed)
    # But keep 'main_category_url' skips in faq_tracking (FAQ-specific)
    cur.execute("""
        DELETE FROM pa.faq_tracking
        WHERE status = 'skipped'
          AND (skip_reason IS NULL OR skip_reason != 'main_category_url')
    """)
    removed_faq = cur.rowcount
    print(f"[MIGRATE] Removed {removed_faq} skipped rows from faq_tracking")

    conn.commit()
    cur.close()
    return_db_connection(conn)

    print(f"[MIGRATE] Done! Shared table has {migrated} URLs. "
          f"Removed {removed_kopteksten} from kopteksten_check, {removed_faq} from faq_tracking.")


if __name__ == "__main__":
    migrate()
