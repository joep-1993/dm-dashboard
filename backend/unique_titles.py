"""
Unique Titles Service

Manages custom page titles and descriptions for the website-configuration API.
Stores data in local PostgreSQL and generates CSV for API upload.

Storage (post Big Bang refactor):
  pa.urls                   — catalog (url_id, url)
  pa.unique_titles_content  — h1_title / title / description / original_h1 /
                              title_score / title_score_issue
  pa.unique_titles_jobs     — status / attempts / last_error /
                              http_status / final_url / last_checked_at
"""
import csv
import io
import os
import requests
from datetime import datetime
from typing import List, Dict, Optional
from backend.database import get_db_connection, return_db_connection
from backend.url_catalog import canonicalize_url, get_url_id, bulk_upsert_urls

# API Configuration
UNIQUE_TITLES_API_URL = "https://website-configuration.api.beslist.nl/custom-title-description/import-per-url"
UNIQUE_TITLES_API_KEY = os.getenv("UNIQUE_TITLES_API_KEY", "")


def init_unique_titles_table():
    """No-op after Big Bang — tables created by migration step 1."""
    print("[UNIQUE_TITLES] Tables already initialized via migration; init_unique_titles_table() is a no-op")


def upsert_title(url: str, title: str, description: str, h1_title: str) -> bool:
    """Insert or update a title record. URLs are canonicalized to ensure uniqueness."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        url_id = get_url_id(cur, url)
        if url_id is None:
            print(f"[UNIQUE_TITLES] Cannot canonicalize URL: {url!r}")
            return False
        cur.execute("""
            INSERT INTO pa.unique_titles_content
                (url_id, h1_title, title, description, created_at, updated_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (url_id) DO UPDATE SET
                h1_title = EXCLUDED.h1_title,
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                updated_at = CURRENT_TIMESTAMP
        """, (url_id, h1_title, title, description))
        # Mark job as success
        cur.execute("""
            INSERT INTO pa.unique_titles_jobs (url_id, status, created_at, updated_at)
            VALUES (%s, 'success', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (url_id) DO UPDATE SET
                status = 'success',
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
        """, (url_id,))
        conn.commit()
        return True
    except Exception as e:
        print(f"[UNIQUE_TITLES] Error upserting {url}: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        return_db_connection(conn)


def bulk_upsert_titles(titles: List[Dict]) -> Dict:
    """Bulk insert/update title records.

    Args:
        titles: List of dicts with url, title, description, h1_title

    Returns:
        Dict with success_count and error_count
    """
    conn = get_db_connection()
    cur = conn.cursor()
    success_count = 0
    error_count = 0
    try:
        # Resolve url_ids in one shot
        url_id_by_canon = bulk_upsert_urls(cur, (t.get('url', '') for t in titles))
        for item in titles:
            try:
                canon = canonicalize_url(item.get('url', ''))
                if canon is None:
                    error_count += 1
                    continue
                url_id = url_id_by_canon.get(canon)
                if url_id is None:
                    error_count += 1
                    continue
                cur.execute("""
                    INSERT INTO pa.unique_titles_content
                        (url_id, h1_title, title, description, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (url_id) DO UPDATE SET
                        h1_title = EXCLUDED.h1_title,
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        updated_at = CURRENT_TIMESTAMP
                """, (url_id, item.get('h1_title', ''), item.get('title', ''), item.get('description', '')))
                cur.execute("""
                    INSERT INTO pa.unique_titles_jobs (url_id, status, created_at, updated_at)
                    VALUES (%s, 'success', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (url_id) DO UPDATE SET
                        status = 'success',
                        last_error = NULL,
                        updated_at = CURRENT_TIMESTAMP
                """, (url_id,))
                success_count += 1
            except Exception as e:
                print(f"[UNIQUE_TITLES] Error on row: {e}")
                error_count += 1
        conn.commit()
    except Exception as e:
        print(f"[UNIQUE_TITLES] Bulk upsert error: {e}")
        conn.rollback()
    finally:
        cur.close()
        return_db_connection(conn)
    return {"success_count": success_count, "error_count": error_count}


def queue_urls_for_generation(urls: List[str]) -> Dict:
    """Add URLs to pa.urls + pa.unique_titles_jobs (status=pending).
    Existing jobs are left untouched.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    added = 0
    skipped = 0
    invalid = 0
    try:
        for raw in urls:
            canon = canonicalize_url(raw)
            if canon is None:
                invalid += 1
                continue
            url_id = get_url_id(cur, canon)
            if url_id is None:
                invalid += 1
                continue
            cur.execute("""
                INSERT INTO pa.unique_titles_jobs (url_id, status, created_at, updated_at)
                VALUES (%s, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (url_id) DO NOTHING
            """, (url_id,))
            if cur.rowcount > 0:
                added += 1
            else:
                skipped += 1
        conn.commit()
    except Exception as e:
        print(f"[UNIQUE_TITLES] Queue error: {e}")
        conn.rollback()
    finally:
        cur.close()
        return_db_connection(conn)
    return {"added": added, "skipped": skipped, "invalid": invalid}


def get_all_titles(limit: int = 0) -> List[Dict]:
    """Get all titles from database (rows with at least h1_title or title)."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        query = """
            SELECT u.url, c.title, c.description, c.h1_title, c.created_at
            FROM pa.unique_titles_content c
            JOIN pa.urls u ON c.url_id = u.url_id
            WHERE c.h1_title IS NOT NULL OR c.title IS NOT NULL
            ORDER BY u.url
        """
        params = ()
        if isinstance(limit, int) and limit > 0:
            query += " LIMIT %s"
            params = (limit,)
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def get_titles_count() -> int:
    """Count rows with at least h1_title or title populated."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT COUNT(*) AS count
            FROM pa.unique_titles_content
            WHERE h1_title IS NOT NULL OR title IS NOT NULL
        """)
        return cur.fetchone()['count']
    finally:
        cur.close()
        return_db_connection(conn)


def generate_csv_for_upload() -> str:
    """Generate semicolon-separated CSV of all titles for the website-configuration API."""
    titles = get_all_titles()
    output = io.StringIO()
    writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['id', 'url', 'title', 'description', 'h1_title', 'active', 'created_at', 'updated_at'])
    for idx, item in enumerate(titles, start=1):
        created_at_str = ''
        if item.get('created_at'):
            if isinstance(item['created_at'], datetime):
                created_at_str = item['created_at'].strftime('%d-%m-%Y')
            else:
                created_at_str = str(item['created_at'])
        writer.writerow([
            idx,
            item.get('url', ''),
            item.get('title', ''),
            item.get('description', ''),
            item.get('h1_title', ''),
            1,
            created_at_str,
            ''
        ])
    return output.getvalue()


def upload_titles_to_api() -> Dict:
    """Generate CSV from database and upload to the website-configuration API."""
    csv_content = generate_csv_for_upload()
    csv_bytes = csv_content.encode('utf-8')
    row_count = csv_content.count('\n') - 1
    print(f"[UNIQUE_TITLES] Uploading {row_count} titles to API...")
    headers = {"X-Api-Key": UNIQUE_TITLES_API_KEY}
    files = {'file': ('unique_titles.csv', csv_bytes, 'text/csv')}
    try:
        response = requests.post(UNIQUE_TITLES_API_URL, headers=headers, files=files, timeout=600)
        result = {
            "success": response.status_code in (200, 201),
            "status_code": response.status_code,
            "rows_uploaded": row_count,
            "api_response": response.json() if response.headers.get('content-type', '').startswith('application/json') else response.text[:500]
        }
        if result["success"]:
            print(f"[UNIQUE_TITLES] Upload successful: {result['api_response']}")
        else:
            print(f"[UNIQUE_TITLES] Upload failed: {response.status_code} - {response.text[:200]}")
        return result
    except Exception as e:
        print(f"[UNIQUE_TITLES] Upload error: {e}")
        return {"success": False, "error": str(e), "rows_uploaded": row_count}


def delete_title(url: str) -> bool:
    """Delete unique_titles content + job for a URL. The catalog (pa.urls) row stays
    because other tools (Kopteksten, FAQ) may reference it.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        canon = canonicalize_url(url)
        if canon is None:
            return False
        cur.execute("SELECT url_id FROM pa.urls WHERE url = %s", (canon,))
        row = cur.fetchone()
        if not row:
            return False
        url_id = row['url_id']
        cur.execute("DELETE FROM pa.unique_titles_content WHERE url_id = %s", (url_id,))
        deleted_content = cur.rowcount
        cur.execute("DELETE FROM pa.unique_titles_jobs WHERE url_id = %s", (url_id,))
        conn.commit()
        return deleted_content > 0
    finally:
        cur.close()
        return_db_connection(conn)


def search_titles(query: str, limit: int = 100) -> List[Dict]:
    """Search titles by URL or title content (ILIKE). Exact-match URLs sort first."""
    # Strip domain prefix so full URLs match relative paths in DB
    search_query = query
    for prefix in ("https://www.beslist.nl", "http://www.beslist.nl",
                   "https://beslist.nl", "http://beslist.nl"):
        if search_query.lower().startswith(prefix):
            search_query = search_query[len(prefix):]
            break
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT u.url, c.title, c.description, c.h1_title, c.created_at,
                CASE
                    WHEN LOWER(u.url) = LOWER(%s) THEN 0
                    WHEN LOWER(u.url) LIKE LOWER(%s) THEN 1
                    ELSE 2
                END AS sort_rank
            FROM pa.unique_titles_content c
            JOIN pa.urls u ON c.url_id = u.url_id
            WHERE u.url ILIKE %s OR c.title ILIKE %s
            ORDER BY sort_rank, u.url
            LIMIT %s
        """, (search_query, f'%{search_query}%', f'%{search_query}%', f'%{search_query}%', limit))
        rows = cur.fetchall()
        return [{k: v for k, v in dict(row).items() if k != 'sort_rank'} for row in rows]
    finally:
        cur.close()
        return_db_connection(conn)
