"""
Unique Titles Service

Manages custom page titles and descriptions for the website-configuration API.
Stores data in local PostgreSQL and generates CSV for API upload.
"""
import csv
import io
import requests
from datetime import datetime
from typing import List, Dict, Optional
from backend.database import get_db_connection, return_db_connection

# API Configuration
UNIQUE_TITLES_API_URL = "https://website-configuration.api.beslist.nl/custom-title-description/import-per-url"
UNIQUE_TITLES_API_KEY = "Sectional~Publisher~Dumpling1"


def init_unique_titles_table():
    """Create the unique_titles table if it doesn't exist."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pa.unique_titles (
                url VARCHAR(2000) PRIMARY KEY,
                title TEXT,
                description TEXT,
                h1_title VARCHAR(500),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("[UNIQUE_TITLES] Table initialized")
    finally:
        cur.close()
        return_db_connection(conn)


def upsert_title(url: str, title: str, description: str, h1_title: str) -> bool:
    """
    Insert or update a title record.
    Updates created_at timestamp on every change.
    URLs are stored in lowercase to ensure case-insensitive uniqueness.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Convert URL to lowercase for case-insensitive uniqueness
    url_lower = url.lower() if url else ''

    try:
        cur.execute("""
            INSERT INTO pa.unique_titles (url, title, description, h1_title, created_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (url) DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                h1_title = EXCLUDED.h1_title,
                created_at = CURRENT_TIMESTAMP
        """, (url_lower, title, description, h1_title))
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
    """
    Bulk insert/update title records.
    URLs are stored in lowercase to ensure case-insensitive uniqueness.

    Args:
        titles: List of dicts with url, title, description, h1_title

    Returns:
        Dict with success count and error count
    """
    conn = get_db_connection()
    cur = conn.cursor()

    success_count = 0
    error_count = 0

    try:
        for item in titles:
            try:
                # Convert URL to lowercase for case-insensitive uniqueness
                url_lower = item.get('url', '').lower()
                cur.execute("""
                    INSERT INTO pa.unique_titles (url, title, description, h1_title, created_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (url) DO UPDATE SET
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        h1_title = EXCLUDED.h1_title,
                        created_at = CURRENT_TIMESTAMP
                """, (
                    url_lower,
                    item.get('title', ''),
                    item.get('description', ''),
                    item.get('h1_title', '')
                ))
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

    return {
        "success_count": success_count,
        "error_count": error_count
    }


def get_all_titles() -> List[Dict]:
    """Get all titles from database."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT url, title, description, h1_title, created_at
            FROM pa.unique_titles
            WHERE h1_title IS NOT NULL OR title IS NOT NULL
            ORDER BY url
        """)
        rows = cur.fetchall()
        return [dict(row) for row in rows]
    finally:
        cur.close()
        return_db_connection(conn)


def get_titles_count() -> int:
    """Get total count of titles in database."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) as count FROM pa.unique_titles WHERE h1_title IS NOT NULL OR title IS NOT NULL")
        return cur.fetchone()['count']
    finally:
        cur.close()
        return_db_connection(conn)


def generate_csv_for_upload() -> str:
    """
    Generate CSV content for API upload from database.

    Format: id;url;title;description;h1_title;active;created_at;updated_at
    - id: auto-generated starting from 1
    - active: always 1
    - updated_at: empty
    - created_at: from database
    """
    titles = get_all_titles()

    output = io.StringIO()
    writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)

    # Header
    writer.writerow(['id', 'url', 'title', 'description', 'h1_title', 'active', 'created_at', 'updated_at'])

    # Data rows
    for idx, item in enumerate(titles, start=1):
        created_at_str = ''
        if item.get('created_at'):
            if isinstance(item['created_at'], datetime):
                created_at_str = item['created_at'].strftime('%d-%m-%Y')
            else:
                created_at_str = str(item['created_at'])

        writer.writerow([
            idx,                          # id
            item.get('url', ''),          # url
            item.get('title', ''),        # title
            item.get('description', ''),  # description
            item.get('h1_title', ''),     # h1_title
            1,                            # active (always 1)
            created_at_str,               # created_at
            ''                            # updated_at (empty)
        ])

    return output.getvalue()


def upload_titles_to_api() -> Dict:
    """
    Generate CSV from database and upload to the API.

    Returns:
        Dict with upload result
    """
    # Generate CSV
    csv_content = generate_csv_for_upload()
    csv_bytes = csv_content.encode('utf-8')

    # Count rows (excluding header)
    row_count = csv_content.count('\n') - 1

    print(f"[UNIQUE_TITLES] Uploading {row_count} titles to API...")

    headers = {
        "X-Api-Key": UNIQUE_TITLES_API_KEY,
    }

    files = {
        'file': ('unique_titles.csv', csv_bytes, 'text/csv')
    }

    try:
        response = requests.post(
            UNIQUE_TITLES_API_URL,
            headers=headers,
            files=files,
            timeout=600  # 10 minute timeout
        )

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
        return {
            "success": False,
            "error": str(e),
            "rows_uploaded": row_count
        }


def delete_title(url: str) -> bool:
    """Delete a title record by URL."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("DELETE FROM pa.unique_titles WHERE url = %s", (url,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        return_db_connection(conn)


def search_titles(query: str, limit: int = 100) -> List[Dict]:
    """Search titles by URL or title content."""
    # Strip domain prefix so full URLs match relative paths in DB
    search_query = query
    for prefix in ("https://www.beslist.nl", "http://www.beslist.nl", "https://beslist.nl", "http://beslist.nl"):
        if search_query.lower().startswith(prefix):
            search_query = search_query[len(prefix):]
            break

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT url, title, description, h1_title, created_at,
                CASE
                    WHEN LOWER(url) = LOWER(%s) THEN 0
                    WHEN LOWER(url) LIKE LOWER(%s) THEN 1
                    ELSE 2
                END AS sort_rank
            FROM pa.unique_titles
            WHERE url ILIKE %s OR title ILIKE %s
            ORDER BY sort_rank, url
            LIMIT %s
        """, (search_query, f'%{search_query}%', f'%{search_query}%', f'%{search_query}%', limit))
        rows = cur.fetchall()
        return [{k: v for k, v in dict(row).items() if k != 'sort_rank'} for row in rows]
    finally:
        cur.close()
        return_db_connection(conn)
