"""
IndexNow Service

Submits URLs to the IndexNow API for rapid search engine indexing.
Deduplicates against previously submitted URLs stored in Redshift.
"""
import math
import json
import requests
from datetime import datetime
from typing import List, Dict
from backend.database import get_redshift_connection, return_redshift_connection

# IndexNow settings
KEY = "2e11f87f415a492294eaf378a8a52004"
KEY_LOCATION = "https://www.beslist.nl/2e11f87f415a492294eaf378a8a52004.txt"
HOST = "www.beslist.nl"
BATCH_SIZE = 10000
REDSHIFT_TABLE = "pa.index_now_joep"


def ensure_table_exists():
    """Create the Redshift tracking table if it doesn't exist."""
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {REDSHIFT_TABLE} (
                url VARCHAR(2000),
                submitted_date DATE,
                response_code INTEGER
            )
        """)
        conn.commit()
        cur.close()
    finally:
        return_redshift_connection(conn)


def get_existing_urls() -> set:
    """Get all previously submitted URLs from Redshift."""
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT url FROM {REDSHIFT_TABLE}")
        urls = set(row["url"] for row in cur.fetchall())
        cur.close()
        return urls
    finally:
        return_redshift_connection(conn)


def _send_batch(urls: List[str]) -> int:
    """Send a batch of URLs to the IndexNow API. Returns HTTP status code."""
    payload = {
        "host": HOST,
        "key": KEY,
        "keyLocation": KEY_LOCATION,
        "urlList": urls,
    }
    response = requests.post(
        "https://api.indexnow.org/IndexNow",
        headers={"Content-Type": "application/json; charset=utf-8"},
        data=json.dumps(payload),
        timeout=30,
    )
    return response.status_code


def _save_to_redshift(urls: List[str], response_code: int):
    """Write submitted URLs to the Redshift tracking table."""
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        today = datetime.today().date()
        for url in urls:
            cur.execute(
                f"INSERT INTO {REDSHIFT_TABLE} (url, submitted_date, response_code) VALUES (%s, %s, %s)",
                (url, today, response_code),
            )
        conn.commit()
        cur.close()
    finally:
        return_redshift_connection(conn)


def submit_urls(urls: List[str]) -> Dict:
    """
    Submit URLs to IndexNow API, deduplicating against previously submitted URLs.

    Returns dict with submission results and stats.
    """
    ensure_table_exists()
    existing = get_existing_urls()

    new_urls = [u for u in urls if u not in existing]
    skipped = len(urls) - len(new_urls)

    if not new_urls:
        return {
            "status": "success",
            "total_input": len(urls),
            "new_urls": 0,
            "skipped_duplicates": skipped,
            "batches": [],
            "message": "No new URLs to submit — all already submitted previously.",
        }

    num_batches = math.ceil(len(new_urls) / BATCH_SIZE)
    batches = []

    for i in range(num_batches):
        start = i * BATCH_SIZE
        batch = new_urls[start : start + BATCH_SIZE]
        response_code = _send_batch(batch)
        _save_to_redshift(batch, response_code)
        batches.append({
            "batch_number": i + 1,
            "urls_count": len(batch),
            "response_code": response_code,
            "success": response_code == 200,
        })

    total_submitted = sum(b["urls_count"] for b in batches if b["success"])
    total_failed = sum(b["urls_count"] for b in batches if not b["success"])

    return {
        "status": "success",
        "total_input": len(urls),
        "new_urls": len(new_urls),
        "skipped_duplicates": skipped,
        "total_submitted": total_submitted,
        "total_failed": total_failed,
        "batches": batches,
    }


def get_submission_history(limit: int = 100) -> List[Dict]:
    """Get recent submission history from Redshift."""
    ensure_table_exists()
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT submitted_date, response_code, COUNT(*) as url_count
            FROM {REDSHIFT_TABLE}
            GROUP BY submitted_date, response_code
            ORDER BY submitted_date DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "date": str(row["submitted_date"]),
                "response_code": row["response_code"],
                "url_count": row["url_count"],
            }
            for row in rows
        ]
    finally:
        return_redshift_connection(conn)
