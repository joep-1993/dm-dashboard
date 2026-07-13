"""
IndexNow Service

Submits URLs to the IndexNow API for rapid search engine indexing.
Deduplicates against previously submitted URLs stored in Redshift
(pa.index_now_joep) — the same table the daily n8n indexnow_submitter flow
writes to, so the manual path and the automated flow share one source of truth.
"""
import math
import json
import requests
from datetime import datetime
from typing import List, Dict
from backend.database import (
    get_redshift_connection,
    return_redshift_connection,
)

# IndexNow settings
KEY = "2e11f87f415a492294eaf378a8a52004"
KEY_LOCATION = "https://www.beslist.nl/2e11f87f415a492294eaf378a8a52004.txt"
HOST = "www.beslist.nl"
BATCH_SIZE = 10000
DAILY_LIMIT = 10000
TABLE = "pa.index_now_joep"


def ensure_table_exists():
    """Ensure the tracking table exists on Redshift.

    The whole IndexNow path (manual submit + the daily n8n flow) shares one
    table: pa.index_now_joep on Redshift. It already exists and is maintained
    by n8n, so this is normally a no-op; the CREATE guards a fresh environment.
    Redshift has no CREATE INDEX, so none are declared here. `id` is an IDENTITY
    column, so inserts omit it.
    """
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id BIGINT IDENTITY(1,1),
                url VARCHAR(2000) NOT NULL,
                submitted_date DATE NOT NULL,
                response_code INTEGER
            )
        """)
        conn.commit()
        cur.close()
    finally:
        return_redshift_connection(conn)


def get_existing_urls() -> set:
    """Get all previously submitted URLs (from Redshift)."""
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT url FROM {TABLE}")
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


def _save_submissions(urls: List[str], response_code: int):
    """Write submitted URLs to the tracking table on Redshift.

    Redshift is columnar — single-row INSERTs are pathologically slow, so we
    write multi-row VALUES statements in chunks (matching how the n8n flow
    bulk-inserts).
    """
    if not urls:
        return
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        today = datetime.today().date()
        CHUNK = 1000
        for start in range(0, len(urls), CHUNK):
            chunk = urls[start : start + CHUNK]
            placeholders = ",".join(["(%s, %s, %s)"] * len(chunk))
            params = []
            for url in chunk:
                params.extend([url, today, response_code])
            cur.execute(
                f"INSERT INTO {TABLE} (url, submitted_date, response_code) VALUES {placeholders}",
                params,
            )
        conn.commit()
        cur.close()
    finally:
        return_redshift_connection(conn)


def get_today_count() -> int:
    """Get the number of URLs submitted today (from Redshift)."""
    ensure_table_exists()
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        today = datetime.today().date()
        cur.execute(
            f"SELECT COUNT(*) as cnt FROM {TABLE} WHERE submitted_date = %s AND response_code = 200",
            (today,)
        )
        row = cur.fetchone()
        cur.close()
        return row["cnt"] if row else 0
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
            "today_count": get_today_count(),
            "daily_limit": DAILY_LIMIT,
            "message": "No new URLs to submit — all already submitted previously.",
        }

    # Enforce daily limit
    today_count = get_today_count()
    remaining = max(0, DAILY_LIMIT - today_count)
    truncated = 0
    if remaining == 0:
        return {
            "status": "error",
            "total_input": len(urls),
            "new_urls": len(new_urls),
            "skipped_duplicates": skipped,
            "today_count": today_count,
            "daily_limit": DAILY_LIMIT,
            "batches": [],
            "message": f"Daily limit reached ({DAILY_LIMIT:,} URLs). Try again tomorrow.",
        }
    if len(new_urls) > remaining:
        truncated = len(new_urls) - remaining
        new_urls = new_urls[:remaining]

    num_batches = math.ceil(len(new_urls) / BATCH_SIZE)
    batches = []

    for i in range(num_batches):
        start = i * BATCH_SIZE
        batch = new_urls[start : start + BATCH_SIZE]
        response_code = _send_batch(batch)
        _save_submissions(batch, response_code)
        batches.append({
            "batch_number": i + 1,
            "urls_count": len(batch),
            "response_code": response_code,
            "success": response_code == 200,
        })

    total_submitted = sum(b["urls_count"] for b in batches if b["success"])
    total_failed = sum(b["urls_count"] for b in batches if not b["success"])

    result = {
        "status": "success",
        "total_input": len(urls),
        "new_urls": len(new_urls),
        "skipped_duplicates": skipped,
        "total_submitted": total_submitted,
        "total_failed": total_failed,
        "today_count": today_count + total_submitted,
        "daily_limit": DAILY_LIMIT,
        "batches": batches,
    }
    if truncated > 0:
        result["truncated"] = truncated
        result["message"] = f"{truncated:,} URLs skipped due to daily limit ({DAILY_LIMIT:,})."
    return result


def get_submission_history(limit: int = 100) -> List[Dict]:
    """Get recent submission history.

    Reads from the Redshift copy of pa.index_now_joep — that is where the
    daily n8n `indexnow_submitter` flow logs its runs (it fetches candidate
    URLs from Redshift's datamart.* and reuses that same connection to log).
    The PostgreSQL copy that the manual submit path writes to stopped being
    fed on 2026-03-27, so history must come from Redshift to reflect the live
    (n8n-driven) submissions.
    """
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT submitted_date, response_code, COUNT(*) as url_count
            FROM {TABLE}
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
