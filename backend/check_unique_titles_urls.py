"""
Check status codes for all URLs in unique_titles table.

Rate limited to max 2 URLs/second with configurable parallel workers.
Stores status_code and final_url (if redirected) in the database.
"""
import asyncio
import aiohttp
import time
import sys
from datetime import datetime
from backend.database import get_db_connection, return_db_connection

def log(msg):
    """Print with immediate flush."""
    print(msg)
    sys.stdout.flush()

# Configuration
USER_AGENT = "Beslist script voor SEO"
MAX_REQUESTS_PER_SECOND = 4
DEFAULT_WORKERS = 20
REQUEST_TIMEOUT = 30
BASE_URL = "https://www.beslist.nl"


def add_status_columns():
    """Add status_code and final_url columns if they don't exist."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Add status_code column
        cur.execute("""
            ALTER TABLE pa.unique_titles
            ADD COLUMN IF NOT EXISTS status_code INTEGER
        """)

        # Add final_url column (for redirects)
        cur.execute("""
            ALTER TABLE pa.unique_titles
            ADD COLUMN IF NOT EXISTS final_url VARCHAR(2000)
        """)

        # Add checked_at column
        cur.execute("""
            ALTER TABLE pa.unique_titles
            ADD COLUMN IF NOT EXISTS checked_at TIMESTAMP
        """)

        conn.commit()
        log("[URL_CHECK] Added status_code, final_url, and checked_at columns")
    except Exception as e:
        log(f"[URL_CHECK] Error adding columns: {e}")
        conn.rollback()
    finally:
        cur.close()
        return_db_connection(conn)


def get_unchecked_urls(limit: int = None):
    """Get URLs that haven't been checked yet."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        if limit:
            cur.execute("""
                SELECT url FROM pa.unique_titles
                WHERE status_code IS NULL
                ORDER BY url
                LIMIT %s
            """, (limit,))
        else:
            cur.execute("""
                SELECT url FROM pa.unique_titles
                WHERE status_code IS NULL
                ORDER BY url
            """)
        rows = cur.fetchall()
        return [row['url'] for row in rows]
    finally:
        cur.close()
        return_db_connection(conn)


def get_total_url_count():
    """Get total count of URLs."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) as count FROM pa.unique_titles")
        return cur.fetchone()['count']
    finally:
        cur.close()
        return_db_connection(conn)


def update_url_status(url: str, status_code: int, final_url: str = None):
    """Update the status code and final URL for a given URL."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE pa.unique_titles
            SET status_code = %s, final_url = %s, checked_at = CURRENT_TIMESTAMP
            WHERE url = %s
        """, (status_code, final_url, url))
        conn.commit()
    except Exception as e:
        log(f"[URL_CHECK] Error updating {url}: {e}")
        conn.rollback()
    finally:
        cur.close()
        return_db_connection(conn)


def batch_update_url_status(results: list):
    """Batch update status codes for multiple URLs."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        for url, status_code, final_url in results:
            cur.execute("""
                UPDATE pa.unique_titles
                SET status_code = %s, final_url = %s, checked_at = CURRENT_TIMESTAMP
                WHERE url = %s
            """, (status_code, final_url, url))
        conn.commit()
    except Exception as e:
        log(f"[URL_CHECK] Batch update error: {e}")
        conn.rollback()
    finally:
        cur.close()
        return_db_connection(conn)


class RateLimiter:
    """Rate limiter to control requests per second."""

    def __init__(self, max_per_second: float):
        self.max_per_second = max_per_second
        self.min_interval = 1.0 / max_per_second
        self.last_request_time = 0
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_request_time
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self.last_request_time = time.time()


async def check_url(session: aiohttp.ClientSession, url: str, rate_limiter: RateLimiter):
    """Check a single URL and return status code and final URL."""
    await rate_limiter.acquire()

    # Build full URL
    if url.startswith('/'):
        full_url = f"{BASE_URL}{url}"
    elif url.startswith('http'):
        full_url = url
    else:
        full_url = f"{BASE_URL}/{url}"

    try:
        async with session.get(full_url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as response:
            final_url = str(response.url)

            # Only store final_url if it's different from original
            if final_url == full_url:
                final_url = None

            return (url, response.status, final_url)
    except asyncio.TimeoutError:
        return (url, -1, None)  # -1 for timeout
    except aiohttp.ClientError as e:
        return (url, -2, None)  # -2 for connection error
    except Exception as e:
        log(f"[URL_CHECK] Error checking {url}: {e}")
        return (url, -3, None)  # -3 for other errors


async def process_urls(urls: list, num_workers: int = DEFAULT_WORKERS, max_rps: float = MAX_REQUESTS_PER_SECOND):
    """Process all URLs with rate limiting and parallel workers."""
    rate_limiter = RateLimiter(max_rps)

    headers = {
        'User-Agent': USER_AGENT
    }

    connector = aiohttp.TCPConnector(limit=num_workers)

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        total = len(urls)
        completed = 0
        results_buffer = []
        batch_size = 100

        status_counts = {200: 0, 301: 0, 302: 0, 404: 0, 500: 0, 502: 0, 503: 0, -1: 0, -2: 0, -3: 0}

        start_time = time.time()

        # Create tasks for all URLs
        tasks = [check_url(session, url, rate_limiter) for url in urls]

        # Process with progress updates
        for coro in asyncio.as_completed(tasks):
            result = await coro
            url, status, final_url = result

            results_buffer.append(result)
            completed += 1

            # Track status counts
            if status in status_counts:
                status_counts[status] += 1
            else:
                status_counts[status] = 1

            # Batch update to database
            if len(results_buffer) >= batch_size:
                batch_update_url_status(results_buffer)
                results_buffer = []

            # Progress update
            if completed % 1000 == 0 or completed == total:
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                pct = (completed / total) * 100
                log(f"[URL_CHECK] Progress: {completed:,}/{total:,} ({pct:.1f}%) - {rate:.1f} URLs/sec")

        # Final batch update
        if results_buffer:
            batch_update_url_status(results_buffer)

        elapsed = time.time() - start_time

        log("\n[URL_CHECK] === FINAL RESULTS ===")
        log(f"Total URLs checked: {total:,}")
        log(f"Time elapsed: {elapsed:.1f} seconds")
        log(f"Average rate: {total/elapsed:.1f} URLs/sec")
        log("\nStatus code breakdown:")
        for status, count in sorted(status_counts.items()):
            if count > 0:
                status_name = {
                    -1: "Timeout",
                    -2: "Connection Error",
                    -3: "Other Error",
                    200: "OK",
                    301: "Moved Permanently",
                    302: "Found (Redirect)",
                    404: "Not Found",
                    500: "Server Error",
                    502: "Bad Gateway",
                    503: "Service Unavailable"
                }.get(status, f"HTTP {status}")
                log(f"  {status_name}: {count:,}")

        return status_counts


def get_status_summary():
    """Get summary of status codes in database."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT status_code, COUNT(*) as count
            FROM pa.unique_titles
            GROUP BY status_code
            ORDER BY status_code
        """)
        rows = cur.fetchall()
        return {row['status_code']: row['count'] for row in rows}
    finally:
        cur.close()
        return_db_connection(conn)


async def main(num_workers: int = DEFAULT_WORKERS, max_rps: float = MAX_REQUESTS_PER_SECOND):
    """Main function to check all URLs."""
    log(f"[URL_CHECK] Starting URL status check")
    log(f"[URL_CHECK] Workers: {num_workers}, Max RPS: {max_rps}")
    log(f"[URL_CHECK] User-Agent: {USER_AGENT}")

    # Add columns if needed
    add_status_columns()

    # Get total count
    total = get_total_url_count()
    log(f"[URL_CHECK] Total URLs in database: {total:,}")

    # Get all URLs
    urls = get_unchecked_urls()
    log(f"[URL_CHECK] URLs to check: {len(urls):,}")

    if not urls:
        log("[URL_CHECK] No URLs to check")
        return

    # Process URLs
    await process_urls(urls, num_workers, max_rps)

    # Show final summary
    log("\n[URL_CHECK] === DATABASE SUMMARY ===")
    summary = get_status_summary()
    for status, count in sorted(summary.items()):
        if status is not None:
            log(f"  Status {status}: {count:,}")
        else:
            log(f"  Not checked: {count:,}")


if __name__ == "__main__":
    asyncio.run(main(num_workers=20, max_rps=4))
