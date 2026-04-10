"""
Score unique titles on grammatical correctness and readability (1-10).
Uses OpenAI GPT-4o-mini with 20 concurrent workers, batching 25 titles per request.
Saves scores + issues back to pa.unique_titles.
"""
import os
import sys
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from openai import OpenAI
from backend.database import get_db_connection, return_db_connection

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

BATCH_SIZE = 25  # titles per API call
WORKERS = 20
FETCH_SIZE = 5000  # rows to fetch from DB at a time

# Thread-safe counters
lock = threading.Lock()
stats = {"scored": 0, "errors": 0, "start_time": time.time()}


def sanitize_title(title: str) -> str:
    """Remove/replace characters that could break API requests."""
    if not title:
        return ""
    # Replace null bytes and other control characters
    return ''.join(c if c.isprintable() or c in '\n\t' else ' ' for c in title)


def score_batch(titles: list[dict], retries: int = 2) -> list[dict]:
    """Score a batch of titles using GPT-4o-mini. Returns list of {url, score, issue}."""
    clean_titles = [(i, sanitize_title(t['title'])) for i, t in enumerate(titles)]
    numbered = "\n".join(f"{i+1}. {ct}" for i, (_, ct) in enumerate(clean_titles))

    prompt = (
        "Score each Dutch SEO title on grammatical correctness and readability "
        "(1 = terrible, 10 = perfect).\n"
        "Consider: spelling, grammar, natural word order, readability, awkward repetition, "
        "and overall quality.\n"
        "These are Dutch product category page titles for beslist.nl.\n\n"
        f"Titles:\n{numbered}\n\n"
        'Respond with ONLY a JSON object with a "scores" key containing an array. '
        'Each element: {"n": <number>, "score": <1-10>, "issue": "<brief issue or empty string>"}'
    )

    for attempt in range(retries + 1):
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                response_format={"type": "json_object"},
            )

            content = response.choices[0].message.content.strip()
            parsed = json.loads(content)

            # Handle various response shapes
            if isinstance(parsed, dict):
                results = parsed.get("scores") or parsed.get("results") or list(parsed.values())[0]
            else:
                results = parsed

            scored = []
            for item in results:
                idx = item["n"] - 1
                if 0 <= idx < len(titles):
                    scored.append({
                        "url": titles[idx]["url"],
                        "score": max(1, min(10, int(item["score"]))),
                        "issue": item.get("issue", "")
                    })
            return scored
        except Exception as e:
            if attempt < retries:
                time.sleep(1 * (attempt + 1))
                continue
            raise

    return []


def save_scores(scores: list[dict]):
    """Save scores back to the database."""
    if not scores:
        return
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        from psycopg2.extras import execute_batch
        execute_batch(cur,
            "UPDATE pa.unique_titles SET title_score = %(score)s, title_score_issue = %(issue)s WHERE url = %(url)s",
            [{"score": s["score"], "issue": s["issue"] if s["issue"] else None, "url": s["url"]} for s in scores]
        )
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)


def process_batch(titles: list[dict]):
    """Score a batch and save results."""
    try:
        scores = score_batch(titles)
        save_scores(scores)
        with lock:
            stats["scored"] += len(scores)
    except Exception as e:
        with lock:
            stats["errors"] += 1
        print(f"  [ERROR] Batch failed: {e}")


def fetch_unscored(limit: int) -> list[dict]:
    """Fetch unscored titles from the database."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT url, title FROM pa.unique_titles
            WHERE title_score IS NULL AND title IS NOT NULL AND title != ''
            ORDER BY url
            LIMIT %s
        """, (limit,))
        return [{"url": r["url"], "title": r["title"]} for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def get_remaining_count() -> int:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT count(*) as cnt FROM pa.unique_titles WHERE title_score IS NULL AND title IS NOT NULL AND title != ''")
        return cur.fetchone()["cnt"]
    finally:
        cur.close()
        return_db_connection(conn)


def main():
    remaining = get_remaining_count()
    print(f"[SCORE] {remaining:,} titles remaining to score")
    print(f"[SCORE] Using {WORKERS} workers, {BATCH_SIZE} titles/batch")
    print(f"[SCORE] Estimated API calls: {remaining // BATCH_SIZE:,}")
    print()

    total_start = time.time()
    round_num = 0

    while True:
        rows = fetch_unscored(FETCH_SIZE)
        if not rows:
            print("[SCORE] All titles scored!")
            break

        round_num += 1
        print(f"[ROUND {round_num}] Fetched {len(rows):,} unscored titles")

        # Split into batches of BATCH_SIZE
        batches = [rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = [executor.submit(process_batch, batch) for batch in batches]
            for future in as_completed(futures):
                future.result()  # raise any exceptions

        elapsed = time.time() - total_start
        rate = stats["scored"] / elapsed if elapsed > 0 else 0
        remaining_est = get_remaining_count()
        eta_minutes = (remaining_est / rate / 60) if rate > 0 else 0

        print(f"  Scored so far: {stats['scored']:,} | Errors: {stats['errors']} | "
              f"Rate: {rate:.0f}/s | Remaining: {remaining_est:,} | ETA: {eta_minutes:.0f}m")

    elapsed = time.time() - total_start
    print(f"\n[DONE] Scored {stats['scored']:,} titles in {elapsed/60:.1f} minutes "
          f"({stats['errors']} errors)")


if __name__ == "__main__":
    main()
