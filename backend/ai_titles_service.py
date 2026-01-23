"""
AI Title Generation Service

Generates SEO-optimized titles using OpenAI based on the N8N workflow.
Processes URLs from unique_titles that need AI-generated titles.
"""
import os
import re
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from openai import OpenAI

from backend.database import get_db_connection, return_db_connection

# Configuration
USER_AGENT = "Beslist script voor SEO"
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")
BASE_URL = "https://www.beslist.nl"

# Processing state
_processing_state = {
    "is_running": False,
    "should_stop": False,
    "total_urls": 0,
    "processed": 0,
    "successful": 0,
    "failed": 0,
    "skipped": 0,
    "current_url": None,
    "started_at": None,
    "last_error": None,
}
_state_lock = threading.Lock()

# Reusable OpenAI client
_openai_client = None


def get_openai_client() -> OpenAI:
    """Get or create the shared OpenAI client."""
    global _openai_client
    if _openai_client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key:
            _openai_client = OpenAI(api_key=api_key)
    return _openai_client


def create_http_session() -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=5, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_http_session = create_http_session()


def init_ai_titles_columns():
    """Add AI processing columns to unique_titles if they don't exist."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Add ai_processed column if it doesn't exist
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'pa' AND table_name = 'unique_titles' AND column_name = 'ai_processed'
                ) THEN
                    ALTER TABLE pa.unique_titles ADD COLUMN ai_processed BOOLEAN DEFAULT FALSE;
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'pa' AND table_name = 'unique_titles' AND column_name = 'ai_processed_at'
                ) THEN
                    ALTER TABLE pa.unique_titles ADD COLUMN ai_processed_at TIMESTAMP;
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'pa' AND table_name = 'unique_titles' AND column_name = 'ai_error'
                ) THEN
                    ALTER TABLE pa.unique_titles ADD COLUMN ai_error TEXT;
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'pa' AND table_name = 'unique_titles' AND column_name = 'original_h1'
                ) THEN
                    ALTER TABLE pa.unique_titles ADD COLUMN original_h1 TEXT;
                END IF;
            END $$;
        """)
        conn.commit()
        print("[AI_TITLES] Columns initialized")
    except Exception as e:
        print(f"[AI_TITLES] Error initializing columns: {e}")
        conn.rollback()
    finally:
        cur.close()
        return_db_connection(conn)


def get_processing_status() -> Dict:
    """Get current AI title processing status."""
    with _state_lock:
        return {
            "is_running": _processing_state["is_running"],
            "total_urls": _processing_state["total_urls"],
            "processed": _processing_state["processed"],
            "successful": _processing_state["successful"],
            "failed": _processing_state["failed"],
            "skipped": _processing_state["skipped"],
            "current_url": _processing_state["current_url"],
            "started_at": _processing_state["started_at"].isoformat() if _processing_state["started_at"] else None,
            "last_error": _processing_state["last_error"],
        }


def get_unprocessed_urls(limit: int = 100) -> List[Dict]:
    """Get URLs that need AI title processing.

    Args:
        limit: Maximum URLs to return. If 0, returns all pending URLs.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Get URLs where:
        # - ai_processed is FALSE or NULL
        # - AND (title is empty/null OR h1_title is empty/null)
        if limit > 0:
            cur.execute("""
                SELECT url, title, description, h1_title
                FROM pa.unique_titles
                WHERE (ai_processed IS NULL OR ai_processed = FALSE)
                AND (title IS NULL OR title = '' OR h1_title IS NULL OR h1_title = '')
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
        else:
            # limit=0 means get all pending URLs
            cur.execute("""
                SELECT url, title, description, h1_title
                FROM pa.unique_titles
                WHERE (ai_processed IS NULL OR ai_processed = FALSE)
                AND (title IS NULL OR title = '' OR h1_title IS NULL OR h1_title = '')
                ORDER BY created_at DESC
            """)
        rows = cur.fetchall()
        return [dict(row) for row in rows]
    finally:
        cur.close()
        return_db_connection(conn)


def get_unprocessed_count() -> int:
    """Get count of URLs needing processing."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT COUNT(*) as count
            FROM pa.unique_titles
            WHERE (ai_processed IS NULL OR ai_processed = FALSE)
            AND (title IS NULL OR title = '' OR h1_title IS NULL OR h1_title = '')
        """)
        return cur.fetchone()['count']
    finally:
        cur.close()
        return_db_connection(conn)


def scrape_page_h1(url: str) -> Optional[Dict]:
    """
    Scrape a Beslist page to extract H1 title and discount.

    Returns dict with h1_title and discount, or None on failure.
    """
    try:
        # Build full URL
        full_url = url if url.startswith('http') else f"{BASE_URL}{url}"

        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        response = _http_session.get(full_url, headers=headers, timeout=30)

        if response.status_code != 200:
            print(f"[AI_TITLES] HTTP {response.status_code} for {url}")
            return None

        html = response.text

        # Extract H1 title (using the CSS class from the N8N flow)
        h1_match = re.search(r'<h1[^>]*class="[^"]*productsTitle[^"]*"[^>]*>(.*?)</h1>', html, re.IGNORECASE | re.DOTALL)
        if not h1_match:
            # Fallback: try any h1
            h1_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.IGNORECASE | re.DOTALL)

        h1_title = h1_match.group(1).strip() if h1_match else None

        # Clean H1 from HTML tags
        if h1_title:
            h1_title = re.sub(r'<[^>]+>', '', h1_title).strip()

        # Extract max discount from page
        discount_matches = re.findall(r'<div class="discountLabel[^"]*">-(\d+)(?:<!--.*?-->)?%</div>', html)
        discounts = [int(d) for d in discount_matches]
        discount = max(discounts) if discounts else None

        return {
            "h1_title": h1_title,
            "discount": discount,
        }

    except Exception as e:
        print(f"[AI_TITLES] Scrape error for {url}: {e}")
        return None


def generate_ai_title(h1_title: str, url: str) -> Optional[Dict]:
    """
    Use OpenAI to generate an improved H1 title.

    Based on the N8N flow prompt:
    - Reorder words for better grammar
    - Put brand first
    - Use adjective forms for materials/colors
    """
    client = get_openai_client()
    if not client:
        print("[AI_TITLES] No OpenAI API key configured")
        return None

    prompt = f"""Je bent een SEO-expert. Maak van '{h1_title}' een goedlopende en grammaticaal correcte titel zonder "-". Gebruik alléén de woorden die je krijgt - bedenk geen woorden zelf. Overbodige woorden mag je weglaten. Je mag de volgorde aanpassen om een beter lopende zin te maken.

Regels:
1. Zorg dat het merk ALTIJD vooraan in de titel staat, dus "Apple iPhones" in plaats van "iPhones van Apple".
2. Gebruik ALTIJD bijvoeglijke naamwoorden voor materialen en kleuren. NOOIT "in" of "van" gebruiken.
   - FOUT: "fonteinkranen in zilver en messing" of "fonteinkranen van messing"
   - GOED: "Zilveren messing fonteinkranen"
   - FOUT: "bank in hout" of "bank van hout"
   - GOED: "Houten bank"
   - FOUT: "schoenen in rood"
   - GOED: "Rode schoenen"
3. Zet kleuren en materialen als bijvoeglijk naamwoord VOOR het zelfstandig naamwoord.

Voorbeeld:
"Schoenen - Nike - Rode - Met veters" wordt "Rode Nike schoenen met veters".
"Saniclear - Zilver - Messing - Design Fonteinkranen" wordt "Zilveren messing Saniclear design fonteinkranen".

Ik wil het antwoord graag in dit json formaat terug:
{{"oude_titel": "{h1_title}", "h1_title": "nieuwe_titel_hier", "url": "{url}"}}

Geef ALLEEN de JSON terug, geen andere tekst."""

    try:
        response = client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.7,
            response_format={"type": "json_object"}
        )

        content = response.choices[0].message.content.strip()

        # Parse JSON response
        import json
        result = json.loads(content)

        return {
            "h1_title": result.get("h1_title", h1_title),
            "original_h1": result.get("oude_titel", h1_title),
        }

    except Exception as e:
        print(f"[AI_TITLES] OpenAI error: {e}")
        return None


def update_title_record(url: str, h1_title: str, title: str, description: str, original_h1: str = None, error: str = None):
    """Update a unique_titles record with AI-generated content."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        if error:
            cur.execute("""
                UPDATE pa.unique_titles
                SET ai_processed = TRUE,
                    ai_processed_at = CURRENT_TIMESTAMP,
                    ai_error = %s
                WHERE url = %s
            """, (error, url))
        else:
            cur.execute("""
                UPDATE pa.unique_titles
                SET h1_title = %s,
                    title = %s,
                    description = %s,
                    original_h1 = %s,
                    ai_processed = TRUE,
                    ai_processed_at = CURRENT_TIMESTAMP,
                    ai_error = NULL
                WHERE url = %s
            """, (h1_title, title, description, original_h1, url))

        conn.commit()
        return True
    except Exception as e:
        print(f"[AI_TITLES] DB update error for {url}: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        return_db_connection(conn)


def process_single_url(url: str) -> Dict:
    """Process a single URL for AI title generation."""
    result = {"url": url, "status": "pending"}

    try:
        # Step 1: Scrape page for H1
        scraped = scrape_page_h1(url)

        if not scraped or not scraped.get("h1_title"):
            result["status"] = "failed"
            result["reason"] = "Could not extract H1 from page"
            update_title_record(url, None, None, None, error="scrape_failed")
            return result

        h1_title = scraped["h1_title"]
        discount = scraped.get("discount")

        # Step 2: Generate AI title
        ai_result = generate_ai_title(h1_title, url)

        if not ai_result:
            result["status"] = "failed"
            result["reason"] = "AI generation failed"
            update_title_record(url, None, None, None, error="ai_failed")
            return result

        new_h1 = ai_result["h1_title"]
        original_h1 = ai_result.get("original_h1", h1_title)

        # Step 3: Create SEO title
        # Format: "{h1} kopen? | Tot !!DISCOUNT!! korting! | beslist.nl"
        seo_title = f"{new_h1} kopen? | Tot !!DISCOUNT!! korting! | beslist.nl"

        # Step 4: Create SEO description
        # Format: "Zoek je {h1}? &#10062; Vergelijk !!NR!! aanbiedingen en bespaar op je aankoop &#10062; Shop {h1} met !!DISCOUNT!! korting online! &#10062; beslist.nl"
        seo_description = f"Zoek je {new_h1}? &#10062; Vergelijk !!NR!! aanbiedingen en bespaar op je aankoop &#10062; Shop {new_h1} met !!DISCOUNT!! korting online! &#10062; beslist.nl"

        # Step 5: Update database
        if update_title_record(url, new_h1, seo_title, seo_description, original_h1):
            result["status"] = "success"
            result["h1_title"] = new_h1
            result["title"] = seo_title
            result["description"] = seo_description
        else:
            result["status"] = "failed"
            result["reason"] = "Database update failed"

        return result

    except Exception as e:
        result["status"] = "failed"
        result["reason"] = str(e)
        return result


def _run_processing(max_urls: int = 100):
    """Background thread for processing URLs.

    Args:
        max_urls: Maximum number of URLs to process in this batch. If 0, process all pending.
    """
    global _processing_state

    with _state_lock:
        _processing_state["is_running"] = True
        _processing_state["should_stop"] = False
        _processing_state["processed"] = 0
        _processing_state["successful"] = 0
        _processing_state["failed"] = 0
        _processing_state["skipped"] = 0
        _processing_state["started_at"] = datetime.now()
        _processing_state["last_error"] = None

    try:
        # Get URLs to process (max_urls=0 means all pending)
        urls = get_unprocessed_urls(max_urls)
        total = len(urls)

        with _state_lock:
            _processing_state["total_urls"] = total

        if total == 0:
            print("[AI_TITLES] No URLs to process")
            return

        batch_msg = "all pending" if max_urls == 0 else f"batch of {max_urls}"
        print(f"[AI_TITLES] Starting processing of {total} URLs ({batch_msg})")

        for url_data in urls:
            # Check stop flag
            with _state_lock:
                if _processing_state["should_stop"]:
                    print("[AI_TITLES] Processing stopped by user")
                    break
                _processing_state["current_url"] = url_data["url"]

            # Process URL
            result = process_single_url(url_data["url"])

            with _state_lock:
                _processing_state["processed"] += 1
                if result["status"] == "success":
                    _processing_state["successful"] += 1
                elif result["status"] == "failed":
                    _processing_state["failed"] += 1
                    _processing_state["last_error"] = result.get("reason", "Unknown error")
                else:
                    _processing_state["skipped"] += 1

            # Small delay to avoid rate limiting
            time.sleep(0.5)

    except Exception as e:
        print(f"[AI_TITLES] Processing error: {e}")
        with _state_lock:
            _processing_state["last_error"] = str(e)

    finally:
        with _state_lock:
            _processing_state["is_running"] = False
            _processing_state["current_url"] = None
        print("[AI_TITLES] Processing complete")


def start_processing(batch_size: int = 100) -> Dict:
    """Start AI title processing in background.

    Args:
        batch_size: Number of URLs to process in this batch. If 0, process all pending.
    """
    with _state_lock:
        if _processing_state["is_running"]:
            return {"status": "error", "message": "Processing already running"}

    thread = threading.Thread(target=_run_processing, args=(batch_size,), daemon=True)
    thread.start()

    batch_msg = "all pending URLs" if batch_size == 0 else f"batch of {batch_size}"
    return {"status": "started", "message": f"AI title processing started ({batch_msg})"}


def stop_processing() -> Dict:
    """Stop AI title processing."""
    with _state_lock:
        if not _processing_state["is_running"]:
            return {"status": "error", "message": "No processing running"}

        _processing_state["should_stop"] = True

    return {"status": "stopping", "message": "Stop signal sent"}


def get_ai_titles_stats() -> Dict:
    """Get statistics about AI title processing."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        stats = {}

        cur.execute("SELECT COUNT(*) as count FROM pa.unique_titles")
        stats["total_urls"] = cur.fetchone()["count"]

        cur.execute("SELECT COUNT(*) as count FROM pa.unique_titles WHERE ai_processed = TRUE")
        stats["ai_processed"] = cur.fetchone()["count"]

        cur.execute("""
            SELECT COUNT(*) as count FROM pa.unique_titles
            WHERE (ai_processed IS NULL OR ai_processed = FALSE)
            AND (title IS NULL OR title = '' OR h1_title IS NULL OR h1_title = '')
        """)
        stats["pending"] = cur.fetchone()["count"]

        cur.execute("SELECT COUNT(*) as count FROM pa.unique_titles WHERE ai_error IS NOT NULL")
        stats["with_errors"] = cur.fetchone()["count"]

        return stats
    finally:
        cur.close()
        return_db_connection(conn)


def get_recent_results(limit: int = 20) -> List[Dict]:
    """Get recently processed AI titles."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT url, title, h1_title, original_h1, ai_processed_at, ai_error
            FROM pa.unique_titles
            WHERE ai_processed = TRUE
            ORDER BY ai_processed_at DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        return [dict(row) for row in rows]
    finally:
        cur.close()
        return_db_connection(conn)
