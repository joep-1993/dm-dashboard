"""
AI Title Generation Service

Generates SEO-optimized titles using OpenAI based on the N8N workflow.
Processes URLs from unique_titles that need AI-generated titles.
"""
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue
from typing import Dict, List, Optional
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from openai import OpenAI

from backend.database import get_db_connection, return_db_connection
from backend.faq_service import fetch_products_api

# Configuration
USER_AGENT = "Beslist script voor SEO"
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")
BASE_URL = "https://www.beslist.nl"

# Words that should be lowercase unless at start of sentence
LOWERCASE_WORDS = {"met", "in", "zonder", "van", "voor", "tot", "op", "aan", "uit", "bij", "naar", "over", "onder", "tegen", "tussen", "door", "om", "en", "of"}


def normalize_preposition_case(text: str) -> str:
    """
    Ensure prepositions like 'met', 'in', 'zonder' are lowercase,
    unless they are at the start of the sentence.

    Examples:
        "Blauwe Feestwimpers Met Glitter" -> "Blauwe Feestwimpers met Glitter"
        "Met glitter feestwimpers" -> "Met glitter feestwimpers" (start of sentence)
    """
    if not text:
        return text

    words = text.split()
    result = []

    for i, word in enumerate(words):
        # Check if word (without punctuation) is a preposition
        word_lower = word.lower().rstrip('.,!?;:')
        if word_lower in LOWERCASE_WORDS and i > 0:
            # Not at start, make lowercase but preserve any trailing punctuation
            if word[-1] in '.,!?;:':
                result.append(word_lower + word[-1])
            else:
                result.append(word_lower)
        else:
            result.append(word)

    return ' '.join(result)


def format_dimensions(text: str) -> str:
    """
    Format dimension patterns to include 'x' between measurements.

    Examples:
        "31 cm 115 cm" -> "31 cm x 115 cm"
        "100 cm 50 cm 30 cm" -> "100 cm x 50 cm x 30 cm"
        "2 meter 3 meter" -> "2 meter x 3 meter"
    """
    if not text:
        return text

    # Pattern matches: number + unit, followed by space and another number + unit
    # Units: cm, mm, m, meter, inch, inches, "
    # This pattern finds consecutive dimension patterns and adds 'x' between them
    pattern = r'(\d+(?:[.,]\d+)?\s*(?:cm|mm|m|meter|inch|inches|"))\s+(\d+(?:[.,]\d+)?\s*(?:cm|mm|m|meter|inch|inches|"))'

    # Keep applying the pattern until no more matches (handles 3+ dimensions)
    prev_text = None
    while prev_text != text:
        prev_text = text
        text = re.sub(pattern, r'\1 x \2', text, flags=re.IGNORECASE)

    return text


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

    prompt = f"""Je bent een SEO-expert. Maak van '{h1_title}' een goedlopende en grammaticaal correcte titel zonder "-". Gebruik UITSLUITEND de woorden die je krijgt - verzin ABSOLUUT GEEN nieuwe woorden, maten, kleuren of andere informatie. Je mag WEL "met" of "zonder" toevoegen waar grammaticaal nodig (zie regel 8). Voeg NOOIT zelf "voor", "van" of "in" toe, maar als deze woorden al in een facetwaarde staan, behoud ze dan. Overbodige woorden mag je weglaten. Je mag de volgorde aanpassen om een beter lopende zin te maken.

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
4. Doelgroepen (Heren, Dames, Kinderen, Jongens, Meisjes, Baby) staan ALTIJD direct VOOR de productnaam, NOOIT met "voor" ervoor.
   - FOUT: "vesten voor heren"
   - GOED: "Heren vesten"
   - FOUT: "schoenen voor kinderen"
   - GOED: "Kinderen schoenen"
5. Zet maten (zoals Maat S, Maat M, Maat L, Maat XL, Maat 38, Maat 42, etc.) helemaal ACHTERAAN in de titel, ZONDER "met" ervoor. Maten staan altijd los achteraan.
   - FOUT: "Nike Heren Maat L tanktops"
   - GOED: "Nike Heren tanktops Maat L"
   - FOUT: "Maat 42 sneakers"
   - GOED: "Sneakers Maat 42"
   - FOUT: "Blauwe cardigans Maat XS met lange mouwen"
   - GOED: "Blauwe cardigans met lange mouwen Maat XS"
   - FOUT: "Imprimétops met Maat 40" (NOOIT "met" voor maten!)
   - GOED: "Imprimétops Maat 40"
6. Als een serie/productlijn de merknaam al bevat, noem het merk NIET apart.
   - FOUT: "Adidas Groene Kinderen Adidas Originals trainingspakken" (Adidas dubbel)
   - GOED: "Groene Adidas Originals Kinderen trainingspakken"
   - FOUT: "Samsung Samsung Galaxy smartphones"
   - GOED: "Samsung Galaxy smartphones"
7. Zet conditie (Nieuw/Nieuwe) en formaat (Kleine/Grote) als bijvoeglijk naamwoord VOOR de productnaam, nooit erachter.
   - FOUT: "Low frost Tafelmodel D Nieuwe Kleine"
   - GOED: "Nieuwe kleine Low Frost tafelmodel Energieklasse D"
   - FOUT: "Inductie kookplaat Nieuwe"
   - GOED: "Nieuwe inductie kookplaat"
8. BELANGRIJK: Producteigenschappen zoals "Korte mouwen", "Lange mouwen", "Capuchon", "Ronde hals", "V-hals" mogen NOOIT los voor de productnaam staan. Voeg ALTIJD "met" toe en zet ze NA de productnaam. Dit geldt ook voor facetwaarden die beginnen met "Met" of "Zonder".
   Bundel alles in ÉÉN "met X, Y en Z" clause. Gebruik "met" maar één keer, daarna komma's en "en".
   - FOUT: "Heren Slim fit poloshirts Lange mouwen" (ALTIJD "met" toevoegen!)
   - GOED: "Heren Slim fit poloshirts met lange mouwen"
   - FOUT: "Heren poloshirts met borstzak en print met korte mouwen" (twee keer "met")
   - GOED: "Heren poloshirts met korte mouwen, borstzak en print"
   - FOUT: "Stretch Heren Korte mouwen Poloshirts"
   - GOED: "Stretch Heren Poloshirts met korte mouwen"
   - FOUT: "Capuchon Heren jassen met rits"
   - GOED: "Heren jassen met capuchon en rits"

Voorbeeld:
"Schoenen - Nike - Rode - Met veters" wordt "Rode Nike schoenen met veters".
"Saniclear - Zilver - Messing - Design Fonteinkranen" wordt "Zilveren messing Saniclear design fonteinkranen".
"Nike - Heren - Maat L - Tanktops" wordt "Nike Heren tanktops Maat L".
"Adidas - Groen - Kinderen - Adidas Originals Trainingspakken" wordt "Groene Adidas Originals Kinderen trainingspakken".
"Tafelmodel Low frost D Nieuw Kleine" wordt "Nieuwe kleine Low Frost tafelmodel Energieklasse D".
"Stretch - Heren - Korte mouwen - Met borstzak - Met print - Poloshirts" wordt "Stretch Heren Poloshirts met korte mouwen, borstzak en print".
"Dutch Dandies - Heren - Slim fit - Lange mouwen - Poloshirts" wordt "Dutch Dandies Heren Slim fit poloshirts met lange mouwen".

Ik wil het antwoord graag in dit json formaat terug:
{{"oude_titel": "{h1_title}", "h1_title": "nieuwe_titel_hier", "url": "{url}"}}

Geef ALLEEN de JSON terug, geen andere tekst."""

    try:
        response = client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.3,
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


def generate_title_from_api(url: str) -> Optional[Dict]:
    """
    Generate title using productsearch API + OpenAI improvement.

    This method:
    1. Fetches H1 and facet data from the productsearch API
    2. Uses OpenAI to improve the H1 while keeping facet values intact
    3. Returns the improved H1 and original H1

    Returns dict with h1_title, original_h1, or None on failure.
    """
    # Step 1: Fetch from productsearch API
    page_data = fetch_products_api(url)

    if not page_data:
        print(f"[AI_TITLES] API fetch failed for {url}")
        return None

    if page_data.get("error"):
        print(f"[AI_TITLES] API error for {url}: {page_data.get('error')}")
        return None

    api_h1 = page_data.get("h1_title", "")
    selected_facets = page_data.get("selected_facets", [])

    if not api_h1:
        print(f"[AI_TITLES] No H1 from API for {url}")
        return None

    # Step 2: Use OpenAI to improve the H1
    client = get_openai_client()
    if not client:
        # If no OpenAI, just return the API H1
        return {
            "h1_title": api_h1,
            "original_h1": api_h1,
        }

    # Build facet values list - these should stay together
    facet_values = [f['detail_value'] for f in selected_facets]
    facet_values_str = ", ".join([f'"{v}"' for v in facet_values])

    # Build facet info for context
    facet_info = ", ".join([f"{f['facet_name']}: \"{f['detail_value']}\"" for f in selected_facets])

    prompt = f"""Je bent een SEO-expert. Verbeter deze titel tot een goedlopende en grammaticaal correcte H1 zonder "-".

Huidige titel van API: "{api_h1}"

Facetten (naam: waarde): {facet_info}

BELANGRIJK - Facetwaarden die INTACT moeten blijven (niet splitsen of herschikken):
{facet_values_str}

Regels:
1. Gebruik UITSLUITEND de woorden uit de titel en facetten hierboven. Verzin ABSOLUUT GEEN nieuwe woorden, maten, kleuren of andere informatie die niet in de titel of facetten staat. Je mag WEL "met" of "zonder" toevoegen waar grammaticaal nodig (zie regel 10). Voeg NOOIT zelf "voor", "van" of "in" toe, maar als deze woorden al in een facetwaarde staan, behoud ze dan.
2. Facetwaarden zijn vaste combinaties en mogen NIET opgesplitst worden.
   Bijvoorbeeld: "Rode Duivels" is één thema, niet "Rode" + "Duivels".
3. Merk ALTIJD vooraan (bijv. "Apple iPhones" niet "iPhones van Apple").
4. Kleuren en materialen als bijvoeglijk naamwoord VOOR het zelfstandig naamwoord.
5. Doelgroepen (Heren, Dames, Kinderen, Jongens, Meisjes, Baby) staan ALTIJD direct VOOR de productnaam, NOOIT met "voor" ervoor.
   Bijvoorbeeld: "Nylon Heren vesten met capuchon" niet "Nylon vesten voor heren met capuchon".
6. NOOIT "in", "van" of "voor" toevoegen. (Maar WEL "met" toevoegen voor producteigenschappen, zie regel 10.)
7. Zet maten (zoals Maat S, Maat M, Maat L, Maat XL, Maat 38, Maat 42, etc.) helemaal ACHTERAAN in de titel, ZONDER "met" ervoor. Maten staan altijd los achteraan.
   Bijvoorbeeld: "Blauwe cardigans met lange mouwen Maat XS" niet "Blauwe cardigans Maat XS met lange mouwen".
   Bijvoorbeeld: "Imprimétops Maat 40" niet "Imprimétops met Maat 40".
8. Als een serie/productlijn de merknaam al bevat, noem het merk NIET apart.
   Bijvoorbeeld: "Groene Adidas Originals Kinderen trainingspakken" niet "Adidas Groene Kinderen Adidas Originals trainingspakken".
9. Zet conditie (Nieuw/Nieuwe) en formaat (Kleine/Grote) als bijvoeglijk naamwoord VOOR de productnaam, nooit erachter.
   Bijvoorbeeld: "Nieuwe kleine Low Frost tafelmodel" niet "Low frost Tafelmodel Nieuwe Kleine".
10. BELANGRIJK: Producteigenschappen zoals "Korte mouwen", "Lange mouwen", "Capuchon", "Ronde hals", "V-hals" mogen NOOIT los voor de productnaam staan. Voeg ALTIJD "met" toe en zet ze NA de productnaam. Dit geldt ook voor facetwaarden die beginnen met "Met" of "Zonder".
   Bundel alles in ÉÉN "met X, Y en Z" clause. Gebruik "met" maar één keer, daarna komma's en "en".
   Bijvoorbeeld: "Heren Slim fit poloshirts met lange mouwen" niet "Heren Slim fit poloshirts Lange mouwen".
   Bijvoorbeeld: "Heren poloshirts met korte mouwen, borstzak en print" niet "Heren poloshirts met borstzak en print met korte mouwen".
   Bijvoorbeeld: "Heren jassen met capuchon en rits" niet "Capuchon Heren jassen met rits".
11. Maak de titel natuurlijk lopend Nederlands.

Geef ALLEEN de verbeterde titel terug, geen uitleg."""

    try:
        response = client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.3
        )

        improved_h1 = response.choices[0].message.content.strip().strip('"')

        return {
            "h1_title": improved_h1,
            "original_h1": api_h1,
        }

    except Exception as e:
        print(f"[AI_TITLES] OpenAI improvement error for {url}: {e}")
        # Return API H1 as fallback
        return {
            "h1_title": api_h1,
            "original_h1": api_h1,
        }


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


def process_single_url(url: str, use_api: bool = True) -> Dict:
    """Process a single URL for AI title generation.

    Args:
        url: The URL to process
        use_api: If True, use productsearch API + OpenAI for faceted URLs.
                 If False, always use scraping + OpenAI method.
    """
    result = {"url": url, "status": "pending"}

    try:
        # Check if URL has facets (contains "~~" or "/c/")
        has_facets = "~~" in url or "/c/" in url

        # Use productsearch API + OpenAI method
        ai_result = generate_title_from_api(url)

        if not ai_result:
            result["status"] = "failed"
            result["reason"] = "API could not fetch data for URL"
            update_title_record(url, None, None, None, error="api_failed")
            print(f"[AI_TITLES] API failed for {url}")
            return result

        new_h1 = ai_result["h1_title"]
        original_h1 = ai_result.get("original_h1", new_h1)

        # Step 3: Apply text formatting
        # Format dimensions (e.g., "31 cm 115 cm" -> "31 cm x 115 cm")
        new_h1 = format_dimensions(new_h1)
        # Normalize preposition case (e.g., "Met glitter" -> "met glitter" unless at start)
        new_h1 = normalize_preposition_case(new_h1)

        # Step 4: Create SEO title
        # Format: "{h1} kopen? ✔️ Tot !!DISCOUNT!! korting! | beslist.nl"
        seo_title = f"{new_h1} kopen? ✔️ Tot !!DISCOUNT!! korting! | beslist.nl"

        # Step 5: Create SEO description
        # Format: "Zoek je {h1}? &#10062; Vergelijk !!NR!! aanbiedingen en bespaar op je aankoop &#10062; Shop {h1} met !!DISCOUNT!! korting online! &#10062; beslist.nl"
        seo_description = f"Zoek je {new_h1}? &#10062; Vergelijk !!NR!! aanbiedingen en bespaar op je aankoop &#10062; Shop {new_h1} met !!DISCOUNT!! korting online! &#10062; beslist.nl"

        # Step 6: Update database
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


def _process_url_with_delay(url: str, use_api: bool = True) -> Dict:
    """Process a single URL with rate limiting delay."""
    # Check stop flag before processing
    with _state_lock:
        if _processing_state["should_stop"]:
            return {"url": url, "status": "skipped", "reason": "stopped"}

    result = process_single_url(url, use_api=use_api)

    # Rate limit: 0.5s delay = max 2 URLs per worker per second
    time.sleep(0.5)

    return result


def _run_processing(max_urls: int = 100, num_workers: int = 15, use_api: bool = True):
    """Background thread for processing URLs with multiple workers.

    Args:
        max_urls: Maximum number of URLs to process in this batch. If 0, process all pending.
        num_workers: Number of parallel workers (default 15).
        use_api: If True, use productsearch API for faceted URLs. If False, use scraping.
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
        method_msg = "API+OpenAI" if use_api else "Scraping+OpenAI"
        print(f"[AI_TITLES] Starting processing of {total} URLs ({batch_msg}) with {num_workers} workers using {method_msg}")

        # Process URLs using thread pool - submit in small chunks to allow stopping
        chunk_size = num_workers * 2
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            url_index = 0
            while url_index < total:
                # Check stop flag before submitting next chunk
                with _state_lock:
                    if _processing_state["should_stop"]:
                        print("[AI_TITLES] Processing stopped by user")
                        break

                # Submit a chunk of URLs
                chunk_end = min(url_index + chunk_size, total)
                future_to_url = {
                    executor.submit(_process_url_with_delay, urls[i]["url"], use_api): urls[i]["url"]
                    for i in range(url_index, chunk_end)
                }

                # Process results as they complete
                stopped = False
                for future in as_completed(future_to_url):
                    with _state_lock:
                        if _processing_state["should_stop"]:
                            print("[AI_TITLES] Processing stopped by user")
                            stopped = True
                            break

                    url = future_to_url[future]
                    try:
                        result = future.result()

                        with _state_lock:
                            _processing_state["processed"] += 1
                            _processing_state["current_url"] = url
                            if result["status"] == "success":
                                _processing_state["successful"] += 1
                            elif result["status"] == "failed":
                                _processing_state["failed"] += 1
                                _processing_state["last_error"] = f"{result.get('reason', 'Unknown error')} ({url})"
                            else:
                                _processing_state["skipped"] += 1

                    except Exception as e:
                        with _state_lock:
                            _processing_state["processed"] += 1
                            _processing_state["failed"] += 1
                            _processing_state["last_error"] = str(e)

                if stopped:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                url_index = chunk_end

    except Exception as e:
        print(f"[AI_TITLES] Processing error: {e}")
        with _state_lock:
            _processing_state["last_error"] = str(e)

    finally:
        with _state_lock:
            _processing_state["is_running"] = False
            _processing_state["current_url"] = None
        print("[AI_TITLES] Processing complete")


def start_processing(batch_size: int = 100, num_workers: int = 15, use_api: bool = True) -> Dict:
    """Start AI title processing in background.

    Args:
        batch_size: Number of URLs to process in this batch. If 0, process all pending.
        num_workers: Number of parallel workers (default 15).
        use_api: If True, use productsearch API for faceted URLs. If False, use scraping.
    """
    with _state_lock:
        if _processing_state["is_running"]:
            return {"status": "error", "message": "Processing already running"}

    thread = threading.Thread(target=_run_processing, args=(batch_size, num_workers, use_api), daemon=True)
    thread.start()

    batch_msg = "all pending URLs" if batch_size == 0 else f"batch of {batch_size}"
    method_msg = "API+OpenAI" if use_api else "Scraping+OpenAI"
    return {"status": "started", "message": f"AI title processing started ({batch_msg}, {num_workers} workers, {method_msg})"}


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
