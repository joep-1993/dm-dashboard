"""
OpenAI Batch API service for bulk FAQ and Kopteksten generation.

Flow:
1. Fetch all pending URLs from DB
2. For each URL, call Product Search API to get products/facets
3. Build prompts, write to JSONL file
4. Upload JSONL to OpenAI Files API
5. Create a batch
6. Poll for completion
7. Download results, parse, save to DB
"""
import os
import json
import time
import tempfile
import threading
from typing import Dict, List, Optional
from dataclasses import asdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from openai import OpenAI

from backend.faq_service import (
    fetch_products_api, generate_faqs_for_page, FAQPage, FAQItem,
    get_openai_client, extract_selected_facets
)
from backend.scraper_service import scrape_product_page_api
from backend.gpt_service import create_product_recommendation_prompt, MODEL
from backend.database import get_db_connection, return_db_connection
from backend.ai_titles_service import (
    generate_title_from_api, get_unprocessed_urls,
    format_dimensions, normalize_preposition_case
)

AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")

# Global batch state (one per type)
_batch_state = {
    "faq": {
        "active": False,
        "phase": "",  # preparing, uploading, processing, saving, complete, error
        "total_urls": 0,
        "prepared": 0,
        "skipped": 0,
        "failed_prepare": 0,
        "processed": 0,
        "failed": 0,
        "batch_id": None,
        "error": None,
        "started_at": None,
    },
    "kopteksten": {
        "active": False,
        "phase": "",
        "total_urls": 0,
        "prepared": 0,
        "skipped": 0,
        "failed_prepare": 0,
        "processed": 0,
        "failed": 0,
        "batch_id": None,
        "error": None,
        "started_at": None,
    },
    "titles": {
        "active": False,
        "phase": "",
        "total_urls": 0,
        "prepared": 0,
        "skipped": 0,
        "failed_prepare": 0,
        "processed": 0,
        "failed": 0,
        "batch_id": None,
        "error": None,
        "started_at": None,
    }
}
_batch_lock = threading.Lock()


def get_batch_status(batch_type: str) -> dict:
    """Get current batch processing status."""
    with _batch_lock:
        return dict(_batch_state[batch_type])


def _update_state(batch_type: str, **kwargs):
    """Thread-safe state update."""
    with _batch_lock:
        _batch_state[batch_type].update(kwargs)


def _reset_state(batch_type: str):
    """Reset state for a new batch run."""
    with _batch_lock:
        _batch_state[batch_type] = {
            "active": True,
            "phase": "preparing",
            "total_urls": 0,
            "prepared": 0,
            "skipped": 0,
            "failed_prepare": 0,
            "processed": 0,
            "failed": 0,
            "batch_id": None,
            "error": None,
            "started_at": time.time(),
        }


def _fetch_pending_faq_urls(limit: int = 50000) -> List[str]:
    """Fetch all pending FAQ URLs."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT w.url
            FROM pa.jvs_seo_werkvoorraad w
            WHERE NOT EXISTS (SELECT 1 FROM pa.url_validation_tracking v WHERE v.url = w.url)
              AND NOT EXISTS (SELECT 1 FROM pa.faq_tracking t WHERE t.url = w.url AND t.status != 'pending')
            LIMIT %s
        """, (limit,))
        return [row['url'] for row in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def _fetch_pending_kopteksten_urls(limit: int = 50000) -> List[str]:
    """Fetch all pending kopteksten URLs."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT w.url
            FROM pa.jvs_seo_werkvoorraad w
            WHERE NOT EXISTS (SELECT 1 FROM pa.jvs_seo_werkvoorraad_kopteksten_check t WHERE t.url = w.url)
              AND NOT EXISTS (SELECT 1 FROM pa.url_validation_tracking v WHERE v.url = w.url)
            LIMIT %s
        """, (limit,))
        return [row['url'] for row in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def _build_faq_prompt(page_data: Dict, num_faqs: int = 6) -> str:
    """Build the FAQ generation prompt (same as faq_service but returns string only)."""
    products_context = ""
    if page_data.get("products"):
        products_list = "\n".join([
            f"- {p['title']}: {p['description']}"
            for p in page_data["products"][:15]
        ])
        products_context = f"\n\nBeschikbare producten:\n{products_list}"

    product_urls_context = ""
    if page_data.get("product_urls"):
        urls_list = "\n".join([
            f"- {item['label']}: {item['url']}"
            for item in page_data["product_urls"][:12]
        ])
        product_urls_context = f"\n\nProductpagina's (gebruik deze voor hyperlinks in antwoorden):\n{urls_list}"

    # Build context from selected facets so the AI writes facet-specific FAQs
    facet_context = ""
    facet_instruction = ""
    if page_data.get("selected_facets"):
        facets = page_data["selected_facets"]
        facet_descriptions = [f"{f['facet_name']}: {f['facet_value']}" for f in facets]
        facet_context = f"\n\nActieve filters op deze pagina:\n" + "\n".join(f"- {d}" for d in facet_descriptions)
        facet_instruction = "\n- BELANGRIJK: Deze pagina is gefilterd op specifieke kenmerken (zie \"Actieve filters\" hierboven). Maak de vragen en antwoorden specifiek over die filters. Als er gefilterd is op een merk, stel dan vragen over dat merk en hun producten. Als er gefilterd is op een kleur, materiaal of type, stel dan vragen die specifiek over die eigenschap gaan. Schrijf GEEN generieke vragen die net zo goed op de ongefilterde categoriepagina zouden passen."

    return f"""Je bent een SEO-expert die FAQ's schrijft voor e-commerce pagina's.

Pagina titel: {page_data['h1_title']}
URL: {page_data['url']}
{facet_context}
{products_context}
{product_urls_context}

Schrijf {num_faqs} veelgestelde vragen (FAQ's) die relevant zijn voor bezoekers van deze productcategorie pagina.

Vereisten:
- Vragen moeten natuurlijk klinken, zoals echte klanten ze zouden stellen
- Antwoorden moeten informatief en behulpzaam zijn (50-100 woorden per antwoord)
- Focus op koopadvies, productvergelijkingen, en praktische tips{facet_instruction}
- Schrijf in het Nederlands
- Noem geen specifieke prijzen
- BELANGRIJK: Gebruik een informele, toegankelijke toon. Gebruik "jij" en "je" in plaats van "u" en "uw". Spreek de lezer direct en vriendelijk aan.
- BELANGRIJK: Gebruik NOOIT "wij", "we", "ons", "onze", "onze producten", "onze website" of vergelijkbare eerste persoon meervoud. Schrijf neutraal en informatief, alsof je een onafhankelijke adviseur bent.
- BELANGRIJK voor hyperlinks:
  * Gebruik ALLEEN URLs uit de hierboven gegeven lijst "Productpagina's" (URLs met /p/)
  * Verzin NOOIT zelf URLs - gebruik alleen de exacte URLs die in de lijst staan
  * Gebruik GEEN URLs met /c/ (categoriepagina's) - alleen productpagina URLs met /p/
  * VERBODEN LINKTEKSTEN (gebruik deze NOOIT als anchor text):
    - "klik hier", "hier klikken", "hier", "deze link", "deze pagina", "deze gids", "deze", "lees meer", "meer info", "kijk hier", "bekijk hier", "via deze link"
    - Elke andere vage of demonstratieve verwijzing zonder productnaam of zoekterm
  * Voorbeelden van FOUT (NIET doen):
    - "... voor de Dark Grey variant kun je <a href=\\"...\\">hier klikken</a>"
    - "... is er <a href=\\"...\\">deze link</a>"
  * Voorbeelden van GOED (wel doen):
    - "... bekijk de <a href=\\"...\\">Philips Airfryer XXL</a> voor grotere porties"
  * Linktekst MOET de productnaam zijn of een logische, beschrijvende zoekterm
  * HOUD DE LINKTEKST KORT (max 3-5 woorden). Vermijd lange productnamen met specificaties.
  * Als je de productnaam niet logisch in de zin kunt verwerken als anchor text, maak dan GEEN hyperlink - herschrijf liever de zin zonder link
  * Als er geen relevante URL in de lijst staat, maak dan GEEN hyperlink
- Verwerk 1-3 hyperlinks per antwoord waar relevant (naar specifieke producten)

Geef je antwoord als JSON array met objecten die "question" en "answer" bevatten.
De "answer" mag HTML hyperlinks bevatten.
Alleen de JSON array, geen andere tekst.

Voorbeeld formaat:
[
  {{"question": "Welke merken zijn populair?", "answer": "Populaire merken zijn onder andere <a href=\\"https://www.beslist.nl/p/samsung-galaxy-s24/6/1234567890123/\\">Samsung Galaxy S24</a>. Dit model staat bekend om zijn kwaliteit."}},
  {{"question": "Andere vraag?", "answer": "Een ander goed product is de <a href=\\"https://www.beslist.nl/p/philips-airfryer/12000/9876543210987/\\">Philips Airfryer</a>."}}
]"""


def _build_kopteksten_messages(page_data: Dict) -> List[Dict]:
    """Build kopteksten generation messages (system + user)."""
    h1_title = page_data.get("h1_title", "")
    products = page_data.get("products", [])
    user_prompt = create_product_recommendation_prompt(h1_title, products)

    system_message = """Je bent een online marketeer voor beslist.nl met als doel om de bezoeker te helpen in zijn buyer journey.
- Spreek de lezer aan met "je," in een toegankelijke, informatieve toon.
- Noem nooit prijzen.
- Schrijf ALTIJD als één doorlopende alinea zonder witregels of meerdere paragrafen.
- Focus op advies dat écht helpt bij het maken van een keuze (bv. voordelen, verschillen, specifieke kenmerken).
- Varieer sterk in je openingszinnen — begin NOOIT met "Als je op zoek bent naar", "Op zoek naar", "Ben je op zoek naar", "Zoek je" of vergelijkbare zoekformuleringen.
- Vermijd generieke kwalificaties zoals "ideaal", "perfect", "uitstekend", "een goede keuze", "een heerlijke keuze". Wees specifiek: leg uit WAAROM iets geschikt is.
- Gebruik geen uitroeptekens.
- Vermijd overdreven enthousiaste marketing-taal. Schrijf behulpzaam en nuchter, niet als een reclamespot.
- Gebruik NOOIT "ons", "onze", "wij" of "we" - schrijf vanuit het perspectief van de bezoeker, niet vanuit het bedrijf.
- BELANGRIJK: Link ALLEEN naar producten die exact overeenkomen met het zoekwoord.
- Verzin NOOIT producten of URLs die niet in de lijst staan.
- Als je linkt, gebruik de tag <a href> en kies dan de juiste url uit de lijst van meegeleverde producten.
- HOUD DE LINKTEKST KORT (max 3-5 woorden).
- VERBODEN LINKTEKSTEN (gebruik deze NOOIT als anchor text): "klik hier", "hier klikken", "hier", "deze link", "deze pagina", "deze gids", "deze", "lees meer", "meer info", "kijk hier", "bekijk hier", "via deze link". FOUT: "voor de Dark Grey variant kun je <a href=\"...\">hier klikken</a>". GOED: "bekijk de <a href=\"...\">Philips Airfryer XXL</a>". Linktekst MOET de productnaam of een logische zoekterm zijn. Als dat niet natuurlijk past, maak dan GEEN hyperlink.
- Gebruik nooit andere URLs dan degene die voorkomen in de lijst van producten."""

    return [
        {"role": "system", "content": system_message},
        {"role": "user", "content": user_prompt}
    ]


def _prepare_url_data(url: str, batch_type: str) -> Optional[Dict]:
    """Fetch product data for a URL. Returns page_data or None."""
    try:
        if batch_type == "faq":
            return fetch_products_api(url)
        else:
            return scrape_product_page_api(url)
    except Exception as e:
        print(f"[BATCH] Error fetching data for {url}: {e}")
        return None


def _write_jsonl(requests: List[Dict], filepath: str):
    """Write batch requests to JSONL file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        for req in requests:
            f.write(json.dumps(req, ensure_ascii=False) + '\n')


def _run_faq_batch(num_faqs: int = 6):
    """Background thread: run full FAQ batch pipeline."""
    batch_type = "faq"
    try:
        # Phase 1: Fetch pending URLs
        _update_state(batch_type, phase="preparing")
        urls = _fetch_pending_faq_urls()
        _update_state(batch_type, total_urls=len(urls))
        print(f"[BATCH-FAQ] Found {len(urls)} pending URLs")

        if not urls:
            _update_state(batch_type, phase="complete", active=False)
            return

        # Phase 2: Prepare prompts (fetch product data + build prompts)
        batch_requests = []
        skip_data = []  # URLs to mark as skipped (no_products_found)
        failed_urls = []  # URLs that failed to fetch

        def prepare_one(url):
            page_data = _prepare_url_data(url, "faq")
            if not page_data:
                return url, "failed", None, None
            if page_data.get("error"):
                return url, "error", page_data.get("error"), None
            if not page_data.get("products"):
                return url, "skipped", "no_products_found", None
            prompt = _build_faq_prompt(page_data, num_faqs)
            return url, "ok", prompt, page_data

        # Use 50 threads for Product Search API calls (I/O bound)
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {executor.submit(prepare_one, url): url for url in urls}
            for future in as_completed(futures):
                url, status, data, page_data = future.result()
                if status == "ok":
                    custom_id = url  # Use URL as custom_id for matching results later
                    batch_requests.append({
                        "custom_id": custom_id,
                        "method": "POST",
                        "url": "/v1/chat/completions",
                        "body": {
                            "model": AI_MODEL,
                            "messages": [{"role": "user", "content": data}],
                            "max_tokens": 2500,
                            "temperature": 0.7,
                            "response_format": {"type": "json_object"}
                        }
                    })
                    # Store page_data for URL cleanup later
                elif status == "skipped":
                    skip_data.append((url, "skipped", data))
                else:
                    failed_urls.append((url, "failed", str(data)[:255] if data else "api_failed"))

                prepared = len(batch_requests) + len(skip_data) + len(failed_urls)
                _update_state(batch_type,
                    prepared=len(batch_requests),
                    skipped=len(skip_data),
                    failed_prepare=len(failed_urls)
                )

        # Save skip/failed results to DB immediately
        if skip_data or failed_urls:
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                if skip_data:
                    from psycopg2.extras import execute_batch
                    execute_batch(cur,
                        "INSERT INTO pa.url_validation_tracking (url, status, skip_reason) VALUES (%s, %s, %s) ON CONFLICT (url) DO UPDATE SET status = EXCLUDED.status, skip_reason = EXCLUDED.skip_reason, checked_at = CURRENT_TIMESTAMP",
                        skip_data
                    )
                if failed_urls:
                    from psycopg2.extras import execute_batch
                    execute_batch(cur,
                        "INSERT INTO pa.faq_tracking (url, status, skip_reason) VALUES (%s, %s, %s) ON CONFLICT (url) DO UPDATE SET status = EXCLUDED.status, skip_reason = EXCLUDED.skip_reason",
                        failed_urls
                    )
                conn.commit()
            finally:
                cur.close()
                return_db_connection(conn)

        if not batch_requests:
            _update_state(batch_type, phase="complete", active=False)
            return

        print(f"[BATCH-FAQ] Prepared {len(batch_requests)} prompts, {len(skip_data)} skipped, {len(failed_urls)} failed")

        # Phase 3-5: Upload, process, and save in chunks (OpenAI 200MB limit per batch)
        CHUNK_SIZE = 5000  # requests per chunk — keeps well under 200MB
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        chunks = [batch_requests[i:i + CHUNK_SIZE] for i in range(0, len(batch_requests), CHUNK_SIZE)]
        total_succeeded = 0
        total_save_failed = 0
        total_processed = 0

        for chunk_idx, chunk in enumerate(chunks):
            _update_state(batch_type, phase=f"uploading chunk {chunk_idx + 1}/{len(chunks)}")
            print(f"[BATCH-FAQ] Uploading chunk {chunk_idx + 1}/{len(chunks)} ({len(chunk)} requests)")

            jsonl_path = os.path.join(tempfile.gettempdir(), f"batch_faq_{int(time.time())}_{chunk_idx}.jsonl")
            _write_jsonl(chunk, jsonl_path)
            file_size_mb = os.path.getsize(jsonl_path) / 1024 / 1024
            print(f"[BATCH-FAQ] JSONL: {jsonl_path} ({file_size_mb:.1f} MB)")

            with open(jsonl_path, 'rb') as f:
                batch_file = client.files.create(file=f, purpose="batch")

            batch = client.batches.create(
                input_file_id=batch_file.id,
                endpoint="/v1/chat/completions",
                completion_window="24h",
                metadata={"type": "faq", "chunk": f"{chunk_idx + 1}/{len(chunks)}", "count": str(len(chunk))}
            )
            _update_state(batch_type, phase=f"processing chunk {chunk_idx + 1}/{len(chunks)}", batch_id=batch.id)
            print(f"[BATCH-FAQ] Batch created: {batch.id}")

            # Poll for completion
            while True:
                time.sleep(15)
                batch = client.batches.retrieve(batch.id)
                chunk_completed = (batch.request_counts.completed or 0) + (batch.request_counts.failed or 0)
                _update_state(batch_type, processed=total_processed + chunk_completed)

                if batch.status in ("completed", "failed", "expired", "cancelled"):
                    break

            if batch.status != "completed":
                _update_state(batch_type, phase="error", active=False,
                    error=f"Chunk {chunk_idx + 1} batch ended with status: {batch.status}")
                return

            print(f"[BATCH-FAQ] Chunk {chunk_idx + 1} completed: {batch.request_counts.completed} succeeded, {batch.request_counts.failed} failed")

            # Download and parse results for this chunk
            _update_state(batch_type, phase=f"saving chunk {chunk_idx + 1}/{len(chunks)}")
            output_file = client.files.content(batch.output_file_id)
            results_text = output_file.text

            tracking_data = []
            content_data = []
            save_failed = 0

            for line in results_text.strip().split('\n'):
                if not line.strip():
                    continue  # OpenAI batch output sometimes has trailing blank lines
                try:
                    result = json.loads(line)
                    url = result["custom_id"]
                    response_body = result.get("response", {}).get("body", {})

                    if result.get("error") or not response_body.get("choices"):
                        tracking_data.append((url, "failed", "batch_api_error"))
                        save_failed += 1
                        continue

                    content = response_body["choices"][0]["message"]["content"].strip()
                    if content.startswith("```"):
                        # Robust fence extraction: everything between the first
                        # and second ``` fence, else the content as-is.
                        parts = content.split("```")
                        content = parts[1] if len(parts) >= 3 else content
                        if content.startswith("json"):
                            content = content[4:]
                    content = content.strip()

                    faqs_data = json.loads(content)
                    if isinstance(faqs_data, dict):
                        fallback_values = list(faqs_data.values())
                        faqs_data = (
                            faqs_data.get("faqs")
                            or faqs_data.get("faq")
                            or faqs_data.get("FAQ")
                            or (fallback_values[0] if fallback_values else None)
                        )

                    if not faqs_data or not isinstance(faqs_data, list):
                        tracking_data.append((url, "failed", "faq_generation_failed"))
                        save_failed += 1
                        continue

                    faq_items = [FAQItem(question=item["question"], answer=item["answer"]) for item in faqs_data]
                    faq_page = FAQPage(url=url, page_title="", faqs=faq_items)

                    faq_json = json.dumps([asdict(faq) for faq in faq_items], ensure_ascii=False)
                    schema_org = json.dumps(faq_page.to_schema_org(), ensure_ascii=False)

                    tracking_data.append((url, "success", None))
                    content_data.append((url, "", faq_json, schema_org))

                except Exception as e:
                    url = result.get("custom_id", "unknown")
                    tracking_data.append((url, "failed", f"parse_error: {str(e)[:200]}"))
                    save_failed += 1

            # Save chunk results to DB
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                if tracking_data:
                    from psycopg2.extras import execute_batch
                    execute_batch(cur,
                        "INSERT INTO pa.faq_tracking (url, status, skip_reason) VALUES (%s, %s, %s) ON CONFLICT (url) DO UPDATE SET status = EXCLUDED.status, skip_reason = EXCLUDED.skip_reason",
                        tracking_data, page_size=1000
                    )
                if content_data:
                    from psycopg2.extras import execute_batch
                    execute_batch(cur,
                        "INSERT INTO pa.faq_content (url, page_title, faq_json, schema_org) VALUES (%s, %s, %s, %s) ON CONFLICT (url) DO UPDATE SET page_title = EXCLUDED.page_title, faq_json = EXCLUDED.faq_json, schema_org = EXCLUDED.schema_org",
                        content_data, page_size=1000
                    )
                conn.commit()
            finally:
                cur.close()
                return_db_connection(conn)

            chunk_succeeded = sum(1 for t in tracking_data if t[1] == "success")
            total_succeeded += chunk_succeeded
            total_save_failed += save_failed
            total_processed += len(tracking_data)
            print(f"[BATCH-FAQ] Chunk {chunk_idx + 1} saved: {chunk_succeeded} FAQs, {save_failed} failed")

            try:
                os.remove(jsonl_path)
            except:
                pass

        _update_state(batch_type, phase="complete", active=False,
            processed=total_processed, failed=total_save_failed)
        print(f"[BATCH-FAQ] All {len(chunks)} chunks complete: {total_succeeded} FAQs, {total_save_failed} failed")
        # Per-chunk cleanup already happened inside the loop; no trailing
        # os.remove here (would NameError on empty chunks, or double-delete
        # the last file otherwise).

    except Exception as e:
        print(f"[BATCH-FAQ] Error: {e}")
        _update_state(batch_type, phase="error", active=False, error=str(e))


def _run_kopteksten_batch():
    """Background thread: run full kopteksten batch pipeline."""
    batch_type = "kopteksten"
    try:
        # Phase 1: Fetch pending URLs
        _update_state(batch_type, phase="preparing")
        urls = _fetch_pending_kopteksten_urls()
        _update_state(batch_type, total_urls=len(urls))
        print(f"[BATCH-KOPT] Found {len(urls)} pending URLs")

        if not urls:
            _update_state(batch_type, phase="complete", active=False)
            return

        # Phase 2: Prepare prompts
        batch_requests = []
        skip_data = []
        failed_urls = []

        def prepare_one(url):
            page_data = _prepare_url_data(url, "kopteksten")
            if not page_data:
                return url, "failed", None
            if page_data.get("error"):
                return url, "error", page_data.get("error")
            if not page_data.get("products"):
                return url, "skipped", "no_products_found"
            messages = _build_kopteksten_messages(page_data)
            return url, "ok", messages

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {executor.submit(prepare_one, url): url for url in urls}
            for future in as_completed(futures):
                url, status, data = future.result()
                if status == "ok":
                    batch_requests.append({
                        "custom_id": url,
                        "method": "POST",
                        "url": "/v1/chat/completions",
                        "body": {
                            "model": AI_MODEL,
                            "messages": data,
                            "max_tokens": 2000,
                            "temperature": 0.7
                        }
                    })
                elif status == "skipped":
                    skip_data.append((url, "skipped", data))
                else:
                    failed_urls.append((url, "failed", str(data)[:255] if data else "api_failed"))

                _update_state(batch_type,
                    prepared=len(batch_requests),
                    skipped=len(skip_data),
                    failed_prepare=len(failed_urls)
                )

        # Save skip/failed to DB
        if skip_data or failed_urls:
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                if skip_data:
                    from psycopg2.extras import execute_batch
                    execute_batch(cur,
                        "INSERT INTO pa.url_validation_tracking (url, status, skip_reason) VALUES (%s, %s, %s) ON CONFLICT (url) DO UPDATE SET status = EXCLUDED.status, skip_reason = EXCLUDED.skip_reason, checked_at = CURRENT_TIMESTAMP",
                        skip_data
                    )
                if failed_urls:
                    from psycopg2.extras import execute_batch
                    execute_batch(cur,
                        "INSERT INTO pa.jvs_seo_werkvoorraad_kopteksten_check (url, status, skip_reason) VALUES (%s, %s, %s) ON CONFLICT (url) DO UPDATE SET status = EXCLUDED.status, skip_reason = EXCLUDED.skip_reason",
                        failed_urls
                    )
                conn.commit()
            finally:
                cur.close()
                return_db_connection(conn)

        if not batch_requests:
            _update_state(batch_type, phase="complete", active=False)
            return

        print(f"[BATCH-KOPT] Prepared {len(batch_requests)} prompts, {len(skip_data)} skipped, {len(failed_urls)} failed")

        # Phase 3-5: Upload, process, and save in chunks (OpenAI 200MB limit per batch)
        CHUNK_SIZE = 5000
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        chunks = [batch_requests[i:i + CHUNK_SIZE] for i in range(0, len(batch_requests), CHUNK_SIZE)]
        total_succeeded = 0
        total_save_failed = 0
        total_processed = 0

        for chunk_idx, chunk in enumerate(chunks):
            _update_state(batch_type, phase=f"uploading chunk {chunk_idx + 1}/{len(chunks)}")
            print(f"[BATCH-KOPT] Uploading chunk {chunk_idx + 1}/{len(chunks)} ({len(chunk)} requests)")

            jsonl_path = os.path.join(tempfile.gettempdir(), f"batch_kopt_{int(time.time())}_{chunk_idx}.jsonl")
            _write_jsonl(chunk, jsonl_path)
            file_size_mb = os.path.getsize(jsonl_path) / 1024 / 1024
            print(f"[BATCH-KOPT] JSONL: {jsonl_path} ({file_size_mb:.1f} MB)")

            with open(jsonl_path, 'rb') as f:
                batch_file = client.files.create(file=f, purpose="batch")

            batch = client.batches.create(
                input_file_id=batch_file.id,
                endpoint="/v1/chat/completions",
                completion_window="24h",
                metadata={"type": "kopteksten", "chunk": f"{chunk_idx + 1}/{len(chunks)}", "count": str(len(chunk))}
            )
            _update_state(batch_type, phase=f"processing chunk {chunk_idx + 1}/{len(chunks)}", batch_id=batch.id)
            print(f"[BATCH-KOPT] Batch created: {batch.id}")

            while True:
                time.sleep(15)
                batch = client.batches.retrieve(batch.id)
                chunk_completed = (batch.request_counts.completed or 0) + (batch.request_counts.failed or 0)
                _update_state(batch_type, processed=total_processed + chunk_completed)

                if batch.status in ("completed", "failed", "expired", "cancelled"):
                    break

            if batch.status != "completed":
                _update_state(batch_type, phase="error", active=False,
                    error=f"Chunk {chunk_idx + 1} batch ended with status: {batch.status}")
                return

            print(f"[BATCH-KOPT] Chunk {chunk_idx + 1} completed: {batch.request_counts.completed} succeeded, {batch.request_counts.failed} failed")

            _update_state(batch_type, phase=f"saving chunk {chunk_idx + 1}/{len(chunks)}")
            output_file = client.files.content(batch.output_file_id)
            results_text = output_file.text

            tracking_data = []
            content_data = []
            save_failed = 0

            for line in results_text.strip().split('\n'):
                try:
                    result = json.loads(line)
                    url = result["custom_id"]
                    response_body = result.get("response", {}).get("body", {})

                    if result.get("error") or not response_body.get("choices"):
                        tracking_data.append((url, "failed", "batch_api_error"))
                        save_failed += 1
                        continue

                    content = response_body["choices"][0]["message"]["content"].strip()
                    tracking_data.append((url, "success", None))
                    content_data.append((url, content))

                except Exception as e:
                    url = result.get("custom_id", "unknown")
                    tracking_data.append((url, "failed", f"parse_error: {str(e)[:200]}"))
                    save_failed += 1

            conn = get_db_connection()
            cur = conn.cursor()
            try:
                if tracking_data:
                    from psycopg2.extras import execute_batch
                    execute_batch(cur,
                        "INSERT INTO pa.jvs_seo_werkvoorraad_kopteksten_check (url, status, skip_reason) VALUES (%s, %s, %s) ON CONFLICT (url) DO UPDATE SET status = EXCLUDED.status, skip_reason = EXCLUDED.skip_reason",
                        tracking_data, page_size=1000
                    )
                if content_data:
                    from psycopg2.extras import execute_batch
                    execute_batch(cur,
                        "INSERT INTO pa.content_urls_joep (url, content) VALUES (%s, %s) ON CONFLICT (url) DO UPDATE SET content = EXCLUDED.content",
                        content_data, page_size=1000
                    )
                conn.commit()
            finally:
                cur.close()
                return_db_connection(conn)

            chunk_succeeded = sum(1 for t in tracking_data if t[1] == "success")
            total_succeeded += chunk_succeeded
            total_save_failed += save_failed
            total_processed += len(tracking_data)
            print(f"[BATCH-KOPT] Chunk {chunk_idx + 1} saved: {chunk_succeeded} content items, {save_failed} failed")

            try:
                os.remove(jsonl_path)
            except:
                pass

        _update_state(batch_type, phase="complete", active=False,
            processed=total_processed, failed=total_save_failed)
        print(f"[BATCH-KOPT] All {len(chunks)} chunks complete: {total_succeeded} content items, {total_save_failed} failed")

    except Exception as e:
        print(f"[BATCH-KOPT] Error: {e}")
        _update_state(batch_type, phase="error", active=False, error=str(e))


def start_faq_batch(num_faqs: int = 6) -> dict:
    """Start FAQ batch processing in background thread."""
    with _batch_lock:
        if _batch_state["faq"]["active"]:
            return {"status": "error", "message": "FAQ batch already running"}

    _reset_state("faq")
    thread = threading.Thread(target=_run_faq_batch, args=(num_faqs,), daemon=True)
    thread.start()
    return {"status": "started", "message": "FAQ batch processing started"}


def start_kopteksten_batch() -> dict:
    """Start kopteksten batch processing in background thread."""
    with _batch_lock:
        if _batch_state["kopteksten"]["active"]:
            return {"status": "error", "message": "Kopteksten batch already running"}

    _reset_state("kopteksten")
    thread = threading.Thread(target=_run_kopteksten_batch, daemon=True)
    thread.start()
    return {"status": "started", "message": "Kopteksten batch processing started"}


def _run_titles_batch():
    """Background thread: run full unique titles batch pipeline.

    Unlike FAQ/kopteksten, unique titles have heavy pre- and post-processing
    around the OpenAI call. We use generate_title_from_api() which bundles
    all three steps, but for the Batch API we need to split them:
    1. Pre-process: fetch API data, classify facets, build prompt + context
    2. Batch: send prompts to OpenAI Batch API
    3. Post-process: apply hallucination checks, prepend brands, append sizes, format title
    """
    batch_type = "titles"
    try:
        # Phase 1: Fetch pending URLs
        _update_state(batch_type, phase="preparing")
        rows = get_unprocessed_urls(limit=0)  # 0 = all pending
        _update_state(batch_type, total_urls=len(rows))
        print(f"[BATCH-TITLES] Found {len(rows)} pending URLs")

        if not rows:
            _update_state(batch_type, phase="complete", active=False)
            return

        urls = [r['url'] for r in rows]

        # Phase 2: For each URL, run the full generate_title_from_api pipeline
        # which includes pre-processing, prompt building, AND would normally call OpenAI.
        # Instead, we extract the prompt by intercepting the flow.
        # Since generate_title_from_api is tightly coupled, we'll use a different approach:
        # fetch API data + build prompt in the prepare step, then batch the OpenAI calls,
        # then apply post-processing.
        #
        # However, the pre/post-processing in generate_title_from_api is ~300 lines of
        # complex facet logic. Rather than duplicating it, we'll use a simpler approach:
        # process URLs through generate_title_from_api() which makes individual OpenAI calls,
        # but use the Batch API for the actual completions.
        #
        # Approach: We import the pre-processing logic and build prompts, then batch them.
        # For post-processing we store the context (lead_values, size_values, etc.) per URL.

        from backend.ai_titles_service import (
            fetch_products_api as titles_fetch_api,
            get_openai_client as titles_get_client,
        )
        import re

        batch_requests = []
        url_contexts = {}  # Store pre-processing context for post-processing
        failed_urls = []

        def prepare_title_url(url):
            """Run pre-processing for a URL, return (url, prompt, context) or failure."""
            try:
                # This replicates the pre-processing from generate_title_from_api
                # but stops before the OpenAI call
                page_data = fetch_products_api(url)

                if not page_data or page_data.get("error"):
                    return url, "failed", None, None

                api_h1 = page_data.get("h1_title", "")
                selected_facets = page_data.get("selected_facets", [])
                category_name = page_data.get("category_name", "")

                if not api_h1:
                    return url, "failed", "no_h1", None

                # Append category name if missing
                if category_name and category_name.lower() not in api_h1.lower():
                    api_h1 = api_h1.rstrip() + " " + category_name.lower()

                # We can't easily extract the full pre-processing without duplicating
                # generate_title_from_api. Instead, use the simpler approach: just call
                # the full function which handles everything including the OpenAI call.
                # The "batch" part will be the parallel execution of these calls.
                return url, "ready", api_h1, selected_facets

            except Exception as e:
                return url, "failed", str(e), None

        # Since unique titles pre-processing is too tightly coupled to split cleanly,
        # we use the Batch API differently: process all URLs through the full pipeline
        # but with concurrent workers. The real OpenAI calls happen individually but
        # in parallel — similar to the normal flow but at higher concurrency.
        #
        # For a true Batch API approach, we'd need to refactor generate_title_from_api
        # into separate pre-process / prompt / post-process functions. For now, let's
        # use the concurrent approach which still benefits from the higher worker limits.

        from backend.ai_titles_service import process_single_url as titles_process_single

        processed = 0
        failed = 0
        succeeded = 0

        # Process in chunks to update progress
        chunk_size = 500
        for i in range(0, len(urls), chunk_size):
            chunk = urls[i:i + chunk_size]

            with ThreadPoolExecutor(max_workers=50) as executor:
                futures = {executor.submit(titles_process_single, url, True): url for url in chunk}
                for future in as_completed(futures):
                    result = future.result()
                    processed += 1
                    if result.get("status") == "success":
                        succeeded += 1
                    else:
                        failed += 1
                    _update_state(batch_type, processed=processed, failed=failed)

        _update_state(batch_type, phase="complete", active=False,
            processed=processed, failed=failed)
        print(f"[BATCH-TITLES] Complete: {succeeded} succeeded, {failed} failed")

    except Exception as e:
        print(f"[BATCH-TITLES] Error: {e}")
        _update_state(batch_type, phase="error", active=False, error=str(e))


def start_titles_batch() -> dict:
    """Start unique titles batch processing in background thread."""
    with _batch_lock:
        if _batch_state["titles"]["active"]:
            return {"status": "error", "message": "Titles batch already running"}

    _reset_state("titles")
    thread = threading.Thread(target=_run_titles_batch, daemon=True)
    thread.start()
    return {"status": "started", "message": "Titles batch processing started"}
