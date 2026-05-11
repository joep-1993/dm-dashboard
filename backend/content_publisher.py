"""
Content Publisher Service

Publishes generated content (content_top) and FAQ content to the website-configuration API.
Supports batched publishing to handle large datasets.
"""
import os
import json
import requests
import threading
import time
from typing import List, Dict, Optional
from backend.database import get_db_connection, return_db_connection

# API Configuration
CONTENT_API_URLS = {
    "dev": "http://dev.website-configuration.api.beslist.nl:5900/automated-content",
    "staging": "https://website-configuration-staging.api.beslist.nl/automated-content",
    "production": "https://website-configuration.api.beslist.nl/automated-content"
}

CONTENT_API_KEYS = {
    "dev": os.getenv("CONTENT_API_KEY_DEV", ""),
    "staging": os.getenv("CONTENT_API_KEY_STAGING", ""),
    "production": os.getenv("CONTENT_API_KEY_PROD", "")
}

# Default environment
DEFAULT_ENV = os.getenv("CONTENT_API_ENV", "dev")

# Background task storage
_publish_tasks = {}
_task_lock = threading.Lock()


def get_api_config(environment: str = None) -> tuple:
    """Get API URL and key for the specified environment."""
    env = environment or DEFAULT_ENV
    if env not in CONTENT_API_URLS:
        raise ValueError(f"Unknown environment: {env}. Valid options: {list(CONTENT_API_URLS.keys())}")
    return CONTENT_API_URLS[env], CONTENT_API_KEYS[env]


def faq_json_to_html(faq_json_str: str) -> str:
    """
    Convert FAQ JSON array to HTML format.

    Input format: [{"question": "...", "answer": "..."}, ...]
    Output format: <div class="faq-item"><h3>Question</h3><p>Answer</p></div>...
    """
    if not faq_json_str:
        return ""

    try:
        faq_list = json.loads(faq_json_str)
        if not isinstance(faq_list, list):
            return ""

        html_parts = []
        for item in faq_list:
            question = item.get("question", "")
            answer = item.get("answer", "")
            if question and answer:
                html_parts.append(
                    f'<div class="faq-item">'
                    f'<h3>{question}</h3>'
                    f'<p>{answer}</p>'
                    f'</div>'
                )

        return "".join(html_parts)
    except (json.JSONDecodeError, TypeError):
        return ""


def schema_org_to_script_tag(schema_org_str: str) -> str:
    """
    Return schema.org JSON-LD for content_faq (raw JSON, no script tags).

    Input: JSON-LD string (FAQPage schema)
    Output: JSON-LD string as-is
    """
    if not schema_org_str:
        return ""

    return schema_org_str


def faq_json_to_content_bottom(faq_json_str: str) -> str:
    """
    Convert FAQ JSON array to content_bottom format.
    Only includes Q&As that have internal links (beslist.nl).

    Input format: [{"question": "...", "answer": "..."}, ...]
    Output format: <br /><strong>Question</strong><br>Answer<br>...
    """
    import re

    if not faq_json_str:
        return ""

    try:
        faq_list = json.loads(faq_json_str)
        if not isinstance(faq_list, list):
            return ""

        # Simple pattern to detect internal links (beslist.nl in href)
        internal_link_pattern = re.compile(r'href="[^"]*beslist\.nl', re.IGNORECASE)

        html_parts = []
        for item in faq_list:
            question = item.get("question", "")
            answer = item.get("answer", "")

            # Only include if question or answer has internal links
            has_internal_link = (
                internal_link_pattern.search(question) or
                internal_link_pattern.search(answer)
            )

            if question and answer and has_internal_link:
                html_parts.append(
                    f'<strong>{question}</strong><br>{answer}<br>'
                )

        if not html_parts:
            return ""

        # Start with <br /> and join all parts with <br /> for blank lines between questions
        return "<br />" + "<br />".join(html_parts)
    except (json.JSONDecodeError, TypeError):
        return ""


def get_all_content_for_publishing() -> List[Dict]:
    """
    Fetch all content (content_top and FAQ) from database, merged by URL.
    Returns a list of dicts with url, content_top, content_bottom, content_faq.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Pull content_top (kopteksten) + faq_json from the new schema, joined
        # through the URL catalog. LEFT JOINs from pa.urls so a row appears as
        # long as either content table has data.
        cur.execute("""
            SELECT
                u.url AS url,
                k.content AS content_top,
                f.faq_json AS faq_json
            FROM pa.urls u
            LEFT JOIN pa.kopteksten_content k ON k.url_id = u.url_id
            LEFT JOIN pa.faq_content_v2  f ON f.url_id = u.url_id
            LEFT JOIN pa.url_validation v ON v.url_id = u.url_id
            WHERE (k.content IS NOT NULL OR f.faq_json IS NOT NULL)
              AND (v.is_valid IS NULL OR v.is_valid = TRUE)
        """)

        rows = cur.fetchall()

        # Build result list with unique URLs
        url_data = {}
        for row in rows:
            url = row['url']
            if url not in url_data:
                url_data[url] = {
                    "url": url,
                    "content_top": "",
                    "content_bottom": "",
                    "content_faq": "",
                    "country_language": "nl-nl"
                }

            # Update content_top if available
            if row['content_top']:
                url_data[url]["content_top"] = row['content_top']

            # Convert FAQ JSON to HTML if available
            if row['faq_json']:
                url_data[url]["content_faq"] = faq_json_to_html(row['faq_json'])
                url_data[url]["content_bottom"] = faq_json_to_content_bottom(row['faq_json'])

        return list(url_data.values())

    finally:
        cur.close()
        return_db_connection(conn)


def get_content_batch(offset: int = 0, limit: int = 100) -> List[Dict]:
    """
    Fetch a batch of content for publishing.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                u.url AS url,
                k.content AS content_top,
                f.faq_json AS faq_json,
                f.schema_org AS schema_org
            FROM pa.urls u
            LEFT JOIN pa.kopteksten_content k ON k.url_id = u.url_id
            LEFT JOIN pa.faq_content_v2  f ON f.url_id = u.url_id
            LEFT JOIN pa.url_validation v ON v.url_id = u.url_id
            WHERE (k.content IS NOT NULL OR f.faq_json IS NOT NULL)
              AND (v.is_valid IS NULL OR v.is_valid = TRUE)
            ORDER BY u.url
            LIMIT %s OFFSET %s
        """, (limit, offset))

        rows = cur.fetchall()

        # Case-insensitive deduplication to prevent publish failures
        result = []
        seen_urls_lower = set()

        for row in rows:
            url = row['url']
            url_lower = url.lower()

            if url_lower in seen_urls_lower:
                continue
            seen_urls_lower.add(url_lower)

            content_top = sanitize_for_api(row['content_top'] or "")
            # Use schema_org wrapped in script tag for content_faq
            content_faq = sanitize_for_api(schema_org_to_script_tag(row['schema_org'])) if row['schema_org'] else ""
            content_bottom = sanitize_for_api(faq_json_to_content_bottom(row['faq_json'])) if row['faq_json'] else ""

            item = {
                "url": url,
                "content_top": content_top,
                "content_bottom": content_bottom,
                "content_faq": content_faq,
                "country_language": "nl-nl"
            }
            result.append(item)

        return result

    finally:
        cur.close()
        return_db_connection(conn)


def get_total_content_count() -> int:
    """Get total count of unique URLs with content."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT COUNT(*) AS count
            FROM pa.urls u
            LEFT JOIN pa.kopteksten_content k ON k.url_id = u.url_id
            LEFT JOIN pa.faq_content_v2  f ON f.url_id = u.url_id
            LEFT JOIN pa.url_validation v ON v.url_id = u.url_id
            WHERE (k.content IS NOT NULL OR f.faq_json IS NOT NULL)
              AND (v.is_valid IS NULL OR v.is_valid = TRUE)
        """)
        return cur.fetchone()['count']
    finally:
        cur.close()
        return_db_connection(conn)


def sanitize_for_api(text: str) -> str:
    """
    Sanitize text content for the website-configuration API.
    Escapes characters that might cause SQL issues on the receiving end.
    """
    if not text:
        return ""
    # First normalize double single quotes to single (legacy data issue)
    # Then replace single quotes with HTML entity to avoid SQL escaping issues
    text = text.replace("''", "'")
    return text.replace("'", "&#39;")


def _normalize_url(url: str) -> str:
    """Apply Beslist's URL canonicalization rules before publishing:
      - strip query string (everything from '?', including tracking params)
      - strip URL fragment (everything from '#')
      - trailing-slash rule by structure:
          * URL contains '/c/'  → MUST NOT end with '/'
          * URL contains '/r/' but not '/c/' → MUST end with '/'
          * URL contains neither            → MUST end with '/'
    Case is preserved so production stores the URL as the publisher sent it.
    """
    if not url:
        return ""
    # Strip query string and fragment
    url = url.split('?', 1)[0].split('#', 1)[0]
    if '/c/' in url:
        url = url.rstrip('/')
    else:
        if not url.endswith('/'):
            url = url + '/'
    return url


def get_all_content_items() -> List[Dict]:
    """
    Fetch ALL content items from database for publishing.
    Returns a list of dicts with url, content_top, content_bottom, content_faq.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Get all unique URLs with their content in a single query
        cur.execute("""
            SELECT
                u.url AS url,
                k.content AS content_top,
                f.faq_json AS faq_json,
                f.schema_org AS schema_org
            FROM pa.urls u
            LEFT JOIN pa.kopteksten_content k ON k.url_id = u.url_id
            LEFT JOIN pa.faq_content_v2  f ON f.url_id = u.url_id
            LEFT JOIN pa.url_validation v ON v.url_id = u.url_id
            WHERE (k.content IS NOT NULL OR f.faq_json IS NOT NULL)
              AND (v.is_valid IS NULL OR v.is_valid = TRUE)
            ORDER BY u.url
        """)

        rows = cur.fetchall()

        # Single dedup pass over the normalized URL form. We canonicalize
        # via _normalize_url (strip ?…, strip #…, fix trailing slash by
        # /c/-vs-/r/ rule) and use the lowercased canonical form as the
        # dedup key. The URL we SEND to production is the normalized form
        # — production should accept that as-is.
        result = []
        seen_canon_lower = set()
        normalised_collisions = 0

        for row in rows:
            url_raw = row['url']
            url_norm = _normalize_url(url_raw)
            canon_key = url_norm.lower()
            if canon_key in seen_canon_lower:
                normalised_collisions += 1
                continue
            seen_canon_lower.add(canon_key)

            content_top = sanitize_for_api(row['content_top'] or "")
            # Use schema_org wrapped in script tag for content_faq
            content_faq = sanitize_for_api(schema_org_to_script_tag(row['schema_org'])) if row['schema_org'] else ""
            content_bottom = sanitize_for_api(faq_json_to_content_bottom(row['faq_json'])) if row['faq_json'] else ""

            item = {
                "url": url_norm,
                "content_top": content_top,
                "content_bottom": content_bottom,
                "content_faq": content_faq,
                "country_language": "nl-nl"
            }
            result.append(item)

        if normalised_collisions > 0:
            print(f"[Publisher] Skipped {normalised_collisions} URLs that collapsed to "
                  f"the same canonical form (query/fragment/case/slash variants)")

        # Alphabetical ordering for the payload (matches what the upstream
        # API previously saw).
        result.sort(key=lambda it: it["url"])
        return result

    finally:
        cur.close()
        return_db_connection(conn)


def publish_all_content(environment: str = None, content_type: str = "all", task_id: str = None) -> Dict:
    """
    Publish content in a single API call.

    Args:
        environment: Target environment (dev, staging, production)
        content_type: What to publish - "all", "seo_only", or "faq_only"
        task_id: Optional task ID for progress tracking

    Returns:
        Dict with results
    """
    env = environment or DEFAULT_ENV
    api_url, api_key = get_api_config(env)

    def _update_progress(phase: str, **kwargs):
        if task_id:
            with _task_lock:
                if task_id in _publish_tasks:
                    _publish_tasks[task_id]["progress"] = {"phase": phase, **kwargs}

    t0 = time.time()
    print(f"[Publisher] Fetching content from database...")
    _update_progress("fetching")
    content_items = get_all_content_items()
    t1 = time.time()
    print(f"[Publisher] Fetched {len(content_items)} items in {t1-t0:.1f}s")

    # Filter based on content_type
    if content_type == "seo_only":
        content_items = [
            {**item, "content_faq": "", "content_bottom": ""}
            for item in content_items
            if item.get("content_top")
        ]
        print(f"[Publisher] Publishing SEO content only")
    elif content_type == "faq_only":
        content_items = [
            {**item, "content_top": ""}
            for item in content_items
            if item.get("content_faq")
        ]
        print(f"[Publisher] Publishing FAQ content only")
    else:
        print(f"[Publisher] Publishing all content")

    total_count = len(content_items)
    print(f"[Publisher] Total URLs to publish: {total_count}")
    print(f"[Publisher] Target environment: {env} ({api_url})")
    _update_progress("building_payload", total_items=total_count)

    if not content_items:
        return {
            "success": True,
            "message": "No items to publish",
            "environment": env,
            "total_urls": 0
        }

    # Build payload
    payload = {"data": content_items}
    t2 = time.time()
    payload_json = json.dumps(payload)
    payload_size = len(payload_json)
    t3 = time.time()
    print(f"[Publisher] Payload size: {payload_size / 1024 / 1024:.2f} MB (serialized in {t3-t2:.1f}s)")

    # Free the list to reduce memory usage during upload
    del content_items
    del payload

    headers = {
        "X-Api-Key": api_key,
        "Content-Type": "application/json"
    }

    try:
        print(f"[Publisher] Sending request to {api_url}...")
        _update_progress("uploading", total_items=total_count, payload_size_mb=round(payload_size / 1024 / 1024, 2))
        response = requests.post(
            api_url,
            headers=headers,
            data=payload_json,
            timeout=1800  # 30 minute timeout for large payload
        )
        t4 = time.time()
        print(f"[Publisher] Response: {response.status_code} in {t4-t3:.1f}s (total: {t4-t0:.1f}s)")

        return {
            "success": response.status_code in (200, 201),
            "status_code": response.status_code,
            "environment": env,
            "api_url": api_url,
            "total_urls": total_count,
            "items_published": total_count if response.status_code in (200, 201) else 0,
            "payload_size_mb": round(payload_size / 1024 / 1024, 2),
            "timing": {
                "fetch_db_sec": round(t1-t0, 1),
                "serialize_sec": round(t3-t2, 1),
                "upload_sec": round(t4-t3, 1),
                "total_sec": round(t4-t0, 1)
            },
            "response": response.text if response.text else None
        }
    except requests.RequestException as e:
        return {
            "success": False,
            "error": str(e),
            "environment": env,
            "api_url": api_url,
            "total_urls": total_count
        }


def publish_content_batched(batch_size: int = 5000, limit: int = None, dry_run: bool = False, environment: str = None) -> Dict:
    """
    Publish content in batches to avoid overwhelming the API.

    Args:
        batch_size: Number of items per API request (default: 5000)
        limit: Maximum total items to publish (None = all)
        dry_run: If True, just return stats without making API calls
        environment: Target environment (dev, staging, production)

    Returns:
        Dict with results including per-batch status
    """
    env = environment or DEFAULT_ENV
    api_url, api_key = get_api_config(env)

    print(f"[Publisher] Fetching content from database...")
    all_items = get_all_content_items()

    # Apply limit if specified
    if limit is not None:
        all_items = all_items[:limit]

    total_count = len(all_items)
    print(f"[Publisher] Total URLs to publish: {total_count}")
    print(f"[Publisher] Batch size: {batch_size}")
    print(f"[Publisher] Target environment: {env} ({api_url})")

    if dry_run:
        num_batches = (total_count + batch_size - 1) // batch_size if total_count > 0 else 0
        return {
            "success": True,
            "dry_run": True,
            "environment": env,
            "api_url": api_url,
            "total_urls": total_count,
            "batch_size": batch_size,
            "num_batches": num_batches,
            "payload_size_mb": round(len(json.dumps({"data": all_items})) / 1024 / 1024, 2)
        }

    if not all_items:
        return {
            "success": True,
            "message": "No items to publish",
            "environment": env,
            "total_urls": 0
        }

    headers = {
        "X-Api-Key": api_key,
        "Content-Type": "application/json"
    }

    # Process in batches
    total_published = 0
    batch_results = []

    for i in range(0, total_count, batch_size):
        batch_num = (i // batch_size) + 1
        batch_items = all_items[i:i + batch_size]

        payload = {"data": batch_items}
        payload_size = len(json.dumps(payload))

        print(f"[Publisher] Batch {batch_num}: Sending {len(batch_items)} items ({payload_size / 1024 / 1024:.2f} MB)...")

        try:
            response = requests.post(
                api_url,
                headers=headers,
                json=payload,
                timeout=300  # 5 minute timeout per batch
            )

            batch_success = response.status_code in (200, 201)
            batch_result = {
                "batch": batch_num,
                "items": len(batch_items),
                "success": batch_success,
                "status_code": response.status_code,
                "response": response.text if response.text else None
            }

            if batch_success:
                total_published += len(batch_items)
                print(f"[Publisher] Batch {batch_num}: SUCCESS ({len(batch_items)} items)")
            else:
                print(f"[Publisher] Batch {batch_num}: FAILED (status {response.status_code})")
                batch_results.append(batch_result)
                # Stop on first failure
                return {
                    "success": False,
                    "environment": env,
                    "api_url": api_url,
                    "total_urls": total_count,
                    "items_published": total_published,
                    "failed_at_batch": batch_num,
                    "batch_results": batch_results,
                    "error": f"Batch {batch_num} failed with status {response.status_code}"
                }

            batch_results.append(batch_result)

        except requests.RequestException as e:
            print(f"[Publisher] Batch {batch_num}: ERROR - {str(e)}")
            batch_results.append({
                "batch": batch_num,
                "items": len(batch_items),
                "success": False,
                "error": str(e)
            })
            return {
                "success": False,
                "environment": env,
                "api_url": api_url,
                "total_urls": total_count,
                "items_published": total_published,
                "failed_at_batch": batch_num,
                "batch_results": batch_results,
                "error": str(e)
            }

    return {
        "success": True,
        "environment": env,
        "api_url": api_url,
        "total_urls": total_count,
        "items_published": total_published,
        "batch_size": batch_size,
        "num_batches": len(batch_results),
        "batch_results": batch_results
    }


# Background task functions
def _run_publish_task(task_id: str, environment: str, content_type: str = "all"):
    """Background worker to run the publish task."""
    with _task_lock:
        _publish_tasks[task_id]["status"] = "running"
        _publish_tasks[task_id]["started_at"] = time.time()
        _publish_tasks[task_id]["progress"] = {"phase": "fetching", "total_items": 0}

    try:
        result = publish_all_content(environment=environment, content_type=content_type, task_id=task_id)
        with _task_lock:
            _publish_tasks[task_id]["status"] = "completed"
            _publish_tasks[task_id]["result"] = result
            _publish_tasks[task_id]["completed_at"] = time.time()

        # Record successful publish in log table
        if result.get("success"):
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO pa.publish_log
                        (environment, content_type, total_urls, status, payload_size_mb, duration_sec)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    environment,
                    content_type,
                    result.get("total_urls", 0),
                    "success",
                    result.get("payload_size_mb"),
                    result.get("timing", {}).get("total_sec"),
                ))
                conn.commit()
                cur.close()
                return_db_connection(conn)
            except Exception as log_err:
                print(f"[Publisher] Warning: Failed to log publish: {log_err}")
    except Exception as e:
        print(f"[Publisher] Error: {str(e)}")
        with _task_lock:
            _publish_tasks[task_id]["status"] = "failed"
            _publish_tasks[task_id]["error"] = str(e)
            _publish_tasks[task_id]["completed_at"] = time.time()


def start_publish_task(environment: str, content_type: str = "all") -> str:
    """
    Start a background publish task.
    Returns task_id that can be used to check status.

    Args:
        environment: Target environment (dev, staging, production)
        content_type: What to publish - "all", "seo_only", or "faq_only"
    """
    import uuid
    task_id = str(uuid.uuid4())[:8]

    with _task_lock:
        _publish_tasks[task_id] = {
            "status": "pending",
            "environment": environment,
            "content_type": content_type,
            "created_at": time.time(),
            "started_at": None,
            "completed_at": None,
            "result": None,
            "error": None
        }

    # Start background thread
    thread = threading.Thread(target=_run_publish_task, args=(task_id, environment, content_type))
    thread.daemon = True
    thread.start()

    return task_id


def get_publish_task_status(task_id: str) -> Dict:
    """Get the status of a publish task."""
    with _task_lock:
        if task_id not in _publish_tasks:
            return {"error": "Task not found", "task_id": task_id}
        return {"task_id": task_id, **_publish_tasks[task_id]}


def generate_curl_command(content_items: List[Dict] = None, limit: int = 10, environment: str = None) -> str:
    """
    Generate a curl command for publishing content.

    Args:
        content_items: Optional list of content items. If None, fetches from database.
        limit: Maximum number of items to include in the command
        environment: Target environment (dev, staging, production)

    Returns:
        A curl command string
    """
    api_url, api_key = get_api_config(environment)

    if content_items is None:
        content_items = get_content_batch(0, limit)
    else:
        content_items = content_items[:limit]

    payload = {"data": content_items}
    json_str = json.dumps(payload, indent=4, ensure_ascii=False)

    # Escape single quotes for shell
    json_str_escaped = json_str.replace("'", "'\\''")

    curl_cmd = f"""curl --location '{api_url}' \\
--header 'X-Api-Key: {api_key}' \\
--header 'Content-Type: application/json' \\
--data '{json_str_escaped}'"""

    return curl_cmd


# CLI for testing
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        cmd = sys.argv[1]

        if cmd == "count":
            count = get_total_content_count()
            print(f"Total URLs with content: {count}")

        elif cmd == "sample":
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 5
            items = get_content_batch(0, limit)
            print(json.dumps(items, indent=2, ensure_ascii=False))

        elif cmd == "curl":
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            print(generate_curl_command(limit=limit))

        elif cmd == "publish":
            dry_run = "--dry-run" in sys.argv
            result = publish_all_content(dry_run=dry_run)
            print(json.dumps(result, indent=2))

        else:
            print("Usage: python content_publisher.py [count|sample|curl|publish]")
            print("  count           - Show total URLs with content")
            print("  sample [n]      - Show sample of n content items (default: 5)")
            print("  curl [n]        - Generate curl command with n items (default: 10)")
            print("  publish [--dry-run] - Publish all content (use --dry-run to test)")
    else:
        print("Usage: python content_publisher.py [count|sample|curl|publish]")
