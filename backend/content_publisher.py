"""
Content Publisher Service

Publishes kopteksten (content_top) to the website-configuration `/automated-content`
endpoint. FAQ content does NOT travel this path — see "SCOPE" below.

TWO PROPERTIES OF /automated-content THAT SHAPE THIS MODULE (verified against
staging 2026-08-04, by seeding one URL and posting a payload containing only a
different one):

1. It is a full-set REPLACE, not an upsert. Any URL absent from the payload is
   DELETED from the store. So every publish must carry the complete set we want
   live, and a partial payload is a deletion.
2. It requires url + content_top + content_bottom + country_language on every
   row; a missing field 400s the whole request ("Row N: Missing field(s): ...").

SCOPE — content_top only (changed 2026-08-04)
This used to also ship content_bottom (FAQ Q&A rendered to HTML) and content_faq
(the schema.org JSON-LD). Both are gone:

  * content_faq was never stored. The endpoint accepts the field and silently
    discards it — a record posted with content_faq comes back without it, and the
    newer /automated-content/records endpoint rejects it outright with
    "content_faq: This field was not expected." It was 792 MB of every upload
    that landed nowhere.
  * content_bottom is now owned by FAQ "Publish 2.0" (backend/faq_v2_publisher.py),
    which posts one record per QUESTION to /faq instead of one HTML blob per URL.

Because property 2 makes content_bottom mandatory, it is sent as "" rather than
omitted — which is also what retires the values already stored: the first publish
after this change clears content_bottom for every URL it carries.

Only URLs that actually have a content_top are sent. Combined with property 1,
that means FAQ-only URLs are dropped from this store — correct, since with
content_bottom gone they would be rows with no content at all.
"""
import os
import json
import requests
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import List, Dict, Optional
from backend.database import get_db_connection, return_db_connection

# API Configuration
CONTENT_API_URLS = {
    "dev": "http://dev.website-configuration.api.beslist.nl:5900/automated-content",
    "staging": "https://website-configuration-staging.api.beslist.nl/automated-content",
    "production": "https://website-configuration.api.beslist.nl/automated-content"
}

# The per-record sibling of the endpoint above, and a genuinely different animal: it
# upserts on (url, country_language) instead of replacing the whole store. So one URL
# can be pushed on its own — ~1 KB instead of the ~280 MB the batch moves.
# Semantics verified on staging 2026-08-04:
#   * a field OMITTED or explicitly null KEEPS its stored value
#   * "" CLEARS it
#   * country_language defaults to nl-nl
#   * an unexpected field is a hard 400 ("<field>: This field was not expected")
CONTENT_RECORDS_API_URLS = {
    "dev": "http://dev.website-configuration.api.beslist.nl:5900/automated-content/records",
    "staging": "https://website-configuration-staging.api.beslist.nl/automated-content/records",
    "production": "https://website-configuration.api.beslist.nl/automated-content/records",
}

CONTENT_API_KEYS = {
    "dev": os.getenv("CONTENT_API_KEY_DEV", ""),
    "staging": os.getenv("CONTENT_API_KEY_STAGING", ""),
    "production": os.getenv("CONTENT_API_KEY_PROD", "")
}

# Default environment
DEFAULT_ENV = os.getenv("CONTENT_API_ENV", "dev")

# Transient transport failures when posting to the website-configuration API.
# The envoy/LB proxy in front of it (DNS resolves to several backends)
# occasionally kills a TLS connection mid-upload on a large payload — this
# surfaces as SSLEOFError "EOF occurred in violation of protocol", a bare
# ConnectionError, or a half-sent ChunkedEncodingError. The publish is a
# full-set upsert, so re-POSTing the identical payload is safe; retry with
# exponential backoff (each attempt opens a fresh TLS session and can land on a
# different healthy backend) before surfacing the failure to the user.
_RETRYABLE_POST_EXC = (
    requests.exceptions.SSLError,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.Timeout,
)


_post_deadline_executor = ThreadPoolExecutor(max_workers=2)

# Hard wall-clock cap per POST attempt. On Windows, requests' (connect, read)
# timeout does not cover the SSL handshake — it can hang indefinitely. Running
# the POST in a thread and using Future.result(timeout=) guarantees we regain
# control. Set slightly above the requests timeout so the requests timeout
# fires first under normal conditions.
POST_DEADLINE = 2100  # 35 minutes — above the 1800s requests timeout


def _post_with_retry(api_url, *, headers, timeout, data=None, json=None,
                     max_attempts=4, on_retry=None):
    """POST with exponential-backoff retry on transient transport failures.

    Only transport-level errors are retried; an HTTP error *response* (4xx/5xx)
    is returned as-is for the caller to handle. Raises the last exception when
    every attempt fails. `on_retry(attempt, max_attempts, backoff_s, exc)` is
    called before each sleep so callers can surface retry state in the UI.

    Each attempt is wrapped in a hard wall-clock deadline via a thread executor
    to guard against Windows SSL handshake hangs where the requests timeout
    parameter does not fire.
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            future = _post_deadline_executor.submit(
                requests.post, api_url, headers=headers, data=data,
                json=json, timeout=timeout,
            )
            try:
                return future.result(timeout=POST_DEADLINE)
            except FuturesTimeoutError:
                future.cancel()
                raise requests.exceptions.Timeout(
                    f"POST to {api_url} exceeded hard deadline of {POST_DEADLINE}s"
                )
        except _RETRYABLE_POST_EXC as e:
            last_exc = e
            if attempt == max_attempts:
                break
            backoff = min(5 * 2 ** (attempt - 1), 60)  # 5s, 10s, 20s, cap 60s
            print(f"[Publisher] POST attempt {attempt}/{max_attempts} failed "
                  f"({type(e).__name__}: {e}); retrying in {backoff}s...")
            if on_retry:
                try:
                    on_retry(attempt, max_attempts, backoff, e)
                except Exception:
                    pass
            time.sleep(backoff)
    raise last_exc

# Background task storage
_publish_tasks = {}
_task_lock = threading.Lock()


def get_api_config(environment: str = None) -> tuple:
    """Get API URL and key for the specified environment."""
    env = environment or DEFAULT_ENV
    if env not in CONTENT_API_URLS:
        raise ValueError(f"Unknown environment: {env}. Valid options: {list(CONTENT_API_URLS.keys())}")
    return CONTENT_API_URLS[env], CONTENT_API_KEYS[env]


# The publishable set: every URL with a non-empty content_top that has not failed
# validation. Kept in one place because the count endpoint, the preview and the
# real publish must agree — on a replace-all endpoint a count that disagrees with
# the payload is a count that under-reports deletions.
_PUBLISHABLE_WHERE = """
    FROM pa.urls u
    JOIN pa.kopteksten_content k ON k.url_id = u.url_id
    LEFT JOIN pa.url_validation v ON v.url_id = u.url_id
    WHERE k.content IS NOT NULL AND k.content <> ''
      AND (v.is_valid IS NULL OR v.is_valid = TRUE)
"""


def get_content_batch(offset: int = 0, limit: int = 100) -> List[Dict]:
    """
    Fetch a batch of content for publishing (preview / curl helpers).
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT u.url AS url, k.content AS content_top"
                    + _PUBLISHABLE_WHERE
                    + " ORDER BY u.url LIMIT %s OFFSET %s", (limit, offset))

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

            item = {
                "url": url,
                "content_top": sanitize_for_api(row['content_top'] or ""),
                # Mandatory field, deliberately blank — see SCOPE in the module
                # docstring. Sending "" is what clears the retired FAQ blobs.
                "content_bottom": "",
                "country_language": "nl-nl"
            }
            result.append(item)

        return result

    finally:
        cur.close()
        return_db_connection(conn)


def get_total_content_count() -> int:
    """Count of URLs that a publish would send (and therefore keep live)."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) AS count" + _PUBLISHABLE_WHERE)
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
    Fetch ALL publishable items from the database.
    Returns a list of dicts with url, content_top, content_bottom, country_language.

    This is the complete set that will be live after the publish — anything not in
    it gets deleted, because /automated-content is a replace-all.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Get all unique URLs with their content in a single query
        cur.execute("SELECT u.url AS url, k.content AS content_top"
                    + _PUBLISHABLE_WHERE
                    + " ORDER BY u.url")

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

            item = {
                "url": url_norm,
                "content_top": sanitize_for_api(row['content_top'] or ""),
                # Mandatory field, deliberately blank — see SCOPE in the module
                # docstring. Sending "" is what clears the retired FAQ blobs.
                "content_bottom": "",
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


def publish_content_url(url: str, environment: str = None) -> Dict:
    """Push ONE url's content_top, synchronously, via /automated-content/records.

    Backs the Push button on the Kopteksten URL Lookup card. The batch endpoint
    cannot do this: it is a full-set replace, so posting one URL would delete every
    other one. This endpoint upserts on (url, country_language) and leaves the rest
    of the store alone.

    content_bottom is OMITTED here, not sent as "". On this endpoint an omitted field
    keeps its stored value while "" clears it — and the batch deliberately sends ""
    to retire the old FAQ blobs. A single-URL push has no business re-clearing
    anything, and omitting it also means this cannot fight FAQ Publish 2.0 over a
    field that store now owns.
    """
    env = environment or DEFAULT_ENV
    if env not in CONTENT_RECORDS_API_URLS:
        return {"success": False, "message": f"unknown environment {env!r}"}
    api_url = CONTENT_RECORDS_API_URLS[env]
    api_key = CONTENT_API_KEYS.get(env, "")
    if not api_key:
        return {"success": False, "message": f"no API key configured for {env!r}"}

    from backend.url_catalog import canonicalize_url
    canon = canonicalize_url(url)
    if not canon:
        return {"success": False, "message": f"could not canonicalize URL: {url!r}"}

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT k.content
              FROM pa.urls u
              JOIN pa.kopteksten_content k ON k.url_id = u.url_id
             WHERE u.url = %s
        """, (canon,))
        row = cur.fetchone()
        cur.close()
    finally:
        return_db_connection(conn)

    if not row or not (row["content"] or "").strip():
        return {"success": False, "url": canon, "env": env,
                "message": "no koptekst stored for this URL"}

    started = time.time()
    # Same shape as the batch's rows minus content_bottom; any extra key would 400.
    record = {"url": _normalize_url(canon),
              "content_top": sanitize_for_api(row["content"]),
              "country_language": "nl-nl"}
    try:
        resp = _post_with_retry(
            api_url,
            headers={"X-Api-Key": api_key, "Content-Type": "application/json"},
            data=json.dumps([record], ensure_ascii=False).encode("utf-8"),
            timeout=120,
        )
    except requests.RequestException as e:
        return {"success": False, "url": canon, "env": env, "error": str(e)}

    ok = 200 <= resp.status_code < 300
    out = {"success": ok, "url": record["url"], "env": env, "api_url": api_url,
           "status_code": resp.status_code, "chars": len(record["content_top"]),
           "duration_sec": round(time.time() - started, 1)}
    if not ok:
        out["response"] = (resp.text or "")[:500]
    else:
        try:
            out["records"] = (resp.json() or {}).get("records")
        except Exception:
            pass
    return out


def unpublish_content_url(url: str, environment: str = None) -> Dict:
    """Remove ONE url's koptekst from the live store — the production half of a delete.

    Deleting a koptekst in the tool used to leave it live until the next full batch
    publish, because the batch prunes only as a side effect of replacing. This makes
    the removal immediate.

    WHY THIS IS NOT ALWAYS A RECORD DELETE
    `content_bottom` sits in the SAME record and belongs to FAQ Publish 2.0, so
    `DELETE ?url=` would take the FAQ content with it. So:
      * content_bottom still populated -> push content_top = "" (on this endpoint ""
        clears a field), which removes the koptekst and leaves the FAQ blob alone;
      * content_bottom empty/absent    -> DELETE the whole record, so an all-empty row
        is not left behind as litter.
    One GET decides which, and it is cheap (single url, no wildcard).

    The push-state row is dropped either way, so the incremental publisher stops
    believing this url is live with content.
    """
    env = environment or DEFAULT_ENV
    if env not in CONTENT_RECORDS_API_URLS:
        return {"success": False, "message": f"unknown environment {env!r}"}
    api_url = CONTENT_RECORDS_API_URLS[env]
    api_key = CONTENT_API_KEYS.get(env, "")
    if not api_key:
        return {"success": False, "message": f"no API key configured for {env!r}"}

    from backend.url_catalog import canonicalize_url
    canon = canonicalize_url(url)
    if not canon:
        return {"success": False, "message": f"could not canonicalize URL: {url!r}"}
    wire = _normalize_url(canon)

    started = time.time()
    try:
        g = requests.get(api_url, headers={"X-Api-Key": api_key},
                         params={"url": wire}, timeout=60)
        live = g.json() if (g.text or "").startswith("[") else []
    except (requests.RequestException, ValueError) as e:
        return {"success": False, "url": wire, "env": env, "error": str(e)}

    if not live:
        _drop_push_state(canon, env)
        return {"success": True, "url": wire, "env": env, "action": "nothing_live",
                "message": "no live record for this URL"}

    has_bottom = any((r.get("content_bottom") or "").strip() for r in live)
    try:
        if has_bottom:
            # Clear only our field; the FAQ blob stays.
            resp = _post_with_retry(
                api_url,
                headers={"X-Api-Key": api_key, "Content-Type": "application/json"},
                data=json.dumps([{"url": wire, "content_top": "",
                                  "country_language": "nl-nl"}], ensure_ascii=False).encode("utf-8"),
                timeout=120,
            )
            action = "cleared_content_top"
        else:
            resp = requests.delete(api_url, headers={"X-Api-Key": api_key},
                                   params={"url": wire}, timeout=60)
            action = "deleted_record"
    except requests.RequestException as e:
        return {"success": False, "url": wire, "env": env, "error": str(e)}

    # 404 on the delete means it is already gone, which is the desired end state.
    ok = 200 <= resp.status_code < 300 or resp.status_code == 404
    if ok:
        _drop_push_state(canon, env)
    out = {"success": ok, "url": wire, "env": env, "action": action,
           "status_code": resp.status_code, "kept_content_bottom": has_bottom,
           "duration_sec": round(time.time() - started, 1)}
    if not ok:
        out["response"] = (resp.text or "")[:500]
    return out


def _drop_push_state(canon_url: str, env: str) -> None:
    """Forget that we pushed this url, so the incremental publisher's state matches
    reality. Best-effort: the state table may not exist yet on a fresh install."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM pa.kopteksten_push_state s
             USING pa.urls u
             WHERE u.url_id = s.url_id AND s.env = %s AND u.url = %s
        """, (env, canon_url))
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        print(f"[Publisher] could not drop push state for {canon_url}: {e}")
    finally:
        return_db_connection(conn)


def publish_all_content(environment: str = None, task_id: str = None) -> Dict:
    """
    Publish all kopteksten in a single API call.

    There is no content-type selector any more. It used to offer seo_only /
    faq_only, and both were unsafe against a replace-all endpoint: seo_only
    dropped every FAQ-only URL and blanked content_bottom on the rest, while
    faq_only blanked content_top for the entire corpus. With FAQ moved to
    Publish 2.0 there is exactly one thing this endpoint publishes — content_top
    — so the only correct payload is the whole publishable set.

    Args:
        environment: Target environment (dev, staging, production)
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
        _payload_mb = round(payload_size / 1024 / 1024, 2)
        _update_progress("uploading", total_items=total_count, payload_size_mb=_payload_mb)

        def _on_retry(attempt, max_attempts, backoff, exc):
            _update_progress("retrying", total_items=total_count,
                             payload_size_mb=_payload_mb, attempt=attempt,
                             max_attempts=max_attempts, retry_in_sec=backoff,
                             last_error=f"{type(exc).__name__}: {exc}")

        response = _post_with_retry(
            api_url,
            headers=headers,
            data=payload_json,
            timeout=1800,  # 30 minute timeout for large payload
            on_retry=_on_retry,
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


# Background task functions
def _run_publish_task(task_id: str, environment: str):
    """Background worker to run the publish task."""
    with _task_lock:
        _publish_tasks[task_id]["status"] = "running"
        _publish_tasks[task_id]["started_at"] = time.time()
        _publish_tasks[task_id]["progress"] = {"phase": "fetching", "total_items": 0}

    try:
        result = publish_all_content(environment=environment, task_id=task_id)
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
                    # The column predates the selector; there is only one kind of
                    # publish now, and naming the field it carries is more use to
                    # the history table than a constant "all".
                    "content_top",
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


def start_publish_task(environment: str) -> str:
    """
    Start a background publish task.
    Returns task_id that can be used to check status.

    Args:
        environment: Target environment (dev, staging, production)
    """
    import uuid
    task_id = str(uuid.uuid4())[:8]

    with _task_lock:
        _publish_tasks[task_id] = {
            "status": "pending",
            "environment": environment,
            "content_type": "content_top",
            "created_at": time.time(),
            "started_at": None,
            "completed_at": None,
            "result": None,
            "error": None
        }

    # Start background thread
    thread = threading.Thread(target=_run_publish_task, args=(task_id, environment))
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
            # No dry-run: this used to pass dry_run=, which publish_all_content has
            # never accepted (TypeError). Use `count`/`sample` to inspect first —
            # and note that a publish REPLACES the live set.
            result = publish_all_content()
            print(json.dumps(result, indent=2))

        else:
            print("Usage: python content_publisher.py [count|sample|curl|publish]")
            print("  count           - Show total URLs that would be published")
            print("  sample [n]      - Show sample of n content items (default: 5)")
            print("  curl [n]        - Generate curl command with n items (default: 10)")
            print("  publish         - Publish all content (REPLACES the live set)")
    else:
        print("Usage: python content_publisher.py [count|sample|curl|publish]")
