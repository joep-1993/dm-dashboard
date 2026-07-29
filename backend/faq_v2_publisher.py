"""
FAQ "Publish 2.0" — pushes FAQ content to the website-configuration `/faq`
section, which is a DIFFERENT store from the `/automated-content` endpoint that
the existing Publish button uses.

Why a second publisher at all: `/automated-content` takes one record per URL
carrying content_top + content_bottom + content_faq as blobs. `/faq` takes one
record per QUESTION, so the FAQ becomes queryable/orderable server-side instead
of an opaque HTML blob. Contract verified live against production 2026-07-29:

    POST /faq            body = NON-EMPTY JSON array of records
      required : url, question, answer
      optional : country_code (default "NL"), sort_order (default 0)
      any other field -> 400 "<field>: This field was not expected."
                         (position / page_title / schema_org / content_faq all
                          rejected — schema_org has NO home in this endpoint)
      response : {"status":"OK","records":N}
    GET  /faq?url=<enc>  -> [] or [{id,url,question,country_code,answer,
                                    sort_order,created_at,updated_at}, ...]
                            requires the url param; there is NO list-all.
    DELETE /faq?url=<enc> -> {"status":"OK","records":N}   (url-scoped)
    PUT                   -> 405

TWO SEMANTICS THAT SHAPE THIS MODULE:

1. Upsert key is (url, question) — re-posting the same pair updates the answer
   in place. So this is safe to re-run.
2. It is ADDITIVE, not a full-set replace. Questions we DON'T send are left
   alone. Regenerating a URL's FAQ with different question text therefore leaves
   the old questions live. A true replace needs DELETE-then-POST per URL, and at
   280k+ URLs that is 280k extra round trips — impractical, so it is offered as
   an opt-in (`replace=True`) for narrow re-pushes, NOT the default. See the
   `replace` docstring below.

Scale: ~280,600 URLs x ~6 Q&A = ~1.7M records. Everything here streams from a
server-side cursor; nothing materialises the full set.
"""
import json
import os
import threading
import time
import uuid

from backend.content_publisher import _post_with_retry
from backend.database import get_db_connection, return_db_connection

FAQ_API_URLS = {
    "dev": "http://dev.website-configuration.api.beslist.nl:5900/faq",
    "staging": "https://website-configuration-staging.api.beslist.nl/faq",
    "production": "https://website-configuration.api.beslist.nl/faq",
}
# Same keys as /automated-content on the same host (verified: the prod key
# authenticates on /faq; the staging key 401s against production).
FAQ_API_KEYS = {
    "dev": lambda: os.getenv("CONTENT_API_KEY_DEV", ""),
    "staging": lambda: os.getenv("CONTENT_API_KEY_STAGING", ""),
    "production": lambda: os.getenv("CONTENT_API_KEY_PROD", ""),
}

# Records per POST. /page-titles on the same host uses 5000 and validates a body
# atomically, so a batch is all-or-nothing; keep it modest so one rejected
# record costs less work to isolate.
BATCH_SIZE = 2000
# Rows pulled per server-side cursor fetch. 1.7M records will not fit in memory.
CURSOR_ITERSIZE = 2000

_tasks = {}
_task_lock = threading.Lock()


def _set_progress(task_id, **kw):
    if not task_id:
        return
    with _task_lock:
        t = _tasks.get(task_id)
        if t is not None:
            t.setdefault("progress", {}).update(kw)


def _iter_records(cur, limit=None):
    """Yield flat /faq records from pa.faq_content_v2, streaming.

    faq_json is TEXT holding a JSON array of {question, answer} (verified: that
    is the only key shape present). sort_order is the array index, so the
    published order matches the generated order. Rows whose JSON is unparseable
    or not a list are skipped and counted rather than aborting the run.
    """
    sql = """
        SELECT u.url, f.faq_json
        FROM pa.faq_content_v2 f
        JOIN pa.urls u ON u.url_id = f.url_id
        WHERE f.faq_json IS NOT NULL AND f.faq_json <> ''
        ORDER BY f.url_id
    """
    if limit:
        sql += " LIMIT %s"
        cur.execute(sql, (limit,))
    else:
        cur.execute(sql)

    for row in cur:
        url, raw = row["url"], row["faq_json"]
        try:
            items = json.loads(raw)
        except Exception:
            yield ("__skip__", url, "unparseable faq_json")
            continue
        if not isinstance(items, list):
            yield ("__skip__", url, f"faq_json is {type(items).__name__}, not a list")
            continue
        for i, it in enumerate(items):
            if not isinstance(it, dict):
                continue
            q = (it.get("question") or "").strip()
            a = (it.get("answer") or "").strip()
            if not q or not a:
                # url+question+answer are all required by the API; an incomplete
                # pair would 400 the whole batch.
                continue
            yield ("__rec__", url, {
                "url": url,
                "question": q,
                "answer": a,
                "country_code": "NL",
                "sort_order": i,
            })


def _post_batch(records, env):
    api_url = FAQ_API_URLS[env]
    key = FAQ_API_KEYS[env]()
    resp = _post_with_retry(
        api_url,
        headers={"X-Api-Key": key, "Content-Type": "application/json"},
        data=json.dumps(records, ensure_ascii=False).encode("utf-8"),
        timeout=600,
    )
    ok = 200 <= resp.status_code < 300
    return ok, resp.status_code, (resp.text or "")[:500]


def _delete_url(url, env):
    """DELETE /faq?url=… — the only removal primitive (url-scoped)."""
    import urllib.parse

    import requests
    api_url = f"{FAQ_API_URLS[env]}?url={urllib.parse.quote(url, safe='')}"
    r = requests.delete(api_url, headers={"X-Api-Key": FAQ_API_KEYS[env]()}, timeout=60)
    return 200 <= r.status_code < 300, r.status_code


def publish_faq_v2(env="production", limit=None, replace=False, task_id=None):
    """Push every FAQ Q&A pair to the /faq section.

    limit   — cap on URLs (not records); for a trial run.
    replace — DELETE each URL's existing questions before posting it, making the
              published set exactly match ours. Costs one extra request PER URL,
              so only use it on a small `limit`. Off by default: with ~280k URLs
              it would mean ~280k deletes, and a delete that succeeds before a
              failing post would drop that URL's FAQ entirely.
    """
    if env not in FAQ_API_URLS:
        return {"success": False, "message": f"unknown env {env!r}"}
    if not FAQ_API_KEYS[env]():
        return {"success": False, "message": f"no API key configured for env {env!r}"}

    started = time.time()
    _set_progress(task_id, phase="counting")

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*) AS n FROM pa.faq_content_v2 f
            JOIN pa.urls u ON u.url_id = f.url_id
            WHERE f.faq_json IS NOT NULL AND f.faq_json <> ''
        """)
        total_urls = cur.fetchone()["n"]
        cur.close()
        if limit:
            total_urls = min(total_urls, limit)
        _set_progress(task_id, phase="pushing", total_urls=total_urls,
                      urls_done=0, records_pushed=0, batches=0, failed=0)

        # Server-side cursor: withhold=False is fine, we consume it in one pass.
        cur = conn.cursor(name="faq_v2_stream")
        cur.itersize = CURSOR_ITERSIZE

        batch, batch_results = [], []
        pushed = failed = urls_done = 0
        skipped, seen_urls = [], set()
        deleted_urls = set()

        def flush():
            nonlocal batch, pushed, failed
            if not batch:
                return
            ok, code, text = _post_batch(batch, env)
            batch_results.append({"count": len(batch), "ok": ok, "status_code": code,
                                  "response": text if not ok else ""})
            if ok:
                pushed += len(batch)
            else:
                failed += len(batch)
            batch = []
            _set_progress(task_id, records_pushed=pushed, failed=failed,
                          batches=len(batch_results), urls_done=urls_done)

        for kind, url, payload in _iter_records(cur, limit):
            if kind == "__skip__":
                if len(skipped) < 50:
                    skipped.append({"url": url, "reason": payload})
                continue
            if url not in seen_urls:
                seen_urls.add(url)
                urls_done += 1
                if replace and url not in deleted_urls:
                    try:
                        _delete_url(url, env)
                        deleted_urls.add(url)
                    except Exception as e:
                        if len(skipped) < 50:
                            skipped.append({"url": url, "reason": f"delete failed: {e}"})
            batch.append(payload)
            if len(batch) >= BATCH_SIZE:
                flush()
        flush()
        cur.close()
    finally:
        return_db_connection(conn)

    result = {
        "success": failed == 0,
        "env": env,
        "api_url": FAQ_API_URLS[env],
        "total_urls": total_urls,
        "urls_processed": urls_done,
        "records_pushed": pushed,
        "records_failed": failed,
        "batches": len(batch_results),
        "replace": replace,
        "duration_sec": round(time.time() - started, 1),
    }
    if skipped:
        result["skipped"] = skipped
        result["skipped_count"] = len(skipped)
    bad = [b for b in batch_results if not b["ok"]]
    if bad:
        result["failed_batches"] = bad[:10]
    return result


# ---------------------------------------------------------------------------
# Background task wrapper — same shape as content_publisher's, so the frontend
# can poll it with the identical pattern.
# ---------------------------------------------------------------------------
def _run(task_id, env, limit, replace):
    with _task_lock:
        _tasks[task_id].update(status="running", started_at=time.time())
    try:
        res = publish_faq_v2(env=env, limit=limit, replace=replace, task_id=task_id)
        with _task_lock:
            _tasks[task_id].update(status="completed", result=res,
                                   completed_at=time.time())
    except Exception as e:
        with _task_lock:
            _tasks[task_id].update(status="failed", error=str(e),
                                   completed_at=time.time())


def start_faq_v2_task(env="production", limit=None, replace=False):
    task_id = str(uuid.uuid4())
    with _task_lock:
        _tasks[task_id] = {"status": "queued", "env": env, "progress": {}}
    threading.Thread(target=_run, args=(task_id, env, limit, replace),
                     daemon=True).start()
    return task_id


def get_faq_v2_status(task_id):
    with _task_lock:
        t = _tasks.get(task_id)
        return dict(t) if t else {"error": "Task not found"}


def get_faq_v2_stats():
    """Row/record counts for the button's confirm dialog."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*) AS urls FROM pa.faq_content_v2 f
            JOIN pa.urls u ON u.url_id = f.url_id
            WHERE f.faq_json IS NOT NULL AND f.faq_json <> ''
        """)
        urls = cur.fetchone()["urls"]
        cur.close()
        return {"urls": urls, "est_records": urls * 6, "api_url": FAQ_API_URLS["production"]}
    finally:
        return_db_connection(conn)
