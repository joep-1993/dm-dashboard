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
   280k+ URLs that is 280k extra round trips — offered as `replace=True` for
   narrow re-pushes, NOT the default.

INCREMENTAL PUSHES (mode="new", the default)
`pa.faq_content_v2` carries no push state of its own, so this module keeps its
own: `pa.faq_v2_push_state` stores the md5 of the faq_json that was last
successfully pushed per url_id. A URL needs pushing when it has no state row, or
its current md5 differs. md5 rather than a `updated_at > last_run` watermark
deliberately: with ~850 batches a partial failure is likely, and a timestamp
watermark would silently skip whatever failed mid-run, whereas per-URL state only
advances for URLs whose own batch succeeded.

Because state is stamped per batch, a URL's records must never straddle two
batches — `_iter_url_groups` yields whole URLs and the batch is flushed *before*
adding a group that would overflow it.

Scale: ~280,600 URLs x ~6 Q&A = ~1.7M records for a full push; on a weekly
cadence mode="new" is roughly a quarter of that. Everything streams from a
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

STATE_DDL = """
CREATE TABLE IF NOT EXISTS pa.faq_v2_push_state (
    url_id      BIGINT PRIMARY KEY,
    content_md5 TEXT NOT NULL,
    records     INTEGER,
    pushed_at   TIMESTAMP DEFAULT now()
);
"""

# md5(f.faq_json) is computed by Postgres on both the read and the compare side,
# so there is no chance of a Python/Postgres hashing mismatch.
_BASE_FROM = """
    FROM pa.faq_content_v2 f
    JOIN pa.urls u ON u.url_id = f.url_id
    LEFT JOIN pa.faq_v2_push_state s ON s.url_id = f.url_id
    WHERE f.faq_json IS NOT NULL AND f.faq_json <> ''
"""
_NEW_ONLY = " AND (s.url_id IS NULL OR s.content_md5 <> md5(f.faq_json))"


def _ensure_state_table(cur):
    cur.execute(STATE_DDL)


def _set_progress(task_id, **kw):
    if not task_id:
        return
    with _task_lock:
        t = _tasks.get(task_id)
        if t is not None:
            t.setdefault("progress", {}).update(kw)


def _iter_url_groups(cur, limit=None, mode="new"):
    """Yield (url_id, url, md5, [records]) — one group per URL, streaming.

    Whole URLs, never partial: the caller stamps push state per batch, which is
    only correct if a URL's records all land in the same batch.

    faq_json is TEXT holding a JSON array of {question, answer} (verified: the
    only key shape present). sort_order is the array index, so published order
    matches generated order. Unparseable / non-list JSON is reported as a skip
    rather than aborting the run.
    """
    sql = ("SELECT f.url_id, u.url, f.faq_json, md5(f.faq_json) AS md5"
           + _BASE_FROM + (_NEW_ONLY if mode == "new" else "")
           + " ORDER BY f.url_id")
    if limit:
        sql += " LIMIT %s"
        cur.execute(sql, (limit,))
    else:
        cur.execute(sql)

    for row in cur:
        url_id, url, raw, md5 = row["url_id"], row["url"], row["faq_json"], row["md5"]
        try:
            items = json.loads(raw)
        except Exception:
            yield (url_id, url, md5, None, "unparseable faq_json")
            continue
        if not isinstance(items, list):
            yield (url_id, url, md5, None, f"faq_json is {type(items).__name__}, not a list")
            continue
        recs = []
        for i, it in enumerate(items):
            if not isinstance(it, dict):
                continue
            q = (it.get("question") or "").strip()
            a = (it.get("answer") or "").strip()
            if not q or not a:
                # url+question+answer are all required; an incomplete pair would
                # 400 the entire batch.
                continue
            recs.append({"url": url, "question": q, "answer": a,
                         "country_code": "NL", "sort_order": i})
        if not recs:
            yield (url_id, url, md5, None, "no complete question/answer pairs")
            continue
        yield (url_id, url, md5, recs, None)


def _post_batch(records, env):
    resp = _post_with_retry(
        FAQ_API_URLS[env],
        headers={"X-Api-Key": FAQ_API_KEYS[env](), "Content-Type": "application/json"},
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


def _stamp_state(rows):
    """Record (url_id, md5, n) as successfully pushed. Own connection + commit:
    state must survive even if a later batch dies."""
    if not rows:
        return
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.executemany("""
            INSERT INTO pa.faq_v2_push_state (url_id, content_md5, records, pushed_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (url_id) DO UPDATE
              SET content_md5 = EXCLUDED.content_md5,
                  records     = EXCLUDED.records,
                  pushed_at   = now()
        """, rows)
        conn.commit()
        cur.close()
    finally:
        return_db_connection(conn)


def publish_faq_v2(env="production", limit=None, replace=False, mode="new", task_id=None):
    """Push FAQ Q&A pairs to the /faq section.

    mode    — "new" (default): only URLs never pushed or whose faq_json changed
              since their last successful push. "all": every URL, every run.
    limit   — cap on URLs (not records); for a trial run.
    replace — DELETE each URL's questions before posting it, so the published set
              matches ours exactly. One extra request PER URL, so only sensible
              with a small `limit`. Off by default.
    """
    if env not in FAQ_API_URLS:
        return {"success": False, "message": f"unknown env {env!r}"}
    if not FAQ_API_KEYS[env]():
        return {"success": False, "message": f"no API key configured for env {env!r}"}
    if mode not in ("new", "all"):
        return {"success": False, "message": f"unknown mode {mode!r}"}

    started = time.time()
    _set_progress(task_id, phase="counting", mode=mode)

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        _ensure_state_table(cur)
        conn.commit()
        cur.execute("SELECT count(*) AS n" + _BASE_FROM + (_NEW_ONLY if mode == "new" else ""))
        total_urls = cur.fetchone()["n"]
        cur.close()
        if limit:
            total_urls = min(total_urls, limit)
        _set_progress(task_id, phase="pushing", total_urls=total_urls, urls_done=0,
                      records_pushed=0, batches=0, failed=0)

        cur = conn.cursor(name="faq_v2_stream")
        cur.itersize = CURSOR_ITERSIZE

        batch, batch_state, batch_results = [], [], []
        pushed = failed = urls_done = urls_failed = 0
        skipped = []

        def flush():
            nonlocal batch, batch_state, pushed, failed, urls_failed
            if not batch:
                return
            ok, code, text = _post_batch(batch, env)
            batch_results.append({"count": len(batch), "urls": len(batch_state), "ok": ok,
                                  "status_code": code, "response": "" if ok else text})
            if ok:
                pushed += len(batch)
                _stamp_state(batch_state)      # only successful URLs advance
            else:
                failed += len(batch)
                urls_failed += len(batch_state)
            batch, batch_state = [], []
            _set_progress(task_id, records_pushed=pushed, failed=failed,
                          batches=len(batch_results), urls_done=urls_done)

        for url_id, url, md5, recs, skip_reason in _iter_url_groups(cur, limit, mode):
            if skip_reason:
                if len(skipped) < 50:
                    skipped.append({"url": url, "reason": skip_reason})
                continue
            # Never split a URL across batches — push state is stamped per batch.
            if batch and len(batch) + len(recs) > BATCH_SIZE:
                flush()
            if replace:
                try:
                    _delete_url(url, env)
                except Exception as e:
                    if len(skipped) < 50:
                        skipped.append({"url": url, "reason": f"delete failed: {e}"})
            batch.extend(recs)
            batch_state.append((url_id, md5, len(recs)))
            urls_done += 1
        flush()
        cur.close()
    finally:
        return_db_connection(conn)

    result = {
        "success": failed == 0,
        "env": env,
        "mode": mode,
        "api_url": FAQ_API_URLS[env],
        "total_urls": total_urls,
        "urls_processed": urls_done,
        "urls_failed": urls_failed,
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
def _run(task_id, env, limit, replace, mode):
    with _task_lock:
        _tasks[task_id].update(status="running", started_at=time.time())
    try:
        res = publish_faq_v2(env=env, limit=limit, replace=replace, mode=mode,
                             task_id=task_id)
        with _task_lock:
            _tasks[task_id].update(status="completed", result=res,
                                   completed_at=time.time())
    except Exception as e:
        with _task_lock:
            _tasks[task_id].update(status="failed", error=str(e),
                                   completed_at=time.time())


def start_faq_v2_task(env="production", limit=None, replace=False, mode="new"):
    task_id = str(uuid.uuid4())
    with _task_lock:
        _tasks[task_id] = {"status": "queued", "env": env, "mode": mode, "progress": {}}
    threading.Thread(target=_run, args=(task_id, env, limit, replace, mode),
                     daemon=True).start()
    return task_id


def get_faq_v2_status(task_id):
    with _task_lock:
        t = _tasks.get(task_id)
        return dict(t) if t else {"error": "Task not found"}


def get_faq_v2_stats():
    """Counts for the button's confirm dialog: how much a "new" run would push
    versus the whole corpus, plus when anything was last pushed."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        _ensure_state_table(cur)
        conn.commit()
        cur.execute("SELECT count(*) AS n" + _BASE_FROM)
        total = cur.fetchone()["n"]
        cur.execute("SELECT count(*) AS n" + _BASE_FROM + _NEW_ONLY)
        pending = cur.fetchone()["n"]
        cur.execute("SELECT max(pushed_at) AS last, count(*) AS n FROM pa.faq_v2_push_state")
        r = cur.fetchone()
        cur.close()
        return {
            "urls_total": total,
            "urls_pending": pending,
            "urls_pushed": r["n"],
            "last_pushed_at": r["last"].isoformat() if r["last"] else None,
            # ~6 Q&A per URL, measured over a 200-row sample.
            "est_records_pending": pending * 6,
            "est_records_total": total * 6,
            "api_url": FAQ_API_URLS["production"],
        }
    finally:
        return_db_connection(conn)
