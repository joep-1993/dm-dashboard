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
   the old questions live. A true replace needs DELETE-then-POST per URL, so
   `replace=True` is the DEFAULT everywhere (2026-08-31). It was off until then,
   which is how the live store came to hold 53% of URLs with more than 6
   questions — see publish_faq_v2 for the measurements and the reasoning.

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
from concurrent.futures import ThreadPoolExecutor, as_completed

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
# Concurrency for the replace=True DELETE phase. One DELETE per URL at ~0.45s
# each is ~3h over the ~23k URLs a weekly run touches, which overran the daily
# automation's publish timeout; they are independent url-scoped calls, so they
# fan out.
DELETE_WORKERS = 20

_tasks = {}
_task_lock = threading.Lock()

# Push state is per (url_id, ENV). It has to be: pushing a URL to staging must not
# make the next production "new" run skip it. The original table was keyed on
# url_id alone, which silently coupled the two environments.
STATE_DDL = """
CREATE TABLE IF NOT EXISTS pa.faq_v2_push_state (
    url_id      BIGINT NOT NULL,
    env         TEXT   NOT NULL DEFAULT 'production',
    content_md5 TEXT NOT NULL,
    records     INTEGER,
    pushed_at   TIMESTAMP DEFAULT now(),
    PRIMARY KEY (url_id, env)
);
"""

# In-place migration of the pre-env table.  The ALTER TABLE takes an
# AccessExclusiveLock even when ADD COLUMN IF NOT EXISTS is a no-op, which
# deadlocks against concurrent publish runs holding RowExclusiveLock.  So we
# first check with a cheap catalogue query whether the migration is needed at
# all — once the env column and composite PK exist, this is a pure read.
STATE_MIGRATE = """
DO $$
BEGIN
    -- Only touch the table if the env column is missing.
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'pa'
           AND table_name   = 'faq_v2_push_state'
           AND column_name  = 'env'
    ) THEN
        ALTER TABLE pa.faq_v2_push_state
            ADD COLUMN env TEXT NOT NULL DEFAULT 'production';
    END IF;

    -- Only rebuild the PK if it is still the old single-column form.
    IF EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'pa.faq_v2_push_state'::regclass
           AND conname  = 'faq_v2_push_state_pkey'
           AND pg_get_constraintdef(oid) = 'PRIMARY KEY (url_id)'
    ) THEN
        ALTER TABLE pa.faq_v2_push_state DROP CONSTRAINT faq_v2_push_state_pkey;
        ALTER TABLE pa.faq_v2_push_state ADD PRIMARY KEY (url_id, env);
    END IF;
END $$;
"""

# md5(f.faq_json) is computed by Postgres on both the read and the compare side,
# so there is no chance of a Python/Postgres hashing mismatch.
#
# The state join is env-scoped, so every query built on _BASE_FROM takes the env
# as its FIRST bind parameter — a URL is "already pushed" only for the env it was
# pushed to.
_BASE_FROM = """
    FROM pa.faq_content_v2 f
    JOIN pa.urls u ON u.url_id = f.url_id
    LEFT JOIN pa.faq_v2_push_state s ON s.url_id = f.url_id AND s.env = %s
    WHERE f.faq_json IS NOT NULL AND f.faq_json <> ''
"""
_NEW_ONLY = " AND (s.url_id IS NULL OR s.content_md5 <> md5(f.faq_json))"


def _ensure_state_table(cur):
    cur.execute(STATE_DDL)
    cur.execute(STATE_MIGRATE)


def _set_progress(task_id, **kw):
    if not task_id:
        return
    with _task_lock:
        t = _tasks.get(task_id)
        if t is not None:
            t.setdefault("progress", {}).update(kw)


def _is_cancelled(task_id):
    """Non-raising cancel check, polled once per URL by the push loop.

    A full run is ~850 POSTs of 2000 records over many minutes, so it needs a
    stop that isn't "restart uvicorn" — and a restart mid-run also loses the
    in-memory task, so the UI would just 404. Cooperative flag rather than
    killing the thread: state is stamped per successful batch, so stopping
    between URLs leaves the published set consistent and the unpushed URLs are
    simply picked up by the next mode="new" run.
    """
    if not task_id:
        return False
    with _task_lock:
        t = _tasks.get(task_id)
        return bool(t and t.get("cancel"))


def _build_records(url, raw):
    """Turn one stored faq_json blob into /faq records.

    Returns (records, skip_reason) — exactly one of the two is None. Shared by the
    bulk run and the single-URL publish so the two can never disagree about what a
    valid record is.

    faq_json is TEXT holding a JSON array of {question, answer} (verified: the only
    key shape present). sort_order is the array index, so published order matches
    generated order.
    """
    try:
        items = json.loads(raw)
    except Exception:
        return None, "unparseable faq_json"
    if not isinstance(items, list):
        return None, f"faq_json is {type(items).__name__}, not a list"
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
        return None, "no complete question/answer pairs"
    return recs, None


def _iter_url_groups(cur, limit=None, mode="new", env="production"):
    """Yield (url_id, url, md5, [records]) — one group per URL, streaming.

    Whole URLs, never partial: the caller stamps push state per batch, which is
    only correct if a URL's records all land in the same batch.

    Record building is delegated to _build_records; unparseable / non-list JSON is
    reported as a skip rather than aborting the run.
    """
    sql = ("SELECT f.url_id, u.url, f.faq_json, md5(f.faq_json) AS md5"
           + _BASE_FROM + (_NEW_ONLY if mode == "new" else "")
           + " ORDER BY f.url_id")
    if limit:
        sql += " LIMIT %s"
        cur.execute(sql, (env, limit))
    else:
        cur.execute(sql, (env,))

    for row in cur:
        url_id, url, raw, md5 = row["url_id"], row["url"], row["faq_json"], row["md5"]
        recs, skip_reason = _build_records(url, raw)
        yield (url_id, url, md5, recs, skip_reason)


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


def _delete_urls_parallel(urls, env):
    """DELETE a whole batch of URLs concurrently.

    Returns [(url, error_str), ...] for the calls that raised — same reporting
    contract as the sequential version it replaces: a non-2xx DELETE is not a
    failure here either, because the POST that follows still upserts the current
    questions.
    """
    if not urls:
        return []
    failures = []
    with ThreadPoolExecutor(max_workers=DELETE_WORKERS) as pool:
        futures = {pool.submit(_delete_url, u, env): u for u in urls}
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                failures.append((futures[fut], str(e)))
    return failures


def _stamp_state(rows, env):
    """Record (url_id, md5, n) as successfully pushed to `env`. Own connection +
    commit: state must survive even if a later batch dies."""
    if not rows:
        return
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.executemany("""
            INSERT INTO pa.faq_v2_push_state (url_id, env, content_md5, records, pushed_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (url_id, env) DO UPDATE
              SET content_md5 = EXCLUDED.content_md5,
                  records     = EXCLUDED.records,
                  pushed_at   = now()
        """, [(url_id, env, md5, n) for url_id, md5, n in rows])
        conn.commit()
        cur.close()
    finally:
        return_db_connection(conn)


def publish_faq_v2(env="production", limit=None, replace=True, mode="new", task_id=None):
    """Push FAQ Q&A pairs to the /faq section.

    mode    — "new" (default): only URLs never pushed or whose faq_json changed
              since their last successful push. "all": every URL, every run.
    limit   — cap on URLs (not records); for a trial run.
    replace — DELETE each URL's questions before posting it, so the published set
              matches ours exactly. ON by default, and it should stay that way.

    Why replace defaults to true (changed 2026-08-31). It used to default to
    false to save one DELETE per URL. That saving is what let the live store rot:
    /faq upserts on (url, question), and a regenerated FAQ has new question TEXT,
    so nothing overwrites the previous set — it just accumulates. Measured before
    the fix: 53% of published URLs carried more than 6 questions, average 13.7,
    worst 42. The saving was never real either — in mode="new" only URLs whose
    faq_json changed get pushed, so the DELETEs scale with regeneration volume,
    not with the ~280k published URLs.

    The default lives HERE, not only at the caller. Fixing it at the API endpoint
    first was not enough: the endpoint is one caller of several, and a caller that
    simply omits the argument — which is every caller that has not thought about
    it — is exactly the caller that must not get the additive behaviour.
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
        cur.execute("SELECT count(*) AS n" + _BASE_FROM + (_NEW_ONLY if mode == "new" else ""),
                    (env,))
        total_urls = cur.fetchone()["n"]
        cur.close()
        if limit:
            total_urls = min(total_urls, limit)
        _set_progress(task_id, phase="pushing", total_urls=total_urls, urls_done=0,
                      records_pushed=0, batches=0, failed=0)

        cur = conn.cursor(name="faq_v2_stream")
        cur.itersize = CURSOR_ITERSIZE

        batch, batch_state, batch_results = [], [], []
        batch_urls_to_delete = []
        pushed = failed = urls_done = urls_failed = 0
        skipped = []

        def flush():
            nonlocal batch, batch_state, batch_urls_to_delete
            nonlocal pushed, failed, urls_failed
            if not batch:
                return
            # Replace happens per batch rather than per URL, but still strictly
            # before this batch's POST, so every URL is delete-then-post exactly
            # as before — only the waiting is now shared.
            for u, err in _delete_urls_parallel(batch_urls_to_delete, env):
                if len(skipped) < 50:
                    skipped.append({"url": u, "reason": f"delete failed: {err}"})
            ok, code, text = _post_batch(batch, env)
            batch_results.append({"count": len(batch), "urls": len(batch_state), "ok": ok,
                                  "status_code": code, "response": "" if ok else text})
            if ok:
                pushed += len(batch)
                _stamp_state(batch_state, env)   # only successful URLs advance, per env
            else:
                failed += len(batch)
                urls_failed += len(batch_state)
            batch, batch_state, batch_urls_to_delete = [], [], []
            _set_progress(task_id, records_pushed=pushed, failed=failed,
                          batches=len(batch_results), urls_done=urls_done)

        cancelled = False
        for url_id, url, md5, recs, skip_reason in _iter_url_groups(cur, limit, mode, env):
            # Checked per URL, i.e. between batches at worst: a POST in flight is
            # never abandoned half-written, so the API never sees a torn batch.
            if _is_cancelled(task_id):
                cancelled = True
                break
            if skip_reason:
                if len(skipped) < 50:
                    skipped.append({"url": url, "reason": skip_reason})
                continue
            # Never split a URL across batches — push state is stamped per batch.
            if batch and len(batch) + len(recs) > BATCH_SIZE:
                flush()
            if replace:
                batch_urls_to_delete.append(url)
            batch.extend(recs)
            batch_state.append((url_id, md5, len(recs)))
            urls_done += 1
        if cancelled:
            # Drop the part-filled batch instead of firing one last POST: Cancel
            # means stop writing to the live API now. Those URLs stay unstamped,
            # so the next "new" run pushes them — hence urls_done is rolled back
            # to what was actually published, not what was read.
            urls_done -= len(batch_state)
            batch, batch_state, batch_urls_to_delete = [], [], []
            _set_progress(task_id, phase="cancelled", urls_done=urls_done)
        else:
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
        "cancelled": cancelled,
        "duration_sec": round(time.time() - started, 1),
    }
    if skipped:
        result["skipped"] = skipped
        result["skipped_count"] = len(skipped)
    bad = [b for b in batch_results if not b["ok"]]
    if bad:
        result["failed_batches"] = bad[:10]
    return result


def publish_faq_v2_url(url, env="production", replace=True):
    """Push ONE url's FAQ to /faq, synchronously.

    Backs the Publish button on the URL Lookup card, so it runs inline rather than
    as a background task: one URL is ~6 records and a single POST, and the caller
    is looking at the answer.

    replace=True by default, unlike the bulk run. /faq is additive, so without a
    DELETE first, questions that were regenerated under different wording stay
    live alongside the new ones — and the whole point of publishing from the lookup
    card is to make the live set match the FAQ shown on screen. It is one extra
    request for one URL, which is exactly the "narrow re-push" the module docstring
    reserves replace for.

    Push state is stamped on success, so a later mode="new" bulk run skips this URL
    instead of re-sending it.
    """
    if env not in FAQ_API_URLS:
        return {"success": False, "message": f"unknown env {env!r}"}
    if not FAQ_API_KEYS[env]():
        return {"success": False, "message": f"no API key configured for env {env!r}"}

    started = time.time()
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        _ensure_state_table(cur)
        conn.commit()
        # Match on the stored url. The lookup card hands back pa.urls.url verbatim,
        # so this is an exact hit; canonicalizing again would only risk drifting
        # away from the row the user is looking at.
        cur.execute("""
            SELECT f.url_id, u.url, f.faq_json, md5(f.faq_json) AS md5
            FROM pa.faq_content_v2 f
            JOIN pa.urls u ON u.url_id = f.url_id
            WHERE u.url = %s
              AND f.faq_json IS NOT NULL AND f.faq_json <> ''
            LIMIT 1
        """, (url,))
        row = cur.fetchone()
        cur.close()
    finally:
        return_db_connection(conn)

    if not row:
        return {"success": False, "url": url, "env": env,
                "message": "no FAQ content stored for this URL"}

    records, skip_reason = _build_records(row["url"], row["faq_json"])
    if skip_reason:
        return {"success": False, "url": row["url"], "env": env, "message": skip_reason}

    # A failed DELETE is reported, not fatal: the POST still upserts the current
    # questions, it just cannot guarantee that superseded ones are gone.
    deleted = None
    if replace:
        try:
            ok_del, code_del = _delete_url(row["url"], env)
            deleted = {"ok": ok_del, "status_code": code_del}
        except Exception as e:
            deleted = {"ok": False, "error": str(e)}

    ok, code, text = _post_batch(records, env)
    if ok:
        _stamp_state([(row["url_id"], row["md5"], len(records))], env)

    result = {
        "success": ok,
        "url": row["url"],
        "env": env,
        "api_url": FAQ_API_URLS[env],
        "records_pushed": len(records) if ok else 0,
        "records": len(records),
        "replace": replace,
        "status_code": code,
        "duration_sec": round(time.time() - started, 1),
    }
    if deleted is not None:
        result["deleted_first"] = deleted
    if not ok:
        result["response"] = text
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
            # A stopped run gets its own terminal status rather than
            # completed+flag — UI_BLUEPRINT: a cancelled run must not be able to
            # render as "Done" just because nothing failed.
            _tasks[task_id].update(status="cancelled" if res.get("cancelled") else "completed",
                                   result=res, completed_at=time.time())
    except Exception as e:
        with _task_lock:
            _tasks[task_id].update(status="failed", error=str(e),
                                   completed_at=time.time())


def start_faq_v2_task(env="production", limit=None, replace=True, mode="new"):
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


def cancel_faq_v2_task(task_id):
    """Request a stop. Returns False for an unknown or already-finished task, so
    the endpoint can 404/400 instead of silently accepting a no-op."""
    with _task_lock:
        t = _tasks.get(task_id)
        if t is None:
            return False
        if t.get("status") not in ("queued", "running"):
            return False
        t["cancel"] = True
        return True


def get_faq_v2_stats(env="production"):
    """Counts for the button's confirm dialog: how much a "new" run would push
    versus the whole corpus, plus when anything was last pushed.

    Env-scoped, because push state is: staging and production each have their own
    pending count, and the dialog has to quote the one the user is about to run.
    """
    if env not in FAQ_API_URLS:
        env = "production"
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        _ensure_state_table(cur)
        conn.commit()
        cur.execute("SELECT count(*) AS n" + _BASE_FROM, (env,))
        total = cur.fetchone()["n"]
        cur.execute("SELECT count(*) AS n" + _BASE_FROM + _NEW_ONLY, (env,))
        pending = cur.fetchone()["n"]
        cur.execute("""SELECT max(pushed_at) AS last, count(*) AS n
                       FROM pa.faq_v2_push_state WHERE env = %s""", (env,))
        r = cur.fetchone()
        cur.close()
        return {
            "env": env,
            "urls_total": total,
            "urls_pending": pending,
            "urls_pushed": r["n"],
            "last_pushed_at": r["last"].isoformat() if r["last"] else None,
            # ~6 Q&A per URL, measured over a 200-row sample.
            "est_records_pending": pending * 6,
            "est_records_total": total * 6,
            "api_url": FAQ_API_URLS[env],
            "has_api_key": bool(FAQ_API_KEYS[env]()),
        }
    finally:
        return_db_connection(conn)
