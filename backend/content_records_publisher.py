"""Kopteksten "Publish 2.0" — incremental content_top pushes via
`POST /automated-content/records`, the per-record sibling of the batch endpoint.

WHY THIS EXISTS
`/automated-content` is a full-set REPLACE: every publish has to carry all ~251k
publishable URLs (~280 MB, 14-21 min) because anything absent is deleted. Editing one
koptekst therefore costs a corpus upload. `/automated-content/records` upserts on
(url, country_language), so only what actually changed needs to go over the wire.

THE RECORDS ENDPOINT CANNOT BE ENUMERATED
Verified 2026-08-05: `GET /automated-content/records?url=*` is hard-capped at 1,000
rows and takes no offset/page parameter, so the live set cannot be read back. An
upsert-only publish would therefore let retired URLs live forever with no way to even
discover them — a regression against the batch, which prunes as a side effect of
replacing.

So this module tracks its own state instead of asking the API:

    pa.kopteksten_push_state (url_id, env, content_md5, pushed_at)

A URL needs pushing when it has no state row for this env, or its content md5 differs
from the one last pushed. A URL needs RETIRING when it HAS a state row but is no longer
publishable (content deleted or emptied, validation marked it invalid). Both sets are
derivable locally, which is what makes removal possible without enumeration.

RETIRING IS A PUSH OF content_top = "", NOT A DELETE
Because "" clears a field on this endpoint, a retired URL rides along in the ordinary
chunked upsert instead of costing its own HTTP DELETE — the difference between ~11
extra chunks and ~21,810 round trips on the current backlog. It leaves an empty record
rather than no record, which is litter rather than breakage, and is not a new state
anyway: 7.2% of live production records already carry an empty content_top (72 of a
1,000-record sample), which is how the old "all" batch published the 13,902 FAQ-only
URLs. A Publish All clears the empty rows, since replace-all drops anything absent.

WHAT THIS STILL CANNOT REACH
Only URLs with a state row can be retired. Records the old batch left behind were never
tracked here, so the ~21,810 currently live-but-unqualified are invisible to this module
and need one Publish All. Seeding does not help with that either — it stamps the
PUBLISHABLE set, and those URLs are by definition not in it.

SEEDING IS SOUND, AND ONLY BECAUSE THE BATCH IS REPLACE-ALL
On first use the state table is empty, so `mode="new"` would push everything. That is
correct but wasteful, and it also means the first prune knows nothing about records the
old batch left behind. `seed_push_state()` fixes both: immediately after a batch
publish the live set is EXACTLY the publishable set (that is what replace-all means),
so stamping the current publishable set as "already pushed" is a true statement about
the store rather than an assumption.

Per-env state, like faq_v2_push_state: pushing to staging must not make the next
production "new" run skip a URL. The FAQ table learned that the hard way — it was
originally keyed on url_id alone, which silently coupled the environments.
"""
import json
import os
import threading
import time
import uuid

import requests

from backend.content_publisher import (CONTENT_API_KEYS, _normalize_url,
                                       _post_with_retry, sanitize_for_api)
from backend.database import get_db_connection, return_db_connection

RECORDS_API_URLS = {
    "dev": "http://dev.website-configuration.api.beslist.nl:5900/automated-content/records",
    "staging": "https://website-configuration-staging.api.beslist.nl/automated-content/records",
    "production": "https://website-configuration.api.beslist.nl/automated-content/records",
}

# Records per POST. The batch moved ~1.1 KB per record, so 2,000 is ~2 MB — small
# enough that a rejected chunk is cheap to isolate, large enough that a full 251k
# push is ~126 requests rather than thousands.
CHUNK_SIZE = 2000

STATE_TABLE = "pa.kopteksten_push_state"

_tasks = {}
_task_lock = threading.Lock()

STATE_DDL = f"""
CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
    url_id      BIGINT NOT NULL,
    env         TEXT   NOT NULL DEFAULT 'production',
    content_md5 TEXT   NOT NULL,
    pushed_at   TIMESTAMP DEFAULT now(),
    PRIMARY KEY (url_id, env)
);
"""

# The publishable set, identical to content_publisher._PUBLISHABLE_WHERE. Duplicated
# as SQL rather than imported so this module's queries can add md5 and the state join
# without reshaping the batch's own query; if the definition changes, both must move.
_PUBLISHABLE = """
    FROM pa.urls u
    JOIN pa.kopteksten_content k ON k.url_id = u.url_id
    LEFT JOIN pa.url_validation v ON v.url_id = u.url_id
    WHERE k.content IS NOT NULL AND k.content <> ''
      AND (v.is_valid IS NULL OR v.is_valid = TRUE)
"""


def _ensure_state(cur):
    cur.execute(STATE_DDL)


def _set_progress(task_id, **kw):
    if not task_id:
        return
    with _task_lock:
        t = _tasks.get(task_id)
        if t is not None:
            t.setdefault("progress", {}).update(kw)


def _is_cancelled(task_id):
    """Cooperative stop, polled between chunks. State is stamped per successful chunk,
    so stopping mid-run leaves the store consistent and the unpushed URLs are simply
    picked up by the next mode="new" run."""
    if not task_id:
        return False
    with _task_lock:
        t = _tasks.get(task_id)
        return bool(t and t.get("cancel"))


def _headers(env):
    return {"X-Api-Key": CONTENT_API_KEYS.get(env, ""), "Content-Type": "application/json"}


def seed_push_state(env: str = "production") -> dict:
    """Stamp the current publishable set as already-pushed for `env`.

    Run this straight after a full batch publish, never at a random moment: the claim
    it encodes ("these URLs are live with this content") is only true because the batch
    replaced the store with exactly this set.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        _ensure_state(cur)
        cur.execute(f"""
            INSERT INTO {STATE_TABLE} (url_id, env, content_md5, pushed_at)
            SELECT u.url_id, %s, md5(k.content), now()
            {_PUBLISHABLE}
            ON CONFLICT (url_id, env) DO UPDATE
              SET content_md5 = EXCLUDED.content_md5, pushed_at = now()
        """, (env,))
        n = cur.rowcount
        conn.commit()
        cur.close()
    finally:
        return_db_connection(conn)
    return {"env": env, "seeded": n}


def get_stats(env: str = "production") -> dict:
    """Counts for the button's confirm dialog: how much a "new" run would push, how
    much a prune would delete, and when anything was last pushed."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        _ensure_state(cur)
        conn.commit()
        cur.execute(f"SELECT count(*) AS n {_PUBLISHABLE}")
        total = cur.fetchone()["n"]
        cur.execute(f"""
            SELECT count(*) AS n
            {_PUBLISHABLE}
              AND NOT EXISTS (
                    SELECT 1 FROM {STATE_TABLE} s
                     WHERE s.url_id = u.url_id AND s.env = %s
                       AND s.content_md5 = md5(k.content))
        """, (env,))
        pending = cur.fetchone()["n"]
        cur.execute(f"""
            SELECT count(*) AS n FROM {STATE_TABLE} s
             WHERE s.env = %s
               AND NOT EXISTS (
                     SELECT 1 {_PUBLISHABLE} AND u.url_id = s.url_id)
        """, (env,))
        stale = cur.fetchone()["n"]
        cur.execute(f"""SELECT max(pushed_at) AS last, count(*) AS n
                          FROM {STATE_TABLE} WHERE env = %s""", (env,))
        r = cur.fetchone()
        cur.close()
    finally:
        return_db_connection(conn)
    return {"env": env, "urls_total": total, "urls_pending": pending,
            "urls_stale": stale, "urls_pushed": r["n"],
            "last_pushed_at": r["last"].isoformat() if r["last"] else None,
            "api_url": RECORDS_API_URLS.get(env),
            "has_api_key": bool(CONTENT_API_KEYS.get(env))}


def _fetch_candidates(env, mode, limit):
    only_changed = "" if mode == "all" else f"""
              AND NOT EXISTS (
                    SELECT 1 FROM {STATE_TABLE} s
                     WHERE s.url_id = u.url_id AND s.env = %(env)s
                       AND s.content_md5 = md5(k.content))"""
    sql = (f"SELECT u.url_id, u.url, k.content, md5(k.content) AS md5 {_PUBLISHABLE}"
           f"{only_changed} ORDER BY u.url")
    if limit:
        sql += " LIMIT %(limit)s"
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        _ensure_state(cur)
        conn.commit()
        cur.execute(sql, {"env": env, "limit": limit})
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        return_db_connection(conn)


def _fetch_stale(env, limit=None):
    sql = f"""
        SELECT s.url_id, u.url
          FROM {STATE_TABLE} s
          JOIN pa.urls u ON u.url_id = s.url_id
         WHERE s.env = %(env)s
           AND NOT EXISTS (SELECT 1 {_PUBLISHABLE} AND u.url_id = s.url_id)
         ORDER BY u.url
    """
    if limit:
        sql += " LIMIT %(limit)s"
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, {"env": env, "limit": limit})
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        return_db_connection(conn)


def _stamp(rows, env):
    """Record (url_id, md5) as pushed. Own connection + commit: state must survive
    even if a later chunk dies."""
    if not rows:
        return
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.executemany(f"""
            INSERT INTO {STATE_TABLE} (url_id, env, content_md5, pushed_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (url_id, env) DO UPDATE
              SET content_md5 = EXCLUDED.content_md5, pushed_at = now()
        """, [(uid, env, md5) for uid, md5 in rows])
        conn.commit()
        cur.close()
    finally:
        return_db_connection(conn)


def _unstamp(url_ids, env):
    if not url_ids:
        return
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {STATE_TABLE} WHERE env = %s AND url_id = ANY(%s)",
                    (env, list(url_ids)))
        conn.commit()
        cur.close()
    finally:
        return_db_connection(conn)


def publish_records(env: str = "production", mode: str = "new", limit: int = None,
                    chunk_size: int = CHUNK_SIZE, prune: bool = True,
                    task_id: str = None) -> dict:
    """Upsert content_top for new/changed URLs, and retire the ones whose content is gone.

    mode  — "new" (default): only URLs never pushed to this env or whose content
            changed since. "all": every publishable URL.
    prune — also retire URLs that have a state row but are no longer publishable.
            ON by default: a publish that adds content but never removes it does not
            actually make the store match our intent, which is what the button claims.

    RETIRING IS A PUSH OF content_top = "", NOT A DELETE (Joep's idea, 2026-08-05)
    An earlier version issued one HTTP DELETE per retired URL, which for the current
    backlog would have been ~21,810 requests. Since "" clears a field on this endpoint,
    a retired URL can instead ride along in the ordinary chunked upsert — ~11 extra
    chunks instead of 21,810 round trips, and one code path instead of two.
    Safe because it is not a new state: 7.2% of live production records already carry an
    empty content_top (72 of a 1,000-record sample), which is how the old "all" batch
    published the 13,902 FAQ-only URLs.
    The trade is that the row survives as an empty record rather than disappearing. That
    is litter, not breakage, and a Publish All clears it since replace-all drops
    anything absent.

    content_bottom is deliberately NOT sent. On this endpoint an omitted field keeps
    its stored value and "" clears it — and content_bottom is retired, owned by the
    /faq store. Sending it either way would fight that publisher.
    """
    if env not in RECORDS_API_URLS:
        return {"success": False, "message": f"unknown env {env!r}"}
    if not CONTENT_API_KEYS.get(env):
        return {"success": False, "message": f"no API key configured for env {env!r}"}
    if mode not in ("new", "all"):
        return {"success": False, "message": f"unknown mode {mode!r}"}

    api_url = RECORDS_API_URLS[env]
    started = time.time()
    _set_progress(task_id, phase="counting", mode=mode)

    rows = _fetch_candidates(env, mode, limit)
    total = len(rows)
    _set_progress(task_id, phase="pushing", total_urls=total, urls_done=0,
                  chunks=0, failed=0)

    pushed = failed = 0
    chunk_results = []
    cancelled = False

    for start in range(0, total, chunk_size):
        if _is_cancelled(task_id):
            cancelled = True
            break
        chunk = rows[start:start + chunk_size]
        payload = [{"url": _normalize_url(r["url"]),
                    "content_top": sanitize_for_api(r["content"]),
                    "country_language": "nl-nl"} for r in chunk]
        try:
            resp = _post_with_retry(
                api_url, headers=_headers(env),
                data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                timeout=600,
            )
            ok = 200 <= resp.status_code < 300
            body = (resp.text or "")[:300]
        except requests.RequestException as e:
            ok, body = False, str(e)[:300]
        chunk_results.append({"n": len(chunk), "ok": ok, "body": "" if ok else body})
        if ok:
            pushed += len(chunk)
            _stamp([(r["url_id"], r["md5"]) for r in chunk], env)
        else:
            failed += len(chunk)
        _set_progress(task_id, urls_done=pushed, failed=failed,
                      chunks=len(chunk_results))

    result = {
        "success": failed == 0,
        "env": env, "mode": mode, "api_url": api_url,
        "candidates": total, "urls_pushed": pushed, "urls_failed": failed,
        "chunks": len(chunk_results), "cancelled": cancelled,
        "duration_sec": round(time.time() - started, 1),
    }
    bad = [c for c in chunk_results if not c["ok"]]
    if bad:
        result["failed_chunks"] = bad[:10]

    if prune and not cancelled:
        _set_progress(task_id, phase="retiring")
        stale = _fetch_stale(env)
        retired = retire_failed = 0
        for start in range(0, len(stale), chunk_size):
            if _is_cancelled(task_id):
                result["cancelled"] = True
                break
            chunk = stale[start:start + chunk_size]
            payload = [{"url": _normalize_url(r["url"]), "content_top": "",
                        "country_language": "nl-nl"} for r in chunk]
            try:
                resp = _post_with_retry(
                    api_url, headers=_headers(env),
                    data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                    timeout=600,
                )
                ok = 200 <= resp.status_code < 300
            except requests.RequestException:
                ok = False
            if ok:
                retired += len(chunk)
                # Drop the state row: the url is neither publishable nor live-with-content
                # any more, so it must stop being reported as stale on every future run.
                _unstamp([r["url_id"] for r in chunk], env)
            else:
                retire_failed += len(chunk)
            _set_progress(task_id, phase="retiring", retired=retired,
                          retire_failed=retire_failed)
        result["stale_found"] = len(stale)
        result["urls_retired"] = retired
        result["retire_failed"] = retire_failed
        if retire_failed:
            result["success"] = False

    _set_progress(task_id, phase="cancelled" if result["cancelled"] else "done")
    return result


# ---------------------------------------------------------------------------
# Background task wrapper — same shape as faq_v2_publisher's, so the frontend can
# poll it with the identical pattern.
# ---------------------------------------------------------------------------
def _run(task_id, env, mode, limit, prune):
    with _task_lock:
        _tasks[task_id].update(status="running", started_at=time.time())
    try:
        res = publish_records(env=env, mode=mode, limit=limit, prune=prune,
                              task_id=task_id)
        with _task_lock:
            _tasks[task_id].update(
                status="cancelled" if res.get("cancelled") else "completed",
                result=res, completed_at=time.time())
    except Exception as e:
        with _task_lock:
            _tasks[task_id].update(status="failed", error=str(e),
                                   completed_at=time.time())


def start_task(env="production", mode="new", limit=None, prune=True) -> str:
    task_id = str(uuid.uuid4())
    with _task_lock:
        _tasks[task_id] = {"status": "queued", "env": env, "mode": mode,
                           "prune": prune, "progress": {}}
    threading.Thread(target=_run, args=(task_id, env, mode, limit, prune),
                     daemon=True).start()
    return task_id


def get_status(task_id):
    with _task_lock:
        t = _tasks.get(task_id)
        return dict(t) if t else {"error": "Task not found"}


def cancel_task(task_id):
    """Request a stop. False for an unknown or already-finished task, so the endpoint
    can 404/400 rather than silently accept a no-op."""
    with _task_lock:
        t = _tasks.get(task_id)
        if t is None or t.get("status") not in ("queued", "running"):
            return False
        t["cancel"] = True
        return True
