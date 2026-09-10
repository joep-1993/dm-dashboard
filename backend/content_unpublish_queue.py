"""Drain pa.content_unpublish_queue — the half of a content delete that has to reach
the live website-configuration store.

WHY THIS EXISTS
A deleted koptekst/FAQ row stays live until something pushes the removal. Until now
the only mechanism was the prune in content_records_publisher, which walks
pa.kopteksten_push_state — so it could only see URLs whose STATE row survived, and a
bulk cleanup that (correctly) removed the satellite rows along with the content made
the live records invisible forever. The FAQ side had no prune at all, and /faq is
additive, so nothing would ever have removed those. See
migrations/2026-09-10-content-unpublish-queue.sql for the full diagnosis and the
triggers that fill this queue.

The queue is written by triggers, so it does not matter HOW the rows left — the daily
validator, the tool's delete button, an ad-hoc bulk transaction, a cascade from
pa.urls, or a TRUNCATE. All of them land here.

WHAT IT SKIPS, AND WHY THAT IS NOT A GAP
A URL that still has a pa.kopteksten_jobs / pa.faq_jobs row is left alone. That is the
regeneration case: the link validator deletes a content row and resets the job to
'pending' precisely so the content comes back, and unpublishing in between would
strip the page and republish it hours later for nothing. A delete that is meant to be
permanent takes the job row with it (the cleanup recipe in cc1/LEARNINGS.md says to),
so "has a job row" is exactly the line between the two intents. Those URLs are
reported as `skipped_regenerating` rather than silently dropped.

THE CEILING
A pending set larger than CEILING is not daily drift, it is an accident — someone
emptied a content table. The drain refuses it and says so instead of unpublishing at
scale on its own; `force=True` (or --force) overrides once a human has looked.
"""
import argparse
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor

from backend.database import get_db_connection, return_db_connection

log = logging.getLogger(__name__)

QUEUE_TABLE = "pa.content_unpublish_queue"

# Per kind: the content table that means "it came back", and the job table that means
# "the pipeline is going to bring it back".
KINDS = {
    "koptekst": {"content": "pa.kopteksten_content", "jobs": "pa.kopteksten_jobs"},
    "faq":      {"content": "pa.faq_content_v2",     "jobs": "pa.faq_jobs"},
}

MAX_PER_RUN = 5000     # one bulk cleanup's worth; the rest waits for the next run
CEILING = 25000        # above this, refuse and shout
WORKERS = 8


def _clear_returned(kind):
    """Drop tombstones whose content row is back: the ordinary publish path owns them
    again, and leaving the tombstone would unpublish live content on a later run."""
    t = KINDS[kind]
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            DELETE FROM {QUEUE_TABLE} q
             WHERE q.kind = %s
               AND EXISTS (SELECT 1 FROM {t['content']} c WHERE c.url_id = q.url_id)
        """, (kind,))
        n = cur.rowcount
        conn.commit()
        cur.close()
        return n
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)


def _counts(cur, kind):
    t = KINDS[kind]
    cur.execute(f"""
        SELECT count(*) FILTER (WHERE actionable)                        AS pending,
               count(*) FILTER (WHERE NOT actionable AND has_job)        AS skipped_regenerating,
               count(*) FILTER (WHERE NOT actionable AND NOT has_job)    AS unusable
          FROM (
            SELECT q.url IS NOT NULL AND NOT EXISTS (
                       SELECT 1 FROM {t['jobs']} j WHERE j.url_id = q.url_id) AS actionable,
                   EXISTS (SELECT 1 FROM {t['jobs']} j WHERE j.url_id = q.url_id) AS has_job
              FROM {QUEUE_TABLE} q
             WHERE q.kind = %s AND q.actioned_at IS NULL
          ) s
    """, (kind,))
    return cur.fetchone()


def _fetch_actionable(cur, kind, limit):
    t = KINDS[kind]
    cur.execute(f"""
        SELECT q.url_id, q.url
          FROM {QUEUE_TABLE} q
         WHERE q.kind = %s
           AND q.actioned_at IS NULL
           AND q.url IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM {t['jobs']} j WHERE j.url_id = q.url_id)
         ORDER BY q.deleted_at
         LIMIT %s
    """, (kind, limit))
    return [(r['url_id'], r['url']) for r in cur.fetchall()]


def stats(kind=None):
    """Pending / skipped / unusable per kind — for the daily summary and /health."""
    out = {}
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        for k in ([kind] if kind else KINDS):
            row = _counts(cur, k)
            out[k] = {"pending": row['pending'],
                      "skipped_regenerating": row['skipped_regenerating'],
                      "unusable": row['unusable']}
        cur.close()
        return out
    finally:
        return_db_connection(conn)


def _delete_koptekst(url, env):
    from backend.content_publisher import unpublish_content_url
    res = unpublish_content_url(url, environment=env)
    return bool(res.get("success")), res.get("error") or res.get("message") or \
        f"HTTP {res.get('status_code')}"


def _delete_faq(url, env):
    from backend.faq_v2_publisher import _delete_url
    try:
        ok, status = _delete_url(url, env)
    except Exception as e:
        return False, repr(e)
    # Already gone is the end state we want, not a failure.
    return (ok or status == 404), f"HTTP {status}"


_DELETERS = {"koptekst": _delete_koptekst, "faq": _delete_faq}


def _drop_faq_push_state(url_ids, env):
    """Forget that we pushed these FAQs.

    Only needed for the FAQ side: unpublish_content_url() already drops the koptekst
    state row itself. A state row left behind claims the URL is live with content,
    which is exactly the litter that made this queue necessary — and it would also
    reappear in any future audit as a phantom orphan.
    """
    if not url_ids:
        return 0
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM pa.faq_v2_push_state WHERE env = %s AND url_id = ANY(%s)",
                    (env, list(url_ids)))
        n = cur.rowcount
        conn.commit()
        cur.close()
        return n
    except Exception as e:
        conn.rollback()
        log.warning("content_unpublish_queue: could not drop faq push state: %s", e)
        return 0
    finally:
        return_db_connection(conn)


def _stamp(rows, failures):
    """Mark what landed; count an attempt and keep the error for what did not."""
    if not rows and not failures:
        return
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        if rows:
            cur.execute(f"""
                UPDATE {QUEUE_TABLE} SET actioned_at = now(), last_error = NULL
                 WHERE kind = %s AND url_id = ANY(%s)
            """, (rows[0][0], [r[1] for r in rows]))
        for kind, url_id, err in failures:
            cur.execute(f"""
                UPDATE {QUEUE_TABLE}
                   SET attempts = attempts + 1, last_error = %s
                 WHERE kind = %s AND url_id = %s
            """, (err[:500] if err else None, kind, url_id))
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)


def drain(kind, env="production", limit=MAX_PER_RUN, force=False, dry_run=False):
    """Remove from the live store what was deleted locally. Returns a summary dict."""
    if kind not in KINDS:
        raise ValueError(f"unknown kind {kind!r}")

    returned = _clear_returned(kind)
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        counts = _counts(cur, kind)
        todo = _fetch_actionable(cur, kind, limit)
        cur.close()
    finally:
        return_db_connection(conn)

    out = {"kind": kind, "env": env,
           "pending": counts['pending'],
           "cleared_returned": returned,
           "skipped_regenerating": counts['skipped_regenerating'],
           "unusable": counts['unusable'],
           "unpublished": 0, "failed": 0}

    if counts['pending'] > CEILING and not force:
        out["refused"] = (f"{counts['pending']} pending is above the ceiling of "
                          f"{CEILING} — that is a bulk event, not drift. Nothing was "
                          f"unpublished; rerun with force=True once checked.")
        log.error("content_unpublish_queue[%s]: %s", kind, out["refused"])
        return out

    if dry_run or not todo:
        out["dry_run"] = bool(dry_run)
        return out

    deleter = _DELETERS[kind]
    done, failures = [], []
    with ThreadPoolExecutor(WORKERS) as ex:
        results = ex.map(lambda r: (r[0], *deleter(r[1], env)), todo)
        for url_id, ok, err in results:
            if ok:
                done.append((kind, url_id))
            else:
                failures.append((kind, url_id, err))
    _stamp(done, failures)
    if kind == "faq" and done:
        out["push_state_dropped"] = _drop_faq_push_state([u for _, u in done], env)

    out["unpublished"] = len(done)
    out["failed"] = len(failures)
    if failures:
        out["first_errors"] = [f"{u}: {e}" for _, u, e in failures[:5]]
        log.warning("content_unpublish_queue[%s]: %d of %d failed, e.g. %s",
                    kind, len(failures), len(todo), out["first_errors"])
    else:
        log.info("content_unpublish_queue[%s]: unpublished %d (%d left for "
                 "regeneration, %d cleared because content returned)",
                 kind, len(done), out["skipped_regenerating"], returned)
    return out


def main():
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--kind", choices=sorted(KINDS), help="default: both")
    ap.add_argument("--env", default="production")
    ap.add_argument("--limit", type=int, default=MAX_PER_RUN)
    ap.add_argument("--commit", action="store_true", help="without this it is a dry run")
    ap.add_argument("--force", action="store_true", help="ignore the ceiling")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    for k in ([args.kind] if args.kind else sorted(KINDS)):
        res = drain(k, env=args.env, limit=args.limit, force=args.force,
                    dry_run=not args.commit)
        print(f"{k}: {res}")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()
