#!/usr/bin/env python3
"""One-off: remove the maincat-level kopteksten of the 2026-08-06 cleanup from the live store.

WHY THIS EXISTS (diagnosed 2026-09-10)
On 2026-08-06 koptekst + FAQ were deleted for 4,699 maincat-level `/c/` URLs (see
cc1/TASKS.md). Deliberately DB-only: the plan was to let the next full batch publish
prune the live store as a side effect of its replace-all. That publish never ran, so
5 weeks later all 4,304 kopteksten whose row is still absent are STILL live — proven
by GET /automated-content/records, which returns the byte-identical text that now
only exists in pa.kopteksten_content_bak_maincat_c_20260806.

Nothing self-heals this, for two reasons:
  * The incremental publisher's prune (`content_records_publisher._fetch_stale`)
    walks pa.kopteksten_push_state, and the cleanup deleted those 4,343 state rows
    together with the content. The store's only pointer went with them, so prune is
    blind to exactly this set. (Verified: 0 push_state rows inside the scope.)
  * The link validator selects `FROM pa.kopteksten_content`, so these texts are never
    checked, never reset to 'pending' on a dead product link, never regenerated.
    Measured 2026-09-10 on their 9,731 product links: 5/120 sampled links are 404
    (~4%, so ~390 dead links live) against 0/120 in the still-monitored set.

WHAT IT DOES
One HTTP DELETE per URL against /automated-content/records (production only — staging
holds no records for this set and push_state is production-only). A 404 means it was
already gone, which is the desired end state and counts as success.

WHY DELETE AND NOT THE PRUNE'S content_top = "" TRICK
The prune retires a URL by upserting an empty content_top so it can ride along in a
chunked publish (~11 chunks instead of 21,810 round trips). That trade only pays when
the URLs are already in a payload. Here there is no payload, the set is 4,304 not
21,810, and an empty record is litter that keeps claiming a URL we deleted on purpose.
A real DELETE is the state we want and costs a few minutes.

WHAT IT DOES NOT DO
Nothing in Postgres, apart from one bulk DELETE of any pa.kopteksten_push_state rows
that turn up inside the scope (0 at the time of writing — the guard is for a rerun
after someone republishes part of the set).

The live PAGES keep rendering the koptekst until their HTML falls out of CloudFront
(~7 days). Verify against the store with --verify, never against beslist.nl.

Usage:
    unpublish_maincat_c_20260806.py                     # dry run: scope + live sample
    unpublish_maincat_c_20260806.py --verify            # sample the store, write nothing
    unpublish_maincat_c_20260806.py --commit --limit 25 # bounded pilot, do this first
    unpublish_maincat_c_20260806.py --commit            # the full run
    unpublish_maincat_c_20260806.py --commit --resume    # continue after a stop
Ctrl-C finishes the requests in flight, checkpoints, and exits.
"""
import argparse
import json
import os
import random
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor

REPO = "/home/joepvanschagen/projects/dm-dashboard"
sys.path.insert(0, REPO)
os.chdir(REPO)

import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from backend.content_publisher import (  # noqa: E402
    CONTENT_API_KEYS,
    CONTENT_RECORDS_API_URLS,
    _normalize_url,
)
from backend.database import get_db_connection, return_db_connection  # noqa: E402
from backend.url_catalog import canonicalize_url  # noqa: E402

BAK_TABLE = "pa.kopteksten_content_bak_maincat_c_20260806"
PROGRESS_FILE = os.path.join(REPO, "logs", "unpublish_maincat_c_progress.json")
RESULTS_FILE = os.path.join(REPO, "logs", "unpublish_maincat_c_results.jsonl")

# Expected scope. The recipe in cc1/LEARNINGS.md ("contentrijen verwijderen uit de
# shared Postgres") asserts the scope size before the first write; same here, because
# a scope that grew means someone republished part of the set and this script would
# then delete fresh content.
EXPECT = 4304

DEFAULT_WORKERS = 8

_stop = False


def _on_sigint(signum, frame):
    global _stop
    if _stop:                      # second Ctrl-C: the user means now
        raise KeyboardInterrupt
    _stop = True
    print("\n[unpublish] Ctrl-C — finishing what is in flight, then checkpointing. "
          "Ctrl-C again to abort immediately.", flush=True)


def fetch_scope():
    """The backed-up maincat-level URLs that still have no local content row.

    Rows that DO have one again were regenerated after the cleanup; they are live
    legitimately and must not be touched.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT b.url_id, u.url
              FROM {BAK_TABLE} b
              JOIN pa.urls u ON u.url_id = b.url_id
             WHERE NOT EXISTS (
                       SELECT 1 FROM pa.kopteksten_content c WHERE c.url_id = b.url_id)
             ORDER BY u.url
        """)
        rows = [(r['url_id'], r['url']) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        return_db_connection(conn)


def drop_push_state(url_ids, env):
    """Bulk-drop any push-state rows inside the scope, so the incremental publisher
    does not believe these URLs are live with content. One query, not one per URL."""
    if not url_ids:
        return 0
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM pa.kopteksten_push_state
             WHERE env = %s AND url_id = ANY(%s)
        """, (env, list(url_ids)))
        n = cur.rowcount
        conn.commit()
        cur.close()
        return n
    except Exception as e:
        conn.rollback()
        print(f"[unpublish] could not drop push state: {e}")
        return 0
    finally:
        return_db_connection(conn)


def _wire(url):
    canon = canonicalize_url(url)
    return _normalize_url(canon) if canon else None


def get_record(url, env):
    """(status_code, content_top_length) for one URL in the live store.

    content_top_length is None when there is no record, 0 for a record whose
    content_top is empty — the prune's retired shape, which renders no koptekst.
    """
    wire = _wire(url)
    api = CONTENT_RECORDS_API_URLS[env]
    try:
        r = requests.get(api, headers={"X-Api-Key": CONTENT_API_KEYS[env]},
                         params={"url": wire}, timeout=45)
    except Exception as e:
        return repr(e), None
    if not (200 <= r.status_code < 300):
        return r.status_code, None
    try:
        rows = r.json() or []
    except Exception:
        return r.status_code, None
    if not rows:
        return r.status_code, None
    return r.status_code, len(rows[0].get("content_top") or "")


def delete_record(url, env, attempts=3):
    """DELETE one record, with a short retry.

    content_publisher.unpublish_content_url() is the single-URL sibling of this and
    has no retry (its caller is a button, and a person can press it again) plus a
    per-URL push_state DELETE, which at this scale is 4,304 pointless round trips to
    Postgres for rows the cleanup already removed. Same primitives, imported, so the
    wire format cannot drift from what the publisher writes.
    """
    wire = _wire(url)
    if not wire:
        return {"url": url, "ok": False, "error": "could not canonicalize"}
    api = CONTENT_RECORDS_API_URLS[env]
    headers = {"X-Api-Key": CONTENT_API_KEYS[env]}
    last = None
    for i in range(attempts):
        try:
            r = requests.delete(api, headers=headers, params={"url": wire}, timeout=60)
            if 200 <= r.status_code < 300 or r.status_code == 404:
                return {"url": wire, "ok": True, "status": r.status_code,
                        "action": "nothing_live" if r.status_code == 404 else "deleted"}
            last = f"HTTP {r.status_code}: {(r.text or '')[:200]}"
        except Exception as e:
            last = repr(e)
        time.sleep(0.5 * (i + 1))
    return {"url": wire, "ok": False, "error": last}


def load_progress():
    try:
        with open(PROGRESS_FILE) as f:
            return set(json.load(f).get("done_url_ids", []))
    except Exception:
        return set()


def save_progress(done):
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    tmp = PROGRESS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"done_url_ids": sorted(done), "saved_at": time.time()}, f)
    os.replace(tmp, PROGRESS_FILE)


def sample_store(scope, env, n):
    """GET a random sample and report what the store actually holds."""
    picks = random.sample(scope, min(n, len(scope)))
    live = empty = absent = errors = 0
    with ThreadPoolExecutor(DEFAULT_WORKERS) as ex:
        for code, length in ex.map(lambda r: get_record(r[1], env), picks):
            if not isinstance(code, int):
                errors += 1
            elif length is None:
                absent += 1
            elif length == 0:
                empty += 1
            else:
                live += 1
    print(f"  store sample ({len(picks)}): {live} with a koptekst, {empty} empty "
          f"record, {absent} no record, {errors} error")
    return live, empty, absent, errors


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", default="production",
                    choices=sorted(CONTENT_RECORDS_API_URLS))
    ap.add_argument("--commit", action="store_true", help="actually DELETE")
    ap.add_argument("--verify", action="store_true",
                    help="sample the live store and exit, writing nothing")
    ap.add_argument("--limit", type=int, help="only the first N URLs (pilot)")
    ap.add_argument("--sample", type=int, default=40, help="URLs to probe in a sample")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    ap.add_argument("--resume", action="store_true",
                    help="skip URLs already recorded in the progress file")
    ap.add_argument("--force", action="store_true",
                    help="proceed even though the scope is not the expected size")
    args = ap.parse_args()

    if not CONTENT_API_KEYS.get(args.env):
        sys.exit(f"No API key configured for env {args.env!r} — set "
                 f"CONTENT_API_KEY_{'PROD' if args.env == 'production' else args.env.upper()}")

    scope = fetch_scope()
    print(f"[unpublish] env={args.env}  scope={len(scope)} URLs "
          f"(expected {EXPECT})")
    if len(scope) != EXPECT and not args.force:
        print("  scope differs from the expected size. If content was regenerated "
              "this is fine and --force proceeds; if it GREW, find out why first.")
        if args.commit:
            sys.exit("refusing to commit without --force")

    if args.verify:
        sample_store(scope, args.env, args.sample)
        return

    done = load_progress() if args.resume else set()
    todo = [r for r in scope if r[0] not in done]
    if args.limit:
        todo = todo[:args.limit]
    print(f"[unpublish] {len(todo)} to do"
          + (f" ({len(done)} already done)" if done else ""))

    if not args.commit:
        sample_store(scope, args.env, args.sample)
        print("[unpublish] dry run — nothing written. Add --commit to delete.")
        return

    signal.signal(signal.SIGINT, _on_sigint)
    os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
    tally = {"deleted": 0, "nothing_live": 0, "failed": 0}
    started = time.time()
    with open(RESULTS_FILE, "a") as log, ThreadPoolExecutor(args.workers) as ex:
        futures = {}
        for url_id, url in todo:
            if _stop:
                break
            futures[ex.submit(delete_record, url, args.env)] = url_id
        for i, (fut, url_id) in enumerate(futures.items(), 1):
            res = fut.result()
            res["url_id"] = url_id
            log.write(json.dumps(res, ensure_ascii=False) + "\n")
            if res.get("ok"):
                tally[res.get("action", "deleted")] += 1
                done.add(url_id)
            else:
                tally["failed"] += 1
            if i % 250 == 0:
                log.flush()
                save_progress(done)
                rate = i / max(time.time() - started, 1)
                print(f"  {i}/{len(futures)}  {tally}  {rate:.1f}/s", flush=True)
    save_progress(done)

    dropped = drop_push_state([r[0] for r in todo], args.env)
    print(f"[unpublish] done in {time.time() - started:.0f}s: {tally}"
          f"  (push_state rows dropped: {dropped})")
    print(f"[unpublish] per-URL log: {RESULTS_FILE}")
    if tally["failed"]:
        print("  rerun with --commit --resume to retry the failures")
    print("[unpublish] the live PAGES keep serving their koptekst until the HTML "
          "leaves CloudFront (~7 days). Check the store with --verify, not beslist.nl.")


if __name__ == "__main__":
    main()
