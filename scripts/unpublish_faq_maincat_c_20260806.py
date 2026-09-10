#!/usr/bin/env python3
"""One-off: remove the maincat-level FAQs of the 2026-08-06 cleanup from the live /faq store.

The FAQ half of scripts/unpublish_maincat_c_20260806.py — read that file's header for
the diagnosis. Same cleanup, same deferred-live-change, but /faq is worse off:

  * There is no prune at all. content_records_publisher has `_fetch_stale`, which was
    merely blind here (the cleanup deleted the push_state rows it walks). faq_v2_publisher
    has no stale/retire path in the first place.
  * /faq is additive, so `replace=True` (the default since 2026-08-31) only helps a URL
    that is IN a payload: it DELETEs then POSTs that one URL. These URLs are in no
    payload, and unlike the kopteksten store there is no replace-all batch that would
    eventually prune them as a side effect. So nothing, ever, cleans this up.
  * pa.main's FAQ link validator selects `FROM pa.faq_content_v2`, so these answers are
    never link-checked either.

Measured 2026-09-10, production: 4,398 URLs whose faq_content_v2 row is gone still serve
questions — 60/60 sampled, average 6.1 questions, worst 12, so roughly 27,000 live
questions with no DB row behind them. Their answers hold 15,436 product links, of which
2/120 sampled are already 404 (~1.7%).

WHAT IT DOES
One DELETE /faq?url=… per URL (production). A 404 counts as success — already gone is the
desired end state. URLs are passed to the API exactly as pa.urls stores them, which is
what the publisher's own _iter_url_groups → _delete_url path does, so the store key
cannot drift.

WHAT IT SKIPS
URLs with a pa.faq_jobs row (4 at the time of writing, all 'pending'). Those are in the
regeneration pipeline: content is coming back and the next publish republishes them, so
they are the daily automation's business, not this script's.

The live PAGES keep rendering the old FAQ block until their HTML leaves CloudFront
(~7 days). Verify against the store with --verify, never against beslist.nl.

Usage:
    unpublish_faq_maincat_c_20260806.py                     # dry run: scope + live sample
    unpublish_faq_maincat_c_20260806.py --verify            # sample the store, write nothing
    unpublish_faq_maincat_c_20260806.py --commit --limit 25 # bounded pilot, do this first
    unpublish_faq_maincat_c_20260806.py --commit            # the full run
    unpublish_faq_maincat_c_20260806.py --commit --resume     # continue after a stop
Ctrl-C finishes what is in flight, checkpoints, and exits.
"""
import argparse
import json
import os
import random
import signal
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

REPO = "/home/joepvanschagen/projects/dm-dashboard"
sys.path.insert(0, REPO)
os.chdir(REPO)

import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from backend.database import get_db_connection, return_db_connection  # noqa: E402
from backend.faq_v2_publisher import FAQ_API_KEYS, FAQ_API_URLS  # noqa: E402

BAK_TABLE = "pa.faq_content_v2_bak_maincat_c_20260806"
PROGRESS_FILE = os.path.join(REPO, "logs", "unpublish_faq_maincat_c_progress.json")
RESULTS_FILE = os.path.join(REPO, "logs", "unpublish_faq_maincat_c_results.jsonl")

EXPECT = 4394          # 4,398 orphans minus the 4 the regeneration pipeline owns
DEFAULT_WORKERS = 8

_stop = False


def _on_sigint(signum, frame):
    global _stop
    if _stop:                      # second Ctrl-C: the user means now
        raise KeyboardInterrupt
    _stop = True
    print("\n[unpublish-faq] Ctrl-C — finishing what is in flight, then checkpointing. "
          "Ctrl-C again to abort immediately.", flush=True)


def fetch_scope():
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT b.url_id, u.url
              FROM {BAK_TABLE} b
              JOIN pa.urls u ON u.url_id = b.url_id
             WHERE NOT EXISTS (
                       SELECT 1 FROM pa.faq_content_v2 c WHERE c.url_id = b.url_id)
               AND NOT EXISTS (
                       SELECT 1 FROM pa.faq_jobs j WHERE j.url_id = b.url_id)
             ORDER BY u.url
        """)
        rows = [(r['url_id'], r['url']) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        return_db_connection(conn)


def drop_satellites(url_ids, env):
    """Drop the leftover pointers for the URLs we just unpublished: the push-state
    row that claims they are live, and the link-validation row that claims they were
    checked. One query each, not one per URL."""
    if not url_ids:
        return {}
    ids = list(url_ids)
    out = {}
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM pa.faq_v2_push_state WHERE env = %s AND url_id = ANY(%s)",
                    (env, ids))
        out["faq_v2_push_state"] = cur.rowcount
        cur.execute("DELETE FROM pa.faq_link_validation WHERE url_id = ANY(%s)", (ids,))
        out["faq_link_validation"] = cur.rowcount
        conn.commit()
        cur.close()
        return out
    except Exception as e:
        conn.rollback()
        print(f"[unpublish-faq] could not drop satellite rows: {e}")
        return out
    finally:
        return_db_connection(conn)


def _api(url, env):
    return f"{FAQ_API_URLS[env]}?url={urllib.parse.quote(url, safe='')}"


def live_count(url, env):
    """Number of questions currently live for one URL, or a status string on error.

    Same shape as the probe in scripts/faq_v2_dedupe_live_sweep.py; kept local for the
    same reason that script kept its own — these are one-off sweeps that should not
    import each other, only the publisher primitives.
    """
    try:
        r = requests.get(_api(url, env), headers={"X-Api-Key": FAQ_API_KEYS[env]()},
                         timeout=45)
    except Exception as e:
        return repr(e)
    if not (200 <= r.status_code < 300):
        return f"HTTP {r.status_code}"
    try:
        return len(r.json() or [])
    except Exception:
        return "unparseable"


def delete_url(url, env, attempts=3):
    """DELETE one URL's questions, with a short retry.

    faq_v2_publisher._delete_url has no retry and treats only 2xx as ok, because its
    caller always POSTs the current questions right after — a missed DELETE is repaired
    by that POST. Here there is no POST behind it, so a failure has to be retried and
    then reported.
    """
    last = None
    for i in range(attempts):
        try:
            r = requests.delete(_api(url, env),
                                headers={"X-Api-Key": FAQ_API_KEYS[env]()}, timeout=60)
            if 200 <= r.status_code < 300 or r.status_code == 404:
                return {"url": url, "ok": True, "status": r.status_code,
                        "action": "nothing_live" if r.status_code == 404 else "deleted"}
            last = f"HTTP {r.status_code}: {(r.text or '')[:200]}"
        except Exception as e:
            last = repr(e)
        time.sleep(0.5 * (i + 1))
    return {"url": url, "ok": False, "error": last}


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
    picks = random.sample(scope, min(n, len(scope)))
    withq = clean = errors = qsum = 0
    worst = 0
    with ThreadPoolExecutor(DEFAULT_WORKERS) as ex:
        for res in ex.map(lambda r: live_count(r[1], env), picks):
            if not isinstance(res, int):
                errors += 1
            elif res == 0:
                clean += 1
            else:
                withq += 1
                qsum += res
                worst = max(worst, res)
    avg = qsum / withq if withq else 0
    print(f"  store sample ({len(picks)}): {withq} still have questions "
          f"(avg {avg:.1f}, worst {worst}), {clean} clean, {errors} error")
    return withq, clean, errors


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", default="production", choices=sorted(FAQ_API_URLS))
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

    if not FAQ_API_KEYS[args.env]():
        sys.exit(f"No API key configured for env {args.env!r}")

    scope = fetch_scope()
    print(f"[unpublish-faq] env={args.env}  scope={len(scope)} URLs (expected {EXPECT})")
    if len(scope) != EXPECT and not args.force:
        print("  scope differs from the expected size. Shrinking is normal (content "
              "regenerated, or a job row appeared); if it GREW, find out why first.")
        if args.commit:
            sys.exit("refusing to commit without --force")

    if args.verify:
        sample_store(scope, args.env, args.sample)
        return

    done = load_progress() if args.resume else set()
    todo = [r for r in scope if r[0] not in done]
    if args.limit:
        todo = todo[:args.limit]
    print(f"[unpublish-faq] {len(todo)} to do"
          + (f" ({len(done)} already done)" if done else ""))

    if not args.commit:
        sample_store(scope, args.env, args.sample)
        print("[unpublish-faq] dry run — nothing written. Add --commit to delete.")
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
            futures[ex.submit(delete_url, url, args.env)] = url_id
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

    dropped = drop_satellites([r[0] for r in todo], args.env)
    print(f"[unpublish-faq] done in {time.time() - started:.0f}s: {tally}"
          f"  (satellite rows dropped: {dropped})")
    print(f"[unpublish-faq] per-URL log: {RESULTS_FILE}")
    if tally["failed"]:
        print("  rerun with --commit --resume to retry the failures")
    print("[unpublish-faq] the live PAGES keep serving the old FAQ block until the HTML "
          "leaves CloudFront (~7 days). Check the store with --verify, not beslist.nl.")


if __name__ == "__main__":
    main()
