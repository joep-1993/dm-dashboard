#!/usr/bin/env python3
"""One-off: strip the accumulated duplicate questions out of the live /faq store.

WHY THIS EXISTS (diagnosed 2026-08-31)
/faq is additive — its upsert key is (url, question), so questions we do not send
are left alone. The bulk publisher ran with replace=False, and the link validator
regenerates a URL's FAQ from scratch (reset_faq_to_pending deletes the local
pa.faq_content_v2 row) whenever it finds a dead product link. A regeneration
produces different question TEXT, so nothing upserts: the previous 6 stay live and
6 new ones land on top. Every validation round added another 6.

Measured on a 60-URL random sample of published URLs:
    53% carried more than 6 live questions, average 13.7, worst 42.
    Example: /products/mode_accessoires/mode_accessoires_457573_457617/c/
             doelgroep_mode_accessoires~457525 had 34 live from 6 push dates
             (3, 5, 17, 18, 19, 20 August), while our DB held the intended 6.

daily_automation.step_publish_faq_v2 now sends replace=True, which stops the
bleeding. It does NOT clean up: in mode="new" a polluted URL is skipped entirely
because its faq_json md5 has not changed. That is what this script is for.

WHY NOT JUST RUN THE BULK PUBLISHER WITH mode="all", replace=True
It would work, but the task lives in uvicorn's memory and always restarts at the
lowest url_id — a restart or a crash six hours in means redoing everything, and
its DELETEs are sequential. This script talks to /faq directly (same primitives,
imported, not reimplemented), deletes in parallel, and checkpoints to a progress
file so --resume picks up where it stopped.

WHAT IT DOES PER CHUNK
    1. DELETE /faq?url=… for every URL in the chunk, in parallel
    2. one POST /faq with the chunk's records, rebuilt from pa.faq_content_v2
So a URL is briefly without live FAQ, between its DELETE and the chunk's POST —
seconds at the default chunk size. Deliberately blind rather than GET-first:
probing every URL first would cost ~258k extra requests to spare the ~47% that
are already clean, which is more total work, not less.

WHAT IT DOES NOT TOUCH
pa.faq_v2_push_state. The sweep republishes byte-identical content, so the stored
md5 stays correct and the daily mode="new" run keeps behaving exactly as before.

NOT COVERED (self-healing, no action needed)
19,865 URLs have live FAQs but no local content row — the validator deleted it and
regeneration has not caught up. 17,870 are faq_jobs='pending' and get regenerated
and republished with replace=True by the normal cycle. The remaining 1,993 are
'failed' and keep their stale live FAQ until the underlying error is fixed.

RUN IT WHEN THE DAILY AUTOMATION IS NOT RUNNING. A regeneration landing between
this script's DB read and its POST would publish content that is one version
stale; the next daily publish repairs it, but there is no reason to race.

Usage:
    faq_v2_dedupe_live_sweep.py                    # dry run: scope + live damage sample
    faq_v2_dedupe_live_sweep.py --verify           # sample live counts, write nothing
    faq_v2_dedupe_live_sweep.py --commit --limit 500   # bounded pilot, do this first
    faq_v2_dedupe_live_sweep.py --commit           # the full sweep
    faq_v2_dedupe_live_sweep.py --commit --resume  # continue after a stop
Ctrl-C finishes the chunk in flight, checkpoints, and exits.
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
from backend.faq_v2_publisher import (  # noqa: E402
    FAQ_API_KEYS,
    FAQ_API_URLS,
    _build_records,
    _post_batch,
)

PROGRESS_FILE = os.path.join(REPO, "logs", "faq_v2_sweep_progress.json")

# URLs per chunk. 6 records each, so 300 lands ~1800 records per POST — just under
# the publisher's BATCH_SIZE of 2000, which the /faq API validates atomically.
DEFAULT_CHUNK = 300
DEFAULT_WORKERS = 6

_stop = False


def _on_sigint(signum, frame):
    global _stop
    if _stop:                      # second Ctrl-C: the user means now
        raise KeyboardInterrupt
    _stop = True
    print("\n[sweep] Ctrl-C — finishing the chunk in flight, then checkpointing. "
          "Ctrl-C again to abort immediately.", flush=True)


def delete_url(url, env, attempts=3):
    """DELETE /faq?url=… with a short retry.

    faq_v2_publisher._delete_url has no retry — it is called from a run that can
    afford to skip a URL and pick it up next time. This sweep cannot: a failed
    DELETE followed by a successful POST re-adds the duplicates we came to remove,
    so the POST has to know the DELETE landed.
    """
    api = f"{FAQ_API_URLS[env]}?url={urllib.parse.quote(url, safe='')}"
    headers = {"X-Api-Key": FAQ_API_KEYS[env]()}
    last = None
    for i in range(attempts):
        try:
            r = requests.delete(api, headers=headers, timeout=60)
            if 200 <= r.status_code < 300:
                return True, r.status_code
            last = r.status_code
        except Exception as e:
            last = repr(e)
        time.sleep(0.5 * (i + 1))
    return False, last


def live_count(url, env):
    """Number of questions currently live for one URL, or None on error."""
    api = f"{FAQ_API_URLS[env]}?url={urllib.parse.quote(url, safe='')}"
    try:
        r = requests.get(api, headers={"X-Api-Key": FAQ_API_KEYS[env]()}, timeout=45)
        if not (200 <= r.status_code < 300):
            return None
        return len(r.json())
    except Exception:
        return None


def fetch_scope(env):
    """(url_count, record_count) the sweep would rewrite."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*) AS urls,
                   coalesce(sum(json_array_length(faq_json::json)), 0) AS records
              FROM pa.faq_content_v2
             WHERE faq_json IS NOT NULL AND faq_json <> ''
        """)
        row = cur.fetchone()
        cur.close()
        return row["urls"], row["records"]
    finally:
        return_db_connection(conn)


def sample_urls(n):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT u.url
              FROM pa.faq_content_v2 f
              JOIN pa.urls u ON u.url_id = f.url_id
             WHERE f.faq_json IS NOT NULL AND f.faq_json <> ''
             ORDER BY random() LIMIT %s
        """, (n,))
        urls = [r["url"] for r in cur.fetchall()]
        cur.close()
        return urls
    finally:
        return_db_connection(conn)


def report_live_damage(env, n, workers):
    """Sample live question counts and print the distribution."""
    urls = sample_urls(n)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        counts = [c for c in ex.map(lambda u: live_count(u, env), urls) if c is not None]
    if not counts:
        print("  could not read any live counts — check the API key")
        return
    over = [c for c in counts if c > 6]
    print(f"  sampled {len(counts)} live URLs")
    print(f"  more than 6 questions : {len(over)} ({100 * len(over) / len(counts):.0f}%)")
    print(f"  average / worst       : {sum(counts) / len(counts):.1f} / {max(counts)}")
    hist = {}
    for c in counts:
        hist[c] = hist.get(c, 0) + 1
    print("  distribution          : " + ", ".join(f"{k}×{v}" for k, v in sorted(hist.items())))


def load_progress(env):
    if not os.path.exists(PROGRESS_FILE):
        return None
    with open(PROGRESS_FILE) as fh:
        p = json.load(fh)
    if p.get("env") != env:
        print(f"[sweep] progress file is for env {p.get('env')!r}, not {env!r} — ignoring")
        return None
    return p


def save_progress(state):
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    tmp = PROGRESS_FILE + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(state, fh, indent=2)
    os.replace(tmp, PROGRESS_FILE)   # atomic: a crash mid-write must not eat the checkpoint


def iter_chunks(after_url_id, limit, chunk_size):
    """Yield lists of (url_id, url, records), ordered by url_id so --resume is exact."""
    conn = get_db_connection()
    try:
        cur = conn.cursor(name="faq_v2_sweep_stream")
        cur.itersize = 2000
        sql = """
            SELECT f.url_id, u.url, f.faq_json
              FROM pa.faq_content_v2 f
              JOIN pa.urls u ON u.url_id = f.url_id
             WHERE f.faq_json IS NOT NULL AND f.faq_json <> ''
               AND f.url_id > %s
             ORDER BY f.url_id
        """
        params = [after_url_id]
        if limit:
            sql += " LIMIT %s"
            params.append(limit)
        cur.execute(sql, params)

        buf = []
        for row in cur:
            recs, skip_reason = _build_records(row["url"], row["faq_json"])
            if skip_reason:
                # Same call the publisher makes. An unpublishable row is left
                # exactly as it is live: deleting it would strip a page's FAQ
                # with nothing to put back.
                continue
            buf.append((row["url_id"], row["url"], recs))
            if len(buf) >= chunk_size:
                yield buf
                buf = []
        if buf:
            yield buf
        cur.close()
    finally:
        return_db_connection(conn)


def run_sweep(env, chunk_size, workers, limit, resume):
    scope_urls, scope_records = fetch_scope(env)
    after = 0
    state = {"env": env, "last_url_id": 0, "urls_done": 0, "records_pushed": 0,
             "urls_failed": 0, "started_at": time.strftime("%Y-%m-%d %H:%M:%S")}
    if resume:
        prev = load_progress(env)
        if prev:
            state = prev
            after = prev["last_url_id"]
            print(f"[sweep] resuming after url_id {after} "
                  f"({prev['urls_done']:,} URLs already done)")
        else:
            print("[sweep] --resume given but no usable progress file — starting at the top")

    print(f"[sweep] env={env} chunk={chunk_size} workers={workers} "
          f"scope={scope_urls:,} URLs / {scope_records:,} records"
          + (f" limit={limit:,}" if limit else ""))

    t0 = time.time()
    failed_urls = []
    for chunk in iter_chunks(after, limit, chunk_size):
        # 1. clear the live set for these URLs, in parallel
        with ThreadPoolExecutor(max_workers=workers) as ex:
            results = list(ex.map(lambda c: delete_url(c[1], env), chunk))

        # 2. only repost URLs whose DELETE actually landed
        records, ok_urls = [], []
        for (url_id, url, recs), (ok, code) in zip(chunk, results):
            if ok:
                records.extend(recs)
                ok_urls.append(url_id)
            else:
                failed_urls.append((url, code))
                state["urls_failed"] += 1

        if records:
            posted, code, text = _post_batch(records, env)
            if posted:
                state["records_pushed"] += len(records)
                state["urls_done"] += len(ok_urls)
            else:
                # The DELETEs already landed, so these URLs are now EMPTY live.
                # Stop rather than plough on: the next --resume run redoes this
                # chunk from the same checkpoint and restores them.
                print(f"\n[sweep] POST failed (HTTP {code}): {text[:300]}")
                print(f"[sweep] {len(ok_urls)} URLs are deleted but not reposted. "
                      f"Re-run with --resume to restore them.")
                save_progress(state)
                return state, failed_urls

        # Checkpoint on the LAST url_id of the chunk, only after its POST succeeded.
        state["last_url_id"] = chunk[-1][0]
        save_progress(state)

        done = state["urls_done"]
        rate = done / max(1e-9, time.time() - t0)
        remaining = max(0, (limit or scope_urls) - done)
        eta = remaining / rate if rate else 0
        print(f"\r[sweep] {done:,}/{limit or scope_urls:,} URLs  "
              f"{state['records_pushed']:,} records  {rate:.1f} URL/s  "
              f"ETA {eta / 3600:.1f}h  failed {state['urls_failed']:,}",
              end="", flush=True)

        if _stop:
            print("\n[sweep] stopped on request — checkpoint saved.")
            break

    print()
    return state, failed_urls


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--commit", action="store_true", help="actually rewrite the live store")
    ap.add_argument("--resume", action="store_true", help="continue from the progress file")
    ap.add_argument("--verify", action="store_true", help="sample live counts and exit")
    ap.add_argument("--env", default="production", choices=list(FAQ_API_URLS))
    ap.add_argument("--limit", type=int, help="cap on URLs, for a pilot run")
    ap.add_argument("--chunk", type=int, default=DEFAULT_CHUNK)
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    ap.add_argument("--sample", type=int, default=60, help="URLs to probe in dry run / --verify")
    args = ap.parse_args()

    if not FAQ_API_KEYS[args.env]():
        sys.exit(f"No API key configured for env {args.env!r} — set CONTENT_API_KEY_"
                 f"{args.env.upper()} in .env")

    if args.verify or not args.commit:
        urls, records = fetch_scope(args.env)
        print(f"Scope: {urls:,} URLs / {records:,} records in pa.faq_content_v2 ({args.env})")
        print(f"Live sample ({args.sample} random published URLs):")
        report_live_damage(args.env, args.sample, args.workers)
        if not args.commit:
            est = urls / max(1, args.workers) * 0.15 / 3600
            print(f"\nDry run — nothing written. The sweep would issue {urls:,} DELETEs and "
                  f"~{records // 2000 + 1:,} POSTs (rough ETA {est:.1f}h at {args.workers} workers).")
            print("Run with --commit --limit 500 first, then --commit for the full sweep.")
        return

    signal.signal(signal.SIGINT, _on_sigint)
    state, failed = run_sweep(args.env, args.chunk, args.workers, args.limit, args.resume)

    print(f"\nDone: {state['urls_done']:,} URLs rewritten, "
          f"{state['records_pushed']:,} records posted, {state['urls_failed']:,} URLs failed.")
    if failed:
        print("First failed URLs (DELETE did not land — these keep their duplicates):")
        for url, code in failed[:20]:
            print(f"  {code}  {url}")
        print("Re-run without --resume to retry them in a later full pass.")
    print(f"Progress file: {PROGRESS_FILE}")


if __name__ == "__main__":
    main()
