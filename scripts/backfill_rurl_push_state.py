#!/usr/bin/env python3
"""Backfill the Auto-Redirects push state from the redirect-tool run history.

WHY: `rurl_processed` gained push-tracking columns (`pushed_at`, `push_status`,
`pushed_target`, …) on 2026-09-11, and from that day `redirect_tool_service.save_run`
fills them on every push. Everything pushed BEFORE that lives only in the per-row
JSON of `redirect_tool_runs.results`, so without this script the coverage report
would open with "0 doorgevoerd" for ~2.900 redirects that have been live for weeks.

WHAT IT DOES: replays every redirect-tool run, oldest first, through the same
`rurl_push_tracking.record_run` the live path uses — so there is exactly one
definition of what an outcome means. Runs from other tools (Canonicals, the 301
Generator, hand-typed rules) simply match nothing in `rurl_processed` and cost a
statement each.

Safe to re-run: `record_run` never lets an older run overwrite a newer outcome,
so a second pass writes nothing new.

Dry run by default; `--commit` writes.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection, return_db_connection  # noqa: E402


def _fetch_runs():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, created_at, label, results
                   FROM redirect_tool_runs
                   ORDER BY created_at, id"""
            )
            return cur.fetchall()
    finally:
        conn.rollback()
        return_db_connection(conn)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--commit", action="store_true", help="actually write")
    args = ap.parse_args()

    from backend import rurl_push_tracking as track

    runs = _fetch_runs()
    print(f"{len(runs)} redirect-tool runs to replay")

    if not args.commit:
        # Dry run: reproduce the match WITHOUT touching the table, so the
        # numbers below are the same ones --commit would write.
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT original_url, reliability_tier FROM rurl_processed")
                known = {re.sub(r"^https?://[^/]+", "", r["original_url"]):
                         (r["reliability_tier"] or "?") for r in cur.fetchall()}
        finally:
            conn.rollback()
            return_db_connection(conn)

        seen: dict[str, tuple] = {}
        for run in runs:
            res = run["results"]
            if isinstance(res, str):
                res = json.loads(res)
            for row in (res or []):
                old = re.sub(r"^https?://[^/]+", "", str(row.get("input_old") or "")).strip()
                if old in known:
                    status, established, _t, _d = track._classify(row)
                    seen[old] = (known[old], status, established)
        tally = Counter((tier, "live" if est else status)
                        for tier, status, est in seen.values())
        print(f"\nwould touch {len(seen)} of {len(known)} known R-URLs:")
        for key in sorted(tally):
            print(f"  tier {key[0]}  {key[1]:<10} {tally[key]:>6}")
        print("\n(dry run — pass --commit to write)")
        return 0

    track.ensure_columns()
    total = 0
    for run in runs:
        res = run["results"]
        if isinstance(res, str):
            res = json.loads(res)
        n = track.record_run(run["id"], res or [], at=run["created_at"])
        total += n
        if n:
            print(f"  run #{run['id']:<4} {str(run['created_at'])[:19]}  "
                  f"{run['label'] or '-':<28} -> {n} R-URLs")
    print(f"\nwrote {total} row updates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
