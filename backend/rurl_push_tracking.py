"""Which Auto-Redirects suggestions actually reached production.

`rurl_processed` records what the optimizer *proposed* for an R-URL. It said
nothing about whether that proposal was ever pushed, so the only way to answer
"how much of Tier A is still waiting?" was to read the per-row JSON of every
redirect-tool run by hand. This module closes that loop: every redirect-tool
run is reconciled against `rurl_processed` right after it is saved, so each
suggestion carries the outcome of its last push attempt.

Matching is on the PATH, not the URL. `rurl_processed.original_url` is
absolute (`https://www.beslist.nl/products/…`) while the redirect tool stores
the path it submitted (`/products/…`), because that is what the Redirect API
takes. Everything here normalises both sides through `PATH_SQL` / `_path()`.

Reconciliation is deliberately driven from `save_run`, not from the
Auto-Redirects push button: the same rows are regularly pushed by pasting an
exported xlsx into the Redirect Tool itself, and those pushes are just as real.
Whatever reaches production through any door gets recorded.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from backend.database import get_db_connection, return_db_connection

logger = logging.getLogger(__name__)

# Absolute-or-relative URL -> path. Used identically in SQL and in Python so a
# row can never match on one side and miss on the other.
PATH_SQL = "regexp_replace({col}, '^https?://[^/]+', '')"
_SCHEME_HOST = re.compile(r"^https?://[^/]+", re.I)

# Outcomes that mean "there is now a redirect on this URL pointing at what we
# proposed". `warning` is a posted rule whose incoming-rule rewire partially
# failed — the rule itself is live, so it counts.
_ESTABLISHED = ("ok", "warning")

MIGRATIONS = (
    "ALTER TABLE rurl_processed ADD COLUMN IF NOT EXISTS push_run_id INT",
    "ALTER TABLE rurl_processed ADD COLUMN IF NOT EXISTS push_status TEXT",
    "ALTER TABLE rurl_processed ADD COLUMN IF NOT EXISTS push_detail TEXT",
    "ALTER TABLE rurl_processed ADD COLUMN IF NOT EXISTS push_attempted_at TIMESTAMPTZ",
    "ALTER TABLE rurl_processed ADD COLUMN IF NOT EXISTS pushed_target TEXT",
    "ALTER TABLE rurl_processed ADD COLUMN IF NOT EXISTS pushed_at TIMESTAMPTZ",
    # The reconcile UPDATE and the coverage report both key on the path, and a
    # seq scan over ~100k rows per redirect-tool run is a needless tax.
    "CREATE INDEX IF NOT EXISTS idx_rurl_processed_path "
    "ON rurl_processed ((" + PATH_SQL.format(col="original_url") + "))",
    "CREATE INDEX IF NOT EXISTS idx_rurl_processed_tier_push "
    "ON rurl_processed (reliability_tier, pushed_at)",
)

_READY = False


def ensure_columns() -> None:
    """Add the push-tracking columns on first use. Idempotent."""
    global _READY
    if _READY:
        return
    from backend import rurl_optimizer_persistence as pers
    pers.ensure_table()          # the base table must exist first
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            for stmt in MIGRATIONS:
                cur.execute(stmt)
        conn.commit()
        _READY = True
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)


def _path(url: Any) -> str:
    return _SCHEME_HOST.sub("", str(url or "")).strip()


def _classify(row: dict) -> tuple[str, bool, Optional[str], Optional[str]]:
    """(status, established, target, detail) for one redirect-tool per-row entry.

    `target` is what WE proposed, not necessarily what was POSTed: the redirect
    tool flattens a chain, so `final_new` can be the end of a chain further
    along than our own suggestion. The stale check downstream asks "does the
    optimizer still propose what we pushed", so `input_new` is the honest side
    of that comparison.
    """
    status = str(row.get("status") or "").lower()
    target = row.get("input_new") or row.get("final_new")
    if status in _ESTABLISHED:
        return "ok", True, target, None
    if status == "skipped":
        # "already redirected" + already_correct = the live rule already points
        # where we want it. Nothing was POSTed, but the desired end state holds.
        if row.get("already_correct"):
            return "live", True, target, row.get("skip_reason")
        return "skipped", False, None, row.get("skip_reason")
    if status == "fail":
        detail = row.get("friendly_message") or row.get("raw_message") or row.get("skip_reason")
        return "fail", False, None, (str(detail)[:500] if detail else None)
    return status or "unknown", False, None, row.get("skip_reason")


_UPDATE_SQL = """
UPDATE rurl_processed p SET
    push_run_id       = v.run_id,
    push_status       = v.st,
    push_detail       = v.detail,
    push_attempted_at = v.ts,
    pushed_target     = CASE WHEN v.established THEN v.tgt ELSE p.pushed_target END,
    pushed_at         = CASE WHEN v.established THEN v.ts  ELSE p.pushed_at END
FROM (VALUES %s) AS v(old_path, run_id, st, detail, tgt, established, ts)
WHERE """ + PATH_SQL.format(col="p.original_url") + """ = v.old_path
  -- Never let an older run overwrite a newer outcome. Makes the reconcile
  -- order-independent, so a backfill and a live push can't fight, and
  -- re-running the same run is a no-op rather than a rewrite.
  AND (p.push_attempted_at IS NULL OR p.push_attempted_at <= v.ts)
"""

_VALUES_TEMPLATE = "(%s,%s::int,%s,%s,%s,%s::boolean,%s::timestamptz)"


def record_run(run_id: int, per_row: Iterable[dict],
               at: Optional[datetime] = None) -> int:
    """Stamp one redirect-tool run's outcome onto the R-URLs it touched.

    Returns how many `rurl_processed` rows were updated — usually far fewer
    than the run has rows, because most redirect-tool runs push URLs the
    optimizer never produced (Canonicals, the 301 Generator, hand-typed rules).
    """
    ensure_columns()
    ts = at or datetime.now(timezone.utc)

    # Last entry wins within a run, same as the redirect API itself (one rule
    # per fromUrl).
    by_path: dict[str, tuple] = {}
    for row in per_row or []:
        old = _path(row.get("input_old"))
        if not old:
            continue
        status, established, target, detail = _classify(row)
        by_path[old] = (old, int(run_id), status, detail,
                        _path(target) or None, established, ts)
    if not by_path:
        return 0

    from psycopg2.extras import execute_values

    conn = get_db_connection()
    try:
        updated = 0
        rows = list(by_path.values())
        with conn.cursor() as cur:
            BATCH = 2000
            for i in range(0, len(rows), BATCH):
                chunk = rows[i:i + BATCH]
                # page_size must cover the whole chunk. execute_values defaults
                # to 100 tuples per statement and leaves `rowcount` holding only
                # the LAST page's tally — which silently under-reports a 500-row
                # push as 100. One statement per chunk keeps the count honest.
                execute_values(cur, _UPDATE_SQL, chunk,
                               template=_VALUES_TEMPLATE, page_size=len(chunk))
                updated += cur.rowcount
        conn.commit()
        return updated
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)


def record_run_safe(run_id: int, per_row: Iterable[dict],
                    at: Optional[datetime] = None) -> int:
    """record_run that never raises. The push already landed in production by
    the time this runs; losing the bookkeeping must not turn a successful push
    into a failed task."""
    try:
        return record_run(run_id, per_row, at=at)
    except Exception:
        logger.exception("rurl push tracking: could not record run %s", run_id)
        return 0


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

# One state per row, disjoint and exhaustive, so the buckets always sum to the
# tier total. Order matters: the first matching branch wins.
STATE_SQL = """
CASE
    WHEN redirect_url IS NULL OR redirect_url = '' THEN 'no_target'
    WHEN pushed_at IS NOT NULL
         AND {pt} = {ru}                                   THEN 'live'
    WHEN pushed_at IS NOT NULL                             THEN 'stale'
    WHEN push_status = 'fail'                              THEN 'failed'
    WHEN push_status = 'skipped'                           THEN 'skipped'
    ELSE 'never'
END
""".format(pt=PATH_SQL.format(col="COALESCE(pushed_target, '')"),
           ru=PATH_SQL.format(col="redirect_url"))

STATES = ("live", "stale", "never", "failed", "skipped", "no_target")

STATE_LABELS = {
    "live": "Doorgevoerd",
    "stale": "Doel gewijzigd",
    "never": "Nog niet gepusht",
    "failed": "Push mislukt",
    "skipped": "Overgeslagen",
    "no_target": "Geen doel gevonden",
}


def coverage() -> dict:
    """Per-tier tally of what is live, what is waiting and what needs redoing."""
    ensure_columns()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT COALESCE(reliability_tier, '?') AS tier,
                           {STATE_SQL} AS state,
                           count(*) AS n
                    FROM rurl_processed
                    GROUP BY 1, 2"""
            )
            grid: dict[str, dict[str, int]] = {}
            for r in cur.fetchall():
                grid.setdefault(r["tier"], {})[r["state"]] = int(r["n"])

            cur.execute(
                """SELECT max(push_attempted_at) AS last_attempt,
                          max(pushed_at)         AS last_push,
                          count(*) FILTER (WHERE push_attempted_at IS NOT NULL) AS attempted
                   FROM rurl_processed"""
            )
            meta = cur.fetchone() or {}
    finally:
        conn.rollback()
        return_db_connection(conn)

    tiers = []
    totals = {s: 0 for s in STATES}
    for tier in sorted(grid):
        counts = {s: int(grid[tier].get(s, 0)) for s in STATES}
        for s in STATES:
            totals[s] += counts[s]
        tiers.append({"tier": tier, "total": sum(counts.values()), **counts})

    return {
        "tiers": tiers,
        "totals": {"total": sum(totals.values()), **totals},
        "labels": STATE_LABELS,
        "last_push_at": (meta.get("last_push").isoformat()
                         if meta.get("last_push") else None),
        "last_attempt_at": (meta.get("last_attempt").isoformat()
                            if meta.get("last_attempt") else None),
        "attempted": int(meta.get("attempted") or 0),
    }


# "There is a redirect live on this source URL." Two ways to earn that: we
# pushed one ourselves (`pushed_at`), or the push was skipped because the URL
# already carried somebody else's rule. Both mean the same thing for anyone
# about to re-run the optimizer over it — the URL 301s, so the scraper would
# read the destination page and the engine would score a redirect it invented
# from the wrong content.
#
# COALESCE, not a bare `push_status = 'skipped'`: for the 106k rows that have
# never been pushed, push_status is NULL, so the comparison yields NULL and the
# whole OR collapses to NULL rather than false. In a WHERE that reads as false
# and looks fine — but the moment anything asks for `NOT (…)`, every one of
# those rows drops out of BOTH sides of the answer. Found while counting rows
# for a delete, where it would have silently protected the entire backlog.
LIVE_SQL = ("(pushed_at IS NOT NULL"
            " OR (COALESCE(push_status, '') = 'skipped'"
            "     AND COALESCE(push_detail, '') = 'source has existing rule'))")


def live_urls(urls: Iterable[str]) -> set[str]:
    """The subset of `urls` that already redirects in production.

    Batched for the same reason `already_processed` is: a Tier-A pool passes
    hundreds of thousands of URLs, and one giant ANY() array is slow to
    serialise and to match.
    """
    ensure_columns()
    url_list = [u for u in urls if u]
    if not url_list:
        return set()
    found: set[str] = set()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            BATCH = 50_000
            for i in range(0, len(url_list), BATCH):
                cur.execute(
                    "SELECT original_url FROM rurl_processed "
                    f"WHERE original_url = ANY(%s) AND {LIVE_SQL}",
                    (url_list[i:i + BATCH],),
                )
                found.update(r["original_url"] for r in cur.fetchall())
        return found
    finally:
        conn.rollback()
        return_db_connection(conn)


def urls_to_skip(urls: Iterable[str], force_reprocess: bool) -> set[str]:
    """Which input URLs a run must not process.

    Normally that is everything the optimizer has already seen — the whole
    point of the `rurl_processed` cache.

    "Force reprocess all" drops that cache on purpose, but it must NOT drop the
    URLs that are already live: re-running one is worse than wasted work. The
    source 301s now, so the scraper follows it, the engine scores the
    destination's content and writes a fresh suggestion over a row we know went
    to production — which is exactly the row you would want left untouched.
    So force narrows the skip list to the live redirects instead of emptying it.
    """
    url_list = [u for u in urls if u]
    if not url_list:
        return set()
    if force_reprocess:
        return live_urls(url_list)
    from backend import rurl_optimizer_persistence as pers
    return pers.already_processed(url_list)


MAX_OUTSTANDING = 50_000


def outstanding(tiers: Optional[list[str]] = None,
                states: Optional[list[str]] = None,
                limit: int = 5000) -> list[dict]:
    """The rows behind a coverage bucket, best score first.

    Defaults to the question that prompted all of this: which Tier A/B
    suggestions are not live yet.
    """
    ensure_columns()
    tiers = [t.strip().upper() for t in (tiers or ["A", "B"]) if t.strip()]
    # "ALL" / "*" means every tier, including the unscored ones. Without it an
    # empty list would silently fall back to the A/B default and quietly answer
    # a different question than the one asked.
    if any(t in ("ALL", "*") for t in tiers):
        tiers = []
    states = [s.strip().lower() for s in (states or ["never", "stale", "failed"])
              if s.strip() in STATES]
    if not states:
        states = ["never"]
    limit = max(1, min(int(limit), MAX_OUTSTANDING))

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT original_url, redirect_url, reliability_tier,
                           reliability_score, reason, processed_at,
                           push_status, push_detail, pushed_target, pushed_at,
                           push_run_id, {STATE_SQL} AS state
                    FROM rurl_processed
                    WHERE ({STATE_SQL}) = ANY(%s)
                      AND (%s::text[] IS NULL
                           OR COALESCE(reliability_tier, '?') = ANY(%s))
                    ORDER BY reliability_score DESC NULLS LAST, original_url
                    LIMIT %s""",
                (states, tiers or None, tiers or None, limit),
            )
            rows = cur.fetchall()
    finally:
        conn.rollback()
        return_db_connection(conn)

    out = []
    for r in rows:
        d = dict(r)
        for key in ("processed_at", "pushed_at"):
            if d.get(key) is not None:
                d[key] = d[key].isoformat()
        out.append(d)
    return out
