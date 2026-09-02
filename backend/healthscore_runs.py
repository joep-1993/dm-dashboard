"""
Healthscore — category run orchestration + run history.

This is the operational layer the tool page drives. `healthscore_keywords`
already owns the payload build, the contract validation, the live diff and the
gated POST; `healthscore_service` owns the selection that fills
`pa.hs2_sitemap`. What was missing was the thing in between: run a category (or
the test bucket) as ONE auditable unit, keep the result, and be able to look at
it again later.

Two-step by design (Joep, 2026-08-19):

    preview  ->  builds the payload, validates it, diffs it against live,
                 prices the drop list in SEO visits, computes per-URL-type
                 coverage. Sends NOTHING.
    push     ->  replays the payload STORED BY THAT PREVIEW. A push is its own
                 run row (mode='push', parent_run_id = the preview) so history
                 shows what was proposed and what was actually shipped.

The push replays the stored payload rather than rebuilding it, because a rebuild
is not guaranteed to reproduce the preview: `fetch_page_headings` reads a rolling
365-day window, so an anchor can change between the two clicks. Pushing what was
reviewed is the whole point of reviewing it.

`POST /sitemap` REPLACES a category and the API has no DELETE — see
keywords_api_sitemap. Every push therefore snapshots the live set first, and
`healthscore_keywords.push()` still demands its per-category confirm token.
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import date, timedelta
from zoneinfo import ZoneInfo

from psycopg2.extras import Json

from backend import healthscore_keywords as kw
from backend.database import (get_db_connection, get_redshift_connection,
                              return_db_connection, return_redshift_connection)

logger = logging.getLogger(__name__)

RUNS_TABLE = "pa.hs2_runs"
CAT_MAP_TABLE = "pa.hs2_cat_maincat"
SITEMAP_TABLE = "pa.hs2_sitemap"

# The 12 buckets HS2.0 is live on: 10 deepest categories plus two whole maincats.
# Kept here (not in healthscore_keywords) because the maincat half only exists as
# a rollout decision, while TEST_CATEGORIES is also the module's own default.
TEST_MAINCATS = {361: "Kantoor", 38000: "Fietsen"}

# Trailing window for the coverage + drop-cost numbers. 90 days matches the
# feature window the score is built on, so "visits" means the same thing in the
# run report as it does in the selection.
VISITS_DAYS = 90

# Snapshots of the live set, taken before every push. This is the only undo.
SNAPSHOT_DIR = "/mnt/c/Users/JoepvanSchagen/Downloads/claude/hs2_run_snapshots"

_REV = ("COALESCE(fv.ww_revenue,0) + COALESCE(fv.cpc_revenue,0) "
        "+ COALESCE(fv.affiliate_revenue,0)")


# --------------------------------------------------------------------------- #
# Run table
# --------------------------------------------------------------------------- #
def _ensure_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
            run_id        BIGSERIAL PRIMARY KEY,
            run_ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at   TIMESTAMPTZ,
            mode          TEXT NOT NULL,
            scope         TEXT NOT NULL,
            label         TEXT,
            as_of         DATE NOT NULL,
            country       TEXT NOT NULL DEFAULT 'nl',
            status        TEXT NOT NULL,
            error         TEXT,
            parent_run_id BIGINT,
            categories    JSONB NOT NULL,
            stats         JSONB,
            detail        JSONB
        )
    """)
    cur.execute(f"CREATE INDEX IF NOT EXISTS hs2_runs_ts_idx "
                f"ON {RUNS_TABLE} (run_ts DESC)")
    cur.close()
    conn.commit()


def _create_run(mode: str, scope: str, label: str, as_of: date, country: str,
                categories: list, parent_run_id: int = None) -> int:
    conn = get_db_connection()
    try:
        _ensure_table(conn)
        cur = conn.cursor()
        cur.execute(f"""
            INSERT INTO {RUNS_TABLE}
                (mode, scope, label, as_of, country, status, parent_run_id, categories)
            VALUES (%s, %s, %s, %s, %s, 'running', %s, %s)
            RETURNING run_id
        """, (mode, scope, label, as_of, country, parent_run_id, Json(categories)))
        run_id = cur.fetchone()["run_id"]
        cur.close()
        conn.commit()
        return int(run_id)
    finally:
        return_db_connection(conn)


def _finish_run(run_id: int, status: str, stats: dict = None,
                detail: dict = None, error: str = None) -> None:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            UPDATE {RUNS_TABLE}
               SET status=%s, stats=%s, detail=%s, error=%s, finished_at=now()
             WHERE run_id=%s
        """, (status, Json(stats) if stats is not None else None,
              Json(detail) if detail is not None else None, error, run_id))
        cur.close()
        conn.commit()
    finally:
        return_db_connection(conn)


def list_runs(limit: int = 200) -> list:
    """Newest first, WITHOUT `detail` — that column holds the payloads and would
    make the history endpoint tens of megabytes."""
    conn = get_db_connection()
    try:
        _ensure_table(conn)
        cur = conn.cursor()
        cur.execute(f"""
            SELECT run_id, run_ts, finished_at, mode, scope, label, as_of, country,
                   status, error, parent_run_id, categories, stats
              FROM {RUNS_TABLE}
             ORDER BY run_ts DESC
             LIMIT %s
        """, (limit,))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        return_db_connection(conn)


def delete_runs(run_ids: list) -> int:
    """Drop run rows by id; returns how many were actually removed.

    Two things this does NOT do, on purpose. It does not touch the snapshot files a
    push wrote to disk — those are the only rollback that exists, so they outlive the
    history row. And it does not refuse a push row: deleting one throws away the
    record of a live write, which is a call for the operator, not for this function.
    The UI names what is being deleted before it asks.
    """
    ids = [int(x) for x in (run_ids or [])]
    if not ids:
        raise ValueError("no run ids given")
    conn = get_db_connection()
    try:
        _ensure_table(conn)
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {RUNS_TABLE} WHERE run_id = ANY(%s)", (ids,))
        n = cur.rowcount
        cur.close()
        conn.commit()
        logger.info("healthscore: deleted %s run(s): %s", n, ids)
        return int(n)
    finally:
        return_db_connection(conn)


def get_run(run_id: int) -> dict:
    conn = get_db_connection()
    try:
        _ensure_table(conn)
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {RUNS_TABLE} WHERE run_id=%s", (run_id,))
        row = cur.fetchone()
        cur.close()
        return dict(row) if row else None
    finally:
        return_db_connection(conn)


def _local(ts) -> str:
    """The shared Postgres runs on Etc/UTC, so a raw strftime here would be an
    hour off the timestamp the page shows (the page appends 'Z' and lets the
    browser localise). Export the same wall clock as the UI."""
    if not ts:
        return ""
    if ts.tzinfo is not None:
        ts = ts.astimezone(ZoneInfo("Europe/Amsterdam"))
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def runs_csv(limit: int = 1000, run_ids: list = None) -> str:
    """The Recent-runs table as CSV — the same columns the UI shows.

    `run_ids` exports only those runs, in the table's own order (newest first), for
    the checkbox selection in the UI. One writer for both cases on purpose: a second
    CSV builder in the frontend would drift from these columns the first time one
    changes.
    """
    cols = ["run_id", "run_ts", "mode", "scope", "label", "as_of", "country", "status",
            "categories", "proposed_records", "live_records", "added_urls", "kept_urls",
            "dropped_urls", "dropped_seo_visits", "problems", "pushed_ok", "pushed_failed"]
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(cols)
    wanted = {int(x) for x in run_ids} if run_ids else None
    for r in list_runs(limit):
        if wanted is not None and int(r["run_id"]) not in wanted:
            continue
        s = r.get("stats") or {}
        w.writerow([
            r["run_id"],
            _local(r.get("run_ts")),
            r["mode"], r["scope"], r.get("label") or "", r["as_of"], r["country"], r["status"],
            len(r.get("categories") or []),
            s.get("proposed_records", ""), s.get("live_records", ""),
            s.get("added_urls", ""), s.get("kept_urls", ""), s.get("dropped_urls", ""),
            s.get("dropped_seo_visits", ""), s.get("problems", ""),
            s.get("pushed_ok", ""), s.get("pushed_failed", ""),
        ])
    return buf.getvalue()


# --------------------------------------------------------------------------- #
# Category picker
# --------------------------------------------------------------------------- #
def latest_as_of() -> date:
    """The as-of of the newest selection build; every run is pinned to one."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT MAX(as_of_date) AS as_of FROM {SITEMAP_TABLE}")
        row = cur.fetchone()
        cur.close()
        return row["as_of"] if row else None
    finally:
        return_db_connection(conn)


def list_categories(scope: str) -> list:
    """Picker options for one scope, from the synced taxonomy map.

    `maincat` returns the 32 top-level buckets, `deepest` every leaf category
    with its maincat as context — two Sneakers in different maincats are
    otherwise indistinguishable in a dropdown.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        if scope == "maincat":
            cur.execute(f"SELECT DISTINCT maincat AS id, maincat_name AS name "
                        f"FROM {CAT_MAP_TABLE} WHERE maincat IS NOT NULL ORDER BY 2")
        else:
            cur.execute(f"SELECT cat AS id, cat_name AS name, maincat_name AS parent "
                        f"FROM {CAT_MAP_TABLE} WHERE cat IS NOT NULL ORDER BY 2")
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        return_db_connection(conn)


def test_bucket() -> list:
    """The 12 live buckets as picker entries, deepest cats first."""
    return ([{"id": c, "name": n, "scope": "deepest"} for c, n in kw.TEST_CATEGORIES.items()]
            + [{"id": c, "name": n, "scope": "maincat"} for c, n in TEST_MAINCATS.items()])


# --------------------------------------------------------------------------- #
# Per-URL SEO visits, by category and URL type (one query per scope)
# --------------------------------------------------------------------------- #
def seo_visits_by_type(cats, as_of: date, scope: str = "deepest",
                       days: int = VISITS_DAYS) -> dict:
    """{cat_id: {npath: {"type_url", "visits", "revenue"}}} over the trailing window.

    Same shape of query as healthscore_keywords.seo_visits_in_maincats — per
    category SET, not per URL, so one round-trip serves a whole run. Revenue is
    OUR revenue (cpc + ww + affiliate), matching healthscore_service's coverage
    and SEO Stats; `*_shop_revenue` is the shop's and would read ~7x too high.
    """
    if not cats:
        return {}
    lo = int((as_of - timedelta(days=days)).strftime("%Y%m%d"))
    hi = int(as_of.strftime("%Y%m%d"))
    cat_expr = "dc.main_category_id" if scope == "maincat" else "dv.deepest_subcat_id"
    sql = f"""
        SELECT {cat_expr}                            AS cat,
               {kw._NORM_RS}                         AS npath,
               COALESCE(dv.type_url, '(none)')       AS type_url,
               COUNT(*)                              AS visits,
               SUM({_REV})                           AS revenue
          FROM datamart.fct_visits fv
          JOIN datamart.dim_visit dv ON fv.dim_visit_key = dv.dim_visit_key
          JOIN chan_deriv.ref_channel_derivation_stats chan
            ON dv.aff_id = chan.aff_id AND dv.channel_id = chan.channel_id
          JOIN datamart.dim_category dc
            ON dc.deepest_category_id = dv.deepest_subcat_id AND dc.deleted_ind = 0
         WHERE dv.is_real_visit = 1
           AND chan.marketing_channel = 'SEO'
           AND fv.dim_date_key BETWEEN %(lo)s AND %(hi)s
           AND dv.url ~ '^https?://www\\.beslist\\.nl/'
           AND {cat_expr} IN %(cats)s
         GROUP BY 1, 2, 3
    """
    out: dict = {int(c): {} for c in cats}
    conn = get_redshift_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"lo": lo, "hi": hi, "cats": tuple(int(c) for c in cats)})
            for r in cur:
                bucket = out.setdefault(int(r["cat"]), {})
                visits, revenue = int(r["visits"] or 0), float(r["revenue"] or 0.0)
                rec = bucket.get(r["npath"])
                if rec is None:
                    rec = bucket[r["npath"]] = {"type_url": r["type_url"], "visits": 0,
                                                "revenue": 0.0, "_top": -1}
                rec["visits"] += visits
                rec["revenue"] += revenue
                # A URL can carry more than one type_url across visits (the column
                # is also NULL on ~30% of rows); the busiest variant wins, same
                # tie-break the anchor picker uses.
                if visits > rec["_top"]:
                    rec["_top"], rec["type_url"] = visits, r["type_url"]
    finally:
        return_redshift_connection(conn)
    for bucket in out.values():
        for v in bucket.values():
            v.pop("_top", None)
    return out


def coverage_rows(payload_urls: set, cat_visits: dict) -> list:
    """The per-URL-type coverage table for one category.

    Columns match the tool's existing coverage table exactly (URL type · SEO
    visits · in set · visit cov · revenue cov) so one renderer serves both.
    `in set` = lands on a URL this run would publish, NOT on the current live
    set: the question a run answers is what ITS selection covers.
    """
    agg: dict = {}
    for npath, v in cat_visits.items():
        t = agg.setdefault(v["type_url"], {"in_v": 0, "tot_v": 0, "in_r": 0.0, "tot_r": 0.0})
        t["tot_v"] += v["visits"]
        t["tot_r"] += v["revenue"]
        # payload urls carry a leading slash and no trailing one; npath matches.
        if npath in payload_urls:
            t["in_v"] += v["visits"]
            t["in_r"] += v["revenue"]

    def _row(type_url, t):
        return {
            "type_url": type_url,
            "in_set_visits": t["in_v"], "total_visits": t["tot_v"],
            "visit_coverage_pct": (100.0 * t["in_v"] / t["tot_v"]) if t["tot_v"] else None,
            "in_set_revenue": round(t["in_r"], 2), "total_revenue": round(t["tot_r"], 2),
            "revenue_coverage_pct": (100.0 * t["in_r"] / t["tot_r"]) if t["tot_r"] else None,
        }

    rows = [_row(tu, t) for tu, t in sorted(agg.items(), key=lambda kv: -kv[1]["tot_v"])]
    if rows:
        allt = {k: sum(t[k] for t in agg.values()) for k in ("in_v", "tot_v", "in_r", "tot_r")}
        rows.insert(0, _row("__ALL__", allt))
    return rows


# --------------------------------------------------------------------------- #
# Preview
# --------------------------------------------------------------------------- #
def _norm_payload_urls(payload: dict) -> set:
    """Payload urls as npaths (no trailing slash), the key the visits use."""
    return {k["url"].rstrip("/") or "/" for k in payload.get("keywords") or []}


def preview(categories: list, as_of: date = None, country: str = "nl",
            include_plp: bool = False, preserve_cross_category: bool = True,
            label: str = None, progress=None) -> dict:
    """Build + validate + diff + price + measure every category. Sends nothing.

    `categories` = [{"id": int, "name": str, "scope": "deepest"|"maincat"}].
    `progress` is an optional callback the router points at the job dict so the UI
    can draw a real progress bar. The unit of work is ONE CATEGORY, because that
    is the only denominator that exists before the run starts (records per
    category are unknown until it is built). It is called with `phase`
    ("prep" | "cat" | "finish"), `done`, `total` and, during "cat", `what`. The
    counter advances only AFTER a category is on the results list, so the bar
    never runs ahead of what has actually been computed; the two opaque phases
    around the loop carry a phase name and no percentage.
    preserve_cross_category defaults ON for deepest-category runs: a partial
    rollout otherwise deletes live URLs that HS2.0 still wants, only because
    their own category was not in this push (2,935 urls / 35,866 visits on the
    10 test cats). It does not apply to a maincat push, which owns its subtree.
    """
    as_of = as_of or latest_as_of()
    if not as_of:
        raise ValueError("no selection built yet — run the sitemap build first")
    if not categories:
        raise ValueError("pick at least one category")
    rep = progress if callable(progress) else (lambda **kw: None)
    total = len(categories)
    # "prep" is the Redshift work below: opaque calls, so it reports a phase and no
    # number. It does carry a `step`, because on a one-category run this phase IS
    # basically the whole run (measured 2026-08-19: ~3 of 3,5 minutes) and one static
    # label for three minutes tells you nothing. A label change at the same
    # percentage is the honest way to show movement in an unmeasurable phase.
    rep(phase="prep", step="headings", done=0, total=total)

    scopes = {c.get("scope", "deepest") for c in categories}
    scope = scopes.pop() if len(scopes) == 1 else "mixed"
    label = label or (categories[0]["name"] if len(categories) == 1
                      else f"{len(categories)} categorieën")
    run_id = _create_run("preview", scope, label, as_of, country, categories)

    try:
        deep = [c for c in categories if c.get("scope", "deepest") == "deepest"]
        main = [c for c in categories if c.get("scope") == "maincat"]

        # One headings fetch and one visits fetch per scope, reused per category.
        headings = kw.fetch_page_headings([c["id"] for c in deep], as_of) if deep else {}
        mc_headings = kw.fetch_maincat_headings([c["id"] for c in main], as_of) if main else {}
        rep(phase="prep", step="visits", done=0, total=total)
        visits = seo_visits_by_type([c["id"] for c in deep], as_of, "deepest") if deep else {}
        mc_visits = seo_visits_by_type([c["id"] for c in main], as_of, "maincat") if main else {}

        results = []
        for i, c in enumerate(categories, 1):
            rep(phase="cat", done=i - 1, total=total, what=c["name"])
            cid, is_main = int(c["id"]), c.get("scope") == "maincat"
            if is_main:
                built = kw.build_maincat_payload(cid, as_of, mc_headings, country, include_plp)
                cat_visits = mc_visits.get(cid, {})
            else:
                built = kw.build_payload(cid, as_of, headings, country, include_plp,
                                         preserve_cross_category)
                cat_visits = visits.get(cid, {})
            payload = built["payload"]
            ours = _norm_payload_urls(payload)

            live = kw.get_live(cid, country)
            diff = kw.diff_against_live(payload, country, live=live) if payload["keywords"] else None
            # De set-comprehensie stond IN de conditie, dus Python bouwde hem
            # opnieuw voor élk element van `live`: O(live x payload). Gemeten op
            # 20.000 live x 17.000 payload was dat 11,2 s tegen 1,2 ms gehesen —
            # en Kantoor heeft 53k live records.
            payload_urls = {x["url"] for x in payload["keywords"]}
            dropped = [k["url"].rstrip("/") for k in live
                       if k["url"] not in payload_urls]
            drop_visits = {u: cat_visits.get(u, {}).get("visits", 0) for u in set(dropped)}
            costly = {u: v for u, v in drop_visits.items() if v > 0}

            results.append({
                "id": cid, "name": c["name"], "scope": "maincat" if is_main else "deepest",
                "payload": payload,
                "records": len(payload["keywords"]),
                "skipped": built["skipped"],
                "preserved": built.get("preserved", 0),
                "rows_considered": built.get("rows_considered"),
                "problems": kw.validate_payload(payload),
                "request": kw.describe_request(payload),
                "diff": diff,
                "drop_cost": {
                    "dropped_urls": len(set(dropped)),
                    "dropped_urls_with_seo_visits": len(costly),
                    "dropped_seo_visits": sum(costly.values()),
                    "worst": sorted(costly.items(), key=lambda kv: -kv[1])[:10],
                },
                "coverage": coverage_rows(ours, cat_visits),
            })

        rep(phase="finish", done=total, total=total)
        stats = _summarise(results)
        detail = {"as_of": str(as_of), "country": country, "include_plp": include_plp,
                  "preserve_cross_category": preserve_cross_category,
                  "visits_days": VISITS_DAYS, "categories": results}
        _finish_run(run_id, "done", stats, detail)
        return {"run_id": run_id, "status": "done", "stats": stats, "detail": detail}
    except Exception as e:  # noqa: BLE001 — the run row must record the failure
        logger.exception("healthscore preview run %s failed", run_id)
        _finish_run(run_id, "error", error=str(e))
        raise


def _summarise(results: list) -> dict:
    """Run-level totals. `problems` is the count of contract violations across
    all categories — anything above zero blocks the push."""
    def s(key, sub=None):
        tot = 0
        for r in results:
            v = r.get(sub, {}) if sub else r
            tot += int((v or {}).get(key) or 0)
        return tot
    return {
        "categories": len(results),
        "proposed_records": s("records"),
        "live_records": s("live_records", "diff"),
        "added_urls": s("added_urls", "diff"),
        "kept_urls": s("kept_urls", "diff"),
        "dropped_urls": s("dropped_urls", "diff"),
        "anchor_changed": s("anchor_changed", "diff"),
        "preserved": s("preserved"),
        "dropped_seo_visits": s("dropped_seo_visits", "drop_cost"),
        "problems": sum(len(r.get("problems") or []) for r in results),
        "visit_coverage_pct": _weighted_cov(results, "visits"),
        "revenue_coverage_pct": _weighted_cov(results, "revenue"),
    }


def _weighted_cov(results: list, what: str) -> float:
    """Coverage over the whole run: summed in-set over summed total, not a mean
    of per-category percentages (which would let a 12-visit category outvote
    Kantoor)."""
    num = den = 0.0
    for r in results:
        allrow = next((x for x in (r.get("coverage") or []) if x["type_url"] == "__ALL__"), None)
        if not allrow:
            continue
        if what == "visits":
            num += allrow["in_set_visits"] or 0
            den += allrow["total_visits"] or 0
        else:
            num += allrow["in_set_revenue"] or 0
            den += allrow["total_revenue"] or 0
    return round(100.0 * num / den, 2) if den else None


# --------------------------------------------------------------------------- #
# Push
# --------------------------------------------------------------------------- #
def push_run(parent_run_id: int, confirm: str, category_ids: list = None,
             snapshot_dir: str = SNAPSHOT_DIR, progress=None) -> dict:
    """Replay a preview's stored payloads against the live API.

    `confirm` must be the literal string "REPLACE" — typed by a human in the UI.
    The per-category token healthscore_keywords.push() insists on is built from
    it here; making an operator type twelve tokens by hand would only train them
    to paste without reading.

    Refuses a preview that reported contract problems, and refuses an empty
    payload (an empty POST wipes the category). Snapshots each live set first —
    that snapshot is the only rollback that exists.
    """
    if confirm != "REPLACE":
        raise PermissionError('refusing to push: type REPLACE to confirm')
    parent = get_run(parent_run_id)
    if not parent:
        raise ValueError(f"run {parent_run_id} not found")
    if parent["mode"] != "preview" or parent["status"] != "done":
        raise ValueError("only a completed preview run can be pushed")
    cats = (parent.get("detail") or {}).get("categories") or []
    # `is not None`, niet de waarheidswaarde: category_ids=[] is een lege SELECTIE,
    # niet "geen filter". Met `if category_ids:` viel een lege lijst door het filter
    # heen, bleef `cats` de volledige preview en vuurde de guard hieronder nooit —
    # oftewel: elke categorie live vervangen.
    if category_ids is not None:
        wanted = {int(x) for x in category_ids}
        cats = [c for c in cats if int(c["id"]) in wanted]
    if not cats:
        raise ValueError("nothing to push")
    bad = [c["name"] for c in cats if c.get("problems")]
    if bad:
        raise ValueError(f"refusing to push, contract problems in: {bad[:5]}")
    empty = [c["name"] for c in cats if not (c.get("payload") or {}).get("keywords")]
    if empty:
        raise ValueError(f"refusing to push an empty payload (would wipe): {empty[:5]}")

    as_of = parent["as_of"]
    country = parent["country"]
    run_id = _create_run("push", parent["scope"], parent.get("label"), as_of, country,
                         [{"id": c["id"], "name": c["name"], "scope": c["scope"]} for c in cats],
                         parent_run_id=parent_run_id)
    results, ok, failed, mismatch = [], 0, 0, 0
    # Same unit as the preview: one category. A failure still advances the
    # counter — the unit is handled, just not well — or the bar hangs on a run
    # that is finished.
    rep = progress if callable(progress) else (lambda **kw: None)
    total = len(cats)
    try:
        for i, c in enumerate(cats, 1):
            rep(phase="cat", done=i - 1, total=total, what=c["name"])
            cid = int(c["id"])
            entry = {"id": cid, "name": c["name"], "scope": c["scope"],
                     "records": len(c["payload"]["keywords"])}
            try:
                snap = kw.snapshot_live(cid, country, out_dir=snapshot_dir,
                                        run_id=run_id)
                entry["snapshot"] = {"records": snap["records"], "file": snap.get("file")}
                resp = kw.push(c["payload"], confirm_token=f"REPLACE {cid}")
                entry["response"] = {"status_code": resp.get("status_code"),
                                     "body": (resp.get("body") or "")[:300]}
                # Vanaf hier IS de push geslaagd (push() gooit nu op een niet-2xx).
                entry["status"] = "ok"
                ok += 1
            except Exception as e:  # noqa: BLE001 — one category failing must not
                entry["status"] = "error"      # abort the ones still queued
                entry["error"] = str(e)
                failed += 1
                logger.exception("healthscore push failed for category %s", cid)
                results.append(entry)
                continue

            # Read the category back: the API accepts and truncates silently, so
            # "it returned 200" is not the same as "it stored what we sent".
            # EIGEN try: dit is verificatie, geen onderdeel van de schrijfactie.
            # get_live() haalt 17k-68k records op met timeout=300 en zonder retry;
            # een blip daar mag een categorie die wél degelijk vervangen is niet als
            # 'error' wegzetten, want dan herstelt iemand een snapshot over een
            # geslaagde push heen.
            try:
                after = kw.get_live(cid, country)
                entry["live_after"] = len(after)
                entry["exact"] = ({k["url"] for k in after}
                                  == {k["url"] for k in c["payload"]["keywords"]})
                if not entry["exact"]:
                    # 2xx maar een andere set terug = stille truncatie. Dat is geen
                    # geslaagde push, ook al klaagde de API niet.
                    entry["status"] = "mismatch"
                    ok -= 1
                    mismatch += 1
            except Exception as e:  # noqa: BLE001
                entry["readback_error"] = str(e)
                logger.warning("healthscore read-back failed for category %s: %s", cid, e)
            results.append(entry)

        rep(phase="finish", done=total, total=total)
        stats = {"categories": len(results), "pushed_ok": ok, "pushed_failed": failed,
                 "pushed_mismatch": mismatch,
                 "readback_unavailable": sum(1 for r in results if r.get("readback_error")),
                 "proposed_records": sum(r["records"] for r in results),
                 "live_records": sum(r.get("live_after") or 0 for r in results),
                 "exact_readback": sum(1 for r in results if r.get("exact")),
                 "problems": 0}
        detail = {"as_of": str(as_of), "country": country, "parent_run_id": parent_run_id,
                  "snapshot_dir": snapshot_dir, "categories": results}
        bad = failed + mismatch
        msg = None
        if bad:
            msg = (f"{failed} of {len(results)} categories failed"
                   + (f", {mismatch} stored a different set than we sent" if mismatch else ""))
        _finish_run(run_id, "done" if not bad else "error", stats, detail, msg)
        return {"run_id": run_id, "stats": stats, "detail": detail}
    except Exception as e:  # noqa: BLE001
        logger.exception("healthscore push run %s failed", run_id)
        _finish_run(run_id, "error", error=str(e))
        raise
