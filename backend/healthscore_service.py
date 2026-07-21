"""
Healthscore 2.0 — service module (dm-tools style, standalone-runnable).

PHASE 1: Coverage KPI harness.
Measures the north-star metric for the HS2.0 redesign:

    % of a month's organic SEO visits that land on a URL present in the
    current HTML-sitemap set.

Data flow (per the HS2.0 architecture decision):
  - READ  from Redshift  (analytics: fct_visits, dim_visit, new_hs_data)
  - WRITE to the n8n Postgres DB used by dm-tools (DATABASE_URL) — results,
    and later the search-volume cache and generated sitemaps.

Definitions (kept consistent with the SEO Stats tool, seo_stats_service.py):
  - Organic SEO visit = datamart.fct_visits joined to dim_visit, is_real_visit=1,
    channel resolved via chan_deriv.ref_channel_derivation_stats, marketing_channel='SEO'.
    (NOT search_console.visits — that column counts all visits, not organic-only.)
  - Revenue = omzet_visit at visit grain = ww_revenue + cpc_revenue + affiliate_revenue
    (our own revenue; secondary sanity metric here, not the cpa_outclicks figure).

The "set" = distinct URLs in bt.new_hs_data for the target month (the sitemap that
was live for that month). URLs are normalized identically on both sides for matching:
strip scheme+host, strip query/fragment, lowercase, drop trailing slash.

Usage:
    python backend/healthscore_service.py                 # previous complete month
    python backend/healthscore_service.py --month 2026-06
    python backend/healthscore_service.py --month 2026-06 --no-write   # print only
"""
from __future__ import annotations

import os
import sys
import argparse
from datetime import date

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
ENV_PATH = os.path.join(os.path.dirname(__file__), "..", ".env")

# Dutch month names as stored in bt.new_hs_data.current_month_year ("Juni 2026").
_NL_MONTHS = {
    1: "januari", 2: "februari", 3: "maart", 4: "april", 5: "mei", 6: "juni",
    7: "juli", 8: "augustus", 9: "september", 10: "oktober", 11: "november", 12: "december",
}

RESULT_TABLE = "pa.healthscore_coverage"

# One reusable URL-normalization expression, applied to BOTH sides so the join
# key is consistent regardless of scheme/host/query/trailing-slash differences.
#   dim_visit.url : https://www.beslist.nl/products/x/  -> /products/x
#   new_hs_data.url: /products/x/                        -> /products/x
def _norm(col: str) -> str:
    return (
        f"rtrim(split_part(split_part("
        f"lower(regexp_replace({col}, '^https?://[^/]+', '')),"
        f"'?', 1), '#', 1), '/')"
    )


# --------------------------------------------------------------------------- #
# .env loading (tolerant: works with or without python-dotenv)
# --------------------------------------------------------------------------- #
def _load_env() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(ENV_PATH)
        if os.getenv("REDSHIFT_HOST") and os.getenv("DATABASE_URL"):
            return
    except Exception:
        pass
    if not os.path.exists(ENV_PATH):
        return
    for line in open(ENV_PATH):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _redshift():
    return psycopg2.connect(
        host=os.environ["REDSHIFT_HOST"], port=os.environ["REDSHIFT_PORT"],
        dbname=os.environ["REDSHIFT_DB"], user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"], connect_timeout=30,
    )


def _postgres():
    return psycopg2.connect(dsn=os.environ["DATABASE_URL"], connect_timeout=20)


# --------------------------------------------------------------------------- #
# Core: compute coverage for one month
# --------------------------------------------------------------------------- #
def _month_bounds(target_month: str):
    """'YYYY-MM' -> (date_key_lo, date_key_hi, normalized_nl_label)."""
    year, month = (int(x) for x in target_month.split("-"))
    lo = year * 10000 + month * 100 + 1
    hi = year * 10000 + month * 100 + 31
    label = f"{_NL_MONTHS[month]} {year}"  # normalized (lower, single space)
    return lo, hi, label


def compute_coverage(target_month: str) -> dict:
    """Compute SEO-visit coverage of the sitemap set for `target_month` (YYYY-MM).

    Returns a dict with per-type_url rows plus an '__ALL__' summary row, all
    computed server-side on Redshift.
    """
    lo, hi, nl_label = _month_bounds(target_month)
    nv, nu = _norm("dv.url"), _norm("nh.url")

    coverage_sql = f"""
        WITH visits AS (
            SELECT {nv}                              AS npath,
                   COALESCE(dv.type_url, '(none)')   AS type_url,
                   COUNT(*)                          AS visits,
                   SUM(COALESCE(fv.ww_revenue,0)
                       + COALESCE(fv.cpc_revenue,0)
                       + COALESCE(fv.affiliate_revenue,0)) AS revenue
            FROM datamart.fct_visits fv
            JOIN datamart.dim_visit dv
              ON fv.dim_visit_key = dv.dim_visit_key
            JOIN chan_deriv.ref_channel_derivation_stats c
              ON dv.aff_id = c.aff_id AND dv.channel_id = c.channel_id
            WHERE fv.dim_date_key BETWEEN %(lo)s AND %(hi)s
              AND dv.is_real_visit = 1
              AND c.marketing_channel = 'SEO'
              AND dv.url ~ '^https?://www\\.beslist\\.nl/'
            GROUP BY 1, 2
        ),
        setu AS (
            SELECT DISTINCT {nu} AS npath
            FROM bt.new_hs_data nh
            WHERE regexp_replace(lower(trim(nh.current_month_year)), '[[:space:]]+', ' ') = %(label)s
              AND nh.country = 'nl'
              AND nh.url IS NOT NULL AND nh.url <> ''
        )
        SELECT v.type_url,
               CASE WHEN s.npath IS NOT NULL THEN 1 ELSE 0 END AS in_set,
               SUM(v.visits)  AS visits,
               SUM(v.revenue) AS revenue
        FROM visits v
        LEFT JOIN setu s ON s.npath = v.npath
        GROUP BY 1, 2
    """
    setsize_sql = f"""
        SELECT COUNT(DISTINCT {nu}) AS n
        FROM bt.new_hs_data nh
        WHERE regexp_replace(lower(trim(nh.current_month_year)), '[[:space:]]+', ' ') = %(label)s
          AND nh.country = 'nl' AND nh.url IS NOT NULL AND nh.url <> ''
    """
    params = {"lo": lo, "hi": hi, "label": nl_label}

    with _redshift() as rs, rs.cursor(cursor_factory=RealDictCursor) as c:
        c.execute(coverage_sql, params)
        raw = c.fetchall()
        c.execute(setsize_sql, params)
        set_url_count = c.fetchone()["n"]

    # Pivot to per-type_url {in_set, total} and an overall __ALL__ row.
    agg: dict[str, dict] = {}
    for r in raw:
        t = agg.setdefault(r["type_url"], {"in_v": 0, "tot_v": 0, "in_r": 0.0, "tot_r": 0.0})
        t["tot_v"] += int(r["visits"] or 0)
        t["tot_r"] += float(r["revenue"] or 0.0)
        if r["in_set"] == 1:
            t["in_v"] += int(r["visits"] or 0)
            t["in_r"] += float(r["revenue"] or 0.0)

    def _row(type_url, t):
        return {
            "type_url": type_url,
            "in_set_visits": t["in_v"], "total_visits": t["tot_v"],
            "visit_coverage_pct": (100.0 * t["in_v"] / t["tot_v"]) if t["tot_v"] else None,
            "in_set_revenue": round(t["in_r"], 2), "total_revenue": round(t["tot_r"], 2),
            "revenue_coverage_pct": (100.0 * t["in_r"] / t["tot_r"]) if t["tot_r"] else None,
        }

    rows = [_row(tu, t) for tu, t in sorted(agg.items(), key=lambda kv: -kv[1]["tot_v"])]
    allt = {"in_v": sum(t["in_v"] for t in agg.values()),
            "tot_v": sum(t["tot_v"] for t in agg.values()),
            "in_r": sum(t["in_r"] for t in agg.values()),
            "tot_r": sum(t["tot_r"] for t in agg.values())}
    rows.insert(0, _row("__ALL__", allt))

    return {"target_month": target_month, "set_url_count": set_url_count, "rows": rows}


# --------------------------------------------------------------------------- #
# Persistence: write results to the n8n Postgres DB
# --------------------------------------------------------------------------- #
def _ensure_result_table(pg) -> None:
    with pg.cursor() as c:
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {RESULT_TABLE} (
                id                   BIGSERIAL PRIMARY KEY,
                run_ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
                target_month         TEXT NOT NULL,
                type_url             TEXT NOT NULL,
                in_set_visits        BIGINT,
                total_visits         BIGINT,
                visit_coverage_pct   DOUBLE PRECISION,
                in_set_revenue       DOUBLE PRECISION,
                total_revenue        DOUBLE PRECISION,
                revenue_coverage_pct DOUBLE PRECISION,
                set_url_count        BIGINT
            )
        """)
    pg.commit()


def write_coverage(result: dict) -> int:
    """Append one run's coverage rows to pa.healthscore_coverage. Returns rows written."""
    pg = _postgres()
    try:
        _ensure_result_table(pg)
        payload = [(
            result["target_month"], r["type_url"], r["in_set_visits"], r["total_visits"],
            r["visit_coverage_pct"], r["in_set_revenue"], r["total_revenue"],
            r["revenue_coverage_pct"], result["set_url_count"],
        ) for r in result["rows"]]
        with pg.cursor() as c:
            execute_values(c, f"""
                INSERT INTO {RESULT_TABLE}
                    (target_month, type_url, in_set_visits, total_visits, visit_coverage_pct,
                     in_set_revenue, total_revenue, revenue_coverage_pct, set_url_count)
                VALUES %s
            """, payload)
        pg.commit()
        return len(payload)
    finally:
        pg.close()


# --------------------------------------------------------------------------- #
# PHASE 2: per-URL feature build (C-urls AND R-urls, uniform)
# --------------------------------------------------------------------------- #
# Window from SEO reasoning: 90d level features (stable enough for the long-tail
# R-urls we're trying to cover) + a 14d momentum term for responsiveness.
FEATURE_TABLE = "pa.hs2_features"


def _feature_windows(as_of: date, window_days: int, momentum_days: int):
    """Return dim_date_key ints for the level, recent and prior windows."""
    from datetime import timedelta

    def key(d: date) -> int:
        return d.year * 10000 + d.month * 100 + d.day

    win_lo = as_of - timedelta(days=window_days - 1)
    rec_lo = as_of - timedelta(days=momentum_days - 1)     # recent = last N days
    prior_hi = rec_lo - timedelta(days=1)
    prior_lo = prior_hi - timedelta(days=momentum_days - 1)  # prior = N days before that
    return {"win_lo": key(win_lo), "as_of": key(as_of),
            "rec_lo": key(rec_lo), "prior_lo": key(prior_lo), "prior_hi": key(prior_hi)}


def _ensure_feature_table(pg) -> None:
    with pg.cursor() as c:
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {FEATURE_TABLE} (
                as_of_date          DATE NOT NULL,
                npath               TEXT NOT NULL,
                sample_url          TEXT,
                type_url            TEXT,
                deepest_category_id BIGINT,
                visits              BIGINT,
                ctr                 DOUBLE PRECISION,
                bounce_rate         DOUBLE PRECISION,
                revenue             DOUBLE PRECISION,
                visits_recent       BIGINT,
                visits_prior        BIGINT,
                momentum            DOUBLE PRECISION,
                PRIMARY KEY (as_of_date, npath)
            )
        """)
        c.execute(f"CREATE INDEX IF NOT EXISTS hs2_feat_cat_idx "
                  f"ON {FEATURE_TABLE} (as_of_date, deepest_category_id)")
    pg.commit()


def build_features(as_of: date, window_days: int = 90, momentum_days: int = 14) -> int:
    """Build per-URL SEO behavioral features as-of `as_of` and write to Postgres.

    Covers every SEO landing URL (C-url, R-url, PLP, ...) with >=1 organic visit
    in the window. R-urls need no special-casing — they are just type_url='R-url'.
    Idempotent per as_of (deletes that snapshot first). Returns rows written.
    """
    import math
    from math import log

    w = _feature_windows(as_of, window_days, momentum_days)
    nv = _norm("dv.url")

    feat_sql = f"""
        SELECT {nv}                                   AS npath,
               MAX(COALESCE(dv.type_url, '(none)'))   AS type_url,
               MAX(dv.deepest_subcat_id)              AS deepest_category_id,
               MIN(dv.url)                            AS sample_url,
               COUNT(*)                               AS visits,
               SUM(COALESCE(fv.number_of_bvb_clicks,0)
                   + COALESCE(fv.number_of_outclicks,0))            AS clicks,
               SUM(CASE WHEN COALESCE(fv.number_of_cpc_productclicks,0)=0
                         AND COALESCE(fv.number_of_ww_productclicks,0)=0
                        THEN 1 ELSE 0 END)                          AS noprod,
               SUM(COALESCE(fv.ww_revenue,0) + COALESCE(fv.cpc_revenue,0)
                   + COALESCE(fv.affiliate_revenue,0))              AS revenue,
               SUM(CASE WHEN fv.dim_date_key BETWEEN %(rec_lo)s AND %(as_of)s
                        THEN 1 ELSE 0 END)                          AS visits_recent,
               SUM(CASE WHEN fv.dim_date_key BETWEEN %(prior_lo)s AND %(prior_hi)s
                        THEN 1 ELSE 0 END)                          AS visits_prior
        FROM datamart.fct_visits fv
        JOIN datamart.dim_visit dv
          ON fv.dim_visit_key = dv.dim_visit_key
        JOIN chan_deriv.ref_channel_derivation_stats c
          ON dv.aff_id = c.aff_id AND dv.channel_id = c.channel_id
        WHERE fv.dim_date_key BETWEEN %(win_lo)s AND %(as_of)s
          AND dv.is_real_visit = 1
          AND c.marketing_channel = 'SEO'
          AND dv.url ~ '^https?://www\\.beslist\\.nl/'
        GROUP BY 1
    """

    rs = _redshift()
    pg = _postgres()
    try:
        _ensure_feature_table(pg)
        with pg.cursor() as pc:
            pc.execute(f"DELETE FROM {FEATURE_TABLE} WHERE as_of_date = %s", (as_of,))
        pg.commit()

        rc = rs.cursor(name="hs2_feat")  # server-side cursor to stream ~1.1M rows
        rc.itersize = 50000
        rc.execute(feat_sql, w)

        written = 0
        batch = []
        for npath, type_url, cat, sample_url, visits, clicks, noprod, rev, rec, prior in rc:
            visits = int(visits or 0)
            ctr = (clicks / visits) if visits else None
            bounce = (noprod / visits) if visits else None
            momentum = log((int(rec or 0) + 1) / (int(prior or 0) + 1))  # +1 smoothing
            batch.append((as_of, npath, sample_url, type_url, cat, visits,
                          ctr, bounce, float(rev or 0.0), int(rec or 0), int(prior or 0), momentum))
            if len(batch) >= 50000:
                with pg.cursor() as pc:
                    execute_values(pc, f"""
                        INSERT INTO {FEATURE_TABLE}
                            (as_of_date, npath, sample_url, type_url, deepest_category_id,
                             visits, ctr, bounce_rate, revenue, visits_recent, visits_prior, momentum)
                        VALUES %s ON CONFLICT (as_of_date, npath) DO NOTHING
                    """, batch, page_size=10000)
                pg.commit()
                written += len(batch)
                print(f"  ... {written:,} rows", file=sys.stderr)
                batch = []
        if batch:
            with pg.cursor() as pc:
                execute_values(pc, f"""
                    INSERT INTO {FEATURE_TABLE}
                        (as_of_date, npath, sample_url, type_url, deepest_category_id,
                         visits, ctr, bounce_rate, revenue, visits_recent, visits_prior, momentum)
                    VALUES %s ON CONFLICT (as_of_date, npath) DO NOTHING
                """, batch, page_size=10000)
            pg.commit()
            written += len(batch)
        rc.close()
        return written
    finally:
        rs.close()
        pg.close()


# --------------------------------------------------------------------------- #
# PHASE 2b: Keyword Planner search-volume cache (n8n Postgres)
# --------------------------------------------------------------------------- #
# Search volume is a slow-moving 12-month average -> fetch monthly, cache, and
# let every twice-weekly run read the cache (0 added API calls per run).
# Default scope = R-url search terms with visit history (the coverage gap);
# category ga_keywords are opt-in (--scope all).
KW_TABLE = "pa.hs_keyword_search_volume"


def _ensure_kw_table(pg) -> None:
    with pg.cursor() as c:
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {KW_TABLE} (
                keyword_norm  TEXT PRIMARY KEY,
                search_volume BIGINT,
                source        TEXT,
                fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)
    pg.commit()


def _gather_keywords(as_of: date, scope: str, window_days: int) -> list:
    """Distinct raw keywords to price: R-url r_terms (+ category ga_keywords if scope='all')."""
    w = _feature_windows(as_of, window_days, 14)
    out = set()
    rs = _redshift()
    try:
        with rs.cursor() as c:
            c.execute("""
                SELECT DISTINCT trim(dv.r_terms)
                FROM datamart.fct_visits fv
                JOIN datamart.dim_visit dv ON fv.dim_visit_key = dv.dim_visit_key
                JOIN chan_deriv.ref_channel_derivation_stats ch
                  ON dv.aff_id = ch.aff_id AND dv.channel_id = ch.channel_id
                WHERE fv.dim_date_key BETWEEN %(win_lo)s AND %(as_of)s
                  AND dv.is_real_visit = 1 AND ch.marketing_channel = 'SEO'
                  AND dv.type_url = 'R-url'
                  AND dv.r_terms IS NOT NULL AND trim(dv.r_terms) <> ''
            """, w)
            out.update(r[0] for r in c.fetchall() if r[0])
            if scope == "all":
                c.execute("""
                    SELECT DISTINCT trim(ga_keyword) FROM bt.hs_sitemap_input_details_deepestcat
                    WHERE country = 'nl' AND ga_keyword IS NOT NULL AND trim(ga_keyword) <> ''
                """)
                out.update(r[0] for r in c.fetchall() if r[0])
    finally:
        rs.close()
    return list(out)


def build_keyword_cache(as_of: date, scope: str = "r_terms",
                        window_days: int = 90, stale_days: int = 25) -> dict:
    """Fetch Google Keyword Planner volumes for the candidate keyword universe and
    upsert into pa.hs_keyword_search_volume. Skips keywords already fetched within
    `stale_days`. Requires the dm-tools venv (google-ads) + GOOGLE_* creds in .env."""
    from keyword_planner_service import get_search_volumes, clean_keyword  # needs google-ads

    raw = _gather_keywords(as_of, scope, window_days)
    # Normalize + dedup to the API's own keyword space so cache keys line up.
    norm_set = {clean_keyword(k) for k in raw}
    norm_set.discard("")

    pg = _postgres()
    try:
        _ensure_kw_table(pg)
        with pg.cursor() as c:
            c.execute(f"SELECT keyword_norm FROM {KW_TABLE} "
                      f"WHERE fetched_at > now() - (%s || ' days')::interval", (stale_days,))
            fresh = {r[0] for r in c.fetchall()}
        to_fetch = sorted(norm_set - fresh)
        print(f"[HS2.0] keyword universe: {len(norm_set):,} unique "
              f"({len(fresh):,} fresh cached, {len(to_fetch):,} to fetch)", file=sys.stderr)
        if not to_fetch:
            return {"universe": len(norm_set), "fetched": 0, "fresh": len(fresh)}

        resp = get_search_volumes(to_fetch)  # cleans/batches/rotates internally
        best: dict = {}
        for r in resp.get("results", []):
            kw = r.get("normalized_keyword") or clean_keyword(r.get("original_keyword", ""))
            if not kw:
                continue
            best[kw] = max(best.get(kw, -1), int(r.get("search_volume", 0) or 0))

        payload = [(kw, vol, scope) for kw, vol in best.items()]
        with pg.cursor() as c:
            execute_values(c, f"""
                INSERT INTO {KW_TABLE} (keyword_norm, search_volume, source)
                VALUES %s
                ON CONFLICT (keyword_norm)
                DO UPDATE SET search_volume = EXCLUDED.search_volume,
                              source = EXCLUDED.source, fetched_at = now()
            """, payload, page_size=5000)
        pg.commit()
        return {"universe": len(norm_set), "fetched": len(payload), "fresh": len(fresh),
                "with_volume": resp.get("successful", 0)}
    finally:
        pg.close()


# --------------------------------------------------------------------------- #
# PHASE 3.5: seasonal per-category caps (yearly coverage-knee + climatology)
# --------------------------------------------------------------------------- #
# A flat per-category cap is wrong in both directions: the median category needs
# ~120 URLs for 90% visit coverage while diffuse ones (Sneakers) need ~12k. So we
# size each category's cap from its own traffic and flex it by season:
#   base_cap_c   = coverage-knee: # URLs to cover knee_p% of category c's own
#                  ALL-CHANNEL visits over the trailing 12 months, clamped [MIN,MAX].
#   season_mult  = climatology: category c's avg ALL-CHANNEL visits in a calendar
#                  month / its avg month (24-month window), dampened by alpha,
#                  clamped. One-month look-ahead: use MAX(index[m], index[m+1]) so
#                  the cap ramps up BEFORE a demand peak (SEO needs lead time) and
#                  stays high through it. Cap-sizing uses all channels for a fuller
#                  demand signal; the coverage KPI + URL score remain SEO-only.
#   cap_c(month) = clamp(round(base_cap_c * season_mult), MIN, MAX)
CAT_MONTH_TABLE = "pa.hs2_cat_month"
KNEE_TABLE = "pa.hs2_cat_knee"
CAP_TABLE = "pa.hs2_cat_cap"
CAP_MIN_DEFAULT = 100
CAP_MAX_DEFAULT = 12000
KNEE_P_DEFAULT = 90
SEASON_ALPHA_DEFAULT = 1.0
SEASON_MULT_MIN_DEFAULT = 0.4
SEASON_MULT_MAX_DEFAULT = 2.5


def _dkey(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def _refresh_cat_month(as_of: date, months: int = 24) -> int:
    """Aggregate ALL-CHANNEL visits per (deepest_subcat_id, yyyymm) over the
    trailing `months` and (re)write pa.hs2_cat_month. Feeds the seasonal
    climatology. All-channel (not SEO-only) so the demand-timing signal that
    sizes caps reflects true seasonal demand, not just realised SEO traffic."""
    from datetime import timedelta
    lo, hi = _dkey(as_of - timedelta(days=int(months * 30.5))), _dkey(as_of)
    sql = f"""
        SELECT dv.deepest_subcat_id AS cat, fv.dim_date_key / 100 AS yyyymm,
               COUNT(*) AS visits, SUM({_REV}) AS revenue
        FROM datamart.fct_visits fv {_ALL_JOIN}
        WHERE fv.dim_date_key BETWEEN %(lo)s AND %(hi)s AND {_ALL_WHERE}
          AND dv.deepest_subcat_id IS NOT NULL
        GROUP BY 1, 2
    """
    with _redshift() as rs, rs.cursor() as c:
        c.execute(sql, {"lo": lo, "hi": hi})
        data = c.fetchall()
    pg = _postgres()
    try:
        with pg.cursor() as c:
            c.execute(f"""CREATE TABLE IF NOT EXISTS {CAT_MONTH_TABLE} (
                cat BIGINT, yyyymm INT, visits BIGINT, revenue DOUBLE PRECISION,
                PRIMARY KEY (cat, yyyymm))""")
            c.execute(f"TRUNCATE {CAT_MONTH_TABLE}")
            execute_values(c, f"INSERT INTO {CAT_MONTH_TABLE} (cat,yyyymm,visits,revenue) VALUES %s",
                           [(r[0], r[1], int(r[2] or 0), float(r[3] or 0)) for r in data],
                           page_size=10000)
        pg.commit()
    finally:
        pg.close()
    return len(data)


def _refresh_cat_knee(as_of: date, months: int = 12) -> int:
    """Coverage-knee per category (URLs to reach 80/90/95% of the category's own
    ALL-CHANNEL visits) over the trailing `months`; (re)write pa.hs2_cat_knee.
    All-channel (not SEO-only) so the bucket size reflects the full demand
    distribution; the URL score that fills the bucket stays SEO-only. All the
    cumulative-share work is done server-side on Redshift."""
    from datetime import timedelta
    lo, hi = _dkey(as_of - timedelta(days=int(months * 30.5))), _dkey(as_of)
    nv = _norm("dv.url")
    sql = f"""
        WITH u AS (
            SELECT dv.deepest_subcat_id AS cat, {nv} AS npath, COUNT(*) AS v
            FROM datamart.fct_visits fv {_ALL_JOIN}
            WHERE fv.dim_date_key BETWEEN %(lo)s AND %(hi)s AND {_ALL_WHERE}
              AND dv.deepest_subcat_id IS NOT NULL
            GROUP BY 1, 2
        ),
        r AS (
            SELECT cat,
                   SUM(v) OVER (PARTITION BY cat ORDER BY v DESC ROWS UNBOUNDED PRECEDING) AS cum,
                   SUM(v) OVER (PARTITION BY cat) AS tot,
                   ROW_NUMBER() OVER (PARTITION BY cat ORDER BY v DESC) AS rn
            FROM u
        )
        SELECT cat, MAX(tot) AS yearly,
               MIN(CASE WHEN cum >= 0.80*tot THEN rn END) AS knee80,
               MIN(CASE WHEN cum >= 0.90*tot THEN rn END) AS knee90,
               MIN(CASE WHEN cum >= 0.95*tot THEN rn END) AS knee95,
               COUNT(*) AS n_urls
        FROM r GROUP BY cat
    """
    with _redshift() as rs, rs.cursor() as c:
        c.execute(sql, {"lo": lo, "hi": hi})
        data = c.fetchall()
    pg = _postgres()
    try:
        with pg.cursor() as c:
            c.execute(f"""CREATE TABLE IF NOT EXISTS {KNEE_TABLE} (
                cat BIGINT PRIMARY KEY, yearly BIGINT, knee80 INT, knee90 INT,
                knee95 INT, n_urls INT)""")
            c.execute(f"TRUNCATE {KNEE_TABLE}")
            execute_values(c, f"INSERT INTO {KNEE_TABLE} (cat,yearly,knee80,knee90,knee95,n_urls) VALUES %s",
                           [(r[0], int(r[1] or 0), r[2], r[3], r[4], r[5]) for r in data],
                           page_size=10000)
        pg.commit()
    finally:
        pg.close()
    return len(data)


def build_category_caps(as_of: date, knee_p: int = KNEE_P_DEFAULT,
                        cap_min: int = CAP_MIN_DEFAULT, cap_max: int = CAP_MAX_DEFAULT,
                        alpha: float = SEASON_ALPHA_DEFAULT,
                        mult_min: float = SEASON_MULT_MIN_DEFAULT,
                        mult_max: float = SEASON_MULT_MAX_DEFAULT,
                        refresh_source: bool = True) -> dict:
    """Build per-category, per-calendar-month caps into pa.hs2_cat_cap.

    refresh_source rebuilds pa.hs2_cat_month (24m climatology) + pa.hs2_cat_knee
    (12m coverage-knee) from Redshift first (heavy); pass False to only recombine
    the persisted tables (fast, for clamp/alpha tuning). Returns a summary."""
    from collections import defaultdict

    if refresh_source:
        _refresh_cat_month(as_of)
        _refresh_cat_knee(as_of)

    pg = _postgres()
    try:
        with pg.cursor(cursor_factory=RealDictCursor) as c:
            c.execute(f"SELECT cat, yearly, knee80, knee90, knee95 FROM {KNEE_TABLE}")
            knee = {r["cat"]: r for r in c.fetchall()}
            c.execute(f"SELECT cat, yyyymm, visits FROM {CAT_MONTH_TABLE}")
            cm: dict = defaultdict(dict)
            for r in c.fetchall():
                cm[r["cat"]][r["yyyymm"]] = int(r["visits"] or 0)
    finally:
        pg.close()

    knee_col = {80: "knee80", 90: "knee90", 95: "knee95"}[knee_p]

    def clamp(x, lo, hi):
        return max(lo, min(hi, x))

    def season_idx(cat):
        clim: dict = defaultdict(list)
        for ym, v in cm.get(cat, {}).items():
            clim[ym % 100].append(v)
        cl = {m: sum(vs) / len(vs) for m, vs in clim.items()}
        base = (sum(cl.values()) / len(cl)) if cl else 0
        return {m: (cl[m] / base if base else 1.0) for m in cl}

    rows = []
    for cat, k in knee.items():
        kv = k[knee_col]
        if kv is None:
            continue
        base = int(clamp(kv, cap_min, cap_max))
        idx = season_idx(cat)
        for m in range(1, 13):
            # One-month look-ahead: SEO takes time to rank, so a month's cap
            # should already anticipate next month's demand (run-up to a peak).
            # Forward MAX over current + next month (Dec wraps to Jan) ramps the
            # cap up before the peak AND holds it high through the peak itself.
            nxt = m % 12 + 1
            raw = max(idx.get(m, 1.0), idx.get(nxt, 1.0))
            mult = clamp(raw ** alpha, mult_min, mult_max)
            cap = int(clamp(round(base * mult), cap_min, cap_max))
            rows.append((cat, m, base, round(raw, 3), cap, int(k["yearly"] or 0)))

    pg = _postgres()
    try:
        with pg.cursor() as c:
            c.execute(f"""CREATE TABLE IF NOT EXISTS {CAP_TABLE} (
                cat BIGINT, calendar_month INT, base_cap INT, season_index DOUBLE PRECISION,
                cap INT, yearly BIGINT, PRIMARY KEY (cat, calendar_month))""")
            c.execute(f"TRUNCATE {CAP_TABLE}")
            execute_values(c, f"INSERT INTO {CAP_TABLE} "
                              f"(cat,calendar_month,base_cap,season_index,cap,yearly) VALUES %s",
                           rows, page_size=10000)
        pg.commit()
    finally:
        pg.close()
    return {"as_of": str(as_of), "cats": len(knee), "cap_rows": len(rows),
            "knee_p": knee_p, "cap_min": cap_min, "cap_max": cap_max, "alpha": alpha}


# --------------------------------------------------------------------------- #
# PHASE 4: selection + writer (score-first top-N per category + new-URL bucket)
# --------------------------------------------------------------------------- #
# Backtested model (stable across Apr->May and May->June splits):
#   score = 0.889 * pct(log visits) + 0.111 * pct(log revenue), within category.
# CTR/bounce/momentum/search-volume earned no weight -> not in the score.
# Cold-start (freshness) handled by a guaranteed bucket of recently-created URLs.
SITEMAP_TABLE = "pa.hs2_sitemap"
W_VISITS_DEFAULT = 0.889
W_REV_DEFAULT = 0.111
CAP_N_DEFAULT = 1000
NEW_URL_DAYS_DEFAULT = 20  # facet-value recency window (per the provided query)

# Guaranteed new-URL bucket: pages whose facet value only appeared within the
# last `days` (both min & max load_start_date recent = genuinely new). Source
# query provided by the user; `url` is the full facet-page path.
_NORM_RS = ("rtrim(split_part(split_part(lower(regexp_replace(url,'^https?://[^/]+','')),'?',1),'#',1),'/')")
NEW_URL_SQL = f"""
    SELECT DISTINCT {_NORM_RS} AS npath, url AS sample_url
    FROM bt.facet_facetvalues fv
    WHERE fv.deleted_ind = 0 AND fv.country = 'nl'
      AND fv.url IS NOT NULL AND fv.url <> ''
      AND fv.facet_id || fv.facet_value_id || fv.country IN (
          SELECT facet_id || facet_value_id || country
          FROM bt.facet_facetvalues
          GROUP BY 1
          HAVING max(load_start_date) > date(sysdate) - %(days)s
             AND min(load_start_date) > date(sysdate) - %(days)s
      )
"""


def _ensure_sitemap_table(pg) -> None:
    with pg.cursor() as c:
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {SITEMAP_TABLE} (
                as_of_date          DATE NOT NULL,
                npath               TEXT NOT NULL,
                sample_url          TEXT,
                deepest_category_id BIGINT,
                type_url            TEXT,
                score               DOUBLE PRECISION,
                rank_in_cat         INT,
                source              TEXT,   -- 'scored' | 'new'
                PRIMARY KEY (as_of_date, npath)
            )
        """)
    pg.commit()


def build_sitemaps(as_of: date, cap_n: int = CAP_N_DEFAULT,
                   w_visits: float = W_VISITS_DEFAULT, w_rev: float = W_REV_DEFAULT,
                   new_url_days: int = NEW_URL_DAYS_DEFAULT,
                   seasonal_caps: bool = True) -> dict:
    """Select the HS2.0 sitemap set for `as_of` and write it to pa.hs2_sitemap.

    scored = top-N per deepest_category_id by the backtested score (reads the
    pa.hs2_features snapshot for `as_of`). N is the per-category seasonal cap from
    pa.hs2_cat_cap for `as_of`'s calendar month (build it with build_category_caps);
    any category missing from that table falls back to the flat `cap_n`. Set
    seasonal_caps=False to force the flat cap everywhere. Then a guaranteed bucket
    of pa.urls created within `new_url_days` is unioned in (never overrides a
    scored row). Idempotent per as_of. Returns a summary."""
    cap_join = (f"LEFT JOIN {CAP_TABLE} cp "
                f"ON cp.cat = r.deepest_category_id AND cp.calendar_month = %(month)s"
                if seasonal_caps else "")
    cap_pred = "COALESCE(cp.cap, %(cap)s)" if seasonal_caps else "%(cap)s"
    pg = _postgres()
    try:
        _ensure_sitemap_table(pg)
        with pg.cursor() as c:
            c.execute(f"DELETE FROM {SITEMAP_TABLE} WHERE as_of_date = %s", (as_of,))
            # --- scored top-N per category (N = seasonal per-cat cap) ---
            c.execute(f"""
                INSERT INTO {SITEMAP_TABLE}
                    (as_of_date, npath, sample_url, deepest_category_id, type_url, score, rank_in_cat, source)
                SELECT %(as_of)s, r.npath, r.sample_url, r.deepest_category_id, r.type_url, r.score, r.rnk, 'scored'
                FROM (
                    SELECT *, row_number() OVER (
                        PARTITION BY deepest_category_id ORDER BY score DESC, visits DESC) AS rnk
                    FROM (
                        SELECT npath, sample_url, type_url, deepest_category_id, visits, revenue,
                               %(wv)s * percent_rank() OVER (
                                   PARTITION BY deepest_category_id ORDER BY ln(1 + visits))
                             + %(wr)s * percent_rank() OVER (
                                   PARTITION BY deepest_category_id ORDER BY ln(1 + GREATEST(revenue, 0))) AS score
                        FROM {FEATURE_TABLE}
                        WHERE as_of_date = %(as_of)s AND deepest_category_id IS NOT NULL
                    ) s
                ) r
                {cap_join}
                WHERE r.rnk <= {cap_pred}
            """, {"as_of": as_of, "wv": w_visits, "wr": w_rev, "cap": cap_n,
                  "month": as_of.month})
            scored_n = c.rowcount
        pg.commit()

        # --- guaranteed new-URL bucket: recently-appeared facet pages (Redshift) ---
        rs = _redshift()
        try:
            with rs.cursor() as rc:
                rc.execute(NEW_URL_SQL, {"days": new_url_days})
                new_rows = rc.fetchall()
        finally:
            rs.close()
        with pg.cursor() as c:
            execute_values(c, f"""
                INSERT INTO {SITEMAP_TABLE}
                    (as_of_date, npath, sample_url, deepest_category_id, type_url, score, rank_in_cat, source)
                VALUES %s
                ON CONFLICT (as_of_date, npath) DO NOTHING
            """, [(as_of, npath, url, None, 'new-url', None, None, 'new')
                  for npath, url in new_rows if npath], page_size=10000)
        pg.commit()

        with pg.cursor() as c:
            c.execute(f"SELECT count(*), count(DISTINCT deepest_category_id) FROM {SITEMAP_TABLE} WHERE as_of_date=%s", (as_of,))
            total, cats = c.fetchone()
            c.execute(f"SELECT source, count(*) FROM {SITEMAP_TABLE} WHERE as_of_date=%s GROUP BY 1", (as_of,))
            by_source = dict(c.fetchall())
        return {"as_of": str(as_of), "cap_n": cap_n, "seasonal_caps": seasonal_caps,
                "total": total, "categories": cats,
                "new_query_rows": len(new_rows), "by_source": by_source}
    finally:
        pg.close()


# --------------------------------------------------------------------------- #
# PHASE 5: shadow comparison (HS2.0 projected vs current live set, out-of-sample)
# --------------------------------------------------------------------------- #
# Re-derives the HS2.0 scored set from a leakage-free predictor window (the 90
# days strictly BEFORE the holdout month), scores it with the locked model, then
# measures how much of the holdout month's real SEO visits/revenue each set
# covers — HS2.0 vs the set that was actually live (bt.new_hs_data). Also splits
# the holdout traffic into what HS2.0 ADDS (only-HS2.0) vs DROPS (only-current).
# Everything runs server-side on Redshift; the summary row lands in Postgres.
SHADOW_TABLE = "pa.hs2_shadow"

_SEO_JOIN = (
    "JOIN datamart.dim_visit dv ON fv.dim_visit_key = dv.dim_visit_key "
    "JOIN chan_deriv.ref_channel_derivation_stats c "
    "  ON dv.aff_id = c.aff_id AND dv.channel_id = c.channel_id"
)
_SEO_WHERE = ("dv.is_real_visit = 1 AND c.marketing_channel = 'SEO' "
              "AND dv.url ~ '^https?://www\\.beslist\\.nl/'")
# All-channel variants: used ONLY for seasonal cap-sizing (base knee + climatology),
# where we want the full demand signal, not just SEO. The coverage KPI and the URL
# score remain SEO-only. No channel join needed (we don't filter marketing_channel).
_ALL_JOIN = "JOIN datamart.dim_visit dv ON fv.dim_visit_key = dv.dim_visit_key"
_ALL_WHERE = ("dv.is_real_visit = 1 "
              "AND dv.url ~ '^https?://www\\.beslist\\.nl/'")
_REV = ("COALESCE(fv.ww_revenue,0) + COALESCE(fv.cpc_revenue,0) "
        "+ COALESCE(fv.affiliate_revenue,0)")


def _predictor_window(holdout_month: str, window_days: int = 90):
    """Holdout 'YYYY-MM' -> (pred_lo_key, pred_hi_key): the `window_days` ending
    the day before the holdout month starts (leakage-free)."""
    from datetime import timedelta
    y, m = (int(x) for x in holdout_month.split("-"))
    first = date(y, m, 1)
    pred_hi = first - timedelta(days=1)
    pred_lo = first - timedelta(days=window_days)

    def key(d: date) -> int:
        return d.year * 10000 + d.month * 100 + d.day
    return key(pred_lo), key(pred_hi)


def _ensure_shadow_table(pg) -> None:
    with pg.cursor() as c:
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {SHADOW_TABLE} (
                id               BIGSERIAL PRIMARY KEY,
                run_ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
                holdout_month    TEXT NOT NULL,
                cap_n            INT,
                w_visits         DOUBLE PRECISION,
                w_rev            DOUBLE PRECISION,
                hs2_set_size     BIGINT,
                cur_set_size     BIGINT,
                total_visits     BIGINT,
                total_revenue    DOUBLE PRECISION,
                hs2_visits       BIGINT,
                hs2_revenue      DOUBLE PRECISION,
                cur_visits       BIGINT,
                cur_revenue      DOUBLE PRECISION,
                hs2_visit_cov    DOUBLE PRECISION,
                hs2_rev_cov      DOUBLE PRECISION,
                cur_visit_cov    DOUBLE PRECISION,
                cur_rev_cov      DOUBLE PRECISION,
                add_visits       BIGINT,
                add_revenue      DOUBLE PRECISION,
                drop_visits      BIGINT,
                drop_revenue     DOUBLE PRECISION
            )
        """)
    pg.commit()


def compute_shadow(holdout_month: str, cap_n: int = CAP_N_DEFAULT,
                   w_visits: float = W_VISITS_DEFAULT, w_rev: float = W_REV_DEFAULT,
                   window_days: int = 90, write: bool = True) -> dict:
    """Out-of-sample shadow: HS2.0 scored set vs live set on `holdout_month`.

    Predictor = SEO visits in the `window_days` before the month (no leakage);
    scored top-`cap_n`/category by the locked model. Returns coverage for both
    sets plus the add/drop traffic split; optionally writes one row to Postgres.
    """
    lo, hi, nl_label = _month_bounds(holdout_month)
    pred_lo, pred_hi = _predictor_window(holdout_month, window_days)
    nv, nn = _norm("dv.url"), _norm("nh.url")

    sql = f"""
        WITH pred_raw AS (
            SELECT {nv} AS npath, MAX(dv.deepest_subcat_id) AS cat,
                   COUNT(*) AS visits, SUM({_REV}) AS revenue
            FROM datamart.fct_visits fv {_SEO_JOIN}
            WHERE fv.dim_date_key BETWEEN %(pred_lo)s AND %(pred_hi)s AND {_SEO_WHERE}
            GROUP BY 1
        ),
        pred AS (
            SELECT npath, cat, visits,
                   %(wv)s * percent_rank() OVER (PARTITION BY cat ORDER BY ln(1 + visits::float8))
                 + %(wr)s * percent_rank() OVER (PARTITION BY cat ORDER BY ln(1 + GREATEST(revenue,0)::float8)) AS score
            FROM pred_raw WHERE cat IS NOT NULL AND visits > 0
        ),
        hs2 AS (
            SELECT npath FROM (
                SELECT npath, row_number() OVER (
                    PARTITION BY cat ORDER BY score DESC, visits DESC) AS rnk FROM pred
            ) r WHERE rnk <= %(cap)s
        ),
        cur AS (
            SELECT DISTINCT {nn} AS npath FROM bt.new_hs_data nh
            WHERE regexp_replace(lower(trim(nh.current_month_year)), '[[:space:]]+', ' ') = %(label)s
              AND nh.country = 'nl' AND nh.url IS NOT NULL AND nh.url <> ''
        ),
        hold AS (
            SELECT {nv} AS npath, COUNT(*) AS jv, SUM({_REV}) AS jr
            FROM datamart.fct_visits fv {_SEO_JOIN}
            WHERE fv.dim_date_key BETWEEN %(lo)s AND %(hi)s AND {_SEO_WHERE}
            GROUP BY 1
        ),
        flags AS (
            SELECT h.jv, h.jr,
                   CASE WHEN x.npath IS NOT NULL THEN 1 ELSE 0 END AS in_hs2,
                   CASE WHEN cu.npath IS NOT NULL THEN 1 ELSE 0 END AS in_cur
            FROM hold h
            LEFT JOIN hs2 x  ON x.npath  = h.npath
            LEFT JOIN cur cu ON cu.npath = h.npath
        ),
        agg AS (
            SELECT SUM(jv) AS tot_v, SUM(jr) AS tot_r,
                   SUM(jv*in_hs2) AS hs2_v, SUM(jr*in_hs2) AS hs2_r,
                   SUM(jv*in_cur) AS cur_v, SUM(jr*in_cur) AS cur_r,
                   SUM(CASE WHEN in_hs2=1 AND in_cur=0 THEN jv ELSE 0 END) AS add_v,
                   SUM(CASE WHEN in_hs2=1 AND in_cur=0 THEN jr ELSE 0 END) AS add_r,
                   SUM(CASE WHEN in_hs2=0 AND in_cur=1 THEN jv ELSE 0 END) AS drop_v,
                   SUM(CASE WHEN in_hs2=0 AND in_cur=1 THEN jr ELSE 0 END) AS drop_r
            FROM flags
        ),
        hs2_sz AS (SELECT COUNT(*) AS n FROM hs2),
        cur_sz AS (SELECT COUNT(*) AS n FROM cur)
        SELECT agg.*, hs2_sz.n AS hs2_size, cur_sz.n AS cur_size
        FROM agg CROSS JOIN hs2_sz CROSS JOIN cur_sz
    """
    params = {"pred_lo": pred_lo, "pred_hi": pred_hi, "lo": lo, "hi": hi,
              "label": nl_label, "wv": w_visits, "wr": w_rev, "cap": cap_n}

    with _redshift() as rs, rs.cursor(cursor_factory=RealDictCursor) as c:
        c.execute(sql, params)
        r = c.fetchone()

    def _f(x):
        return float(x or 0.0)

    tot_v, tot_r = _f(r["tot_v"]), _f(r["tot_r"])
    out = {
        "holdout_month": holdout_month, "cap_n": cap_n, "w_visits": w_visits, "w_rev": w_rev,
        "hs2_set_size": int(r["hs2_size"] or 0), "cur_set_size": int(r["cur_size"] or 0),
        "total_visits": int(tot_v), "total_revenue": round(tot_r, 2),
        "hs2_visits": int(_f(r["hs2_v"])), "hs2_revenue": round(_f(r["hs2_r"]), 2),
        "cur_visits": int(_f(r["cur_v"])), "cur_revenue": round(_f(r["cur_r"]), 2),
        "hs2_visit_cov": (100.0 * _f(r["hs2_v"]) / tot_v) if tot_v else None,
        "hs2_rev_cov": (100.0 * _f(r["hs2_r"]) / tot_r) if tot_r else None,
        "cur_visit_cov": (100.0 * _f(r["cur_v"]) / tot_v) if tot_v else None,
        "cur_rev_cov": (100.0 * _f(r["cur_r"]) / tot_r) if tot_r else None,
        "add_visits": int(_f(r["add_v"])), "add_revenue": round(_f(r["add_r"]), 2),
        "drop_visits": int(_f(r["drop_v"])), "drop_revenue": round(_f(r["drop_r"]), 2),
    }
    if write:
        pg = _postgres()
        try:
            _ensure_shadow_table(pg)
            with pg.cursor() as c:
                c.execute(f"""
                    INSERT INTO {SHADOW_TABLE}
                        (holdout_month, cap_n, w_visits, w_rev, hs2_set_size, cur_set_size,
                         total_visits, total_revenue, hs2_visits, hs2_revenue, cur_visits, cur_revenue,
                         hs2_visit_cov, hs2_rev_cov, cur_visit_cov, cur_rev_cov,
                         add_visits, add_revenue, drop_visits, drop_revenue)
                    VALUES (%(holdout_month)s, %(cap_n)s, %(w_visits)s, %(w_rev)s, %(hs2_set_size)s,
                            %(cur_set_size)s, %(total_visits)s, %(total_revenue)s, %(hs2_visits)s,
                            %(hs2_revenue)s, %(cur_visits)s, %(cur_revenue)s, %(hs2_visit_cov)s,
                            %(hs2_rev_cov)s, %(cur_visit_cov)s, %(cur_rev_cov)s, %(add_visits)s,
                            %(add_revenue)s, %(drop_visits)s, %(drop_revenue)s)
                """, out)
            pg.commit()
        finally:
            pg.close()
    return out


# --------------------------------------------------------------------------- #
# Dashboard readers (Postgres-only, instant — no Redshift round-trip)
# --------------------------------------------------------------------------- #
def get_coverage_history() -> list:
    """All persisted coverage runs (pa.healthscore_coverage), newest month first."""
    pg = _postgres()
    try:
        with pg.cursor(cursor_factory=RealDictCursor) as c:
            c.execute(f"""
                SELECT target_month, type_url, in_set_visits, total_visits, visit_coverage_pct,
                       in_set_revenue, total_revenue, revenue_coverage_pct, set_url_count,
                       run_ts
                FROM {RESULT_TABLE}
                WHERE (target_month, run_ts) IN (
                    SELECT target_month, MAX(run_ts) FROM {RESULT_TABLE} GROUP BY target_month
                )
                ORDER BY target_month DESC, total_visits DESC
            """)
            return c.fetchall()
    finally:
        pg.close()


def get_sitemap_summary() -> dict:
    """Composition of the latest pa.hs2_sitemap snapshot (per source + type_url)."""
    pg = _postgres()
    try:
        with pg.cursor(cursor_factory=RealDictCursor) as c:
            c.execute(f"SELECT MAX(as_of_date) AS as_of FROM {SITEMAP_TABLE}")
            as_of = c.fetchone()["as_of"]
            if not as_of:
                return {"as_of": None}
            c.execute(f"""
                SELECT count(*) AS total, count(DISTINCT deepest_category_id) AS categories
                FROM {SITEMAP_TABLE} WHERE as_of_date = %s
            """, (as_of,))
            head = c.fetchone()
            c.execute(f"SELECT source, count(*) AS n FROM {SITEMAP_TABLE} "
                      f"WHERE as_of_date=%s GROUP BY 1 ORDER BY 2 DESC", (as_of,))
            by_source = c.fetchall()
            c.execute(f"SELECT COALESCE(type_url,'(none)') AS type_url, count(*) AS n "
                      f"FROM {SITEMAP_TABLE} WHERE as_of_date=%s GROUP BY 1 ORDER BY 2 DESC", (as_of,))
            by_type = c.fetchall()
            return {"as_of": str(as_of), "total": head["total"], "categories": head["categories"],
                    "by_source": by_source, "by_type": by_type}
    finally:
        pg.close()


def get_features_summary() -> dict:
    """Row/coverage summary of the latest pa.hs2_features snapshot."""
    pg = _postgres()
    try:
        with pg.cursor(cursor_factory=RealDictCursor) as c:
            c.execute(f"SELECT MAX(as_of_date) AS as_of FROM {FEATURE_TABLE}")
            as_of = c.fetchone()["as_of"]
            if not as_of:
                return {"as_of": None}
            c.execute(f"""
                SELECT count(*) AS urls, count(DISTINCT deepest_category_id) AS categories,
                       SUM(visits) AS visits
                FROM {FEATURE_TABLE} WHERE as_of_date = %s
            """, (as_of,))
            head = c.fetchone()
            c.execute(f"SELECT COALESCE(type_url,'(none)') AS type_url, count(*) AS n "
                      f"FROM {FEATURE_TABLE} WHERE as_of_date=%s GROUP BY 1 ORDER BY 2 DESC", (as_of,))
            by_type = c.fetchall()
            return {"as_of": str(as_of), "urls": head["urls"], "categories": head["categories"],
                    "visits": int(head["visits"] or 0), "by_type": by_type}
    finally:
        pg.close()


def get_shadow_history() -> list:
    """All persisted shadow runs (pa.hs2_shadow), newest first."""
    pg = _postgres()
    try:
        _ensure_shadow_table(pg)
        with pg.cursor(cursor_factory=RealDictCursor) as c:
            c.execute(f"SELECT * FROM {SHADOW_TABLE} ORDER BY run_ts DESC")
            return c.fetchall()
    finally:
        pg.close()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _default_month() -> str:
    t = date.today()
    y, m = (t.year, t.month - 1) if t.month > 1 else (t.year - 1, 12)
    return f"{y:04d}-{m:02d}"


def _print_report(result: dict) -> None:
    print(f"\nHealthscore coverage — {result['target_month']} "
          f"(sitemap set: {result['set_url_count']:,} URLs)\n")
    hdr = f"{'type_url':<28}{'visits':>14}{'in set':>14}{'cover%':>9}{'rev cover%':>12}"
    print(hdr); print("-" * len(hdr))
    for r in result["rows"]:
        vc = f"{r['visit_coverage_pct']:.1f}" if r["visit_coverage_pct"] is not None else "-"
        rc = f"{r['revenue_coverage_pct']:.1f}" if r["revenue_coverage_pct"] is not None else "-"
        label = r["type_url"] if r["type_url"] != "__ALL__" else "ALL (overall)"
        print(f"{label:<28}{r['total_visits']:>14,}{r['in_set_visits']:>14,}{vc:>9}{rc:>12}")
    print()


def _month_end(target_month: str) -> date:
    """'YYYY-MM' -> last day of that month (as_of reference for features)."""
    from datetime import timedelta
    y, m = (int(x) for x in target_month.split("-"))
    first_next = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    return first_next - timedelta(days=1)


def main() -> None:
    ap = argparse.ArgumentParser(description="Healthscore 2.0 — coverage KPI (Phase 1) & feature build (Phase 2)")
    ap.add_argument("--action", choices=["coverage", "features", "keywords", "caps", "sitemap", "shadow"],
                    default="coverage",
                    help="coverage=Phase 1 KPI; features=Phase 2 features; keywords=search-volume cache; "
                         "caps=Phase 3.5 seasonal per-category caps; sitemap=Phase 4 selection; "
                         "shadow=Phase 5 projected-vs-live comparison")
    ap.add_argument("--cap-n", type=int, default=CAP_N_DEFAULT,
                    help="sitemap: fallback flat cap for cats missing from pa.hs2_cat_cap (default 1000)")
    ap.add_argument("--flat-caps", action="store_true",
                    help="sitemap: ignore pa.hs2_cat_cap, use the flat --cap-n everywhere")
    ap.add_argument("--knee-p", type=int, choices=[80, 90, 95], default=KNEE_P_DEFAULT,
                    help="caps: coverage-knee target percent (default 90)")
    ap.add_argument("--cap-max", type=int, default=CAP_MAX_DEFAULT, help="caps: base-cap ceiling (default 12000)")
    ap.add_argument("--cap-min", type=int, default=CAP_MIN_DEFAULT, help="caps: base-cap floor (default 100)")
    ap.add_argument("--alpha", type=float, default=SEASON_ALPHA_DEFAULT, help="caps: seasonality strength (default 1.0)")
    ap.add_argument("--reuse-knee", action="store_true",
                    help="caps: recombine persisted knee/climatology tables without re-querying Redshift")
    ap.add_argument("--month", default=None, help="Target month YYYY-MM (default: previous complete month)")
    ap.add_argument("--as-of", default=None, help="features/keywords: reference date YYYY-MM-DD (default: end of --month)")
    ap.add_argument("--window-days", type=int, default=90, help="features/keywords: level-feature window (default 90)")
    ap.add_argument("--momentum-days", type=int, default=14, help="features: momentum half-window (default 14)")
    ap.add_argument("--scope", choices=["r_terms", "all"], default="r_terms",
                    help="keywords: r_terms = R-url search terms only; all = also category ga_keywords")
    ap.add_argument("--stale-days", type=int, default=25, help="keywords: re-fetch cache entries older than this")
    ap.add_argument("--no-write", action="store_true", help="coverage: print only; do not write to Postgres")
    args = ap.parse_args()

    _load_env()
    target = args.month or _default_month()

    if args.action == "coverage":
        print(f"[HS2.0] Computing SEO-visit coverage for {target} ...", file=sys.stderr)
        result = compute_coverage(target)
        _print_report(result)
        if args.no_write:
            print("(--no-write: results not persisted)")
        else:
            n = write_coverage(result)
            print(f"Wrote {n} rows to {RESULT_TABLE} on the n8n Postgres DB.")
    elif args.action == "features":
        as_of = date.fromisoformat(args.as_of) if args.as_of else _month_end(target)
        print(f"[HS2.0] Building per-URL features as-of {as_of} "
              f"(window {args.window_days}d, momentum {args.momentum_days}d) ...", file=sys.stderr)
        n = build_features(as_of, args.window_days, args.momentum_days)
        print(f"Wrote {n:,} URL feature rows to {FEATURE_TABLE} (as_of {as_of}) on the n8n Postgres DB.")
    elif args.action == "keywords":
        as_of = date.fromisoformat(args.as_of) if args.as_of else _month_end(target)
        print(f"[HS2.0] Building keyword search-volume cache as-of {as_of} "
              f"(scope={args.scope}) ...", file=sys.stderr)
        stats = build_keyword_cache(as_of, args.scope, args.window_days, args.stale_days)
        print(f"Keyword cache updated on {KW_TABLE}: {stats}")
    elif args.action == "caps":
        as_of = date.fromisoformat(args.as_of) if args.as_of else _month_end(target)
        print(f"[HS2.0] Building seasonal per-category caps as-of {as_of} "
              f"(knee P{args.knee_p}%, clamp [{args.cap_min},{args.cap_max}], alpha {args.alpha}"
              f"{', reuse-knee' if args.reuse_knee else ''}) ...", file=sys.stderr)
        stats = build_category_caps(as_of, knee_p=args.knee_p, cap_min=args.cap_min,
                                    cap_max=args.cap_max, alpha=args.alpha,
                                    refresh_source=not args.reuse_knee)
        print(f"Wrote seasonal caps to {CAP_TABLE}: {stats}")
    elif args.action == "sitemap":
        as_of = date.fromisoformat(args.as_of) if args.as_of else _month_end(target)
        mode = "flat cap" if args.flat_caps else "seasonal per-cat caps"
        print(f"[HS2.0] Building sitemap selection as-of {as_of} "
              f"({mode}, fallback N={args.cap_n}) ...", file=sys.stderr)
        stats = build_sitemaps(as_of, args.cap_n, seasonal_caps=not args.flat_caps)
        print(f"Wrote sitemap set to {SITEMAP_TABLE}: {stats}")
    else:  # shadow
        print(f"[HS2.0] Shadow comparison — holdout {target} (cap N={args.cap_n}) ...", file=sys.stderr)
        out = compute_shadow(target, args.cap_n)
        print(f"\nShadow — holdout {target} (out-of-sample):")
        print(f"  Current live : visits {out['cur_visit_cov']:.1f}%  revenue {out['cur_rev_cov']:.1f}%"
              f"  ({out['cur_set_size']:,} urls)")
        print(f"  HS2.0 scored : visits {out['hs2_visit_cov']:.1f}%  revenue {out['hs2_rev_cov']:.1f}%"
              f"  ({out['hs2_set_size']:,} urls)")
        print(f"  Delta        : visits {out['hs2_visit_cov']-out['cur_visit_cov']:+.1f}pp  "
              f"revenue {out['hs2_rev_cov']-out['cur_rev_cov']:+.1f}pp")
        print(f"  Adds {out['add_visits']:,} visits (€{out['add_revenue']:,.0f}) / "
              f"drops {out['drop_visits']:,} visits (€{out['drop_revenue']:,.0f})")
        print(f"Wrote 1 row to {SHADOW_TABLE}.")


if __name__ == "__main__":
    main()
