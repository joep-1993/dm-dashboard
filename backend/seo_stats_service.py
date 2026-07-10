"""
SEO stats — read-only dashboard data for the "SEO stats" tool.

Serves a live web UI:
  - per-day visits + revenue for SEO / DMA organic / GSAAS  (chart + table)
  - channel %-deltas and top maincats/subcats with the most positive deltas

Two grains, merged in Python:
  - VISITS  come from datamart.fct_visits (is_real_visit=1), channel via chan_deriv.
  - REVENUE is Beslist's OWN revenue (click_revenue, "onze omzet") from
    bt.cpa_outclicks_transactional — the figure the Qlik "Beslist omzet en clicks"
    app reports (covers CPR + WW + affiliate). See REV_TABLE_FILTERS below.

Comparison logic (matches performance_standup_service):
  - visits  : yesterday          vs the week before (yesterday - 7d)
  - revenue : day-2 (yesterday-1) vs day-9 (yesterday-8)
Revenue lags one extra day because it settles later than visit counts; on top of
that, click_revenue keeps settling for up to ~180 days after the conversion date,
so the most recent 1-2 days undercount until conversions land.

Category breakdowns use marketing_channel = 'SEO'.
"""

import copy
import time
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from backend.database import (
    get_redshift_connection, return_redshift_connection,
    get_db_connection, return_db_connection,
)

logger = logging.getLogger(__name__)

# marketing_channel value -> short key used in the JSON / chart
CHANNELS: Dict[str, str] = {
    "SEO": "seo",
    "DMA organic": "dma",
    "GSAAS": "gsaas",
}
CHANNEL_LABELS = {"seo": "SEO", "dma": "DMA Organic", "gsaas": "GSAAS"}

TOP_N = 100  # rows returned per top-cat table (frontend slices to the chosen Top X)

# Revenue = Beslist's OWN revenue ("onze omzet") from bt.cpa_outclicks_transactional
# (click_revenue), the same figure the Qlik "Beslist omzet en clicks" app reports.
# It covers CPR (cpa / cpa_cpc / t3_fallback), WW (shoppingcart) AND affiliate
# commission — broader than the old visit-grain cpc+ww metric, which excluded
# affiliate. Visits still come from datamart.fct_visits (visit grain); only the
# revenue source changed, so the two are merged per (date/category, channel).
#
# NOTE: click_revenue is attributed by CONVERSION date and keeps settling for up
# to ~180 days, so the most recent 1-2 days undercount until conversions land
# (same reason the Qlik number keeps moving). The UI carries a settling caveat.
REV_TABLE_FILTERS = (
    "tac.actual_ind = 1 AND tac.deleted_ind = 0 "
    "AND tac.label NOT IN ('cpa_after_180_days','rejected_click')"
)

# ---------------------------------------------------------------------------
# Tiny in-process TTL cache (Redshift is slow; the data is daily-grained)
# ---------------------------------------------------------------------------
_CACHE: Dict[str, Tuple[float, dict]] = {}
_CACHE_TTL = 300  # seconds


def _cache_get(key: str):
    hit = _CACHE.get(key)
    if hit and (time.time() - hit[0]) < _CACHE_TTL:
        return copy.deepcopy(hit[1])  # hand callers a private copy; never the cached object
    return None


def _cache_set(key: str, value: dict):
    _CACHE[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _date_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def _parse_date(s: Optional[str], default: date) -> date:
    if not s:
        return default
    return datetime.strptime(s, "%Y-%m-%d").date()


def _pct_delta(p1: float, p2: float) -> Optional[float]:
    """Percentage change p1 -> p2. None when the baseline is zero."""
    if not p1:
        return None
    return (p2 - p1) / p1 * 100.0


# ---------------------------------------------------------------------------
# Redshift queries
# ---------------------------------------------------------------------------

def _fetch_daily(conn, dates: List[date]) -> Dict[date, Dict]:
    """Visits + revenue per (date, channel) for SEO / DMA organic / GSAAS.

    Visits come from datamart.fct_visits (visit grain); revenue from
    bt.cpa_outclicks_transactional (conversion grain). The two live in
    different tables, so they are queried separately and merged by (date,
    channel). Returns {date: {seo_visits, dma_visits, ..., seo_omzet, ...}}.
    """
    if not dates:
        return {}
    keys = [_date_key(d) for d in dates]
    placeholders = ",".join(["%s"] * len(keys))
    chan_placeholders = ",".join(["%s"] * len(CHANNELS))

    vis_sql = f"""
        SELECT fv.dim_date_key     AS d,
               c.marketing_channel AS chan,
               COUNT(*)            AS visits
        FROM datamart.fct_visits fv
        JOIN datamart.dim_visit dv  ON fv.dim_visit_key = dv.dim_visit_key
        JOIN chan_deriv.ref_channel_derivation_stats c
          ON dv.aff_id = c.aff_id AND dv.channel_id = c.channel_id
        WHERE fv.dim_date_key IN ({placeholders})
          AND c.marketing_channel IN ({chan_placeholders})
          AND dv.is_real_visit = 1
        GROUP BY 1, 2
    """
    rev_sql = f"""
        SELECT tac.date            AS d,
               c.marketing_channel AS chan,
               SUM(tac.click_revenue) AS omzet
        FROM bt.cpa_outclicks_transactional tac
        JOIN chan_deriv.ref_channel_derivation_stats c
          ON tac.aff_id = c.aff_id AND tac.channel_id = c.channel_id
         AND c.deleted_ind = 0
        WHERE tac.date BETWEEN %s AND %s
          AND c.marketing_channel IN ({chan_placeholders})
          AND {REV_TABLE_FILTERS}
        GROUP BY 1, 2
    """
    with conn.cursor() as cur:
        cur.execute(vis_sql, keys + list(CHANNELS.keys()))
        vis_rows = cur.fetchall()
        cur.execute(rev_sql, [dates[0], dates[-1]] + list(CHANNELS.keys()))
        rev_rows = cur.fetchall()

    # Seed every date with zeros so the chart has a continuous series.
    out: Dict[date, Dict] = {}
    for d in dates:
        out[d] = {f"{k}_visits": 0 for k in CHANNELS.values()}
        out[d].update({f"{k}_omzet": 0.0 for k in CHANNELS.values()})

    for r in vis_rows:
        ds = str(r["d"])
        if len(ds) != 8 or not ds.isdigit():
            continue  # skip a malformed/NULL dim_date_key rather than 500 the request
        d = datetime.strptime(ds, "%Y%m%d").date()
        key = CHANNELS.get(r["chan"])
        if not key or d not in out:
            continue
        out[d][f"{key}_visits"] = int(r["visits"] or 0)

    for r in rev_rows:
        d = r["d"]
        if isinstance(d, datetime):  # normalize a DATE/TIMESTAMP column to date
            d = d.date()
        key = CHANNELS.get(r["chan"])
        if not key or d not in out:
            continue
        out[d][f"{key}_omzet"] = float(r["omzet"] or 0.0)
    return out


def _fetch_channel_deltas(conn, vis_p1: date, vis_p2: date,
                          rev_p1: date, rev_p2: date) -> List[Dict]:
    """Per-channel visits (vis dates) and revenue (rev dates) for the delta cards."""
    vp1, vp2 = _date_key(vis_p1), _date_key(vis_p2)
    chan_placeholders = ",".join(["%s"] * len(CHANNELS))
    vis_sql = f"""
        SELECT c.marketing_channel AS chan,
               SUM(CASE WHEN fv.dim_date_key = %s THEN 1 ELSE 0 END) AS vis_p1,
               SUM(CASE WHEN fv.dim_date_key = %s THEN 1 ELSE 0 END) AS vis_p2
        FROM datamart.fct_visits fv
        JOIN datamart.dim_visit dv ON fv.dim_visit_key = dv.dim_visit_key
        JOIN chan_deriv.ref_channel_derivation_stats c
          ON dv.aff_id = c.aff_id AND dv.channel_id = c.channel_id
        WHERE fv.dim_date_key IN (%s, %s)
          AND c.marketing_channel IN ({chan_placeholders})
          AND dv.is_real_visit = 1
        GROUP BY 1
    """
    rev_sql = f"""
        SELECT c.marketing_channel AS chan,
               SUM(CASE WHEN tac.date = %s THEN tac.click_revenue ELSE 0 END) AS rev_p1,
               SUM(CASE WHEN tac.date = %s THEN tac.click_revenue ELSE 0 END) AS rev_p2
        FROM bt.cpa_outclicks_transactional tac
        JOIN chan_deriv.ref_channel_derivation_stats c
          ON tac.aff_id = c.aff_id AND tac.channel_id = c.channel_id
         AND c.deleted_ind = 0
        WHERE tac.date IN (%s, %s)
          AND c.marketing_channel IN ({chan_placeholders})
          AND {REV_TABLE_FILTERS}
        GROUP BY 1
    """
    with conn.cursor() as cur:
        cur.execute(vis_sql, [vp1, vp2, vp1, vp2] + list(CHANNELS.keys()))
        vis_by_chan = {r["chan"]: r for r in cur.fetchall()}
        cur.execute(rev_sql, [rev_p1, rev_p2, rev_p1, rev_p2] + list(CHANNELS.keys()))
        rev_by_chan = {r["chan"]: r for r in cur.fetchall()}

    result: List[Dict] = []
    for chan_name, key in CHANNELS.items():
        rv = vis_by_chan.get(chan_name)
        rr = rev_by_chan.get(chan_name)
        v1 = int(rv["vis_p1"] or 0) if rv else 0
        v2 = int(rv["vis_p2"] or 0) if rv else 0
        m1 = float(rr["rev_p1"] or 0.0) if rr else 0.0
        m2 = float(rr["rev_p2"] or 0.0) if rr else 0.0
        result.append({
            "channel": key,
            "label": CHANNEL_LABELS[key],
            "visits_p1": v1, "visits_p2": v2,
            "visits_delta": v2 - v1, "visits_pct": _pct_delta(v1, v2),
            "revenue_p1": m1, "revenue_p2": m2,
            "revenue_delta": m2 - m1, "revenue_pct": _pct_delta(m1, m2),
        })
    return result


def _fetch_cat_deltas(conn, level: str, vis_p1: date, vis_p2: date,
                      rev_p1: date, rev_p2: date) -> Dict[str, List[Dict]]:
    """Top maincats or subcats (SEO channel) by most-positive delta.
    `level` is 'main', 'sub' or 'deepest'. Returns {'by_visits': [...], 'by_revenue': [...]}.
    """
    if level not in ("main", "sub", "deepest"):
        raise ValueError(f"invalid level: {level!r}")
    vp1, vp2 = _date_key(vis_p1), _date_key(vis_p2)

    # Category name columns (identical on both grains — both resolve via
    # datamart.dim_category, so the (maincat, cat) tuple is a stable merge key).
    if level == "deepest":
        select_cols = ("COALESCE(cat.main_category_name,'-') AS maincat, "
                       "COALESCE(cat.deepest_category_name,'-') AS cat")
        group_by = "1, 2"
    elif level == "sub":
        select_cols = ("COALESCE(cat.main_category_name,'-') AS maincat, "
                       "COALESCE(cat.sub_category_name,'-')  AS cat")
        group_by = "1, 2"
    else:
        select_cols = ("COALESCE(cat.main_category_name,'-') AS maincat, "
                       "NULL::varchar AS cat")
        group_by = "1"

    # The deepest level lists only true leaf categories (is_lowest_category=1) on
    # the visits side. Without this, visits landing on a non-leaf subcategory
    # overview page (e.g. "Zwembaden", which has child categories) would show up
    # as their own row and outrank real leaves like "Parasols". The revenue side
    # joins on deepest_category_id, which is already a leaf, so no extra clause.
    lowest_clause = "AND cat.is_lowest_category = 1" if level == "deepest" else ""

    vis_sql = f"""
        SELECT {select_cols},
               SUM(CASE WHEN fv.dim_date_key = %s THEN 1 ELSE 0 END) AS vis_p1,
               SUM(CASE WHEN fv.dim_date_key = %s THEN 1 ELSE 0 END) AS vis_p2
        FROM datamart.fct_visits fv
        JOIN datamart.dim_visit dv ON fv.dim_visit_key = dv.dim_visit_key
        JOIN chan_deriv.ref_channel_derivation_stats c
          ON dv.aff_id = c.aff_id AND dv.channel_id = c.channel_id
        JOIN datamart.dim_category cat ON fv.dim_category_key = cat.dim_category_key
        WHERE fv.dim_date_key IN (%s, %s)
          AND c.marketing_channel = 'SEO'
          AND dv.is_real_visit = 1
          {lowest_clause}
        GROUP BY {group_by}
    """
    rev_sql = f"""
        SELECT {select_cols},
               SUM(CASE WHEN tac.date = %s THEN tac.click_revenue ELSE 0 END) AS rev_p1,
               SUM(CASE WHEN tac.date = %s THEN tac.click_revenue ELSE 0 END) AS rev_p2
        FROM bt.cpa_outclicks_transactional tac
        JOIN chan_deriv.ref_channel_derivation_stats c
          ON tac.aff_id = c.aff_id AND tac.channel_id = c.channel_id
         AND c.deleted_ind = 0
        JOIN datamart.dim_category cat
          ON tac.deepest_category_id = cat.deepest_category_id AND cat.deleted_ind = 0
        WHERE tac.date IN (%s, %s)
          AND c.marketing_channel = 'SEO'
          AND {REV_TABLE_FILTERS}
        GROUP BY {group_by}
    """
    with conn.cursor() as cur:
        cur.execute(vis_sql, (vp1, vp2, vp1, vp2))
        vis_raw = [dict(r) for r in cur.fetchall()]
        cur.execute(rev_sql, (rev_p1, rev_p2, rev_p1, rev_p2))
        rev_raw = [dict(r) for r in cur.fetchall()]

    # Merge the two grains on the (maincat, cat) tuple. A category may appear on
    # only one side (visits but no conversions, or vice versa) — union the keys.
    def _blank(maincat, cat):
        return {"maincat": maincat, "cat": cat,
                "vis_p1": 0, "vis_p2": 0, "rev_p1": 0.0, "rev_p2": 0.0}

    merged: Dict[Tuple, Dict] = {}
    for r in vis_raw:
        k = (r["maincat"], r.get("cat"))
        m = merged.setdefault(k, _blank(*k))
        m["vis_p1"] += int(r["vis_p1"] or 0)
        m["vis_p2"] += int(r["vis_p2"] or 0)
    for r in rev_raw:
        k = (r["maincat"], r.get("cat"))
        m = merged.setdefault(k, _blank(*k))
        m["rev_p1"] += float(r["rev_p1"] or 0.0)
        m["rev_p2"] += float(r["rev_p2"] or 0.0)

    rows: List[Dict] = []
    for m in merged.values():
        v1, v2 = m["vis_p1"], m["vis_p2"]
        m1, m2 = m["rev_p1"], m["rev_p2"]
        rows.append({
            "maincat": m["maincat"],
            "subcat": m["cat"],
            "visits_p1": v1, "visits_p2": v2,
            "visits_delta": v2 - v1, "visits_pct": _pct_delta(v1, v2),
            "revenue_p1": m1, "revenue_p2": m2,
            "revenue_delta": m2 - m1, "revenue_pct": _pct_delta(m1, m2),
        })

    out = {
        "by_visits": sorted(rows, key=lambda x: x["visits_delta"], reverse=True)[:TOP_N],
        "by_revenue": sorted(rows, key=lambda x: x["revenue_delta"], reverse=True)[:TOP_N],
    }
    # Only the deepest level renders the "declining" (worst) lists; skip the two
    # extra full sorts for main/sub where they're never read.
    if level == "deepest":
        out["worst_by_visits"] = sorted(rows, key=lambda x: x["visits_delta"])[:TOP_N]
        out["worst_by_revenue"] = sorted(rows, key=lambda x: x["revenue_delta"])[:TOP_N]
    return out


# ---------------------------------------------------------------------------
# Public entry points (called from the router via a thread executor)
# ---------------------------------------------------------------------------

def get_daily(start_date: Optional[str], end_date: Optional[str],
              force: bool = False) -> Dict:
    today = date.today()
    end = _parse_date(end_date, today - timedelta(days=1))
    start = _parse_date(start_date, end - timedelta(days=29))
    if start > end:
        start, end = end, start

    cache_key = f"daily:{start.isoformat()}:{end.isoformat()}"
    if not force:
        cached = _cache_get(cache_key)
        if cached:
            return cached

    dates = []
    d = start
    while d <= end:
        dates.append(d)
        d += timedelta(days=1)

    conn = get_redshift_connection()
    try:
        daily = _fetch_daily(conn, dates)
    finally:
        return_redshift_connection(conn)

    result = {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "channels": [
            {"key": k, "label": CHANNEL_LABELS[k]} for k in CHANNELS.values()
        ],
        "daily": [
            dict(date=d.isoformat(), **daily[d]) for d in dates
        ],
        "generated_at": datetime.now().isoformat(),
    }
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Per-date notes (Postgres-backed, shared across users)
# ---------------------------------------------------------------------------

_NOTES_TABLE_READY = False


def _init_notes_table() -> None:
    global _NOTES_TABLE_READY
    if _NOTES_TABLE_READY:
        return
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pa.seo_stats_notes (
                note_date  DATE PRIMARY KEY,
                note       TEXT,
                color      VARCHAR(16),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Add color to pre-existing tables created before this column existed.
        cur.execute("ALTER TABLE pa.seo_stats_notes ADD COLUMN IF NOT EXISTS color VARCHAR(16)")
        conn.commit()
        _NOTES_TABLE_READY = True
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        return_db_connection(conn)


def get_notes(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict:
    """Return {date_iso: note} for a date range (or all notes if no range)."""
    _init_notes_table()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if start_date and end_date:
            cur.execute(
                "SELECT note_date, note, color FROM pa.seo_stats_notes "
                "WHERE note_date BETWEEN %s AND %s ORDER BY note_date",
                (start_date, end_date),
            )
        else:
            cur.execute("SELECT note_date, note, color FROM pa.seo_stats_notes ORDER BY note_date")
        return {"notes": {r["note_date"].isoformat(): {"note": r["note"], "color": r["color"]}
                          for r in cur.fetchall()}}
    finally:
        cur.close()
        return_db_connection(conn)


def set_note(note_date: str, note: str, color: Optional[str] = None) -> Dict:
    """Upsert a note (+color) for a date; an empty note deletes the row."""
    _init_notes_table()
    note = (note or "").strip()
    color = (color or "").strip() or None
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if note:
            cur.execute(
                """
                INSERT INTO pa.seo_stats_notes (note_date, note, color, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (note_date)
                DO UPDATE SET note = EXCLUDED.note, color = EXCLUDED.color,
                              updated_at = CURRENT_TIMESTAMP
                """,
                (note_date, note, color),
            )
        else:
            cur.execute("DELETE FROM pa.seo_stats_notes WHERE note_date = %s", (note_date,))
        conn.commit()
        return {"status": "ok", "date": str(note_date), "note": note, "color": color}
    finally:
        cur.close()
        return_db_connection(conn)


def get_deltas(ref_date: Optional[str] = None, force: bool = False) -> Dict:
    """Channel %-deltas + top maincats/subcats, anchored on `ref_date`
    (default = yesterday). Visits compare ref vs ref-7; revenue ref-1 vs ref-8.
    """
    ref = _parse_date(ref_date, date.today() - timedelta(days=1))

    vis_p2 = ref
    vis_p1 = ref - timedelta(days=7)
    rev_p2 = ref - timedelta(days=1)
    rev_p1 = ref - timedelta(days=8)

    cache_key = f"deltas:{ref.isoformat()}"
    if not force:
        cached = _cache_get(cache_key)
        if cached:
            return cached

    conn = get_redshift_connection()
    try:
        channels = _fetch_channel_deltas(conn, vis_p1, vis_p2, rev_p1, rev_p2)
        maincats = _fetch_cat_deltas(conn, "main", vis_p1, vis_p2, rev_p1, rev_p2)
        subcats = _fetch_cat_deltas(conn, "sub", vis_p1, vis_p2, rev_p1, rev_p2)
        deepestcats = _fetch_cat_deltas(conn, "deepest", vis_p1, vis_p2, rev_p1, rev_p2)
    finally:
        return_redshift_connection(conn)

    result = {
        "comparison": {
            "visits_p1": vis_p1.isoformat(), "visits_p2": vis_p2.isoformat(),
            "revenue_p1": rev_p1.isoformat(), "revenue_p2": rev_p2.isoformat(),
        },
        "channels": channels,
        "maincats": maincats,
        "subcats": subcats,
        "deepestcats": deepestcats,
        "generated_at": datetime.now().isoformat(),
    }
    _cache_set(cache_key, result)
    return result
