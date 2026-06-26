"""
SEO stats — read-only dashboard data for the "SEO stats" tool.

Serves the same numbers the Performance Standup writes to Excel, but as JSON
for a live web UI:
  - per-day visits + revenue for SEO / DMA organic / GSAAS  (chart + table)
  - channel %-deltas and top maincats/subcats with the most positive deltas

Comparison logic (matches performance_standup_service):
  - visits  : yesterday          vs the week before (yesterday - 7d)
  - revenue : day-2 (yesterday-1) vs day-9 (yesterday-8)
Revenue lags one extra day because it settles later than visit counts.

Category breakdowns use marketing_channel = 'SEO' (same as the standup
comparison sheets).
"""

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

# Revenue (cpc + ww) counted ONLY from visits that registered a product click.
# A visit carrying revenue but zero cpc/ww product clicks is a data glitch
# (e.g. Veiligheidshelmen: €217 cpc on a 0-click visit) and is excluded.
REV_EXPR = ("(CASE WHEN COALESCE(fv.number_of_cpc_productclicks,0)=0 "
            "AND COALESCE(fv.number_of_ww_productclicks,0)=0 "
            "THEN 0 ELSE fv.cpc_revenue + fv.ww_revenue END)")

# ---------------------------------------------------------------------------
# Tiny in-process TTL cache (Redshift is slow; the data is daily-grained)
# ---------------------------------------------------------------------------
_CACHE: Dict[str, Tuple[float, dict]] = {}
_CACHE_TTL = 300  # seconds


def _cache_get(key: str):
    hit = _CACHE.get(key)
    if hit and (time.time() - hit[0]) < _CACHE_TTL:
        return hit[1]
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
    """One pass over the date range: visits + revenue per (date, channel)
    for SEO / DMA organic / GSAAS. Returns {date: {seo_visits, dma_visits, ...}}.
    """
    if not dates:
        return {}
    keys = [_date_key(d) for d in dates]
    placeholders = ",".join(["%s"] * len(keys))
    chan_placeholders = ",".join(["%s"] * len(CHANNELS))
    sql = f"""
        SELECT fv.dim_date_key            AS d,
               c.marketing_channel        AS chan,
               COUNT(*)                   AS visits,
               SUM({REV_EXPR}) AS omzet
        FROM datamart.fct_visits fv
        JOIN datamart.dim_visit dv  ON fv.dim_visit_key = dv.dim_visit_key
        JOIN chan_deriv.ref_channel_derivation_stats c
          ON dv.aff_id = c.aff_id AND dv.channel_id = c.channel_id
        WHERE fv.dim_date_key IN ({placeholders})
          AND c.marketing_channel IN ({chan_placeholders})
          AND dv.is_real_visit = 1
        GROUP BY 1, 2
    """
    with conn.cursor() as cur:
        cur.execute(sql, keys + list(CHANNELS.keys()))
        rows = cur.fetchall()

    # Seed every date with zeros so the chart has a continuous series.
    out: Dict[date, Dict] = {}
    for d in dates:
        out[d] = {f"{k}_visits": 0 for k in CHANNELS.values()}
        out[d].update({f"{k}_omzet": 0.0 for k in CHANNELS.values()})

    for r in rows:
        d = datetime.strptime(str(r["d"]), "%Y%m%d").date()
        key = CHANNELS.get(r["chan"])
        if not key or d not in out:
            continue
        out[d][f"{key}_visits"] = int(r["visits"] or 0)
        out[d][f"{key}_omzet"] = float(r["omzet"] or 0.0)
    return out


def _fetch_channel_deltas(conn, vis_p1: date, vis_p2: date,
                          rev_p1: date, rev_p2: date) -> List[Dict]:
    """Per-channel visits (vis dates) and revenue (rev dates) for the delta cards."""
    vp1, vp2 = _date_key(vis_p1), _date_key(vis_p2)
    rp1, rp2 = _date_key(rev_p1), _date_key(rev_p2)
    chan_placeholders = ",".join(["%s"] * len(CHANNELS))
    sql = f"""
        SELECT c.marketing_channel AS chan,
               SUM(CASE WHEN fv.dim_date_key = %s THEN 1 ELSE 0 END) AS vis_p1,
               SUM(CASE WHEN fv.dim_date_key = %s THEN 1 ELSE 0 END) AS vis_p2,
               SUM(CASE WHEN fv.dim_date_key = %s THEN {REV_EXPR} ELSE 0 END) AS rev_p1,
               SUM(CASE WHEN fv.dim_date_key = %s THEN {REV_EXPR} ELSE 0 END) AS rev_p2
        FROM datamart.fct_visits fv
        JOIN datamart.dim_visit dv ON fv.dim_visit_key = dv.dim_visit_key
        JOIN chan_deriv.ref_channel_derivation_stats c
          ON dv.aff_id = c.aff_id AND dv.channel_id = c.channel_id
        WHERE fv.dim_date_key IN (%s, %s, %s, %s)
          AND c.marketing_channel IN ({chan_placeholders})
          AND dv.is_real_visit = 1
        GROUP BY 1
    """
    params = [vp1, vp2, rp1, rp2, vp1, vp2, rp1, rp2] + list(CHANNELS.keys())
    with conn.cursor() as cur:
        cur.execute(sql, params)
        by_chan = {r["chan"]: r for r in cur.fetchall()}

    result: List[Dict] = []
    for chan_name, key in CHANNELS.items():
        r = by_chan.get(chan_name)
        v1 = int(r["vis_p1"] or 0) if r else 0
        v2 = int(r["vis_p2"] or 0) if r else 0
        m1 = float(r["rev_p1"] or 0.0) if r else 0.0
        m2 = float(r["rev_p2"] or 0.0) if r else 0.0
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
    `level` is 'main' or 'sub'. Returns {'by_visits': [...], 'by_revenue': [...]}.
    """
    vp1, vp2 = _date_key(vis_p1), _date_key(vis_p2)
    rp1, rp2 = _date_key(rev_p1), _date_key(rev_p2)

    # The deepest level lists only true leaf categories (is_lowest_category=1).
    # Without this, visits that land on a non-leaf subcategory overview page
    # (e.g. "Zwembaden", which has child categories) would show up as their own
    # row and outrank real leaves like "Parasols".
    lowest_clause = ""
    if level == "deepest":
        select_cols = ("COALESCE(cat.main_category_name,'-') AS maincat, "
                       "COALESCE(cat.deepest_category_name,'-') AS cat")
        group_by = "1, 2"
        lowest_clause = "AND cat.is_lowest_category = 1"
    elif level == "sub":
        select_cols = ("COALESCE(cat.main_category_name,'-') AS maincat, "
                       "COALESCE(cat.sub_category_name,'-')  AS cat")
        group_by = "1, 2"
    else:
        select_cols = ("COALESCE(cat.main_category_name,'-') AS maincat, "
                       "NULL::varchar AS cat")
        group_by = "1"

    sql = f"""
        SELECT {select_cols},
               SUM(CASE WHEN fv.dim_date_key = %s THEN 1 ELSE 0 END) AS vis_p1,
               SUM(CASE WHEN fv.dim_date_key = %s THEN 1 ELSE 0 END) AS vis_p2,
               SUM(CASE WHEN fv.dim_date_key = %s THEN {REV_EXPR} ELSE 0 END) AS rev_p1,
               SUM(CASE WHEN fv.dim_date_key = %s THEN {REV_EXPR} ELSE 0 END) AS rev_p2
        FROM datamart.fct_visits fv
        JOIN datamart.dim_visit dv ON fv.dim_visit_key = dv.dim_visit_key
        JOIN chan_deriv.ref_channel_derivation_stats c
          ON dv.aff_id = c.aff_id AND dv.channel_id = c.channel_id
        JOIN datamart.dim_category cat ON fv.dim_category_key = cat.dim_category_key
        WHERE fv.dim_date_key IN (%s, %s, %s, %s)
          AND c.marketing_channel = 'SEO'
          AND dv.is_real_visit = 1
          {lowest_clause}
        GROUP BY {group_by}
    """
    params = (vp1, vp2, rp1, rp2, vp1, vp2, rp1, rp2)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        raw = [dict(r) for r in cur.fetchall()]

    rows: List[Dict] = []
    for r in raw:
        v1, v2 = int(r["vis_p1"] or 0), int(r["vis_p2"] or 0)
        m1, m2 = float(r["rev_p1"] or 0.0), float(r["rev_p2"] or 0.0)
        rows.append({
            "maincat": r["maincat"],
            "subcat": r.get("cat"),
            "visits_p1": v1, "visits_p2": v2,
            "visits_delta": v2 - v1, "visits_pct": _pct_delta(v1, v2),
            "revenue_p1": m1, "revenue_p2": m2,
            "revenue_delta": m2 - m1, "revenue_pct": _pct_delta(m1, m2),
        })

    by_visits = sorted(rows, key=lambda x: x["visits_delta"], reverse=True)[:TOP_N]
    by_revenue = sorted(rows, key=lambda x: x["revenue_delta"], reverse=True)[:TOP_N]
    worst_by_visits = sorted(rows, key=lambda x: x["visits_delta"])[:TOP_N]
    worst_by_revenue = sorted(rows, key=lambda x: x["revenue_delta"])[:TOP_N]
    return {
        "by_visits": by_visits, "by_revenue": by_revenue,
        "worst_by_visits": worst_by_visits, "worst_by_revenue": worst_by_revenue,
    }


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
