"""
SEO Priority Service

Pipeline:
  1. Pull 2y of /c/ visits + revenue from Redshift.
  2. Fan out each URL's visits/revenue across its facets to build
     (deepest_cat_id, facet_slug) aggregates.
  3. Look up current seoPriority via taxv2 CategoryFacetSettings
     (cached per category).
  4. Apply ON/OFF thresholds → propose action + reason per row.
  5. Persist to pa.seo_prio_runs / pa.seo_prio_results, expose Excel export.

Long-running. Started in a background thread; status polled by the frontend.
"""
import io
import re
import threading
import traceback
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

from backend.database import (
    get_db_connection, return_db_connection,
    get_redshift_connection, return_redshift_connection,
)

TAXV2_BASE = "http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl"
TAXV2_HEADERS = {"X-User-Name": "SEO_JOEP", "Accept": "application/json"}

# In-process state for active runs: run_id -> dict with progress fields
_RUNS: Dict[str, Dict] = {}
_RUNS_LOCK = threading.Lock()

# Default thresholds
DEFAULT_THRESHOLDS = {
    "on_min_visits_pct": 10.0,     # facet share of category visits
    "on_min_revenue_pct": 10.0,    # facet share of category revenue
    "on_min_abs_visits": 50,       # absolute visit floor before flipping ON
    "off_max_visits_pct": 2.0,
    "off_max_revenue_pct": 2.0,
}


# ───────────────────────────── DB schema ─────────────────────────────

def init_seo_prio_tables() -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pa.seo_prio_runs (
                run_id          VARCHAR(64) PRIMARY KEY,
                status          VARCHAR(32) NOT NULL,
                started_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                finished_at     TIMESTAMP,
                params          JSONB,
                progress        INTEGER DEFAULT 0,
                progress_total  INTEGER DEFAULT 0,
                progress_msg    TEXT,
                error           TEXT,
                row_count       INTEGER DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pa.seo_prio_results (
                run_id              VARCHAR(64) NOT NULL,
                main_cat_name       TEXT,
                deepest_cat_name    TEXT,
                deepest_cat_id      VARCHAR(32),
                facet_slug          VARCHAR(255),
                facet_id            VARCHAR(32),
                facet_name          TEXT,
                facet_url_example   TEXT,
                total_visits        BIGINT,
                total_revenue       NUMERIC(18,4),
                url_count           INTEGER,
                pct_visits_in_cat   NUMERIC(8,4),
                pct_revenue_in_cat  NUMERIC(8,4),
                current_seo_prio    VARCHAR(16),
                proposed_seo_prio   VARCHAR(16),
                action              VARCHAR(16),
                reason              TEXT
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS seo_prio_results_run_idx ON pa.seo_prio_results (run_id)")
        conn.commit()
        print("[SEO_PRIO] Tables initialized")
    finally:
        cur.close()
        return_db_connection(conn)


# ───────────────────────────── URL parsing ─────────────────────────────

# Subcat slug like "klussen_486172_574375" → deepest id is the LAST numeric chunk
_SLUG_ID_RE = re.compile(r"_(\d+)(?=_|$)")


def parse_url(url: str) -> Optional[Tuple[str, List[Tuple[str, str]]]]:
    """
    Returns (deepest_cat_id, [(facet_slug, facet_value_id), ...]) or None.

    /products/<root>/<slug>_<...>_<deepest_cat_id>/c/<f1>~<v1>~~<f2>~<v2>...
    """
    try:
        path = url.split("beslist.nl", 1)[1] if "beslist.nl" in url else url
    except Exception:
        return None

    if "/c/" not in path:
        return None

    head, _, facet_part = path.partition("/c/")
    if not facet_part:
        return None

    # head = "/products/<root>/<subcat-slug>"
    parts = [p for p in head.split("/") if p]
    if len(parts) < 3:
        return None
    subcat_slug = parts[-1]
    ids = _SLUG_ID_RE.findall(subcat_slug)
    if not ids:
        return None
    deepest_cat_id = ids[-1]

    facets: List[Tuple[str, str]] = []
    for chunk in facet_part.split("~~"):
        chunk = chunk.strip("/")
        if not chunk:
            continue
        slug, _, val = chunk.partition("~")
        if slug and val:
            facets.append((slug, val.split("/")[0]))

    if not facets:
        return None
    return deepest_cat_id, facets


# ───────────────────────────── Taxv2 helpers ─────────────────────────────

class TaxonomyClient:
    """Cached lookups against taxv2. One instance per run."""

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update(TAXV2_HEADERS)
        # cat_id -> {slug -> {id, name}}  (linked facets)
        self._cat_facets: Dict[str, Dict[str, Dict]] = {}
        # cat_id -> {facet_id -> seoPriority(bool|None)}  (explicit settings)
        self._cat_facet_settings: Dict[str, Dict[str, Optional[bool]]] = {}

    def _get_cat_facets(self, cat_id: str) -> Dict[str, Dict]:
        if cat_id in self._cat_facets:
            return self._cat_facets[cat_id]
        try:
            r = self._session.get(
                f"{TAXV2_BASE}/api/CategoryFacets",
                params={"categoryId": cat_id, "locale": "nl-NL"},
                timeout=20,
            )
            if r.status_code != 200:
                self._cat_facets[cat_id] = {}
                return {}
            data = r.json()
            mapping: Dict[str, Dict] = {}
            for cf in data if isinstance(data, list) else data.get("items", []):
                facet = cf.get("facet") or cf
                slug = (facet.get("urlSlug") or "").lower()
                fid = facet.get("id")
                labels = facet.get("labels") or []
                nl = next((l for l in labels if l.get("locale") == "nl-NL"), {})
                name = nl.get("name") or facet.get("name") or slug
                if slug and fid is not None:
                    mapping[slug] = {"id": str(fid), "name": name}
            self._cat_facets[cat_id] = mapping
            return mapping
        except Exception as e:
            print(f"[SEO_PRIO] CategoryFacets lookup failed for {cat_id}: {e}")
            self._cat_facets[cat_id] = {}
            return {}

    def _get_cat_facet_settings(self, cat_id: str) -> Dict[str, Optional[bool]]:
        if cat_id in self._cat_facet_settings:
            return self._cat_facet_settings[cat_id]
        try:
            r = self._session.get(
                f"{TAXV2_BASE}/api/CategoryFacetSettings",
                params={"categoryId": cat_id},
                timeout=20,
            )
            if r.status_code != 200:
                self._cat_facet_settings[cat_id] = {}
                return {}
            data = r.json()
            items = data if isinstance(data, list) else data.get("items", [])
            mapping: Dict[str, Optional[bool]] = {}
            for s in items:
                fid = s.get("facetId") or s.get("FacetId")
                if fid is None:
                    continue
                mapping[str(fid)] = s.get("seoPriority")
            self._cat_facet_settings[cat_id] = mapping
            return mapping
        except Exception as e:
            print(f"[SEO_PRIO] CategoryFacetSettings lookup failed for {cat_id}: {e}")
            self._cat_facet_settings[cat_id] = {}
            return {}

    def resolve(self, cat_id: str, facet_slug: str) -> Tuple[Optional[str], Optional[str], Optional[bool]]:
        """Return (facet_id, facet_name, current_seoPriority)."""
        facets = self._get_cat_facets(cat_id)
        info = facets.get(facet_slug.lower())
        if not info:
            return None, None, None
        fid = info["id"]
        prio = self._get_cat_facet_settings(cat_id).get(fid)
        return fid, info["name"], prio


# ───────────────────────────── Run management ─────────────────────────────

def _set_status(run_id: str, **fields):
    with _RUNS_LOCK:
        run = _RUNS.setdefault(run_id, {})
        run.update(fields)
    # Persist to DB best-effort
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        sets, params = [], []
        for k, v in fields.items():
            if k in ("status", "progress", "progress_total", "progress_msg",
                     "error", "row_count", "finished_at"):
                sets.append(f"{k} = %s")
                params.append(v)
        if sets:
            params.append(run_id)
            cur.execute(
                f"UPDATE pa.seo_prio_runs SET {', '.join(sets)} WHERE run_id = %s",
                params,
            )
            conn.commit()
        cur.close()
        return_db_connection(conn)
    except Exception as e:
        print(f"[SEO_PRIO] status persist failed: {e}")


def start_run(params: Dict) -> str:
    run_id = uuid.uuid4().hex[:16]
    # Insert run row
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        import json as _json
        cur.execute(
            """INSERT INTO pa.seo_prio_runs (run_id, status, params, progress_msg)
               VALUES (%s, 'queued', %s::jsonb, 'queued')""",
            (run_id, _json.dumps(params)),
        )
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)

    with _RUNS_LOCK:
        _RUNS[run_id] = {"status": "queued", "stop": False, "params": params}

    threading.Thread(
        target=_run_pipeline, args=(run_id, params), daemon=True
    ).start()
    return run_id


def stop_run(run_id: str) -> bool:
    with _RUNS_LOCK:
        run = _RUNS.get(run_id)
        if not run:
            return False
        run["stop"] = True
    return True


def _should_stop(run_id: str) -> bool:
    with _RUNS_LOCK:
        return _RUNS.get(run_id, {}).get("stop", False)


def get_run_status(run_id: str) -> Optional[Dict]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT run_id, status, started_at, finished_at, progress,
                      progress_total, progress_msg, error, row_count, params
               FROM pa.seo_prio_runs WHERE run_id = %s""",
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        cur.close()
        return_db_connection(conn)


def get_run_results(run_id: str, limit: int = 0, offset: int = 0) -> Dict:
    """All results for a run (limit=0 = no cap). Sort/filter/paginate happens client-side."""
    cols = [c for c, _ in EXCEL_COLUMNS]
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COUNT(*) AS c FROM pa.seo_prio_results WHERE run_id = %s",
            (run_id,),
        )
        total = cur.fetchone()["c"]
        sql = f"""SELECT {", ".join(cols)} FROM pa.seo_prio_results
                  WHERE run_id = %s
                  ORDER BY total_visits DESC"""
        params = [run_id]
        if limit and limit > 0:
            sql += " LIMIT %s OFFSET %s"
            params += [limit, offset]
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        return {"total": total, "limit": limit, "offset": offset, "rows": rows}
    finally:
        cur.close()
        return_db_connection(conn)


def get_run_summary(run_id: str) -> Dict:
    """Counts of proposed actions for a completed run."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT action, COUNT(*) AS c
                 FROM pa.seo_prio_results
                WHERE run_id = %s
             GROUP BY action""",
            (run_id,),
        )
        counts = {r["action"]: r["c"] for r in cur.fetchall()}
        return {
            "total":    sum(counts.values()),
            "turn_on":  counts.get("turn_on", 0),
            "turn_off": counts.get("turn_off", 0),
            "keep":     counts.get("keep", 0),
        }
    finally:
        cur.close()
        return_db_connection(conn)


def list_runs(limit: int = 50) -> List[Dict]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT run_id, status, started_at, finished_at, row_count,
                      progress_msg, params
               FROM pa.seo_prio_runs
               ORDER BY started_at DESC LIMIT %s""",
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


# ───────────────────────────── Pipeline ─────────────────────────────

def _fetch_redshift_rows(start_date: str, end_date: str) -> List[Dict]:
    """Run the SEO-prio Redshift query (mirrors query.txt)."""
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                dv.main_cat_name,
                dv.deepest_subcat_name,
                SPLIT_PART(dv.url, '?', 1) AS url,
                COUNT(*) AS visits,
                COALESCE(SUM(fcv.cpc_revenue), 0) + COALESCE(SUM(fcv.ww_revenue), 0) AS revenue
            FROM datamart.fct_visits fcv
            JOIN datamart.dim_visit dv
              ON fcv.dim_visit_key = dv.dim_visit_key
            JOIN datamart.dim_date dat
              ON fcv.dim_date_key = dat.dim_date_key
            JOIN chan_deriv.ref_channel_derivation_stats chan
              ON dv.aff_id = chan.aff_id AND dv.channel_id = chan.channel_id
            WHERE dv.is_real_visit = 1
              AND fcv.dim_date_key BETWEEN %s AND %s
              AND dv.url LIKE '%%beslist.nl%%'
              AND dv.url NOT LIKE '%%/r/%%'
              AND dv.url NOT LIKE '%%/p/%%'
              AND dv.url     LIKE '%%/c/%%'
              AND dv.url NOT LIKE '%%/l/%%'
              AND dv.url NOT LIKE '%%/page_%%'
              AND dv.url NOT LIKE '%%#%%'
              AND dv.deepest_subcat_name IS NOT NULL
              AND dv.main_cat_name IS NOT NULL
            GROUP BY 1, 2, 3
            """,
            (int(start_date), int(end_date)),
        )
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        return_redshift_connection(conn)


def _decide(row: Dict, t: Dict) -> Tuple[str, str, str]:
    """Return (proposed_seo_prio, action, reason)."""
    cur_raw = row["current_seo_prio"]  # bool|None
    cur_on = bool(cur_raw) is True
    visits_pct = row["pct_visits_in_cat"]
    revenue_pct = row["pct_revenue_in_cat"]
    visits = row["total_visits"]
    url_count = row["url_count"]

    # Should it be ON?
    qualifies_on = (
        visits_pct >= t["on_min_visits_pct"]
        and revenue_pct >= t["on_min_revenue_pct"]
        and visits >= t["on_min_abs_visits"]
    )
    qualifies_off = (
        visits_pct < t["off_max_visits_pct"]
        and revenue_pct < t["off_max_revenue_pct"]
    )

    if qualifies_on and not cur_on:
        return ("1", "turn_on",
                f"{visits_pct:.1f}% of category visits, "
                f"{revenue_pct:.1f}% of revenue, "
                f"{visits:,} visits across {url_count} URLs — currently "
                f"{'inherit' if cur_raw is None else 'OFF'}.")
    if qualifies_off and cur_on:
        return ("0", "turn_off",
                f"only {visits_pct:.2f}% visits / {revenue_pct:.2f}% revenue "
                f"in category, currently ON.")
    # Keep
    keep_val = "1" if cur_on else ("0" if cur_raw is False else "inherit")
    return (keep_val, "keep",
            f"{visits_pct:.2f}% visits / {revenue_pct:.2f}% revenue, no flip.")


def _run_pipeline(run_id: str, params: Dict) -> None:
    try:
        _set_status(run_id, status="running", progress=0,
                    progress_msg="fetching from Redshift")

        start_date = params["start_date"]
        end_date = params["end_date"]
        thresholds = {**DEFAULT_THRESHOLDS, **(params.get("thresholds") or {})}

        rows = _fetch_redshift_rows(start_date, end_date)
        if _should_stop(run_id):
            _set_status(run_id, status="stopped", progress_msg="stopped after Redshift")
            return
        _set_status(run_id, progress_msg=f"parsing {len(rows):,} URL rows", progress_total=len(rows))

        # ── Parse + fan-out aggregate ──────────────────────────────────────
        # key = (deepest_cat_id, facet_slug)
        agg: Dict[Tuple[str, str], Dict] = {}
        # For % within category: cat totals across URLs (counted ONCE per URL).
        cat_totals: Dict[str, Dict] = {}

        for i, r in enumerate(rows):
            if i % 5000 == 0 and _should_stop(run_id):
                _set_status(run_id, status="stopped", progress_msg="stopped during parse")
                return
            url = r["url"]
            visits = int(r["visits"] or 0)
            revenue = float(r["revenue"] or 0)
            parsed = parse_url(url)
            if not parsed:
                continue
            cat_id, facets = parsed

            ct = cat_totals.setdefault(cat_id, {
                "main_cat_name": r["main_cat_name"],
                "deepest_cat_name": r["deepest_subcat_name"],
                "visits": 0, "revenue": 0.0, "urls": 0,
            })
            ct["visits"] += visits
            ct["revenue"] += revenue
            ct["urls"] += 1

            seen_slugs = set()
            for slug, _vid in facets:
                slug_l = slug.lower()
                if slug_l in seen_slugs:
                    continue  # don't double-count if URL has the slug twice
                seen_slugs.add(slug_l)
                key = (cat_id, slug_l)
                a = agg.setdefault(key, {
                    "main_cat_name": r["main_cat_name"],
                    "deepest_cat_name": r["deepest_subcat_name"],
                    "deepest_cat_id": cat_id,
                    "facet_slug": slug_l,
                    "visits": 0, "revenue": 0.0, "url_count": 0,
                    "facet_url_example": url,
                })
                a["visits"] += visits
                a["revenue"] += revenue
                a["url_count"] += 1
            if i % 1000 == 0:
                _set_status(run_id, progress=i)

        _set_status(run_id, progress=0,
                    progress_msg=f"resolving taxv2 for {len(agg):,} combos",
                    progress_total=len(agg))

        # ── taxv2 lookup + decision ────────────────────────────────────────
        tax = TaxonomyClient()
        out_rows: List[Dict] = []
        for i, ((cat_id, slug), a) in enumerate(agg.items()):
            if i % 200 == 0:
                if _should_stop(run_id):
                    _set_status(run_id, status="stopped", progress_msg="stopped during taxv2")
                    return
                _set_status(run_id, progress=i)

            fid, fname, cur_prio = tax.resolve(cat_id, slug)
            ct = cat_totals.get(cat_id, {"visits": 0, "revenue": 0.0})
            v_total = ct["visits"] or 0
            r_total = ct["revenue"] or 0.0
            pct_v = (a["visits"] / v_total * 100.0) if v_total else 0.0
            pct_r = (a["revenue"] / r_total * 100.0) if r_total else 0.0

            row = {
                "main_cat_name": a["main_cat_name"],
                "deepest_cat_name": a["deepest_cat_name"],
                "deepest_cat_id": cat_id,
                "facet_slug": slug,
                "facet_id": fid,
                "facet_name": fname or slug,
                "facet_url_example": a["facet_url_example"],
                "total_visits": a["visits"],
                "total_revenue": round(a["revenue"], 4),
                "url_count": a["url_count"],
                "pct_visits_in_cat": round(pct_v, 4),
                "pct_revenue_in_cat": round(pct_r, 4),
                "current_seo_prio": (
                    "ON" if cur_prio is True else
                    "OFF" if cur_prio is False else
                    "inherit"
                ),
            }
            proposed, action, reason = _decide(row, thresholds)
            row["proposed_seo_prio"] = proposed
            row["action"] = action
            row["reason"] = reason
            out_rows.append(row)

        # ── Persist ───────────────────────────────────────────────────────
        _set_status(run_id, progress_msg="saving results")
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM pa.seo_prio_results WHERE run_id = %s", (run_id,))
            for r in out_rows:
                cur.execute(
                    """INSERT INTO pa.seo_prio_results
                       (run_id, main_cat_name, deepest_cat_name, deepest_cat_id,
                        facet_slug, facet_id, facet_name, facet_url_example,
                        total_visits, total_revenue, url_count,
                        pct_visits_in_cat, pct_revenue_in_cat,
                        current_seo_prio, proposed_seo_prio, action, reason)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (run_id, r["main_cat_name"], r["deepest_cat_name"], r["deepest_cat_id"],
                     r["facet_slug"], r["facet_id"], r["facet_name"], r["facet_url_example"],
                     r["total_visits"], r["total_revenue"], r["url_count"],
                     r["pct_visits_in_cat"], r["pct_revenue_in_cat"],
                     r["current_seo_prio"], r["proposed_seo_prio"], r["action"], r["reason"]),
                )
            conn.commit()
        finally:
            cur.close()
            return_db_connection(conn)

        _set_status(run_id, status="completed",
                    finished_at=datetime.utcnow(),
                    progress=len(agg), row_count=len(out_rows),
                    progress_msg=f"done — {len(out_rows):,} combos")
    except Exception as e:
        print(f"[SEO_PRIO] run {run_id} failed: {e}")
        traceback.print_exc()
        _set_status(run_id, status="failed", error=str(e),
                    finished_at=datetime.utcnow())


# ───────────────────────────── Excel export ─────────────────────────────

EXCEL_COLUMNS = [
    ("main_cat_name",       "Main category"),
    ("deepest_cat_name",    "Deepest category"),
    ("deepest_cat_id",      "Cat ID"),
    ("facet_slug",          "Facet slug"),
    ("facet_id",            "Facet ID"),
    ("facet_name",          "Facet name"),
    ("facet_url_example",   "Example URL"),
    ("total_visits",        "Total visits"),
    ("total_revenue",       "Total revenue"),
    ("url_count",           "URLs"),
    ("pct_visits_in_cat",   "% visits in cat"),
    ("pct_revenue_in_cat",  "% revenue in cat"),
    ("current_seo_prio",    "Current seoPriority"),
    ("proposed_seo_prio",   "Proposed seoPriority"),
    ("action",              "Action"),
    ("reason",              "Reason"),
]


def export_excel(run_id: str) -> bytes:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cols_sql = ", ".join(c for c, _ in EXCEL_COLUMNS)
        cur.execute(
            f"""SELECT {cols_sql} FROM pa.seo_prio_results
                WHERE run_id = %s
                ORDER BY total_visits DESC""",
            (run_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)

    df = pd.DataFrame(rows, columns=[c for c, _ in EXCEL_COLUMNS])
    df.columns = [label for _, label in EXCEL_COLUMNS]
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="seo_prio", index=False)
    return buf.getvalue()


# ───────────────────────────── Helpers for UI ─────────────────────────────

def default_date_range() -> Tuple[str, str]:
    today = datetime.utcnow().date()
    end = today - timedelta(days=1)
    start = end - timedelta(days=365 * 2)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
