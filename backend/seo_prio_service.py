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
  6. Push a hand-picked subset back to taxv2 (CategoryFacetSettings.seoPriority).

Long-running. Started in a background thread; status polled by the frontend.
"""
import io
import json
import os
import re
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

from backend import beslist_rate_limit as rate_limit
from backend import taxv2_client as taxv2
from backend.database import (
    get_db_connection, return_db_connection,
    get_redshift_connection, return_redshift_connection,
)

# Gedeelde client, zie backend/taxv2_client.py: één base-URL, één headerset en
# retry op 502/503/504. De retry geldt alleen voor GET — een PUT opnieuw sturen is
# hier niet veilig, want de API kent geen idempotency-key.
TAXV2_BASE = taxv2.BASE
TAXV2_HEADERS = taxv2.headers()

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
    "off_min_abs_visits": 25,      # onder dit volume is "weinig aandeel" ruis, geen signaal
    "off_min_cat_visits": 250,     # en een categorie zelf moet genoeg volume hebben
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
                deepest_cat_id      TEXT,
                deepest_cat_slug    TEXT,
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
        # Columns added after the table shipped — hence ALTERs. deepest_cat_id
        # holds "slug:<urlslug>" when a URL has no taxv2 category, which does
        # not fit the original VARCHAR(32).
        for ddl in (
            "ALTER TABLE pa.seo_prio_results ALTER COLUMN deepest_cat_id TYPE TEXT",
            "ALTER TABLE pa.seo_prio_results ADD COLUMN IF NOT EXISTS deepest_cat_slug TEXT",
            "ALTER TABLE pa.seo_prio_results ADD COLUMN IF NOT EXISTS applied_status VARCHAR(16)",
            "ALTER TABLE pa.seo_prio_results ADD COLUMN IF NOT EXISTS applied_value  VARCHAR(16)",
            "ALTER TABLE pa.seo_prio_results ADD COLUMN IF NOT EXISTS applied_at     TIMESTAMP",
            "ALTER TABLE pa.seo_prio_results ADD COLUMN IF NOT EXISTS applied_error  TEXT",
        ):
            cur.execute(ddl)
        # Audit trail of every push to taxv2. Survives a run being deleted, which
        # is the point: "what did I change in the taxonomy, and when" must not
        # disappear with the analysis that suggested it.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pa.seo_prio_apply_log (
                id            BIGSERIAL PRIMARY KEY,
                run_id        VARCHAR(64),
                category_id   VARCHAR(32),
                category_name TEXT,
                facet_id      VARCHAR(32),
                facet_slug    VARCHAR(255),
                facet_name    TEXT,
                old_value     VARCHAR(16),
                new_value     VARCHAR(16),
                status        VARCHAR(16),
                error         TEXT,
                dry_run       BOOLEAN DEFAULT FALSE,
                applied_by    VARCHAR(64),
                applied_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS seo_prio_apply_log_run_idx ON pa.seo_prio_apply_log (run_id)")
        conn.commit()
        print("[SEO_PRIO] Tables initialized")
    finally:
        cur.close()
        return_db_connection(conn)


# ───────────────────────────── URL parsing ─────────────────────────────

def parse_url(url: str) -> Optional[Tuple[str, List[Tuple[str, str]]]]:
    """
    Returns (deepest_cat_slug, [(facet_slug, facet_value_id), ...]) or None.

    /products/<root>/<subcat-slug>/c/<f1>~<v1>~~<f2>~<v2>...

    The WHOLE subcat slug is the key, not the number at the end of it. That
    trailing number ("tuin_accessoires_504077_5335060" → 5335060) is a legacy
    PDM id which taxv2 does not know: `GET /api/Categories/5335060` 404s, so
    every facet lookup keyed on it came back empty and every row got a NULL
    facet_id and a fake "inherit". The taxv2 id for that slug is a different
    number entirely, and the only mapping between the two is the category's own
    nl-NL urlSlug — see _cat_id_for_slug().
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
    subcat_slug = parts[-1].strip().lower()
    if not subcat_slug:
        return None

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
    return subcat_slug, facets


# ────────────────── URL slug → taxv2 category id ──────────────────
# cat_urls.csv is written by category_lookup.py's taxonomy walk (slug, cat_id
# straight off each category's nl-NL label), so this is a cache of the API and
# not a hand-made list. Loaded per run: 3.5k rows is nothing, and a run that
# starts after a walk should see the walk's output.

_CAT_URLS_CSV = os.path.join(os.path.dirname(__file__), "data", "cat_urls.csv")


def load_cat_id_map() -> Dict[str, str]:
    """{url slug -> taxv2 category id}. Empty dict if the file is unreadable."""
    import csv
    out: Dict[str, str] = {}
    try:
        with open(_CAT_URLS_CSV, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f, delimiter=";"):
                slug = (row.get("url_name") or "").strip().strip("/").lower()
                cid = (row.get("cat_id") or "").strip()
                if slug and cid.isdigit():
                    out[slug] = cid
    except Exception as e:
        print(f"[SEO_PRIO] could not read {_CAT_URLS_CSV}: {e}")
    return out


# ───────────────────────────── Taxv2 helpers ─────────────────────────────

class TaxonomyClient:
    """Cached lookups against taxv2. One instance per run."""

    def __init__(self):
        self._session = taxv2.session()
        # cat_id -> {slug -> {id, name}}  (linked facets)
        self._cat_facets: Dict[str, Dict[str, Dict]] = {}
        # cat_id -> {facet_id -> seoPriority(bool|None)}  (explicit settings)
        self._cat_facet_settings: Dict[str, Dict[str, Optional[bool]]] = {}
        # facet slug -> [{id, name}, ...]  (global search, for hidden facets)
        self._slug_search: Dict[str, List[Dict]] = {}

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
                fid = cf.get("facetId", facet.get("id"))
                # urlSlug is NOT a top-level facet field — it lives per locale
                # inside labels[]. Reading facet["urlSlug"] returned None for
                # every facet, which is how the whole mapping came out empty.
                labels = facet.get("labels") or []
                nl = next((l for l in labels if l.get("locale") == "nl-NL"), {})
                slug = (nl.get("urlSlug") or "").strip().lower()
                name = nl.get("name") or ""
                if not slug:
                    # A facet without an nl-NL label still has an id worth
                    # finding; take the first locale that carries a slug.
                    alt = next((l for l in labels if l.get("urlSlug")), {})
                    slug = (alt.get("urlSlug") or "").strip().lower()
                    name = name or alt.get("name") or slug
                if slug and fid is not None:
                    # isEnabled is the MASTER kill switch on the facet itself.
                    # It rides along in this payload for free — carry it, so
                    # resolve() never has to make a second call to find out the
                    # facet is dead everywhere.
                    mapping[slug] = {"id": str(fid), "name": name or slug,
                                     "enabled": facet.get("isEnabled") is not False}
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

    def _global_facets_by_slug(self, slug: str) -> List[Dict]:
        """Facets anywhere in taxv2 whose nl-NL urlSlug is exactly `slug`."""
        if slug in self._slug_search:
            return self._slug_search[slug]
        out: List[Dict] = []
        try:
            r = self._session.get(
                f"{TAXV2_BASE}/api/Facets",
                params={"searchTerm": slug, "locale": "nl-NL"}, timeout=20,
            )
            if r.status_code == 200:
                data = r.json()
                for f in (data if isinstance(data, list) else data.get("items", [])):
                    nl = next((l for l in (f.get("labels") or [])
                               if l.get("locale") == "nl-NL"), {})
                    if (nl.get("urlSlug") or "").strip().lower() == slug and f.get("id") is not None:
                        out.append({"id": str(f["id"]), "name": nl.get("name") or slug,
                                    "enabled": f.get("isEnabled") is not False})
        except Exception as e:
            print(f"[SEO_PRIO] facet search failed for {slug!r}: {e}")
        self._slug_search[slug] = out
        return out

    def resolve(self, cat_id: str, facet_slug: str
                ) -> Tuple[Optional[str], Optional[str], Optional[bool], Optional[str]]:
        """Return (facet_id, facet_name, current_seoPriority, blocked_reason).

        blocked_reason is None when the facet is a legitimate seoPriority
        target. When it is set, the caller must NOT propose a flip: the facet
        resolved to a real id, but writing seoPriority on it cannot change
        anything a visitor sees.
        """
        slug = facet_slug.lower()
        settings = self._get_cat_facet_settings(cat_id)
        info = self._get_cat_facets(cat_id).get(slug)
        if info:
            return (info["id"], info["name"], settings.get(info["id"]),
                    None if info.get("enabled", True)
                    else f"facet '{slug}' has isEnabled=false in taxv2 "
                         f"(disabled globally) — seoPriority has no effect")

        # A HIDDEN facet is left out of /api/CategoryFacets but keeps its
        # settings row — s_dierenhuis on Insectenhotel is hidden there and still
        # carries seoPriority=true, so "not linked" would have been a lie. Fall
        # back to a global slug search, and only accept a candidate that this
        # category already has a settings row for: that row is the proof it is
        # the right facet for this category, which a bare slug match is not
        # (duplicate slugs across facets are a known trap).
        candidates = [c for c in self._global_facets_by_slug(slug) if c["id"] in settings]
        if len(candidates) == 1:
            c = candidates[0]
            # ...but a settings row is NOT proof the facet is alive. `winkel` on
            # Insectenhotel (9003879) has settings row 40901 left over from the
            # 2026-03-16 bulk seed while all 31 `winkel` facets are
            # isEnabled=false and linked to zero categories. Without this check
            # the fallback promotes that corpse to a writable candidate, and any
            # category where winkel URLs clear the ON thresholds proposes
            # turn_on for a facet that can never render.
            blocked = None if c.get("enabled", True) else (
                f"facet '{slug}' has isEnabled=false in taxv2 (disabled "
                f"globally, not linked to any category) — settings row is a "
                f"leftover, seoPriority has no effect")
            return c["id"], c["name"], settings.get(c["id"]), blocked
        return None, None, None, None


# ───────────────────────────── Run management ─────────────────────────────

def _set_status(run_id: str, **fields):
    with _RUNS_LOCK:
        run = _RUNS.setdefault(run_id, {})
        run.update(fields)
    # Persist to DB best-effort
    conn = cur = None
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
    except Exception as e:
        print(f"[SEO_PRIO] status persist failed: {e}")
    finally:
        # MOET een finally zijn. Deze functie vuurt elke 1000 geparste rijen en elke
        # 200 combo's; stond de release binnen de try, dan lekte elke mislukte UPDATE
        # een verbinding uit de ThreadedConnectionPool(maxconn=60) die ALLE
        # dashboardtools delen — één slechte run legde :8003 plat tot een herstart.
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            return_db_connection(conn)


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


def delete_run(run_id: str) -> bool:
    """Delete a run and its results. Returns False if the run is still active."""
    with _RUNS_LOCK:
        run = _RUNS.get(run_id)
        if run and run.get("status") in ("queued", "running"):
            return False
        _RUNS.pop(run_id, None)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM pa.seo_prio_results WHERE run_id = %s", (run_id,))
        cur.execute("DELETE FROM pa.seo_prio_runs WHERE run_id = %s", (run_id,))
        conn.commit()
        return True
    finally:
        cur.close()
        return_db_connection(conn)


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
    cols = RESULT_COLUMNS
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

def _fetch_redshift_rows(start_date: str, end_date: str,
                         maincat: Optional[str] = None,
                         deepest_cat: Optional[str] = None) -> List[Dict]:
    """Run the SEO-prio Redshift query (mirrors query.txt).

    maincat / deepest_cat narrow the run to one category. They match
    dv.main_cat_name / dv.deepest_subcat_name exactly, which is why the
    dropdowns are fed from get_categories() — the same two columns — rather than
    from the taxonomy API, whose labels do not always agree with Redshift's.
    """
    extra = ""
    args: List = [int(start_date), int(end_date)]
    if maincat:
        extra += "\n              AND dv.main_cat_name = %s"
        args.append(maincat)
    if deepest_cat:
        extra += "\n              AND dv.deepest_subcat_name = %s"
        args.append(deepest_cat)

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
              AND dv.main_cat_name IS NOT NULL""" + extra + """
            GROUP BY 1, 2, 3
            """,
            tuple(args),
        )
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        return_redshift_connection(conn)


# ---------------------------------------------------------------------------
# Category list for the run form's two dropdowns
# ---------------------------------------------------------------------------
# The DISTINCT scan over dim_visit takes ~23s, so it must NEVER run inline on a
# page load — same lesson as the taxonomy walk in category_lookup.py. First
# caller kicks off a background thread and gets {"loading": true}; the frontend
# polls until the pairs arrive. Cached in-process for a day, plus a JSON file so
# a backend restart comes back warm instead of paying the 23s again.
_CAT_CACHE: Dict[str, object] = {"pairs": [], "fetched_at": None}
_CAT_LOCK = threading.Lock()
_CAT_INFLIGHT = False
# A week, not a day: category and deepest-subcat names barely move, the scan is
# expensive and growing, and a stale cache is served immediately anyway (the
# refresh happens behind the user's back). Daily just meant re-paying for a list
# that had not changed.
_CAT_TTL_SECONDS = 7 * 24 * 3600
_CAT_CACHE_FILE = os.path.join(os.path.dirname(__file__), "data", "seo_prio_categories.json")


def _cat_cache_load_file() -> None:
    try:
        with open(_CAT_CACHE_FILE, "r", encoding="utf-8") as f:
            blob = json.load(f)
        pairs = [(p[0], p[1]) for p in blob.get("pairs") or []]
        if pairs:
            _CAT_CACHE["pairs"] = pairs
            _CAT_CACHE["fetched_at"] = blob.get("fetched_at")
            print(f"[SEO_PRIO] category cache loaded from disk ({len(pairs)} pairs)")
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[SEO_PRIO] could not read the category cache: {e}")


def _cat_cache_write_file() -> None:
    """Best-effort: the in-memory cache is already usable, so a write failure
    must not break the request that triggered the refresh."""
    try:
        os.makedirs(os.path.dirname(_CAT_CACHE_FILE), exist_ok=True)
        tmp = _CAT_CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"fetched_at": _CAT_CACHE["fetched_at"],
                       "pairs": [list(p) for p in _CAT_CACHE["pairs"]]}, f)
        os.replace(tmp, _CAT_CACHE_FILE)
    except Exception as e:
        print(f"[SEO_PRIO] could not write the category cache: {e}")


def _cat_cache_is_fresh() -> bool:
    at = _CAT_CACHE.get("fetched_at")
    if not _CAT_CACHE.get("pairs") or not at:
        return False
    try:
        age = (datetime.now() - datetime.fromisoformat(str(at))).total_seconds()
    except Exception:
        return False
    return age < _CAT_TTL_SECONDS


def _refresh_categories() -> None:
    global _CAT_INFLIGHT
    try:
        conn = get_redshift_connection()
        try:
            cur = conn.cursor()
            # Same NOT-NULL + /c/ predicates the run uses, so the dropdowns can
            # only ever offer a category the run can actually match. No date
            # bound: a category the run may cover must be offerable even if it
            # had no visits in the window the user happens to pick.
            cur.execute(
                """
                SELECT dv.main_cat_name, dv.deepest_subcat_name
                FROM datamart.dim_visit dv
                WHERE dv.deleted_ind = 0
                  AND dv.main_cat_name IS NOT NULL
                  AND dv.deepest_subcat_name IS NOT NULL
                  AND dv.url LIKE '%%beslist.nl%%'
                  AND dv.url LIKE '%%/c/%%'
                GROUP BY 1, 2
                """
            )
            pairs = sorted({(r["main_cat_name"], r["deepest_subcat_name"])
                            for r in cur.fetchall()})
        finally:
            return_redshift_connection(conn)
        with _CAT_LOCK:
            _CAT_CACHE["pairs"] = pairs
            _CAT_CACHE["fetched_at"] = datetime.now().isoformat(timespec="seconds")
        _cat_cache_write_file()
        print(f"[SEO_PRIO] category cache refreshed ({len(pairs)} pairs)")
    except Exception as e:
        print(f"[SEO_PRIO] category refresh failed: {e}")
    finally:
        with _CAT_LOCK:
            _CAT_INFLIGHT = False


def get_categories(force: bool = False) -> Dict:
    """{'maincats': [...], 'pairs': [[maincat, deepest], ...], 'loading': bool}.

    Returns immediately, always. A cold or stale cache is refreshed on a daemon
    thread while the caller is told `loading` so the UI can poll.
    """
    global _CAT_INFLIGHT
    if not _CAT_CACHE.get("pairs"):
        _cat_cache_load_file()

    stale = force or not _cat_cache_is_fresh()
    if stale:
        with _CAT_LOCK:
            if not _CAT_INFLIGHT:
                _CAT_INFLIGHT = True
                threading.Thread(target=_refresh_categories, daemon=True).start()

    with _CAT_LOCK:
        pairs = list(_CAT_CACHE.get("pairs") or [])
        fetched_at = _CAT_CACHE.get("fetched_at")
        inflight = _CAT_INFLIGHT
    return {
        "maincats": sorted({m for m, _ in pairs}),
        "pairs": [list(p) for p in pairs],
        "fetched_at": fetched_at,
        # Only "loading" when there is nothing to show yet; a stale-but-present
        # cache is served straight away and refreshed behind the user's back.
        "loading": bool(inflight and not pairs),
    }


# ───────────────── Category facets inspector (read-only) ─────────────────
# Los van de run: kies één categorie en zie welke facetten daar SEO-aan staan,
# en per facet welke facetwaarden aan staan. Geen Redshift, geen thresholds,
# geen schrijfactie — puur de taxonomie voorlezen.
#
# Drie dingen die de vorm van deze code bepalen:
#   1. Facetten van een categorie komen uit `GET /api/Categories/{id}` en NIET uit
#      `/api/CategoryFacets`: dat laatste laat facetten met
#      inheritanceStatus=Dependent stil weg (zie backend/taxv2_client.py — 25 tegen
#      243 facetten op categorie 9003374). Die call geeft ook de EFFECTIEVE
#      seoPriority mét inheritanceStatus, wat precies de vraag "staat dit facet
#      hier aan?" beantwoordt; CategoryFacetSettings geeft alleen de rauwe rij.
#   2. De categoriekiezer draait op TAXONOMIE-ids en niet op de Redshift-namen van
#      de run (`get_categories()`): een naam is geen sleutel in de taxonomie en de
#      twee bronnen zijn het niet altijd eens.
#   3. Facetwaarden worden pas opgehaald als je een facet openklapt. Merk heeft
#      ~12.000 waarden en de API kan niet op seoPriority filteren, dus dat is
#      pagineren en zelf zeven — te duur om voor elk facet vooraf te doen.

_TAX_CAT_TABLE = "pa.hs2_cat_maincat"     # gedeeld met Healthscore (zie healthscore_runs)
# !Overig is de restbak en geen categorie om facetten van te bekijken, dus hij staat
# in geen van de twee dropdowns (Joep, 2026-09-04). Op ID en niet op naam: een
# naamfilter breekt zodra iemand hem hernoemt, en filteren op de "!"-prefix zou een
# toekomstige maincat met datzelfde teken meenemen. Healthscore filtert hem op
# dezelfde id. In deze tabel is het één rij (cat 11111 = maincat 11111, geen kinderen),
# dus dit filter verbergt geen echte categorieën.
_HIDDEN_CAT_ID = 11111                    # !Overig
_TAX_CATS: Dict[str, object] = {"rows": None, "at": 0.0}
_TAX_CATS_TTL = 900                       # 15 min; de tabel verandert zelden


def list_tax_categories(force: bool = False) -> Dict:
    """{'rows': [{id, name, parent}], 'maincats': [{id, name}]} op taxonomie-ids.

    Uit `pa.hs2_cat_maincat`, de gesynchroniseerde taxonomie-map die Healthscore
    ook voor zijn kiezer gebruikt (~3.570 categorieën). Bewust dezelfde tabel:
    twee kiezers die verschillende categorielijsten tonen is erger dan een
    afhankelijkheid tussen twee tools.
    """
    now = time.time()
    if not force and _TAX_CATS["rows"] and now - float(_TAX_CATS["at"]) < _TAX_CATS_TTL:
        return dict(_TAX_CATS["rows"])          # type: ignore[arg-type]

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""SELECT cat AS id, cat_name AS name, maincat_name AS parent
                          FROM {_TAX_CAT_TABLE}
                         WHERE cat IS NOT NULL AND cat_name IS NOT NULL
                           AND cat <> {_HIDDEN_CAT_ID}
                         ORDER BY 2""")
        rows = [{"id": r["id"], "name": r["name"], "parent": r["parent"]}
                for r in cur.fetchall()]
        cur.execute(f"""SELECT DISTINCT maincat AS id, maincat_name AS name
                          FROM {_TAX_CAT_TABLE}
                         WHERE maincat IS NOT NULL AND maincat_name IS NOT NULL
                           AND maincat <> {_HIDDEN_CAT_ID}
                         ORDER BY 2""")
        maincats = [{"id": r["id"], "name": r["name"]} for r in cur.fetchall()]
        cur.close()
    finally:
        return_db_connection(conn)

    out = {"rows": rows, "maincats": maincats}
    _TAX_CATS["rows"], _TAX_CATS["at"] = out, now
    return dict(out)


def _nl_label(labels) -> Dict[str, str]:
    """De nl-NL-label van een categorie of facet, met een bruikbare terugval.

    `urlSlug` en `name` zitten NIET op het object zelf maar per locale in
    `labels[]` — de val die in _get_cat_facets() de hele mapping leeg maakte.
    """
    labels = labels or []
    nl = next((l for l in labels if l.get("locale") == "nl-NL"), None)
    if not nl:
        nl = next((l for l in labels if l.get("urlSlug") or l.get("name")), {})
    return {"name": (nl.get("name") or "").strip(),
            "slug": (nl.get("urlSlug") or "").strip().lower()}


# De zoek-API zegt welke facetten er voor DEZE categorie werkelijk zijn — en dat is
# een veel kortere lijst dan de taxonomie. Parfums: 9 facetten in de index tegen 46
# in de taxonomie. Het verschil zijn vooral dependent facetten die aan een merkwaarde
# hangen die hier niet voorkomt ("Productlijnen: Nintendo" onder Sneakers, Joep
# 2026-09-04). Zonder deze kruiscontrole staat die ruis in het overzicht.
#
# Dezelfde call geeft `isSeoFacet`: de vlag die de SITE leest voor de noscript-
# facetlinks. Die kan afwijken van seoPriority in de taxonomie (Parfums: Collectie
# staat op seoPriority=true en isSeoFacet=false), en juist dat verschil wil je zien.
_SEARCH_API = "https://productsearch-v2.api.beslist.nl/search/products"


def _search_facets(cat_slug: str) -> Dict:
    """Wat de zoekindex van deze categorie kent, zonder filter.

    {'ok', 'total', 'facets': {facet_id: {'is_seo_facet', 'url_name', 'value_ids',
    'seed'}}}. `limit=0` want we willen de facetlijst, geen producten.

    De facet-WAARDEN in dit antwoord zijn afgekapt (merk 100, maat 60, kleur 24, de
    rest 8), dus `value_ids` is een bovenlaag en geen volledige lijst — vandaar
    `_search_pool()` voor het geval een intersectie leeg blijft. `seed` is één geldige
    waarde-id van dat facet, precies wat zo'n vervolgcall nodig heeft.
    """
    out: Dict = {"ok": False, "total": None, "facets": {}}
    if not cat_slug:
        return out
    try:
        rate_limit.productsearch_bucket.acquire()
        r = requests.get(_SEARCH_API, params={
            "category": cat_slug, "countryLanguage": "nl-nl",
            "isBot": "false", "limit": 0}, timeout=30)
        if r.status_code != 200:
            print(f"[SEO_PRIO] search API {r.status_code} for {cat_slug!r}")
            return out
        data = r.json()
        out["facets"] = _index_search_facets(data)
        out["total"] = data.get("total")
        out["ok"] = True
    except Exception as e:
        print(f"[SEO_PRIO] search API failed for {cat_slug!r}: {e}")
    return out


def _index_search_facets(data: Dict) -> Dict[str, Dict]:
    idx: Dict[str, Dict] = {}
    for f in data.get("facets") or []:
        fid = f.get("id")
        if fid is None:
            continue
        vals = [v.get("id") for v in (f.get("values") or []) if v.get("id") is not None]
        idx[str(fid)] = {
            "is_seo_facet": bool(f.get("isSeoFacet")),
            "url_name": f.get("urlName") or "",
            "value_ids": set(vals),
            "seed": vals[0] if vals else None,
        }
    return idx


def _search_pool(cat_slug: str, url_name: str, seed) -> Dict[str, Dict]:
    """Facetten van deze categorie MET één waarde van `url_name` geselecteerd.

    Twee dingen die alleen zo boven water komen:
      * de VOLLEDIGE waardenlijst van dat facet (ongefilterd is hij afgekapt);
      * de dependent facetten die pas verschijnen als hun parent-waarde gekozen is —
        inclusief hun `isSeoFacet`.
    """
    if not (cat_slug and url_name and seed is not None):
        return {}
    try:
        rate_limit.productsearch_bucket.acquire()
        r = requests.get(_SEARCH_API, params={
            "category": cat_slug, f"filters[{url_name}][0]": seed,
            "countryLanguage": "nl-nl", "isBot": "false", "limit": 0}, timeout=30)
        if r.status_code != 200:
            return {}
        return _index_search_facets(r.json())
    except Exception as e:
        print(f"[SEO_PRIO] search pool failed for {cat_slug!r}/{url_name!r}: {e}")
        return {}


def _dependent_presence(cat_slug: str, base: Dict[str, Dict], parent_fid,
                        parent_value_ids, pools: Dict[str, Dict]):
    """Is een dependent facet hier relevant? True / False / None (onbekend).

    Een dependent facet staat NIET in een ongefilterd antwoord: het verschijnt pas als
    een waarde van zijn parent-facet gekozen is. "Komt het facet in de index voor?"
    is er dus de verkeerde vraag; de juiste is of een van zijn PARENT-waarden hier
    voorkomt. Dat scheidt "Kleurtint blauw" onder Sneakers (blauw bestaat daar) van
    "Productlijnen: Nintendo" onder Sneakers (dat merk niet) — 87 van de 88
    productlijn-facetten op Sneakers hangen aan een merk dat er niet is.
    """
    if parent_fid is None or not parent_value_ids:
        return None
    p = base.get(str(parent_fid))
    if not p:
        # Het parent-facet zelf zit hier niet in de index. Is dat parent op zijn beurt
        # dependent, dan weten we het niet; anders kan het kind er ook niet zijn.
        return None
    ids = set(parent_value_ids)
    if ids & p["value_ids"]:
        return True
    # Leeg kan ook betekenen dat de parent-waarde buiten de afgekapte lijst viel: één
    # vervolgcall per parent-facet geeft de volledige lijst.
    key = str(parent_fid)
    if key not in pools:
        pools[key] = _search_pool(cat_slug, p["url_name"], p["seed"])
    pool = pools[key].get(key)
    if not pool:
        return None
    return bool(ids & pool["value_ids"])


_ROOT_SLUGS: Dict[str, str] = {}       # parentId -> urlSlug van de root


def _root_slug(parent_id, own_slug: str) -> str:
    """De slug van de hoogste voorouder, voor het `/products/<root>/<cat>/`-pad.

    Een Beslist-categorie-URL heeft precies twee padsegmenten: de ROOT-slug en de
    slug van de categorie zelf (`parse_url()` hierboven rekent daar ook op). De
    tussenliggende niveaus komen er niet in voor, dus alleen de top is nodig.

    De klim kost 1-2 extra calls van 40-100 KB (0,15s) en die worden per
    parent-id gecached. Valt de klim om, dan is er nog de vorm van de slug zelf:
    die is `<root>_<id>[_<id>]`, dus de staart van `_<cijfers>` eraf geeft de
    root (geverifieerd op 12 willekeurige categorieën, 12/12 gelijk aan de klim).
    """
    cur, last = parent_id, None
    for _ in range(6):
        if cur is None:
            break
        key = str(cur)
        if key in _ROOT_SLUGS:
            last = _ROOT_SLUGS[key]
            break
        try:
            d = taxv2.get_json(f"/api/Categories/{key}", timeout=60)
        except Exception as e:
            print(f"[SEO_PRIO] root-slug climb stopped at {key}: {e}")
            break
        slug = _nl_label(d.get("labels"))["slug"]
        if slug:
            last = slug
        cur = d.get("parentId")
        if cur is None and last:
            _ROOT_SLUGS[key] = last
    if last:
        return last
    return re.sub(r"(_\d+)+$", "", own_slug or "")


def category_facets(cat_id) -> Dict:
    """Alle facetten van één categorie met hun effectieve seoPriority.

    `on` in de rijen is `seoPriority is True`: dat is wat de taxonomie voor DEZE
    categorie zegt, inclusief overerving. Let op dat de site zelf `isSeoFacet`
    uit de zoekindex leest — die kan achterlopen op een verse taxonomie-wijziging.
    """
    cid = str(cat_id).strip()
    if not cid.isdigit():
        raise ValueError("category id must be numeric")

    r = taxv2.get(f"/api/Categories/{cid}", timeout=90)
    if r.status_code == 404:
        raise LookupError(f"category {cid} not found in taxv2")
    r.raise_for_status()
    data = r.json()

    cat = _nl_label(data.get("labels"))
    search = _search_facets(cat["slug"])
    # Gefilterde vervolgcalls, één per parent-facet, gedeeld over alle dependent
    # facetten die eraan hangen (Sneakers: 88 productlijn-facetten, één call voor merk).
    pools: Dict[str, Dict] = {}
    facets = []
    for f in data.get("facets") or []:
        fid = f.get("facetId")
        if fid is None:
            continue
        lab = _nl_label(f.get("labels"))
        dep = f.get("dependentMetadata") or None
        hit = search["facets"].get(str(fid))
        parent_fid = (dep or {}).get("parentFacetId")
        parent_vals = (dep or {}).get("parentFacetValueIds") or []
        if not search["ok"]:
            # Zonder antwoord van de zoek-API weten we niets: dan liever alles laten
            # zien dan stil de helft weglaten.
            present = None
        elif hit is not None:
            present = True
        elif parent_fid is not None:
            present = _dependent_presence(cat["slug"], search["facets"],
                                          parent_fid, parent_vals, pools)
        else:
            present = False
        # is_seo_facet van een dependent facet komt pas terug in een gefilterde call;
        # `pools` heeft die al gedaan voor de relevantietoets, dus lees hem daaruit.
        idx_hit = hit
        if idx_hit is None and parent_fid is not None:
            idx_hit = (pools.get(str(parent_fid)) or {}).get(str(fid))
        facets.append({
            "facet_id": fid,
            # `in_search` = heeft dit facet hier producten achter zich. Voor een
            # dependent facet is dat "komt een van zijn parent-waarden hier voor".
            "in_search": present,
            "is_seo_facet": idx_hit["is_seo_facet"] if idx_hit else None,
            "name": lab["name"] or lab["slug"] or str(fid),
            "slug": lab["slug"],
            "seo_priority": f.get("seoPriority"),
            "on": f.get("seoPriority") is True,
            "inheritance": f.get("inheritanceStatus"),
            "is_enabled": f.get("isEnabled") is not False,
            "is_hidden": bool(f.get("isHidden")),
            "no_index_no_follow": bool(f.get("noIndexNoFollow")),
            "seo_display_limit": f.get("seoDisplayLimit"),
            "display_order": f.get("displayOrder"),
            "is_top_facet": bool(f.get("isTopFacet")),
            # Een dependent facet linkt alleen op de pagina's waar de
            # parent-waarde gekozen is — zonder dat erbij leest "aan" te ruim.
            "parent_facet_id": (dep or {}).get("parentFacetId"),
            "parent_value_count": len((dep or {}).get("parentFacetValueIds") or []),
        })

    # Tweede pas voor de facetten die in de UI vooraan staan: van een dependent
    # facet dat AAN staat wil je ook weten of de site zijn waarden werkelijk linkt,
    # en die `isSeoFacet` komt alleen uit een gefilterde call. Alleen voor die
    # handvol rijen, en per parent-facet één keer.
    if search["ok"]:
        for f in facets:
            if not (f["on"] and f["in_search"] is not False
                    and f["is_seo_facet"] is None and f["parent_facet_id"] is not None):
                continue
            key = str(f["parent_facet_id"])
            if key not in pools:
                p = search["facets"].get(key)
                pools[key] = _search_pool(cat["slug"], p["url_name"], p["seed"]) if p else {}
            hit2 = pools[key].get(str(f["facet_id"]))
            if hit2:
                f["is_seo_facet"] = hit2["is_seo_facet"]

    # Relevant-en-aan eerst, dan de rest van de relevante, dan wat de index niet
    # kent — precies de volgorde waarin de UI ze standaard wel/niet laat zien.
    facets.sort(key=lambda x: (x["in_search"] is False, not x["on"],
                               (x["name"] or "").lower()))
    on_count = sum(1 for f in facets if f["on"])
    relevant = [f for f in facets if f["in_search"] is not False]
    on_relevant = sum(1 for f in relevant if f["on"])
    # Padprefix voor een live facet-URL: /products/<root>/<eigen slug>. De
    # facetwaarde-links in de UI hangen hier `/c/<facet-slug>~<value-id>` achter
    # (geen slash erachter, zie de canonicalisatieregel voor /c/-URLs).
    root = _root_slug(data.get("parentId"), cat["slug"])
    prefix = f"/products/{root}/{cat['slug']}" if root and cat["slug"] else None
    return {
        "category": {"id": int(cid), "name": cat["name"], "slug": cat["slug"],
                     "parent_id": data.get("parentId"),
                     "url_prefix": prefix,
                     "is_enabled": data.get("isEnabled") is not False},
        "facets": facets,
        "facet_count": len(facets),
        "on_count": on_count,
        "relevant_count": len(relevant),
        "on_relevant_count": on_relevant,
        "search_ok": search["ok"],
        "product_total": search["total"],
    }


_VAL_PAGE = 1000            # de API doet er ~0,25s over per 1.000
_VAL_MAX = 60000            # harde bovengrens, zodat één rare facet niets ophangt
_FACET_VALS: Dict[str, Dict] = {}
_FACET_VALS_TTL = 600


def facet_values(facet_id, only_on: bool = True) -> Dict:
    """Facetwaarden van één facet, standaard alleen die met seoPriority=true.

    De API kan niet op seoPriority filteren (`/api/Facets/{id}/values` neemt
    alleen searchTerm/skip/take), dus dit pagineert en zeeft zelf. Resultaat gaat
    10 minuten in een cache: doorklikken op dezelfde categorie zou anders elke
    keer opnieuw 12.000 waarden ophalen.
    """
    fid = str(facet_id).strip()
    if not fid.isdigit():
        raise ValueError("facet id must be numeric")

    key = f"{fid}|{int(bool(only_on))}"
    hit = _FACET_VALS.get(key)
    if hit and time.time() - hit["at"] < _FACET_VALS_TTL:
        return {k: v for k, v in hit.items() if k != "at"}

    values, total, skip, truncated = [], None, 0, False
    while True:
        r = taxv2.get(f"/api/Facets/{fid}/values",
                      params={"skip": skip, "take": _VAL_PAGE}, timeout=60)
        if r.status_code == 404:
            raise LookupError(f"facet {fid} not found in taxv2")
        r.raise_for_status()
        page = r.json()
        items = page.get("items") if isinstance(page, dict) else page
        items = items or []
        if total is None:
            total = page.get("total") if isinstance(page, dict) else None
        for v in items:
            if only_on and v.get("seoPriority") is not True:
                continue
            labels = v.get("labels") or []
            g = next((l for l in labels if l.get("locale") == "global"), None) or (labels[0] if labels else {})
            values.append({
                "id": v.get("id"),
                # nameInColumn is de naam die in de facetkolom staat; nameOnDetail
                # is de langere variant en is de terugval.
                "name": (g.get("nameInColumn") or g.get("nameOnDetail") or "").strip(),
                "sequence": v.get("sequence"),
                "seo_priority": v.get("seoPriority"),
                "invalid_reason": v.get("invalidReason"),
            })
        skip += len(items)
        if len(items) < _VAL_PAGE or (total is not None and skip >= total):
            break
        if skip >= _VAL_MAX:
            truncated = True
            break

    values.sort(key=lambda x: ((x["name"] or "").lower(), x["sequence"] or 0))
    out = {"facet_id": int(fid), "total_values": total if total is not None else skip,
           "scanned": skip, "on_count": len(values) if only_on else None,
           "values": values, "truncated": truncated, "only_on": bool(only_on)}
    _FACET_VALS[key] = {**out, "at": time.time()}
    return out


def _decide(row: Dict, t: Dict, cur_raw: Optional[bool]) -> Tuple[str, str, str]:
    """Return (proposed_seo_prio, action, reason).

    cur_raw is the RAW taxv2 value (True / False / None-for-inherit), passed in
    separately on purpose: row["current_seo_prio"] holds the display label, and
    the old `bool(cur_raw) is True` on that string made "inherit" and "OFF"
    both read as currently-ON — so no row could ever propose turn_on, and every
    quiet facet proposed turning OFF something that was never on.
    """
    cur_on = cur_raw is True
    visits_pct = row["pct_visits_in_cat"]
    revenue_pct = row["pct_revenue_in_cat"]   # None = categorie had geen omzet in het venster
    visits = row["total_visits"]
    url_count = row["url_count"]
    cat_visits = row.get("cat_total_visits") or 0

    # Een categorie zonder omzet in het venster geeft geen omzetsignaal, niet een
    # omzetsignaal van nul. Dan is er niets te beslissen — laat staan wat er staat.
    if revenue_pct is None:
        keep_val = "1" if cur_on else ("0" if cur_raw is False else "inherit")
        return (keep_val, "keep",
                f"{visits_pct:.2f}% visits; categorie had geen omzet in het venster, "
                f"dus geen omzetaandeel te bepalen — niet geflipt.")

    # Should it be ON?
    qualifies_on = (
        visits_pct >= t["on_min_visits_pct"]
        and revenue_pct >= t["on_min_revenue_pct"]
        and visits >= t["on_min_abs_visits"]
    )
    # Spiegel van de ON-kant: zonder absolute vloer stelde 1 visit in een categorie
    # van 60 al voor om productie-seoPriority uit te zetten.
    qualifies_off = (
        visits_pct < t["off_max_visits_pct"]
        and revenue_pct < t["off_max_revenue_pct"]
        and visits >= t.get("off_min_abs_visits", 0)
        and cat_visits >= t.get("off_min_cat_visits", 0)
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
                f"in category ({visits:,} visits of {cat_visits:,}), currently ON.")
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
        maincat = (params.get("maincat") or "").strip() or None
        deepest_cat = (params.get("deepest_cat") or "").strip() or None

        rows = _fetch_redshift_rows(start_date, end_date, maincat, deepest_cat)
        if _should_stop(run_id):
            _set_status(run_id, status="stopped", progress_msg="stopped after Redshift")
            return
        # A category filter that matches nothing is worth saying out loud rather
        # than finishing as a silent 0-row run.
        if not rows and (maincat or deepest_cat):
            scope = " / ".join(x for x in (maincat, deepest_cat) if x)
            _set_status(run_id, status="error", progress_msg=(
                f"no /c/ visits for {scope} between {start_date} and {end_date} "
                f"— check the category name and the date range"))
            return
        _set_status(run_id, progress_msg=f"parsing {len(rows):,} URL rows", progress_total=len(rows))

        cat_ids = load_cat_id_map()
        if not cat_ids:
            print("[SEO_PRIO] cat_urls.csv gave no slug→id mapping — "
                  "every row will come out unwritable")

        # ── Parse + fan-out aggregate ──────────────────────────────────────
        # key = (cat_key, facet_slug), where cat_key is the taxv2 category id
        # when the URL slug resolves and "slug:<urlslug>" when it does not.
        # Aggregating on the taxv2 id (not the slug) keeps two slugs that point
        # at one category from splitting into two half-counted rows.
        agg: Dict[Tuple[str, str], Dict] = {}
        # For % within category: cat totals across URLs (counted ONCE per URL).
        cat_totals: Dict[str, Dict] = {}
        unresolved_slugs = set()

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
            cat_slug, facets = parsed
            cat_id = cat_ids.get(cat_slug)
            if not cat_id:
                unresolved_slugs.add(cat_slug)
            cat_key = cat_id or f"slug:{cat_slug}"

            ct = cat_totals.setdefault(cat_key, {
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
                key = (cat_key, slug_l)
                a = agg.setdefault(key, {
                    "main_cat_name": r["main_cat_name"],
                    "deepest_cat_name": r["deepest_subcat_name"],
                    "deepest_cat_id": cat_id,
                    "deepest_cat_slug": cat_slug,
                    "facet_slug": slug_l,
                    "visits": 0, "revenue": 0.0, "url_count": 0,
                    "facet_url_example": url,
                })
                a["visits"] += visits
                a["revenue"] += revenue
                a["url_count"] += 1
            if i % 1000 == 0:
                _set_status(run_id, progress=i)

        if unresolved_slugs:
            print(f"[SEO_PRIO] {len(unresolved_slugs)} URL slug(s) have no taxv2 "
                  f"category id: {sorted(unresolved_slugs)[:5]}")

        _set_status(run_id, progress=0,
                    progress_msg=f"resolving taxv2 for {len(agg):,} combos",
                    progress_total=len(agg))

        # ── taxv2 lookup + decision ────────────────────────────────────────
        tax = TaxonomyClient()
        out_rows: List[Dict] = []
        for i, ((cat_key, slug), a) in enumerate(agg.items()):
            if i % 200 == 0:
                if _should_stop(run_id):
                    _set_status(run_id, status="stopped", progress_msg="stopped during taxv2")
                    return
                _set_status(run_id, progress=i)

            cat_id = a["deepest_cat_id"]
            if cat_id:
                fid, fname, cur_prio, blocked = tax.resolve(cat_id, slug)
            else:
                # No taxv2 id for this URL slug → nothing to read and nothing
                # that could be written. Say so in the row instead of dressing
                # an unknown up as "inherit".
                fid, fname, cur_prio, blocked = None, None, None, None
            ct = cat_totals.get(cat_key, {"visits": 0, "revenue": 0.0})
            v_total = ct["visits"] or 0
            r_total = ct["revenue"] or 0.0
            pct_v = (a["visits"] / v_total * 100.0) if v_total else 0.0
            # None, niet 0.0. Bij r_total == 0 is het aandeel ONBEPAALD, en 0.0
            # invullen liet het omzetbeen van qualifies_off altijd slagen: elk
            # facet dat ON stond en onder de visits-drempel zat kreeg turn_off
            # voorgesteld met "0.00% revenue in category" als bewijs — een
            # artefact van een lege noemer. Andersom kon qualifies_on daar nooit
            # vuren. _decide() behandelt None nu apart.
            pct_r = (a["revenue"] / r_total * 100.0) if r_total else None

            row = {
                "main_cat_name": a["main_cat_name"],
                "deepest_cat_name": a["deepest_cat_name"],
                "deepest_cat_id": cat_key,
                "deepest_cat_slug": a["deepest_cat_slug"],
                "facet_slug": slug,
                "facet_id": fid,
                "facet_name": fname or slug,
                "facet_url_example": a["facet_url_example"],
                "total_visits": a["visits"],
                "total_revenue": round(a["revenue"], 4),
                "url_count": a["url_count"],
                "pct_visits_in_cat": round(pct_v, 4),
                # None blijft None: de kolom is NULLable en de frontend's fmtPct()
                # rendert null al als lege cel. Een lege cel is eerlijker dan 0,00%.
                "pct_revenue_in_cat": (None if pct_r is None else round(pct_r, 4)),
                # Alleen voor _decide()'s absolute vloer; niet gepersisteerd.
                "cat_total_visits": v_total,
                "current_seo_prio": (
                    "ON" if cur_prio is True else
                    "OFF" if cur_prio is False else
                    "unknown" if not cat_id else
                    "inherit"
                ),
            }
            proposed, action, reason = _decide(row, thresholds, cur_prio)
            row["proposed_seo_prio"] = proposed
            row["action"] = action
            row["reason"] = reason
            if not cat_id:
                row["action"] = "keep"
                row["proposed_seo_prio"] = "unknown"
                row["reason"] = (f"no taxv2 category for URL slug "
                                 f"'{a['deepest_cat_slug']}' — cannot read or write seoPriority")
            elif fid is None:
                row["action"] = "keep"
                row["proposed_seo_prio"] = "unknown"
                row["reason"] = (f"facet '{slug}' is not linked to category {cat_id} "
                                 f"in taxv2 — cannot write seoPriority")
            elif blocked:
                # The facet resolved, but it is dead in taxv2. Flipping
                # seoPriority on it would be a no-op the run would still report
                # as an applied change, so refuse to propose one.
                row["action"] = "keep"
                row["proposed_seo_prio"] = "disabled"
                row["reason"] = blocked
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
                        deepest_cat_slug, facet_slug, facet_id, facet_name,
                        facet_url_example, total_visits, total_revenue, url_count,
                        pct_visits_in_cat, pct_revenue_in_cat,
                        current_seo_prio, proposed_seo_prio, action, reason)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (run_id, r["main_cat_name"], r["deepest_cat_name"], r["deepest_cat_id"],
                     r["deepest_cat_slug"], r["facet_slug"], r["facet_id"], r["facet_name"],
                     r["facet_url_example"], r["total_visits"], r["total_revenue"], r["url_count"],
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


# ────────────────── Apply back to taxv2 (CategoryFacetSettings) ──────────────────
#
# The write is `PUT /api/CategoryFacetSettings` — an upsert whose omitted fields
# are stored as null, i.e. "inherit". Sending only {categoryId, facetId,
# seoPriority} therefore WIPES displayOrder / isHidden / businessRelevance /
# describesVariance / describesCommon on any facet that had them. So every write
# here is read-merge-write: GET the category's settings, carry every existing
# field over, change seoPriority only. Same trap as the facet-value PUT.
#
# Nothing is ever written for a row the run did not propose a flip for, and the
# result is read back and compared before it is reported as applied.

APPLY_MAX_ROWS = 300          # one request; beyond this the HTTP call gets silly
APPLY_CATEGORY_WORKERS = 4    # parallel across categories, sequential within one
APPLY_USER = TAXV2_HEADERS["X-User-Name"]

# Everything the settings row can hold besides seoPriority. unitAmount is live
# in the API but absent from scripts/swagger_taxv2.json, so it is sent and the
# write retried without it if the server rejects the field — better than
# silently nulling a value the spec has not caught up with.
_SETTING_CARRY_FIELDS = (
    "isHidden", "displayOrder", "businessRelevance",
    "describesVariance", "describesCommon", "unitAmount",
)
_OPTIONAL_CARRY_FIELDS = ("unitAmount",)


def _prio_label(v: Optional[bool]) -> str:
    return "ON" if v is True else "OFF" if v is False else "inherit"


# facet_id -> isEnabled. Process-wide: the master flag is a property of the
# facet, not of a run, and Apply re-reads it for every selected row.
_FACET_ENABLED_CACHE: Dict[str, Optional[bool]] = {}
_FACET_ENABLED_LOCK = threading.Lock()


def _facet_is_enabled(session: requests.Session, facet_id: str) -> Optional[bool]:
    """Master isEnabled for a facet. None when taxv2 could not be asked —
    the caller treats that as 'do not block', so an API blip cannot silently
    turn Apply into a no-op."""
    fid = str(facet_id)
    with _FACET_ENABLED_LOCK:
        if fid in _FACET_ENABLED_CACHE:
            return _FACET_ENABLED_CACHE[fid]
    val: Optional[bool] = None
    try:
        r = session.get(f"{TAXV2_BASE}/api/Facets/{fid}", timeout=20)
        if r.status_code == 200:
            val = (r.json() or {}).get("isEnabled") is not False
        elif r.status_code == 404:
            val = False  # facet does not exist — writing to it is meaningless
    except Exception as e:
        print(f"[SEO_PRIO] isEnabled lookup failed for facet {fid}: {e}")
    with _FACET_ENABLED_LOCK:
        _FACET_ENABLED_CACHE[fid] = val
    return val


def _parse_target(value) -> Optional[bool]:
    """'1'/'ON'/True → True, '0'/'OFF'/False → False, anything else → None."""
    if isinstance(value, bool):
        return value
    s = str(value or "").strip().lower()
    if s in ("1", "on", "true", "yes"):
        return True
    if s in ("0", "off", "false", "no"):
        return False
    return None


def _settings_for_category(session: requests.Session, cat_id: str) -> Dict[str, Dict]:
    """{facet_id -> full settings row} for one category. Raises on a bad response,
    because writing without knowing the current values is the wipe scenario."""
    r = session.get(
        f"{TAXV2_BASE}/api/CategoryFacetSettings",
        params={"categoryId": cat_id}, timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"GET CategoryFacetSettings {cat_id} → HTTP {r.status_code}")
    data = r.json()
    items = data if isinstance(data, list) else data.get("items", [])
    out: Dict[str, Dict] = {}
    for s in items:
        fid = s.get("facetId", s.get("FacetId"))
        if fid is not None:
            out[str(fid)] = s
    return out


def _read_back(session: requests.Session, cat_id: str, facet_id: str) -> Optional[bool]:
    """seoPriority as taxv2 has it now. Returns None both for 'inherit' and for a
    response we could not read — the caller only trusts a True/False match."""
    try:
        r = session.get(
            f"{TAXV2_BASE}/api/CategoryFacetSettings/{cat_id}/{facet_id}", timeout=20,
        )
        if r.status_code != 200:
            return None
        return (r.json() or {}).get("seoPriority")
    except Exception:
        # "No setting found…" comes back as plain text, not JSON.
        return None


def _apply_one(session: requests.Session, settings: Dict[str, Dict],
               row: Dict, target: bool, dry_run: bool) -> Dict:
    """Write one facet's seoPriority. Returns a result dict; never raises."""
    cat_id = str(row["deepest_cat_id"])
    facet_id = str(row["facet_id"])
    existing = settings.get(facet_id) or {}
    old = existing.get("seoPriority")

    res = {
        "deepest_cat_id": cat_id,
        "facet_slug": row["facet_slug"],
        "facet_id": facet_id,
        "facet_name": row.get("facet_name"),
        "deepest_cat_name": row.get("deepest_cat_name"),
        "old_value": _prio_label(old),
        "new_value": _prio_label(target),
        "status": "failed",
        "error": None,
    }

    if old is target:
        res.update(status="skipped", error="already " + _prio_label(target) + " in taxv2")
        return res

    # Last line of defence, independent of what the run proposed. Rows persisted
    # before the resolve() guard existed still carry proposed_seo_prio='1' for
    # globally-disabled facets (every `winkel` facet, for one), and re-opening
    # such a run and hitting Apply must not write to them.
    if _facet_is_enabled(session, facet_id) is False:
        res.update(status="skipped",
                   error=f"facet {facet_id} has isEnabled=false in taxv2 — "
                         f"disabled globally, seoPriority would have no effect")
        return res

    try:
        body = {"categoryId": int(cat_id), "facetId": int(facet_id), "seoPriority": target}
    except (TypeError, ValueError):
        res["error"] = f"non-numeric ids (category {cat_id!r}, facet {facet_id!r})"
        return res
    # Carry every field the settings row already has, or the PUT nulls it.
    for f in _SETTING_CARRY_FIELDS:
        if existing.get(f) is not None:
            body[f] = existing[f]

    if dry_run:
        res.update(status="dry_run", error=None)
        res["body"] = body
        return res

    try:
        r = session.put(f"{TAXV2_BASE}/api/CategoryFacetSettings", json=body, timeout=30)
        if r.status_code == 400 and any(f in body for f in _OPTIONAL_CARRY_FIELDS):
            slim = {k: v for k, v in body.items() if k not in _OPTIONAL_CARRY_FIELDS}
            r = session.put(f"{TAXV2_BASE}/api/CategoryFacetSettings", json=slim, timeout=30)
        if r.status_code not in (200, 201, 204):
            res["error"] = f"PUT → HTTP {r.status_code}: {r.text[:200]}"
            return res
        confirmed = _read_back(session, cat_id, facet_id)
        if confirmed is not target:
            res["error"] = (f"PUT accepted but read-back says "
                            f"{_prio_label(confirmed)}, expected {_prio_label(target)}")
            return res
        res["status"] = "applied"
        return res
    except Exception as e:
        res["error"] = str(e)[:300]
        return res


def apply_to_taxonomy(run_id: str, selections: List[Dict], dry_run: bool = False) -> Dict:
    """Push the selected rows' proposed seoPriority to taxv2.

    selections: [{"deepest_cat_id": ..., "facet_slug": ..., "value": optional}].
    The value written comes from the stored row (or an explicit per-row override),
    never from a client-supplied "current" — the run's own proposal is the contract.
    """
    if not selections:
        return {"applied": 0, "failed": 0, "skipped": 0, "dry_run": dry_run, "results": []}
    if len(selections) > APPLY_MAX_ROWS:
        raise ValueError(f"Too many rows in one apply ({len(selections)}); "
                         f"max is {APPLY_MAX_ROWS}")

    # Dedupe on the run's own aggregation key, so the same facet cannot be
    # written twice in one call.
    wanted: Dict[Tuple[str, str], Optional[bool]] = {}
    for s in selections:
        cat = str(s.get("deepest_cat_id") or "").strip()
        slug = str(s.get("facet_slug") or "").strip().lower()
        if not cat or not slug:
            continue
        wanted[(cat, slug)] = _parse_target(s.get("value")) if s.get("value") is not None else None
    if not wanted:
        raise ValueError("No usable selections")

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT deepest_cat_id, facet_slug, facet_id, facet_name,
                      deepest_cat_name, main_cat_name, current_seo_prio,
                      proposed_seo_prio, action, applied_status
                 FROM pa.seo_prio_results
                WHERE run_id = %s
                  AND (deepest_cat_id, facet_slug) IN %s""",
            (run_id, tuple(wanted.keys())),
        )
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)

    found = {(str(r["deepest_cat_id"]), str(r["facet_slug"])) for r in rows}

    results: List[Dict] = []
    # Rows the client asked for but this run does not have.
    for cat, slug in wanted:
        if (cat, slug) not in found:
            results.append({
                "deepest_cat_id": cat, "facet_slug": slug, "facet_id": None,
                "facet_name": slug, "deepest_cat_name": None,
                "old_value": None, "new_value": None,
                "status": "skipped", "error": "not in this run",
            })

    # Group the writable rows per category: one settings GET per category, and
    # sequential writes within it so two facets of the same category never race.
    by_cat: Dict[str, List[Tuple[Dict, bool]]] = {}
    for r in rows:
        key = (str(r["deepest_cat_id"]), str(r["facet_slug"]))
        target = wanted.get(key)
        if target is None:
            target = _parse_target(r["proposed_seo_prio"])
        if target is None:
            results.append({**_stub(r), "status": "skipped",
                            "error": f"nothing to write (proposed = {r['proposed_seo_prio']})"})
            continue
        if not r.get("facet_id"):
            results.append({**_stub(r), "status": "skipped",
                            "error": "facet is not linked to this category in taxv2"})
            continue
        by_cat.setdefault(str(r["deepest_cat_id"]), []).append((r, target))

    def _do_category(cat_id: str, items: List[Tuple[Dict, bool]]) -> List[Dict]:
        session = requests.Session()
        session.headers.update(TAXV2_HEADERS)
        session.headers["Content-Type"] = "application/json"
        try:
            settings = _settings_for_category(session, cat_id)
        except Exception as e:
            # Could not read the current values → refuse to write, or we would be
            # guessing at the fields we are supposed to be preserving.
            return [{**_stub(r), "status": "failed",
                     "error": f"could not read current settings: {e}"} for r, _ in items]
        return [_apply_one(session, settings, r, target, dry_run) for r, target in items]

    if by_cat:
        with ThreadPoolExecutor(max_workers=APPLY_CATEGORY_WORKERS) as pool:
            for chunk in pool.map(lambda kv: _do_category(*kv), list(by_cat.items())):
                results.extend(chunk)

    # De taxonomie is op dit punt al gewijzigd. Kan het logboek niet weg, dan is dat
    # een zichtbaar probleem — geen reden om de uitkomst te verzwijgen, maar ook geen
    # reden om "alles ok" te melden.
    audit_log_error = None
    try:
        _persist_apply_results(run_id, results, dry_run)
    except Exception as e:  # noqa: BLE001
        audit_log_error = str(e)

    counts = {"applied": 0, "failed": 0, "skipped": 0, "dry_run_rows": 0}
    for r in results:
        if r["status"] == "applied":
            counts["applied"] += 1
        elif r["status"] == "failed":
            counts["failed"] += 1
        elif r["status"] == "dry_run":
            counts["dry_run_rows"] += 1
        else:
            counts["skipped"] += 1
    out = {**counts, "dry_run": dry_run, "results": results}
    if audit_log_error:
        out["audit_log_failed"] = True
        out["audit_log_error"] = audit_log_error
    return out


def _stub(row: Dict) -> Dict:
    return {
        "deepest_cat_id": str(row["deepest_cat_id"]),
        "facet_slug": row["facet_slug"],
        "facet_id": row.get("facet_id"),
        "facet_name": row.get("facet_name"),
        "deepest_cat_name": row.get("deepest_cat_name"),
        "old_value": row.get("current_seo_prio"),
        "new_value": ("ON" if row.get("proposed_seo_prio") == "1"
                      else "OFF" if row.get("proposed_seo_prio") == "0" else "inherit"),
        "error": None,
    }


def _persist_apply_results(run_id: str, results: List[Dict], dry_run: bool) -> None:
    """Stamp the result rows and append to the audit log. A dry run only logs."""
    if not results:
        return
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for r in results:
            cur.execute(
                """INSERT INTO pa.seo_prio_apply_log
                   (run_id, category_id, category_name, facet_id, facet_slug,
                    facet_name, old_value, new_value, status, error, dry_run, applied_by)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (run_id, r.get("deepest_cat_id"), r.get("deepest_cat_name"),
                 r.get("facet_id"), r.get("facet_slug"), r.get("facet_name"),
                 r.get("old_value"), r.get("new_value"), r.get("status"),
                 r.get("error"), dry_run, APPLY_USER),
            )
            if dry_run or r["status"] not in ("applied", "failed"):
                continue
            # A successful write makes the run's "current" stale — update it, so a
            # reload does not keep offering a flip that already happened.
            if r["status"] == "applied":
                cur.execute(
                    """UPDATE pa.seo_prio_results
                          SET applied_status = 'applied', applied_value = %s,
                              applied_at = CURRENT_TIMESTAMP, applied_error = NULL,
                              current_seo_prio = %s
                        WHERE run_id = %s AND deepest_cat_id = %s AND facet_slug = %s""",
                    (r["new_value"], r["new_value"], run_id,
                     r["deepest_cat_id"], r["facet_slug"]),
                )
            else:
                cur.execute(
                    """UPDATE pa.seo_prio_results
                          SET applied_status = 'failed', applied_value = %s,
                              applied_at = CURRENT_TIMESTAMP, applied_error = %s
                        WHERE run_id = %s AND deepest_cat_id = %s AND facet_slug = %s""",
                    (r.get("new_value"), r.get("error"), run_id,
                     r["deepest_cat_id"], r["facet_slug"]),
                )
        conn.commit()
    except Exception as e:
        # De rollback gooit ZOWEL de pa.seo_prio_apply_log-inserts als de
        # applied_status-stempels weg, terwijl de PUT's naar taxv2 al gebeurd zijn.
        # Dat mag geen stille print blijven: de aanroeper meldt anders "N applied"
        # terwijl het logboek dat de run expres moet overleven leeg is.
        conn.rollback()
        print(f"[SEO_PRIO] could not persist apply results: {e}")
        raise
    finally:
        cur.close()
        return_db_connection(conn)


def get_apply_log(run_id: Optional[str] = None, limit: int = 200) -> List[Dict]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        sql = """SELECT run_id, category_id, category_name, facet_id, facet_slug,
                        facet_name, old_value, new_value, status, error,
                        dry_run, applied_at
                   FROM pa.seo_prio_apply_log"""
        params: List = []
        if run_id:
            sql += " WHERE run_id = %s"
            params.append(run_id)
        sql += " ORDER BY applied_at DESC, id DESC LIMIT %s"
        params.append(limit)
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


# ───────────────────────────── Excel export ─────────────────────────────

EXCEL_COLUMNS = [
    ("main_cat_name",       "Main category"),
    ("deepest_cat_name",    "Deepest category"),
    ("deepest_cat_id",      "Cat ID"),
    ("deepest_cat_slug",    "Cat URL slug"),
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
    ("applied_status",      "Applied"),
    ("applied_at",          "Applied at"),
]

# What the results API hands the table: the export columns plus the two fields
# only the UI needs (the value written, and why a write failed).
RESULT_COLUMNS = [c for c, _ in EXCEL_COLUMNS] + ["applied_value", "applied_error"]


def export_excel(run_id) -> bytes:
    """One run's results, or several merged into one sheet.

    `run_id` takes a single id or a list of them. With more than one the sheet
    gets a leading `run` column, so a merged export still says which row came
    from where — the same rule the Auto-Redirects multi-run export follows.
    """
    run_ids = [run_id] if isinstance(run_id, str) else list(run_id)
    if not run_ids:
        raise ValueError("export_excel needs at least one run_id")
    multi = len(run_ids) > 1

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cols_sql = ", ".join(c for c, _ in EXCEL_COLUMNS)
        cur.execute(
            f"""SELECT run_id, {cols_sql} FROM pa.seo_prio_results
                WHERE run_id = ANY(%s)
                ORDER BY run_id, total_visits DESC""",
            (run_ids,),
        )
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)

    cols = [c for c, _ in EXCEL_COLUMNS]
    labels = [label for _, label in EXCEL_COLUMNS]
    if multi:
        cols = ["run_id"] + cols
        labels = ["run"] + labels
    df = pd.DataFrame(rows, columns=cols)
    df.columns = labels
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
