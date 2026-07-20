"""
SEO Titles Service
==================

Generates (cat_id, key) page-title BLUEPRINTS for the top SEO-visited faceted
`/c/` URLs that don't have a blueprint yet, and pushes them to the
website-configuration `/page-titles` API (upsert-per-record).

Flow (see /home/joepvanschagen/.claude/plans/proud-singing-lecun.md):
  1. Redshift: top-X SEO-visited faceted /c/ URLs (ordered by visits desc).
  2. parse_url -> (leaf_slug, {facet types}); slug -> cat_id via TaxonomyCache.
  3. canon_key = '~'.join(sorted(lower(types))).
  4. DEDUP: drop combos already in pa.page_titles_existing (the tblPageTitles
     export) or pa.seo_titles_blueprints (what this tool already built/pushed).
  5. For each NEW combo: build a deterministic placeholder blueprint AND (best
     effort) an AI unique title for the source URL (reused ai_titles_service).
  6. Publish: POST blueprints -> /page-titles; push per-URL AI titles via the
     existing unique_titles importer.

Blueprint templates are ported verbatim from
scripts/pagetitles_blueprint_from_urls.py so generated keys stay byte-identical
to the historical deliverable.
"""
import os
import time
import threading
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor

import requests

from backend.database import (
    get_db_connection, return_db_connection,
    get_redshift_connection, return_redshift_connection,
)

# ---------------------------------------------------------------------------
# API config
# ---------------------------------------------------------------------------
PAGE_TITLES_API = {
    "production": "https://website-configuration.api.beslist.nl/page-titles",
    "staging": "https://website-configuration-staging.api.beslist.nl/page-titles",
}
# Prod authenticates with UNIQUE_TITLES_API_KEY; staging with CONTENT_API_KEY_STAGING
# (verified live: prod key -> 401 on staging and vice-versa).
PAGE_TITLES_KEY = {
    "production": lambda: os.getenv("UNIQUE_TITLES_API_KEY", ""),
    "staging": lambda: os.getenv("CONTENT_API_KEY_STAGING", ""),
}
PUSH_BATCH = 5000

# ---------------------------------------------------------------------------
# Blueprint building (ported from scripts/pagetitles_blueprint_from_urls.py)
# ---------------------------------------------------------------------------
SUBCATEGORY_ORDER = 1700
SUBCATEGORY_PH = '!!sub_category!!'
UNKNOWN_ORDER = 1500
IGNORE_FACETS = {'pricemin', 'pricemax'}
COUNTRY_CODE = 'NL'
TAIL_TITLE = 'kopen? ✔️ Tot !!DISCOUNT!! korting! | beslist.nl'
# /page-titles rejects a title over this many characters (400 "too long").
MAX_TITLE_LEN = 200


def canon_key(s):
    """Canonical comparable form of a '~'-joined facet key: lowercase each type
    and re-sort. MUST match scripts/load_pagetitles_existing.py::canon_key."""
    return '~'.join(sorted(t for t in (s or '').lower().split('~') if t))


def parse_url(url):
    """url -> (leaf_slug, set_of_facet_types) or None when not a faceted /c/ url.
    Caller must lowercase the url first."""
    if '/c/' not in url:
        return None
    path, fstr = url.split('/c/', 1)
    segs = [s for s in path.split('/') if s]
    leaf = segs[-1] if segs else ''
    types = set()
    for pair in fstr.split('~~'):
        bits = pair.split('~')
        if len(bits) >= 2 and bits[0]:
            t = unquote(bits[0])
            if t not in IGNORE_FACETS:
                types.add(t)
    return leaf, types


def _resolve_cat(taxonomy_cache, leaf):
    """Leaf slug -> {'cat_id', 'cat_name'} or None. Tries the sub-category map
    first, then falls back to the maincat map (bare-maincat faceted pages, whose
    ids also appear in tblPageTitles)."""
    c = taxonomy_cache.get_category(leaf)
    if c:
        return {"cat_id": c['cat_id'], "cat_name": c.get('deepest_cat', '')}
    m = taxonomy_cache.get_maincat(leaf)
    if m:
        return {"cat_id": m['id'], "cat_name": m.get('name', '')}
    return None


def load_rules():
    """facet_slug -> (order_index, is_type_facet) from pa.facet_position_rules."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT facet_slug, order_index, is_type_facet FROM pa.facet_position_rules")
        rules = {}
        for row in cur.fetchall():
            slug = row['facet_slug']
            order = row['order_index']
            rules[slug] = (order if order is not None else UNKNOWN_ORDER, bool(row['is_type_facet']))
        return rules
    finally:
        cur.close()
        return_db_connection(conn)


def facet_phrase(types, rules):
    """Ordered placeholder phrase for a set of facet types. Inserts
    !!sub_category!! at SUBCATEGORY_ORDER when the set has no type-facet."""
    items = []  # (order, slug, placeholder)
    has_type = False
    for t in types:
        order, is_type = rules.get(t, (UNKNOWN_ORDER, False))
        if is_type:
            has_type = True
        items.append((order, t, f'!!{t}!!'))
    if not has_type:
        items.append((SUBCATEGORY_ORDER, '', SUBCATEGORY_PH))
    items.sort(key=lambda x: (x[0], x[1]))
    return ' '.join(ph for _, _, ph in items)


def _compose_title(phrase):
    """Assemble the page title from the (possibly trimmed) facet phrase.
    Skips an empty phrase so no double space slips in."""
    parts = ['!!current_query!!']
    if phrase:
        parts.append(phrase)
    parts.append(TAIL_TITLE)
    return ' '.join(parts)


def build_blueprint(cat_id, cat_name, types, rules):
    """Return a blueprint dict for a (cat_id, {types}) combo."""
    key = '~'.join(sorted(types))
    phrase = facet_phrase(types, rules)
    title = _compose_title(phrase)
    # /page-titles caps the title at MAX_TITLE_LEN chars. When a deep facet
    # combo overflows, drop trailing (lowest-priority) facet placeholders until
    # it fits — never split a !!placeholder!! and always keep !!current_query!!
    # and the branding tail. h1/description keep the full phrase (no such cap).
    if len(title) > MAX_TITLE_LEN:
        tokens = phrase.split(' ')
        while tokens and len(_compose_title(' '.join(tokens))) > MAX_TITLE_LEN:
            tokens.pop()
        title = _compose_title(' '.join(tokens))
    h1 = phrase
    desc = (f'Zoek je {phrase}? &#10062; Vergelijk !!NR!! aanbiedingen en bespaar op je '
            f'aankoop &#10062; Shop {phrase} met !!DISCOUNT!! korting online! &#10062; beslist.nl')
    return {
        'cat_id': cat_id, 'key': key, 'cat_name': cat_name,
        'title': title, 'h1_title': h1, 'description': desc,
        'country_code': COUNTRY_CODE,
    }


# ---------------------------------------------------------------------------
# Redshift: top-visited faceted /c/ URLs
# ---------------------------------------------------------------------------
def _yyyymmdd(date_str, default):
    """'YYYY-MM-DD' -> int YYYYMMDD, tolerant of already-int or empty input."""
    if not date_str:
        return default
    s = str(date_str).replace('-', '').strip()
    try:
        return int(s)
    except ValueError:
        return default


def fetch_top_urls(top_n, date_from=None, date_to=None):
    """Top-N SEO-visited faceted /c/ URLs, ordered by visits desc.

    Returns list of dicts: {url, visits, revenue}. (Ordering differs from the
    archival notes/query.txt, which sorted by subcat name — we want top visited.)
    """
    dfrom = _yyyymmdd(date_from, 20250101)
    dto = _yyyymmdd(date_to, 20260608)
    sql = """
        SELECT SPLIT_PART(dv.url, '?', 1) AS url,
               count(*) AS visits,
               sum(fcv.cpc_revenue) + sum(fcv.ww_revenue) AS revenue
        FROM datamart.fct_visits fcv
        JOIN datamart.dim_visit dv ON fcv.dim_visit_key = dv.dim_visit_key
        JOIN chan_deriv.ref_channel_derivation_stats chan
             ON dv.aff_id = chan.aff_id AND dv.channel_id = chan.channel_id
        WHERE dv.is_real_visit = 1
          AND chan.marketing_channel = 'SEO'
          AND fcv.dim_date_key BETWEEN %s AND %s
          AND dv.url LIKE '%%beslist.nl%%'
          AND dv.url LIKE '%%/c/%%'
          AND dv.url NOT LIKE '%%/r/%%'
          AND dv.url NOT LIKE '%%+%%'
          AND dv.url NOT LIKE '%%/l/%%'
          AND dv.url NOT LIKE '%%/page_%%'
          AND dv.url NOT LIKE '%%#%%'
        GROUP BY 1
        HAVING count(*) > 0
        ORDER BY visits DESC
        LIMIT %s
    """
    conn = get_redshift_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, (dfrom, dto, int(top_n)))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_redshift_connection(conn)


# ---------------------------------------------------------------------------
# Dedup: existing blueprint combos (Excel snapshot + our own pushes)
# ---------------------------------------------------------------------------
_existing_cache = {"combos": None, "loaded_at": 0.0}
_EXISTING_TTL = 600  # seconds


def load_existing_combos(force=False):
    """Set of (cat_id, canon_key) already covered: pa.page_titles_existing (the
    tblPageTitles export) UNION pa.seo_titles_blueprints (built or pushed)."""
    now = time.time()
    if not force and _existing_cache["combos"] is not None \
            and now - _existing_cache["loaded_at"] < _EXISTING_TTL:
        return _existing_cache["combos"]
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        combos = set()
        cur.execute("SELECT cat_id, canon_key FROM pa.page_titles_existing")
        for row in cur.fetchall():
            combos.add((row['cat_id'], row['canon_key']))
        cur.execute("SELECT cat_id, key FROM pa.seo_titles_blueprints")
        for row in cur.fetchall():
            combos.add((row['cat_id'], canon_key(row['key'])))
        _existing_cache["combos"] = combos
        _existing_cache["loaded_at"] = now
        return combos
    finally:
        cur.close()
        return_db_connection(conn)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def init_seo_titles_table():
    """Create the tool's tables if missing (idempotent)."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pa.seo_titles_blueprints (
                cat_id       INTEGER NOT NULL,
                key          TEXT    NOT NULL,
                cat_name     TEXT,
                title        TEXT,
                h1_title     TEXT,
                description  TEXT,
                country_code TEXT DEFAULT 'NL',
                source_url   TEXT,
                visits       INTEGER,
                revenue      NUMERIC,
                status       TEXT DEFAULT 'built',
                last_error   TEXT,
                created_at   TIMESTAMP DEFAULT now(),
                pushed_at    TIMESTAMP,
                PRIMARY KEY (cat_id, key)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pa.page_titles_existing (
                cat_id       INTEGER NOT NULL,
                key          TEXT    NOT NULL,
                canon_key    TEXT    NOT NULL,
                title        TEXT,
                h1_title     TEXT,
                description  TEXT,
                country_code TEXT DEFAULT 'NL'
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS ix_pte_combo ON pa.page_titles_existing (cat_id, canon_key)")
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)


def _upsert_blueprint(cur, bp, source_url, visits, revenue):
    """Insert/refresh a built blueprint. Never downgrades a 'pushed' row."""
    cur.execute("""
        INSERT INTO pa.seo_titles_blueprints
            (cat_id, key, cat_name, title, h1_title, description, country_code,
             source_url, visits, revenue, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'built', now())
        ON CONFLICT (cat_id, key) DO UPDATE SET
            cat_name    = EXCLUDED.cat_name,
            title       = EXCLUDED.title,
            h1_title    = EXCLUDED.h1_title,
            description = EXCLUDED.description,
            source_url  = EXCLUDED.source_url,
            visits      = EXCLUDED.visits,
            revenue     = EXCLUDED.revenue
        WHERE pa.seo_titles_blueprints.status <> 'pushed'
    """, (bp['cat_id'], bp['key'], bp['cat_name'], bp['title'], bp['h1_title'],
          bp['description'], bp['country_code'], source_url, visits, revenue))


def _has_unique_title(cur, url):
    """True if pa.unique_titles_content already holds a non-empty title for url."""
    cur.execute("""
        SELECT c.title
        FROM pa.unique_titles_content c
        JOIN pa.urls u ON u.url_id = c.url_id
        WHERE u.url = pa.canonicalize_url(%s)
    """, (url,))
    row = cur.fetchone()
    return bool(row and row['title'])


# ---------------------------------------------------------------------------
# Threaded run orchestration (mirrors ai_titles_service pattern)
# ---------------------------------------------------------------------------
_state_lock = threading.Lock()
_seo_state = {"status": "idle"}


def _reset_state(top_n, date_from, date_to):
    with _state_lock:
        _seo_state.clear()
        _seo_state.update({
            "status": "running", "phase": "starting",
            "top_n": top_n, "date_from": date_from, "date_to": date_to,
            "urls_fetched": 0, "scanned": 0, "no_cat": 0, "no_facets": 0,
            "dup": 0, "skipped_existing": 0, "new_combos": 0,
            "titles_generated": 0, "titles_skipped": 0, "titles_failed": 0,
            "message": "", "should_stop": False,
            "started_at": time.time(), "finished_at": None,
        })


def _set(**kw):
    with _state_lock:
        _seo_state.update(kw)


def _inc(key, n=1):
    with _state_lock:
        _seo_state[key] = _seo_state.get(key, 0) + n


def get_run_status():
    with _state_lock:
        return dict(_seo_state)


def stop_run():
    with _state_lock:
        if _seo_state.get("status") == "running":
            _seo_state["should_stop"] = True
            return {"stopped": True}
    return {"stopped": False, "message": "no run in progress"}


def _stopping():
    with _state_lock:
        return _seo_state.get("should_stop", False)


def start_run(top_n=100, date_from=None, date_to=None):
    with _state_lock:
        if _seo_state.get("status") == "running":
            return {"started": False, "message": "a run is already in progress"}
    _reset_state(top_n, date_from, date_to)
    threading.Thread(target=_run, args=(top_n, date_from, date_to), daemon=True).start()
    return {"started": True, "top_n": top_n}


def _run(top_n, date_from, date_to):
    try:
        # Try to keep the taxonomy slug->cat_id map warm.
        from backend.url_validator_service import _cache as taxonomy_cache

        _set(phase="fetching_urls")
        rows = fetch_top_urls(top_n, date_from, date_to)
        _set(urls_fetched=len(rows))

        rules = load_rules()
        existing = load_existing_combos(force=True)

        _set(phase="building_blueprints")
        seen = set()          # every (cat_id, canon_key) examined this run
        created = set()       # unique (cat_id, canon_key) actually built this run
        new_sources = []      # (source_url) per new combo, for AI-title generation
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            for r in rows:
                if _stopping():
                    break
                _inc("scanned")
                url = (r.get('url') or '').lower()
                p = parse_url(url)
                if not p:
                    continue
                leaf, types = p
                cat = _resolve_cat(taxonomy_cache, leaf)
                if not cat:
                    _inc("no_cat")
                    continue
                if not types:
                    _inc("no_facets")
                    continue
                # dedup on the canonical (cat_id, key) — identical form used by
                # load_existing_combos, so the same combo is never counted twice
                ck = (cat['cat_id'], canon_key('~'.join(sorted(types))))
                if ck in seen:
                    _inc("dup")
                    continue
                seen.add(ck)
                if ck in existing:
                    _inc("skipped_existing")
                    continue
                bp = build_blueprint(cat['cat_id'], cat.get('cat_name', ''), types, rules)
                _upsert_blueprint(cur, bp, url, r.get('visits'), r.get('revenue'))
                conn.commit()
                if ck not in created:
                    created.add(ck)
                    new_sources.append(url)
                    _set(new_combos=len(created))  # unique created combos
        finally:
            cur.close()
            return_db_connection(conn)

        # AI unique titles for the source URLs of the new combos (best effort,
        # parallelized). Blueprint push does not depend on these succeeding.
        _set(phase="generating_titles")
        _generate_titles(new_sources)

        _set(phase="done", status="done", finished_at=time.time())
    except Exception as e:
        _set(phase="error", status="error", message=str(e), finished_at=time.time())


def _generate_titles(source_urls, workers=10):
    if not source_urls:
        return
    from backend.ai_titles_service import process_single_url

    # Skip URLs that already have a unique title.
    todo = []
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for u in source_urls:
            try:
                if _has_unique_title(cur, u):
                    _inc("titles_skipped")
                else:
                    todo.append(u)
            except Exception:
                todo.append(u)  # on check failure, attempt generation
    finally:
        cur.close()
        return_db_connection(conn)

    def _one(u):
        if _stopping():
            return
        try:
            res = process_single_url(u)
            if res.get("status") == "success":
                _inc("titles_generated")
            else:
                _inc("titles_failed")
        except Exception:
            _inc("titles_failed")

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_one, todo))


# ---------------------------------------------------------------------------
# Publish: POST built blueprints -> /page-titles, then push per-URL AI titles
# ---------------------------------------------------------------------------
def _post_page_titles(records, env):
    """POST a JSON array of records to /page-titles. Returns (ok, status, text)."""
    url = PAGE_TITLES_API[env]
    key = PAGE_TITLES_KEY[env]()
    if not key:
        return False, 0, f"missing API key for env={env}"
    headers = {"X-Api-Key": key, "Content-Type": "application/json"}
    last = None
    for attempt in range(1, 4):
        try:
            resp = requests.post(url, headers=headers, json=records, timeout=600)
            ok = resp.status_code in (200, 201)
            return ok, resp.status_code, (resp.text or "")[:500]
        except requests.RequestException as e:
            last = e
            time.sleep(2 * attempt)
    return False, 0, f"transport error after retries: {last}"


def remove_blueprints(combos):
    """Delete unpushed (built/failed) blueprints for the given combos. Never
    touches 'pushed' rows so the dedup push-log stays intact."""
    if not combos:
        return {"removed": 0}
    conn = get_db_connection()
    cur = conn.cursor()
    removed = 0
    try:
        for c in combos:
            cur.execute("""
                DELETE FROM pa.seo_titles_blueprints
                WHERE cat_id = %s AND key = %s AND status <> 'pushed'
            """, (int(c['cat_id']), c['key']))
            removed += cur.rowcount
        conn.commit()
        return {"removed": removed}
    finally:
        cur.close()
        return_db_connection(conn)


def publish_built(env="production", push_unique_titles=True, combos=None):
    """Push status='built' blueprints to /page-titles (batched upsert), flip
    successful ones to 'pushed', then push the per-URL AI titles. When `combos`
    is given, only those (cat_id, key) built rows are pushed; otherwise all."""
    if env not in PAGE_TITLES_API:
        return {"success": False, "message": f"unknown env {env!r}"}

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT cat_id, key, title, h1_title, description, country_code
            FROM pa.seo_titles_blueprints
            WHERE status = 'built'
            ORDER BY cat_id, key
        """)
        built = cur.fetchall()
    finally:
        cur.close()
        return_db_connection(conn)

    if combos:
        wanted = {(int(c['cat_id']), c['key']) for c in combos}
        built = [r for r in built if (int(r['cat_id']), r['key']) in wanted]

    if not built:
        return {"success": True, "pushed": 0, "message": "no matching built blueprints to publish"}

    pushed = 0
    failed = 0
    batch_results = []
    for i in range(0, len(built), PUSH_BATCH):
        batch = built[i:i + PUSH_BATCH]
        records = [{
            "cat_id": int(r['cat_id']),
            "key": r['key'],
            "title": r['title'] or "",
            "h1_title": r['h1_title'] or "",
            "description": r['description'] or "",
            "country_code": r['country_code'] or "NL",
        } for r in batch]
        ok, code, text = _post_page_titles(records, env)
        batch_results.append({"batch": i // PUSH_BATCH + 1, "count": len(batch),
                              "ok": ok, "status_code": code, "response": text})
        combos = [(int(r['cat_id']), r['key']) for r in batch]
        c2 = get_db_connection()
        cur2 = c2.cursor()
        try:
            if ok:
                cur2.executemany("""
                    UPDATE pa.seo_titles_blueprints
                    SET status='pushed', pushed_at=now(), last_error=NULL
                    WHERE cat_id=%s AND key=%s
                """, combos)
                pushed += len(batch)
            else:
                cur2.executemany("""
                    UPDATE pa.seo_titles_blueprints
                    SET status='failed', last_error=%s
                    WHERE cat_id=%s AND key=%s
                """, [(text, cid, k) for (cid, k) in combos])
                failed += len(batch)
            c2.commit()
        finally:
            cur2.close()
            return_db_connection(c2)

    result = {
        "success": failed == 0,
        "env": env,
        "pushed": pushed,
        "failed": failed,
        "batches": batch_results,
    }

    # Push per-URL AI titles via the existing importer (full CSV upsert).
    if push_unique_titles and pushed:
        try:
            from backend.unique_titles import upload_titles_to_api
            result["unique_titles_push"] = upload_titles_to_api()
        except Exception as e:
            result["unique_titles_push"] = {"success": False, "error": str(e)}

    load_existing_combos(force=True)  # refresh dedup set with the new pushes
    return result


# ---------------------------------------------------------------------------
# Read helpers for the frontend
# ---------------------------------------------------------------------------
def get_preview(limit=100, status="built"):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT b.cat_id, b.key, b.cat_name, b.title, b.h1_title, b.description,
                   b.source_url, b.visits, b.revenue, b.status, b.created_at, b.pushed_at,
                   c.title AS example_title
            FROM pa.seo_titles_blueprints b
            LEFT JOIN pa.urls u ON u.url = pa.canonicalize_url(b.source_url)
            LEFT JOIN pa.unique_titles_content c ON c.url_id = u.url_id
            WHERE (%s = 'all' OR b.status = %s)
            ORDER BY b.visits DESC NULLS LAST, b.cat_id, b.key
            LIMIT %s
        """, (status, status, limit))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def get_recent(limit=20):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT cat_id, key, cat_name, title, status, visits, created_at, pushed_at
            FROM pa.seo_titles_blueprints
            ORDER BY COALESCE(pushed_at, created_at) DESC
            LIMIT %s
        """, (limit,))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def get_stats():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT count(*) AS n FROM pa.page_titles_existing")
        existing = cur.fetchone()['n']
        cur.execute("""
            SELECT status, count(*) AS n
            FROM pa.seo_titles_blueprints GROUP BY status
        """)
        by_status = {row['status']: row['n'] for row in cur.fetchall()}
        return {
            "existing_blueprints": existing,
            "built": by_status.get("built", 0),
            "pushed": by_status.get("pushed", 0),
            "failed": by_status.get("failed", 0),
        }
    finally:
        cur.close()
        return_db_connection(conn)
