"""
Facet Watch Service
===================

Daily insight into the most recently created/changed facets per MAIN CATEGORY,
built on the Taxonomy API audit log.

Why the audit log and not createdAt/updatedAt
---------------------------------------------
`createdAt` / `updatedAt` on a facet or facet value are BULK STAMPS, not edit
dates: 1.478 of the 1.775 Parfumerie facet values carry 2026-01-27, the migration
batch that seeded the taxonomy, and every main category carries 2026-03-24. Sorting
on them answers "which batch was this row in", not "what changed yesterday". The
audit log (`GET /api/audit-logs`, route A in suggestions_new.txt) is per-mutation:
one row per INSERT/UPDATE/DELETE with a timestamp, the actor, and the changed
fields. That is the only route that can answer this question.

Attributing an event to a main category
---------------------------------------
The audit log carries no category. Each event type exposes a different amount, and
the difference matters because an UPDATE records ONLY the changed field:

    Facet                  INSERT/UPDATE   entityId IS the facetId          -> direct
    Facet Label            INSERT/UPDATE   entityId IS the facetId          -> direct
    Facet Value            INSERT/DELETE   changes.FacetId present          -> direct
    Facet Value            UPDATE          only the delta                   -> value->facet cache
    Facet Value Label      INSERT/UPDATE   entityId is the VALUE id         -> value->facet cache
    Category Facet         INSERT          changes.CategoryId + .FacetId    -> direct
    Category Facet Setting INSERT          changes.CategoryId + .FacetId    -> direct
    Category Facet Setting UPDATE          only the delta + a settings-row id -> UNRESOLVABLE

That last row is a real hole and is reported as such rather than guessed at. Its
`entityId` is a CategoryFacetSettings row id, and the API has no lookup by that id
(`/api/CategoryFacetSettings` requires `categoryId`, 400 without it), while the
matching INSERT event carries a NEGATIVE synthetic entityId so it cannot be joined
to the later UPDATE either. Those events land with `resolution='no_link'` and are
counted separately in the overview — never folded into a main category's totals.

facetId -> main categories comes from `GET /api/Facets/{id}/main-categories`, which
returns `mainCategoryIds` as an array: a shared facet legitimately belongs to
several main categories and is counted under each.

Caches
------
`pa.facet_watch_value_facet` (value id -> facet id) is the one that needs seeding:
`Facet Value UPDATE` and both label events name a value without its facet.
`GET /api/Facets/values` dumps all 555.116 values in one call (~146 MB, ~60 s), so
seeding is one call rather than 555k. After seeding, a miss is rare — new values
arrive through INSERT events, which carry FacetId for free — and is resolved
per-id via `GET /api/Facets/values/{id}`.

Idempotency
-----------
`pa.facet_watch_events.audit_id` is the audit log's own id and the primary key, so
re-ingesting an overlapping window is a no-op. That makes the daily run safe to
re-run and safe to overlap deliberately (the default window reaches one day back
past the last seen event, so an event written while yesterday's run was in flight
is not missed).
"""
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests
from psycopg2.extras import Json, execute_values

from backend import taxv2_client as taxv2
from backend.database import get_db_connection, return_db_connection

logger = logging.getLogger(__name__)

TAX_BASE = taxv2.BASE
# Every GET is open internally; the header is what makes a read attributable in the
# audit log we are reading from. See memory taxonomy_api_user_header.
USER_HEADER = taxv2.USER_NAME
HTTP_TIMEOUT = 90
DUMP_TIMEOUT = 600          # /api/Facets/values is ~146 MB
AUDIT_PAGE = 5000           # Take=10000 works; 5000 keeps a page ~2 s
LOOKUP_WORKERS = 8

# Event types we consider "facet activity". Category Context is a category text
# edit, not a facet change, so it is not ingested.
FACET_ENTITIES = (
    "Facet",
    "Facet Label",
    "Facet Value",
    "Facet Value Label",
    "Category Facet",
    "Category Facet Setting",
    # Added 2026-09-03. It was the largest thing falling outside this tuple:
    # 2.497 of the 3.789 dropped events over 30 days. A productlijn-facet has no
    # CategoryFacet row at all — its main categories come from exactly these
    # value dependencies — so leaving it out made that whole family invisible to
    # the watch. See _extract() for why it resolves to the CHILD facet.
    "Facet Value Dependency",
)

# Which entity names describe the FACET itself rather than one of its values. Used
# by the overview to separate "a new facet appeared" from "a value was added".
FACET_LEVEL = ("Facet", "Facet Label", "Category Facet", "Category Facet Setting",
               # A dependency binds one facet to a value of ANOTHER facet, which
               # changes where the child facet appears — a fact about the facet,
               # not about one of its own values.
               "Facet Value Dependency")

# The product-line family: one facet per brand, created automatically by ListsApi
# and attached to nearly every main category. Two of them ("Productlijnen: UGG",
# "Productlijnen: Luvion") were 2 of the 8 new facets in the 25-28 Aug window but
# fanned out over 10 and 17 main categories, so they alone put a "1 new facet" in
# almost every row of the overview. They are structural noise for this question, so
# every read takes `exclude_auto` and defaults it ON.
#
# Matched on the facet NAME, not the slug. The slug prefixes that look like they
# identify this family do not: `p_pennenbakken` is "Plaatsing" and `pl_klussen` is
# "Serie" -- both real facets that a `p_` / `pl_` prefix rule silently throws away
# (it cost facet 7917 its place in this overview before this was fixed). Measured
# on a 285-facet category: the name rule had 0 false positives, the slug rule 2.
# Matched on the name and NOT on the actor, because ListsApi also adds ordinary
# brand VALUES, which are real changes.
AUTO_FACET_NAMES = ("Productlijn",)
AUTO_FACET_NAME_PREFIX = "Productlijnen:"

# SQL fragment: true when the facet belongs to that family. A NULL name (a facet the
# cache has not resolved yet) counts as NOT auto, so an unknown is never hidden.
_AUTO_SQL = ("COALESCE(fm.facet_name = 'Productlijn'"
             " OR fm.facet_name LIKE 'Productlijnen:%%', false)")

DDL = """
CREATE TABLE IF NOT EXISTS pa.facet_watch_events (
    audit_id        BIGINT PRIMARY KEY,
    ts_utc          TIMESTAMP   NOT NULL,
    action          TEXT        NOT NULL,
    entity_name     TEXT        NOT NULL,
    entity_id       TEXT,
    facet_id        INTEGER,
    facet_value_id  BIGINT,
    category_id     INTEGER,
    main_cat_ids    INTEGER[]   NOT NULL DEFAULT '{}',
    facet_name      TEXT,
    value_name      TEXT,
    changed_fields  TEXT[]      NOT NULL DEFAULT '{}',
    changes         JSONB,
    actor           TEXT,
    resolution      TEXT        NOT NULL,
    ingested_at     TIMESTAMP   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fw_events_ts      ON pa.facet_watch_events (ts_utc DESC);
CREATE INDEX IF NOT EXISTS ix_fw_events_facet   ON pa.facet_watch_events (facet_id);
CREATE INDEX IF NOT EXISTS ix_fw_events_maincat ON pa.facet_watch_events USING gin (main_cat_ids);

CREATE TABLE IF NOT EXISTS pa.facet_watch_value_facet (
    value_id   BIGINT PRIMARY KEY,
    facet_id   INTEGER NOT NULL,
    value_name TEXT,
    seen_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pa.facet_watch_facet_maincat (
    facet_id            INTEGER PRIMARY KEY,
    main_cat_ids        INTEGER[] NOT NULL DEFAULT '{}',
    facet_name          TEXT,
    facet_slug          TEXT,
    category_count      INTEGER,
    is_enabled_anywhere BOOLEAN,
    fetched_at          TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pa.facet_watch_maincats (
    main_cat_id INTEGER PRIMARY KEY,
    name        TEXT,
    fetched_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pa.facet_watch_runs (
    id           SERIAL PRIMARY KEY,
    started_at   TIMESTAMP NOT NULL DEFAULT now(),
    finished_at  TIMESTAMP,
    from_date    TIMESTAMP,
    to_date      TIMESTAMP,
    events_seen  INTEGER DEFAULT 0,
    events_new   INTEGER DEFAULT 0,
    lookups      INTEGER DEFAULT 0,
    status       TEXT DEFAULT 'running',
    message      TEXT
);

-- Zoekvolume per productlijn-naam, uit de Keyword Planner. Een eigen tabel en niet
-- een kolom op de events: één naam komt in tientallen events terug (dezelfde
-- productlijn wordt per maincat opnieuw als waarde aangemaakt), en het volume hoort
-- bij de NAAM, niet bij de mutatie. `fetched_at` maakt het verversbaar zonder de
-- Google-quota bij elke pageload aan te tikken -- de module leest de cache en haalt
-- alleen op wat de gebruiker expliciet vraagt.
CREATE TABLE IF NOT EXISTS pa.facet_watch_keyword_volume (
    keyword       TEXT PRIMARY KEY,
    search_volume INTEGER,
    fetched_at    TIMESTAMP NOT NULL DEFAULT now()
);
"""



def _sess():
    """Sessie per thread, met retry op 502/503/504 — zie backend/taxv2_client.py.

    Hiervoor was dit één gedeelde Session zonder retry, gedeeld met acht
    ThreadPoolExecutor-workers. `requests.Session` is niet gedocumenteerd als
    thread-safe, en het ontbreken van retry is de directe oorzaak van de
    lookup_failed-events: één 502 werd als feit weggeschreven.
    """
    return taxv2.session()


def _get(path, params=None, timeout=HTTP_TIMEOUT):
    return taxv2.get_json(path, params=params, timeout=timeout)


def init_tables():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(DDL)
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)


# ---------------------------------------------------------------------------
# Run status (single in-flight ingest, mirrors the seo_titles_service pattern)
# ---------------------------------------------------------------------------
_state = {"status": "idle", "phase": "", "events_seen": 0, "events_new": 0,
          "lookups": 0, "message": "", "started_at": None, "finished_at": None}
_state_lock = threading.Lock()
_stop = threading.Event()


def _set(**kw):
    with _state_lock:
        _state.update(kw)


def _inc(key, n=1):
    with _state_lock:
        _state[key] = (_state.get(key) or 0) + n


def get_run_state():
    with _state_lock:
        return dict(_state)


def stop_run():
    _stop.set()
    return {"stopping": True}


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------
def refresh_maincats():
    """The 32 main categories, id -> nl-NL name. `/api/Categories?locale=` returns
    ONLY the main categories (not the tree), which is exactly what is wanted here."""
    cats = _get("/api/Categories", {"locale": "nl-NL"})
    rows = []
    for c in cats or []:
        nl = next((l.get("name") for l in c.get("labels") or []
                   if l.get("locale") == "nl-NL"), None)
        rows.append((c["id"], _clean(nl)))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        execute_values(cur, """
            INSERT INTO pa.facet_watch_maincats (main_cat_id, name)
            VALUES %s
            ON CONFLICT (main_cat_id) DO UPDATE
              SET name = EXCLUDED.name, fetched_at = now()
        """, rows)
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)
    return {"main_categories": len(rows)}


def seed_value_facet_map(progress=None):
    """One call to `GET /api/Facets/values` dumps every facet value with its
    facetId (555k rows, ~146 MB, ~60 s). Without this seed, every `Facet Value
    UPDATE` and every label event needs its own GET; with it, a miss is rare."""
    _set(phase="seeding value->facet map")
    items = _get("/api/Facets/values", timeout=DUMP_TIMEOUT)
    if isinstance(items, dict):
        items = items.get("items", [])
    rows = []
    for v in items:
        rows.append((v["id"], v["facetId"], _value_name(v)))
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for i in range(0, len(rows), 10000):
            execute_values(cur, """
                INSERT INTO pa.facet_watch_value_facet (value_id, facet_id, value_name)
                VALUES %s
                ON CONFLICT (value_id) DO UPDATE
                  SET facet_id = EXCLUDED.facet_id,
                      value_name = COALESCE(EXCLUDED.value_name,
                                            pa.facet_watch_value_facet.value_name),
                      seen_at = now()
            """, rows[i:i + 10000])
            conn.commit()
            if progress:
                progress(min(i + 10000, len(rows)), len(rows))
    finally:
        cur.close()
        return_db_connection(conn)
    return {"values": len(rows)}


def _clean(s):
    """Strip NUL and other C0 control characters from a taxonomy string.

    Postgres rejects a string literal containing 0x00 outright ("A string literal
    cannot contain NUL (0x00) characters"), which killed the first full seed run.
    Ten facet values carry one: they are brand names whose accented character was
    mangled on import -- `Oro Bail\x00n` for "Oro Bailen(accent)", `Meli\x00a1`
    for "Melia(accent)" -- so the NUL is upstream data damage, not our encoding.
    Anything we persist goes through here; the mangled name is kept minus the NUL
    rather than dropped, so the row stays findable.
    """
    if not s:
        return s
    return "".join(c for c in s if c == "\n" or c == "\t" or ord(c) >= 32)


def _clean_json(o):
    """Same, recursively, for a `changes` payload before it becomes JSONB."""
    if isinstance(o, str):
        return _clean(o)
    if isinstance(o, dict):
        return {k: _clean_json(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_clean_json(v) for v in o]
    return o


def _value_name(v):
    """Prefer the nl-NL label, fall back to `global`, then to anything."""
    labels = v.get("labels") or []
    for want in ("nl-NL", "global"):
        for l in labels:
            if l.get("locale") == want:
                return _clean(l.get("nameInColumn") or l.get("nameOnDetail"))
    return _clean(labels[0].get("nameInColumn")) if labels else None


# ---------------------------------------------------------------------------
# Resolution caches
# ---------------------------------------------------------------------------
def _load_value_facet(value_ids):
    """value_id -> facet_id for the ids already cached."""
    if not value_ids:
        return {}
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT value_id, facet_id, value_name
                       FROM pa.facet_watch_value_facet WHERE value_id = ANY(%s)""",
                    (list(value_ids),))
        return {r["value_id"]: (r["facet_id"], r["value_name"]) for r in cur.fetchall()}
    finally:
        cur.close()
        return_db_connection(conn)


def _fetch_value_facet(value_id):
    """`GET /api/Facets/values/{id}` — the PATH form. The query form
    (`/api/Facets/values?facetValueId=`) does NOT filter: it returns HTTP 200 with
    a dump of all 555k values, which reads as "the value is gone" if you look at
    item 0. See memory taxonomy_api_bulk_timestamps / the 27-08 LEARNINGS entry."""
    try:
        v = _get(f"/api/Facets/values/{value_id}")
        return v.get("facetId"), _value_name(v)
    except requests.RequestException:
        return None, None


def _fetch_facet_maincats(facet_id):
    try:
        d = _get(f"/api/Facets/{facet_id}/main-categories")
        return {"main_cat_ids": d.get("mainCategoryIds") or [],
                "category_count": d.get("categoryCount"),
                "is_enabled_anywhere": d.get("isEnabledAnywhere")}
    except requests.RequestException:
        return None


def _fetch_facet_meta(facet_id):
    """nl-NL name + slug for display."""
    try:
        f = _get(f"/api/Facets/{facet_id}")
        labels = f.get("labels") or []
        nl = next((l for l in labels if l.get("locale") == "nl-NL"), None) \
            or next((l for l in labels if l.get("locale") == "global"), None) \
            or (labels[0] if labels else {})
        return _clean(nl.get("name")), _clean(nl.get("urlSlug"))
    except requests.RequestException:
        return None, None


# Facetten waarvan de main-category-lookup deze run FAALDE (netwerk/5xx), tegenover
# facetten die aantoonbaar nergens hangen. Het verschil bepaalt of een event
# repareerbaar is of een feit.
_LOOKUP_FAILED: set = set()

# category_id -> main_cat_id, opgelopen via parentId. Categorieën verhuizen zelden,
# dus procesbreed cachen is genoeg; een miss kost één HTTP-call per niveau.
_CAT_MAINCAT_CACHE: dict = {}
_CAT_MAINCAT_LOCK = threading.Lock()
_KNOWN_MAINCATS: set = set()


def _known_maincats():
    """De ids uit pa.facet_watch_maincats, één keer per proces geladen."""
    global _KNOWN_MAINCATS
    if _KNOWN_MAINCATS:
        return _KNOWN_MAINCATS
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT main_cat_id FROM pa.facet_watch_maincats")
        _KNOWN_MAINCATS = {r["main_cat_id"] for r in cur.fetchall()}
    except Exception as ex:  # noqa: BLE001
        logger.warning("kon hoofdcategorieën niet laden: %s", ex)
    finally:
        cur.close()
        return_db_connection(conn)
    return _KNOWN_MAINCATS


def _maincat_of_category(cid, _depth=0):
    """De maincat waar deze categorie onder valt, door parentId omhoog te lopen."""
    if cid is None or _depth > 12:
        return None
    key = int(cid)
    with _CAT_MAINCAT_LOCK:
        if key in _CAT_MAINCAT_CACHE:
            return _CAT_MAINCAT_CACHE[key]
    result = None
    try:
        d = _get(f"/api/Categories/{key}", {"locale": "nl-NL"}) or {}
        parent = d.get("parentId")
        # Geen parent = dit IS een root. Alleen accepteren als het ook echt als
        # hoofdcategorie bekend staat, anders vervuilen we main_cat_ids.
        if parent is None:
            result = key if key in _known_maincats() else None
        else:
            result = _maincat_of_category(parent, _depth + 1)
    except Exception as ex:  # noqa: BLE001
        logger.warning("maincat-lookup mislukt voor categorie %s: %s", key, ex)
    with _CAT_MAINCAT_LOCK:
        _CAT_MAINCAT_CACHE[key] = result
    return result


def _resolve_facets(facet_ids, max_age_days=7):
    """facet_id -> {main_cat_ids, facet_name, facet_slug, ...}, cached in
    pa.facet_watch_facet_maincat. A facet's category attachment changes rarely, so
    a week-old answer is fine; a brand-new facet is never in the cache and is
    always fetched."""
    facet_ids = {int(f) for f in facet_ids if f is not None}
    if not facet_ids:
        return {}
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT facet_id, main_cat_ids, facet_name, facet_slug,
                   category_count, is_enabled_anywhere
            FROM pa.facet_watch_facet_maincat
            WHERE facet_id = ANY(%s)
              AND fetched_at > now() - (%s || ' days')::interval
        """, (list(facet_ids), max_age_days))
        cached = {r["facet_id"]: dict(r) for r in cur.fetchall()}
    finally:
        cur.close()
        return_db_connection(conn)

    todo = [f for f in facet_ids if f not in cached]
    if not todo:
        return cached

    def _one(fid):
        mc = _fetch_facet_maincats(fid)
        name, slug = _fetch_facet_meta(fid)
        return fid, mc, name, slug

    fresh = []
    with ThreadPoolExecutor(max_workers=LOOKUP_WORKERS) as ex:
        for fid, mc, name, slug in ex.map(_one, todo):
            _inc("lookups")
            if mc is None:
                # Could not tell. Leave it out of the cache so the next run retries
                # rather than persisting an empty answer as fact. De EVENTS van deze
                # run kregen tot nu toe wel gewoon resolution='no_maincat' mee, wat
                # ze onzichtbaar maakt voor get_overview/get_facets en na één 502
                # permanent verkeerd toegewezen liet — het standaardvenster is maar
                # last_ts - 1 dag. Markeren, zodat pass 4 er 'lookup_failed' van maakt
                # en de volgende run ze repareert.
                _LOOKUP_FAILED.add(fid)
                cached[fid] = {"facet_id": fid, "main_cat_ids": [], "facet_name": name,
                               "facet_slug": slug, "category_count": None,
                               "is_enabled_anywhere": None}
                continue
            row = {"facet_id": fid, "main_cat_ids": mc["main_cat_ids"],
                   "facet_name": name, "facet_slug": slug,
                   "category_count": mc["category_count"],
                   "is_enabled_anywhere": mc["is_enabled_anywhere"]}
            cached[fid] = row
            fresh.append((fid, row["main_cat_ids"], name, slug,
                          mc["category_count"], mc["is_enabled_anywhere"]))

    if fresh:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            execute_values(cur, """
                INSERT INTO pa.facet_watch_facet_maincat
                    (facet_id, main_cat_ids, facet_name, facet_slug,
                     category_count, is_enabled_anywhere)
                VALUES %s
                ON CONFLICT (facet_id) DO UPDATE SET
                    main_cat_ids = EXCLUDED.main_cat_ids,
                    facet_name = EXCLUDED.facet_name,
                    facet_slug = EXCLUDED.facet_slug,
                    category_count = EXCLUDED.category_count,
                    is_enabled_anywhere = EXCLUDED.is_enabled_anywhere,
                    fetched_at = now()
            """, fresh)
            conn.commit()
        finally:
            cur.close()
            return_db_connection(conn)
    return cached


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------
def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def last_event_ts():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT max(ts_utc) AS mx FROM pa.facet_watch_events")
        return (cur.fetchone() or {}).get("mx")
    finally:
        cur.close()
        return_db_connection(conn)


def _fetch_audit_page(from_iso, to_iso, skip, take):
    """One page, sorted by id ASCENDING so paging is stable while new events keep
    arriving at the top. Sorting descending (the API default) would shift every
    later page whenever an event lands mid-run."""
    return _get("/api/audit-logs", {
        "FromDate": from_iso, "ToDate": to_iso,
        "Skip": skip, "Take": take,
        "SortBy": "id", "SortDescending": "false",
    })


def _extract(ev):
    """audit event -> (facet_id, facet_value_id, category_id) from what the event
    itself carries. Returns Nones where the event does not say."""
    name = ev.get("entityName")
    ch = ev.get("changes") or {}
    eid = ev.get("entityId")

    def _int(x):
        try:
            return int(x)
        except (TypeError, ValueError):
            return None

    def _chg(key):
        """A change value is either a scalar (INSERT/DELETE) or {"Old":..,"New":..}
        (UPDATE). Only the scalar form identifies a row."""
        v = ch.get(key)
        if isinstance(v, dict):
            return _int(v.get("New")) or _int(v.get("Old"))
        return _int(v)

    facet_id = value_id = cat_id = None
    if name in ("Facet", "Facet Label"):
        facet_id = _int(eid)
    elif name in ("Facet Value", "Facet Value Label"):
        value_id = _int(eid)
        facet_id = _chg("FacetId")
    elif name in ("Category Facet", "Category Facet Setting"):
        facet_id = _chg("FacetId")
        cat_id = _chg("CategoryId")
    elif name == "Facet Value Dependency":
        # This one carries no `FacetId`. Its payload is
        # {ChildFacetId, ParentFacetId, ParentFacetValueId, CreatedAt, Notes} and
        # `entityId` is the dependency ROW's id — not a facet and not a value, so
        # it must not be read as either.
        #
        # The change belongs to the CHILD facet: the dependency decides where that
        # facet shows up, and for a productlijn-facet it is the only thing that
        # does. Deliberately NO value_id: `ParentFacetValueId` is a value of the
        # PARENT facet, and pass 2 feeds every (value_id, facet_id) pair it sees
        # into pa.facet_watch_value_facet — pairing the parent's value with the
        # child's facet would poison that map for every later lookup.
        facet_id = _chg("ChildFacetId")
    return facet_id, value_id, cat_id


def _repair_lookup_failed(limit=5000):
    """Vul main_cat_ids alsnog voor events die eerder 'lookup_failed' kregen."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE pa.facet_watch_events e
               SET main_cat_ids = fm.main_cat_ids,
                   resolution   = 'resolved'
              FROM pa.facet_watch_facet_maincat fm
             WHERE fm.facet_id = e.facet_id
               AND e.resolution = 'lookup_failed'
               AND fm.main_cat_ids <> '{}'
               AND e.audit_id IN (
                   SELECT audit_id FROM pa.facet_watch_events
                    WHERE resolution = 'lookup_failed' LIMIT %s)
        """, (limit,))
        n = cur.rowcount
        conn.commit()
        if n:
            logger.info("facet-watch: %s eerder onoplosbare events alsnog toegewezen", n)
        return n
    except Exception as ex:  # noqa: BLE001
        conn.rollback()
        logger.warning("reparatie van lookup_failed-events mislukt: %s", ex)
        return 0
    finally:
        cur.close()
        return_db_connection(conn)


def ingest(from_date=None, to_date=None, resolve_misses=True):
    """Pull audit events for the window, resolve them to main categories, upsert.

    Default window starts one day before the newest event already stored (or 30
    days back on an empty table). The deliberate overlap costs nothing — audit_id
    is the primary key — and covers an event written while the previous run was
    still paging.
    """
    _stop.clear()
    _LOOKUP_FAILED.clear()
    _set(status="running", phase="starting", events_seen=0, events_new=0,
         lookups=0, message="", started_at=time.time(), finished_at=None)
    run_id = None
    try:
        # Repareer eerst wat een eerdere run niet kon opvragen. Zonder deze stap
        # blijft een event dat één keer op een 502 liep voorgoed op lookup_failed
        # staan, want het standaardvenster kijkt maar één dag terug.
        _repair_lookup_failed()
        if from_date is None:
            last = last_event_ts()
            frm = (last - timedelta(days=1)) if last \
                else (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30))
        else:
            frm = from_date if isinstance(from_date, datetime) \
                else datetime.strptime(str(from_date)[:10], "%Y-%m-%d")
        to = to_date if isinstance(to_date, datetime) else (
            datetime.strptime(str(to_date)[:10], "%Y-%m-%d") if to_date
            else datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=1))

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""INSERT INTO pa.facet_watch_runs (from_date, to_date)
                           VALUES (%s, %s) RETURNING id""", (frm, to))
            run_id = cur.fetchone()["id"]
            conn.commit()
        finally:
            cur.close()
            return_db_connection(conn)

        _set(phase="fetching audit log")
        raw = []
        # Alles buiten FACET_ENTITIES werd zonder enige boekhouding weggegooid, dus
        # een nieuwe entiteitsoort verdween geruisloos. Geteld over 30 dagen op
        # 2026-09-02: 3.789 van 70.516 events (5,4%) vallen buiten de tuple, waarvan
        # 2.497 "Facet Value Dependency" — juist de soort die een productlijn-facet
        # aan zijn maincats hangt. Zet het in de run-log in plaats van het te laten
        # verdwijnen.
        dropped = {}
        skip = 0
        while not _stop.is_set():
            page = _fetch_audit_page(_iso(frm), _iso(to), skip, AUDIT_PAGE)
            items = page.get("items") or []
            for i in items:
                if i.get("entityName") in FACET_ENTITIES:
                    raw.append(i)
                else:
                    key = str(i.get("entityName"))
                    dropped[key] = dropped.get(key, 0) + 1
            _inc("events_seen", len(items))
            skip += len(items)
            if len(items) < AUDIT_PAGE or skip >= (page.get("total") or 0):
                break
        if dropped:
            _set(dropped_entities=dict(sorted(dropped.items(), key=lambda kv: -kv[1])))
        _set(phase=f"resolving {len(raw)} facet events")

        # ---- pass 1: what the events themselves say
        pre = []
        for ev in raw:
            fid, vid, cid = _extract(ev)
            pre.append((ev, fid, vid, cid))

        # ---- pass 2: value_id -> facet_id for the events that did not say.
        #
        # Order matters. The batch is asked FIRST: a `Facet Value INSERT` or DELETE
        # names both ids, so a `Facet Value Label INSERT` for the same value is
        # answered for free. Doing the API lookups first instead left 3.315 label
        # events unattributed over a 30-day window -- labels of values that were
        # created and then deleted inside it, so `GET /api/Facets/values/{id}` 404s
        # and the seeded dump (a snapshot of what still EXISTS) cannot help either.
        # The audit log is the only place those ids are still resolvable.
        need_vals = {vid for _e, fid, vid, _c in pre if vid and not fid}
        in_batch = {}
        for _e, fid, vid, _c in pre:
            if vid and fid:
                in_batch.setdefault(vid, (fid, None))
        vmap = _load_value_facet(need_vals)
        for vid, pair in in_batch.items():
            vmap.setdefault(vid, pair)
        misses = [v for v in need_vals if v not in vmap]
        if misses and resolve_misses:
            _set(phase=f"looking up {len(misses)} unseen facet values")

            def _one(vid):
                f, n = _fetch_value_facet(vid)
                return vid, f, n

            found = []
            with ThreadPoolExecutor(max_workers=LOOKUP_WORKERS) as ex:
                for vid, f, n in ex.map(_one, misses):
                    _inc("lookups")
                    if f:
                        vmap[vid] = (f, n)
                        found.append((vid, f, n))
            if found:
                c2 = get_db_connection()
                cu2 = c2.cursor()
                try:
                    execute_values(cu2, """
                        INSERT INTO pa.facet_watch_value_facet (value_id, facet_id, value_name)
                        VALUES %s ON CONFLICT (value_id) DO UPDATE
                          SET facet_id = EXCLUDED.facet_id, seen_at = now()
                    """, found)
                    c2.commit()
                finally:
                    cu2.close()
                    return_db_connection(c2)

        # Persist what the batch taught us, so a later run resolving a label for one
        # of these values does not have to re-derive it (and cannot, once the value
        # is deleted).
        learned = [(vid, pair[0], None) for vid, pair in in_batch.items()]
        if learned:
            c3 = get_db_connection()
            cu3 = c3.cursor()
            try:
                execute_values(cu3, """
                    INSERT INTO pa.facet_watch_value_facet (value_id, facet_id, value_name)
                    VALUES %s ON CONFLICT (value_id) DO UPDATE
                      SET facet_id = EXCLUDED.facet_id, seen_at = now()
                """, list({l[0]: l for l in learned}.values()))
                c3.commit()
            finally:
                cu3.close()
                return_db_connection(c3)

        # ---- pass 3: facet_id -> main categories
        _set(phase="resolving main categories")
        all_fids = set()
        for _e, fid, vid, _c in pre:
            f = fid or (vmap.get(vid, (None, None))[0] if vid else None)
            if f:
                all_fids.add(f)
        fmap = _resolve_facets(all_fids)
        # Een `Category Facet`(-Setting)-event ZEGT dat de koppeling net veranderd is,
        # dus de gecachete main-categories van dat facet zijn per definitie van vóór
        # die wijziging. Met de default max_age_days=7 kreeg de ontvangende maincat
        # daardoor 0 in facets_attached voor precies het event dat die kolom moest
        # vullen. Voor die facetten opnieuw ophalen en het antwoord laten winnen.
        attach_fids = set()
        for ev, fid, vid, _c in pre:
            if ev.get("entityName") in ("Category Facet", "Category Facet Setting"):
                f = fid or (vmap.get(vid, (None, None))[0] if vid else None)
                if f:
                    attach_fids.add(f)
        if attach_fids:
            fmap.update(_resolve_facets(attach_fids, max_age_days=0))

        # ---- pass 4: rows
        rows = []
        for ev, fid, vid, cid in pre:
            vname = None
            if vid and vid in vmap:
                fid = fid or vmap[vid][0]
                vname = vmap[vid][1]
            meta = fmap.get(fid) or {}
            mcs = list(meta.get("main_cat_ids") or [])
            name = ev.get("entityName")
            # Het event draagt zelf een CategoryId, en dat werd wel opgeslagen maar
            # nooit gelezen. Attributie liep volledig via een LIVE lookup van de
            # huidige main-categories — dus een DELETE gaf 404 (mcs leeg) en een
            # ontkoppeling werd toegekend aan de maincats die OVERBLIJVEN, nooit aan
            # de maincat die het facet net verloor. Precies de events waarvoor deze
            # tool bestaat. Vul aan met de maincat van het event zelf.
            if cid:
                own = _maincat_of_category(cid)
                if own and own not in mcs:
                    mcs.append(own)
            if mcs:
                resolution = "resolved"
            elif fid and fid in _LOOKUP_FAILED:
                # Onderscheid "dit facet hangt nergens" van "we konden het niet
                # opvragen". Het tweede is repareerbaar; als no_maincat wegschrijven
                # maakte een transiënte 502 permanent, want het standaardvenster is
                # maar last_ts - 1 dag.
                resolution = "lookup_failed"
            elif name == "Category Facet Setting" and ev.get("action") == "UPDATE" \
                    and not fid:
                # The documented hole: a settings-row id with no way back to a
                # category. Flagged, never folded into a main category's totals.
                resolution = "no_link"
            elif fid:
                resolution = "no_maincat"   # facet exists but hangs under no maincat
            else:
                resolution = "no_link"
            ch = ev.get("changes") or {}
            rows.append((
                ev["id"], ev["timestampUtc"][:26], ev.get("action"), name,
                str(ev.get("entityId")) if ev.get("entityId") is not None else None,
                fid, vid, cid, mcs, meta.get("facet_name"), vname,
                sorted(ch.keys()), Json(_clean_json(ch)),
                _clean(ev.get("user")), resolution,
            ))

        _set(phase=f"writing {len(rows)} events")
        new = 0
        if rows:
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                for i in range(0, len(rows), 2000):
                    chunk = rows[i:i + 2000]
                    execute_values(cur, """
                        INSERT INTO pa.facet_watch_events
                            (audit_id, ts_utc, action, entity_name, entity_id,
                             facet_id, facet_value_id, category_id, main_cat_ids,
                             facet_name, value_name, changed_fields, changes, actor,
                             resolution)
                        VALUES %s
                        ON CONFLICT (audit_id) DO UPDATE SET
                            main_cat_ids = EXCLUDED.main_cat_ids,
                            facet_id     = COALESCE(EXCLUDED.facet_id,
                                                    pa.facet_watch_events.facet_id),
                            facet_name   = COALESCE(EXCLUDED.facet_name,
                                                    pa.facet_watch_events.facet_name),
                            value_name   = COALESCE(EXCLUDED.value_name,
                                                    pa.facet_watch_events.value_name),
                            resolution   = EXCLUDED.resolution
                    """, chunk)
                    conn.commit()
                    new += len(chunk)
            finally:
                cur.close()
                return_db_connection(conn)
        _set(events_new=new)

        msg = (f"{len(raw)} facet events in window, {new} written, "
               f"{get_run_state()['lookups']} api lookups")
        stopped = _stop.is_set()
        if stopped:
            # De pagineerlus breekt midden in het venster af, dus dit is een
            # GEDEELTELIJKE ingest. De DB-rij wist dat al; de module-state die de
            # frontend pollt stond hardgecodeerd op "done" met een bericht dat las
            # als een volledig venster.
            msg += " — AFGEBROKEN, venster niet compleet"
        _finish_run(run_id, "stopped" if stopped else "done", msg, new)
        _set(status="stopped" if stopped else "done",
             phase="stopped" if stopped else "done",
             message=msg, finished_at=time.time())
        return {"success": not stopped, "events": len(raw), "written": new,
                "stopped": stopped, "message": msg}
    except Exception as e:
        logger.exception("facet-watch ingest failed")
        _finish_run(run_id, "error", str(e), 0)
        _set(status="error", phase="error", message=str(e), finished_at=time.time())
        return {"success": False, "message": str(e)}


def _finish_run(run_id, status, message, new):
    if run_id is None:
        return
    st = get_run_state()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""UPDATE pa.facet_watch_runs
                       SET finished_at = now(), status = %s, message = %s,
                           events_seen = %s, events_new = %s, lookups = %s
                       WHERE id = %s""",
                    (status, message, st.get("events_seen") or 0, new,
                     st.get("lookups") or 0, run_id))
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)


def start_ingest_async(from_date=None, to_date=None):
    if get_run_state().get("status") == "running":
        return {"success": False, "message": "an ingest is already running"}
    t = threading.Thread(target=ingest, args=(from_date, to_date), daemon=True)
    t.start()
    return {"success": True, "message": "ingest started"}


# ---------------------------------------------------------------------------
# Read helpers for the frontend
# ---------------------------------------------------------------------------
def get_overview(days=1, exclude_auto=True):
    """Per main category, what happened in the last `days` days.

    A facet on several main categories is counted under EACH of them (unnest), so
    the column totals exceed the event count on purpose -- a shared facet really did
    change for every main category that shows it.

    `exclude_auto` drops the product-line facet family (see AUTO_FACET_SLUG_PREFIXES);
    leave it on unless you specifically want to see the automated churn.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            WITH ev AS (
                SELECT e.*, {_AUTO_SQL} AS is_auto,
                       unnest(e.main_cat_ids) AS main_cat_id
                FROM pa.facet_watch_events e
                LEFT JOIN pa.facet_watch_facet_maincat fm ON fm.facet_id = e.facet_id
                WHERE e.ts_utc > now() - (%s || ' days')::interval
            )
            SELECT m.main_cat_id,
                   COALESCE(m.name, '(' || m.main_cat_id || ')')        AS main_cat_name,
                   count(*)                                            AS events,
                   count(DISTINCT ev.facet_id)                         AS facets_touched,
                   count(DISTINCT CASE WHEN ev.entity_name = 'Facet'
                                        AND ev.action = 'INSERT'
                                       THEN ev.facet_id END)           AS facets_new,
                   count(DISTINCT CASE WHEN ev.entity_name = 'Category Facet'
                                        AND ev.action = 'INSERT'
                                       THEN ev.facet_id END)           AS facets_attached,
                   count(*) FILTER (WHERE ev.entity_name IN ('Facet Value',
                                                             'Facet Value Label')
                                      AND ev.action = 'INSERT')        AS values_added,
                   count(*) FILTER (WHERE ev.entity_name = 'Facet Value'
                                      AND ev.action = 'DELETE')        AS values_deleted,
                   count(*) FILTER (WHERE ev.action = 'UPDATE')        AS updates,
                   count(*) FILTER (WHERE ev.is_auto)                  AS auto_events,
                   max(ev.ts_utc)                                      AS last_change,
                   count(DISTINCT ev.actor)                            AS actors
            FROM ev
            JOIN pa.facet_watch_maincats m ON m.main_cat_id = ev.main_cat_id
            WHERE (NOT %s OR NOT ev.is_auto)
            GROUP BY 1, 2
            ORDER BY events DESC, main_cat_name
        """, (days, exclude_auto))
        rows = [dict(r) for r in cur.fetchall()]

        # Events that could not be tied to a main category, reported rather than
        # hidden -- see the module docstring on Category Facet Setting UPDATE.
        cur.execute("""
            SELECT resolution, entity_name, action, count(*) AS events
            FROM pa.facet_watch_events
            WHERE ts_utc > now() - (%s || ' days')::interval
              AND resolution <> 'resolved'
            GROUP BY 1, 2, 3
            ORDER BY events DESC
        """, (days,))
        unattributed = [dict(r) for r in cur.fetchall()]

        # How much the exclude_auto filter is hiding, so the number is never silent.
        cur.execute(f"""
            SELECT count(*) AS events, count(DISTINCT e.facet_id) AS facets
            FROM pa.facet_watch_events e
            LEFT JOIN pa.facet_watch_facet_maincat fm ON fm.facet_id = e.facet_id
            WHERE e.ts_utc > now() - (%s || ' days')::interval AND {_AUTO_SQL}
        """, (days,))
        auto = dict(cur.fetchone() or {})
        return {"main_categories": rows, "unattributed": unattributed,
                "auto_excluded": auto, "exclude_auto": exclude_auto}
    finally:
        cur.close()
        return_db_connection(conn)


def get_facets(days=1, main_cat_id=None, limit=300, exclude_auto=True):
    """The actual answer to "welke facetten zijn het laatst aangemaakt/gewijzigd":
    one row per (facet, main category), newest change first."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            WITH ev AS (
                SELECT e.*, {_AUTO_SQL} AS is_auto,
                       fm.facet_slug, fm.category_count, fm.is_enabled_anywhere,
                       fm.facet_name AS cached_facet_name,
                       unnest(e.main_cat_ids) AS main_cat_id
                FROM pa.facet_watch_events e
                LEFT JOIN pa.facet_watch_facet_maincat fm ON fm.facet_id = e.facet_id
                WHERE e.ts_utc > now() - (%s || ' days')::interval
                  AND e.facet_id IS NOT NULL
            )
            SELECT ev.facet_id,
                   COALESCE(max(ev.cached_facet_name), max(ev.facet_name)) AS facet_name,
                   max(ev.facet_slug)                              AS facet_slug,
                   bool_or(ev.is_auto)                             AS is_auto,
                   ev.main_cat_id,
                   COALESCE(m.name, '(' || ev.main_cat_id || ')')   AS main_cat_name,
                   min(ev.ts_utc)                                  AS first_change,
                   max(ev.ts_utc)                                  AS last_change,
                   count(*)                                        AS events,
                   bool_or(ev.entity_name = 'Facet' AND ev.action = 'INSERT')
                                                                   AS is_new_facet,
                   bool_or(ev.entity_name = 'Category Facet' AND ev.action = 'INSERT')
                                                                   AS newly_attached,
                   count(*) FILTER (WHERE ev.entity_name LIKE 'Facet Value%%'
                                      AND ev.action = 'INSERT')    AS values_added,
                   count(*) FILTER (WHERE ev.entity_name = 'Facet Value'
                                      AND ev.action = 'DELETE')    AS values_deleted,
                   count(*) FILTER (WHERE ev.action = 'UPDATE')    AS updates,
                   array_agg(DISTINCT ev.actor)                    AS actors,
                   max(ev.category_count)                          AS category_count,
                   bool_or(ev.is_enabled_anywhere)                 AS enabled_anywhere
            FROM ev
            LEFT JOIN pa.facet_watch_maincats m ON m.main_cat_id = ev.main_cat_id
            WHERE (%s::int IS NULL OR ev.main_cat_id = %s)
              AND (NOT %s OR NOT ev.is_auto)
            GROUP BY ev.facet_id, ev.main_cat_id, m.name
            ORDER BY max(ev.ts_utc) DESC
            LIMIT %s
        """, (days, main_cat_id, main_cat_id, exclude_auto, limit))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def get_events(days=1, main_cat_id=None, facet_id=None, entity_name=None,
               action=None, actor=None, limit=500, offset=0):
    """Raw event rows behind the aggregates."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT e.audit_id, e.ts_utc, e.action, e.entity_name, e.entity_id,
                   e.facet_id, e.facet_value_id, e.category_id, e.main_cat_ids,
                   COALESCE(e.facet_name, fm.facet_name) AS facet_name,
                   e.value_name, e.changed_fields, e.changes, e.actor, e.resolution,
                   (SELECT array_agg(m.name ORDER BY m.name)
                    FROM pa.facet_watch_maincats m
                    WHERE m.main_cat_id = ANY(e.main_cat_ids)) AS main_cat_names
            FROM pa.facet_watch_events e
            LEFT JOIN pa.facet_watch_facet_maincat fm ON fm.facet_id = e.facet_id
            WHERE e.ts_utc > now() - (%s || ' days')::interval
              AND (%s::int  IS NULL OR %s = ANY(e.main_cat_ids))
              AND (%s::int  IS NULL OR e.facet_id = %s)
              AND (%s::text IS NULL OR e.entity_name = %s)
              AND (%s::text IS NULL OR e.action = %s)
              AND (%s::text IS NULL OR e.actor = %s)
            ORDER BY e.ts_utc DESC, e.audit_id DESC
            LIMIT %s OFFSET %s
        """, (days, main_cat_id, main_cat_id, facet_id, facet_id,
              entity_name, entity_name, action, action, actor, actor,
              limit, offset))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def get_deletions(days=1, limit=300):
    """Route C — `GET /api/garbage-bin`. The audit log's DELETE rows give the id and
    the old field values but NOT the name; the garbage bin gives the name plus the
    30-day restore window, which is what makes a deletion readable. Live call: the
    bin is small and this is a read-only side panel."""
    since = (datetime.now(timezone.utc).replace(tzinfo=None)
             - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        d = _get("/api/garbage-bin", {"DeletedAfter": since, "Take": limit,
                                      "SortBy": "deletedAtUtc",
                                      "SortDescending": "true"})
    except requests.RequestException as e:
        return {"error": str(e), "items": [], "total": 0}
    items = d.get("items") or []
    # Enrich with the facet the value belonged to, from the cache we already hold.
    vids = [int(i["entityId"]) for i in items
            if i.get("entityType") == "Facet Value"
            and str(i.get("entityId", "")).isdigit()]
    vmap = _load_value_facet(set(vids))
    fmap = _resolve_facets({v[0] for v in vmap.values()})
    for i in items:
        fid = None
        if i.get("entityType") == "Facet Value" and str(i.get("entityId", "")).isdigit():
            fid = vmap.get(int(i["entityId"]), (None, None))[0]
        meta = fmap.get(fid) or {}
        i["facetId"] = fid
        i["facetName"] = meta.get("facet_name")
        i["mainCatIds"] = meta.get("main_cat_ids") or []
    return {"items": items, "total": d.get("total"), "error": None}


def get_status():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT count(*) AS events, min(ts_utc) AS oldest, max(ts_utc) AS newest,
                   count(*) FILTER (WHERE resolution = 'resolved')  AS resolved,
                   count(*) FILTER (WHERE resolution <> 'resolved') AS unattributed
            FROM pa.facet_watch_events
        """)
        ev = dict(cur.fetchone() or {})
        cur.execute("SELECT count(*) AS n FROM pa.facet_watch_value_facet")
        ev["value_facet_cache"] = (cur.fetchone() or {}).get("n", 0)
        cur.execute("SELECT count(*) AS n FROM pa.facet_watch_facet_maincat")
        ev["facet_maincat_cache"] = (cur.fetchone() or {}).get("n", 0)
        cur.execute("SELECT count(*) AS n FROM pa.facet_watch_maincats")
        ev["main_categories"] = (cur.fetchone() or {}).get("n", 0)
        cur.execute("""SELECT id, started_at, finished_at, from_date, to_date,
                              events_seen, events_new, lookups, status, message
                       FROM pa.facet_watch_runs ORDER BY id DESC LIMIT 10""")
        ev["runs"] = [dict(r) for r in cur.fetchall()]
        return ev
    finally:
        cur.close()
        return_db_connection(conn)


def get_main_categories():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT main_cat_id, name FROM pa.facet_watch_maincats
                       ORDER BY name NULLS LAST""")
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


# ---------------------------------------------------------------------------
# Nieuwe productlijnen
# ---------------------------------------------------------------------------
# De naam van een facetwaarde staat NIET in de `Facet Value` INSERT -- die draagt
# alleen {FacetId, CreatedAt, SeoPriority, InvalidReason}. De naam zit in de
# `Facet Value Label` INSERT die er direct achteraan komt, als
# `changes->>'NameInColumn'`. Gemeten over de hele store: 42.943 label-inserts tegen
# 41.782 value-inserts, en voor de 20 verse "Productlijnen: Sage"-waarden was
# `value_name` leeg terwijl het label "Sage the Barista Pro" wél vastlag. Daarom is
# het label-event de primaire naambron en zijn `value_name` / de value-cache alleen
# fallback -- omgekeerd (cache eerst) miste elke waarde die na de laatste seed kwam.
_PL_NAME = ("COALESCE(v.label_name, v.ev_name, vf.value_name)")

# Zelfde familie-definitie als _AUTO_SQL hierboven, maar hier is de familie het
# ONDERWERP in plaats van ruis: dit is de enige module die er bewust naar kijkt.
_PL_FAMILY_SQL = ("(fm.facet_name = 'Productlijn'"
                  " OR fm.facet_name LIKE 'Productlijnen:%%')")


def get_product_lines(days=30, limit=500):
    """Nieuwe productlijnen: facetwaarden die in de productlijn-familie zijn
    aangemaakt, met hun merk, de main categorieën waar ze opduiken en -- als het
    is opgehaald -- het maandelijkse zoekvolume van de naam.

    Ontdubbeld op (merk, naam). ListsApi maakt hetzelfde productlijn-facet
    herhaaldelijk opnieuw aan onder dezelfde naam met een NIEUW facet-id en nieuwe
    waarde-ids: "Productlijnen: Kärcher" staat 9× in de store, elke keer met
    dezelfde 10 waarden. Zonder ontdubbelen leest de module als 90 nieuwe
    productlijnen waar er 10 zijn. `incarnations` houdt bij hoe vaak het langskwam,
    zodat het niet stil verdwijnt.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            WITH pl AS (
                SELECT fm.facet_id, fm.facet_name, fm.main_cat_ids
                FROM pa.facet_watch_facet_maincat fm
                WHERE {_PL_FAMILY_SQL}
            ),
            -- Eén rij per (waarde, facet): de value-insert en zijn label-insert
            -- dragen samen het feit "deze waarde is hier nieuw" plus de naam.
            v AS (
                SELECT e.facet_value_id, e.facet_id,
                       min(e.ts_utc)                             AS first_seen,
                       max(e.ts_utc)                             AS last_seen,
                       max(e.changes->>'NameInColumn')           AS label_name,
                       max(e.value_name)                         AS ev_name,
                       bool_or(e.entity_name = 'Facet Value')    AS has_value_row,
                       max(e.actor)                              AS actor
                FROM pa.facet_watch_events e
                WHERE e.ts_utc > now() - (%s || ' days')::interval
                  AND e.action = 'INSERT'
                  AND e.entity_name IN ('Facet Value', 'Facet Value Label')
                  AND e.facet_value_id IS NOT NULL
                  AND e.facet_id IN (SELECT facet_id FROM pl)
                GROUP BY 1, 2
            ),
            named AS (
                SELECT v.*, pl.facet_name,
                       {_PL_NAME} AS product_line,
                       -- Merk uit de facetnaam: "Productlijnen: Kärcher" -> Kärcher.
                       -- Het generieke per-maincat facet heet gewoon "Productlijn"
                       -- en draagt geen merk; dat blijft NULL in plaats van dat we
                       -- er een merk bij verzinnen.
                       CASE WHEN pl.facet_name LIKE 'Productlijnen: %%'
                            THEN substring(pl.facet_name from 16)
                       END AS brand,
                       -- Hier al platgeslagen naar één rij per maincat: het facet
                       -- hangt aan een reeks main categorieën en de module wil ze
                       -- per productlijn samengevoegd zien. count(DISTINCT ...)
                       -- hieronder is ongevoelig voor de duplicatie die dat geeft.
                       mc.main_cat_id
                FROM v
                JOIN pl ON pl.facet_id = v.facet_id
                LEFT JOIN pa.facet_watch_value_facet vf ON vf.value_id = v.facet_value_id
                LEFT JOIN LATERAL unnest(pl.main_cat_ids) AS mc(main_cat_id) ON true
            ),
            -- De zoekterm is MERK + LIJN, niet de lijnnaam alleen. Gemeten
            -- 03-09-2026: "tasman" is als los woord niets, "ugg tasman" doet
            -- 18.100/mnd. Een lijnnaam die het merk al vooraan draagt ("Sage the
            -- Barista Pro") krijgt het er niet nog eens bij.
            kw AS (
                SELECT n.*,
                       lower(CASE
                         WHEN n.brand IS NOT NULL
                              AND lower(n.product_line) NOT LIKE lower(n.brand) || '%%'
                         THEN n.brand || ' ' || n.product_line
                         ELSE n.product_line
                       END) AS keyword
                FROM named n
            )
            SELECT n.brand,
                   min(n.product_line)                        AS product_line,
                   min(n.keyword)                             AS keyword,
                   count(DISTINCT n.facet_id)                 AS incarnations,
                   count(DISTINCT n.facet_value_id)           AS value_ids,
                   min(n.first_seen)                          AS first_seen,
                   max(n.last_seen)                           AS last_seen,
                   array_agg(DISTINCT n.facet_id)             AS facet_ids,
                   array_remove(array_agg(DISTINCT m.name), NULL) AS main_cat_names,
                   array_remove(array_agg(DISTINCT n.actor), NULL) AS actors,
                   max(kv.search_volume)                      AS search_volume,
                   max(kv.fetched_at)                         AS volume_fetched_at
            FROM kw n
            LEFT JOIN pa.facet_watch_maincats m ON m.main_cat_id = n.main_cat_id
            LEFT JOIN pa.facet_watch_keyword_volume kv ON kv.keyword = n.keyword
            WHERE n.product_line IS NOT NULL AND n.product_line <> ''
            GROUP BY n.brand, lower(n.product_line)
            ORDER BY max(kv.search_volume) DESC NULLS LAST, min(n.first_seen) DESC
            LIMIT %s
        """, (days, limit))
        rows = [dict(r) for r in cur.fetchall()]

        # Nieuwe MERKEN in de familie: een "Productlijnen: X"-facet dat er nog niet
        # was. Dat is een ander feit dan een nieuwe lijn eronder -- het zegt dat een
        # merk voor het eerst productlijnen krijgt -- en hoort daarom apart geteld.
        cur.execute(f"""
            SELECT count(DISTINCT fm.facet_name) AS brands,
                   count(*)                      AS facet_inserts
            FROM pa.facet_watch_events e
            JOIN pa.facet_watch_facet_maincat fm ON fm.facet_id = e.facet_id
            WHERE e.ts_utc > now() - (%s || ' days')::interval
              AND e.entity_name = 'Facet' AND e.action = 'INSERT'
              AND {_PL_FAMILY_SQL}
        """, (days,))
        new_brands = dict(cur.fetchone() or {})

        missing = sum(1 for r in rows if r.get("search_volume") is None)
        return {"product_lines": rows, "new_brand_facets": new_brands,
                "without_volume": missing, "days": days}
    finally:
        cur.close()
        return_db_connection(conn)


def fetch_product_line_volumes(names, chunk=500):
    """Zoekvolume ophalen voor productlijn-namen en cachen.

    Chunks van 500 en niet de 10.000 die `keyword_planner_service.BATCH_SIZE`
    toestaat: de Keyword Planner laat in grote batches stil rijen weg (zie memory
    keyword_planner_large_batch_drops_rows), en een ontbrekende rij is hier niet te
    onderscheiden van "0 zoekvolume". Wat na een chunk niet terugkwam gaat één keer
    apart opnieuw mee; blijft het dan weg, dan wordt het als 0 gecached en zegt de
    UI erbij wanneer het gemeten is.
    """
    from backend.keyword_planner_service import get_search_volumes

    wanted = sorted({(n or "").strip().lower() for n in names if (n or "").strip()})
    if not wanted:
        return {"requested": 0, "fetched": 0, "cached": 0}

    got = {}
    for i in range(0, len(wanted), chunk):
        batch = wanted[i:i + chunk]
        res = get_search_volumes(batch) or {}
        for r in res.get("results", []):
            key = (r.get("original_keyword") or "").strip().lower()
            if key:
                got[key] = int(r.get("search_volume") or 0)
        # Retry alleen wat NIET terugkwam -- niet wat 0 teruggaf.
        missing = [k for k in batch if k not in got]
        if missing:
            res2 = get_search_volumes(missing) or {}
            for r in res2.get("results", []):
                key = (r.get("original_keyword") or "").strip().lower()
                if key:
                    got[key] = int(r.get("search_volume") or 0)
            for k in missing:
                got.setdefault(k, 0)

    rows = [(k, v) for k, v in got.items()]
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        execute_values(cur, """
            INSERT INTO pa.facet_watch_keyword_volume (keyword, search_volume)
            VALUES %s
            ON CONFLICT (keyword) DO UPDATE
              SET search_volume = EXCLUDED.search_volume, fetched_at = now()
        """, rows)
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)
    return {"requested": len(wanted), "fetched": len(rows),
            "with_volume": sum(1 for _, v in rows if v > 0)}


# ---------------------------------------------------------------------------
# Verhuisde facetten (slug-wijzigingen)
# ---------------------------------------------------------------------------
# Wat "verhuisd" hier betekent, en waarom het NIET op naam matcht
# ------------------------------------------------------------------------
# De eerste lezing van "verwijderd op de ene plek, onder dezelfde naam terug op een
# andere" was een DELETE/INSERT-paar op naam. Dat is in deze store niet te bouwen en
# zou ook het verkeerde meten:
#
#   * Er staat GEEN ENKEL `Facet` DELETE of `Category Facet` DELETE in de store
#     (14 entity/action-combinaties, 103k events) -- een facet wordt nooit als
#     verwijderd gelogd, dus de "verwijderd"-helft van het paar bestaat niet.
#   * 12.758 van de 13.113 `Facet Value` DELETEs dragen geen naam; via de
#     value-cache is er 1.164 te herstellen, dus ~91% blijft naamloos.
#   * Wat een naam-match dan wél oplevert is ruis: 1.493 paren over 206 namen, en
#     dat zijn vrijwel allemaal MERK-waarden die tussen de 32 per-maincat
#     `Merk`-facetten schuiven ("acelera" weg bij Merk-Speelgoed, aan bij
#     Merk-Kleding). Dat is een merk dat producten verliest in de ene maincat en
#     wint in de andere -- twee losse pagina's in twee categorieën, geen verhuizing,
#     en een redirect ertussen zou een bezoeker naar een andere categorie sturen.
#
# Wat er WEL is, en wat exact hetzelfde probleem oplost: `Facet Label` UPDATE met
# `UrlSlug` in de changed_fields, dat Old EN New meelevert. De facet-slug staat
# letterlijk in de URL (`/products/<pad>/c/<slug>~<value-id>`), dus een slug-
# wijziging is precies "dezelfde pagina, nieuwe plek" -- met beide kanten hard
# geregistreerd in plaats van geraden.
#
# Waarom dat écht een redirect nodig heeft (gemeten 03-09-2026): de site vangt de
# oude slug wel op, maar 301't naar de KALE categorie en gooit het facet weg:
#   beslist.be/products/meubilair/meubilair_389370/c/stijl_test~393710
#     -> 301 -> beslist.be/products/meubilair/meubilair_389370/
# terwijl .../c/woonstijl~393710 200 geeft. De bezoeker verliest zijn filter.
#
# Twee valkuilen die de implementatie hieronder afdekt:
#
# 1. EEN SLUG IS NIET UNIEK OVER FACETTEN. `type` komt in 1.313 pa.urls-rijen voor,
#    verdeeld over meerdere facetten. Van facet 4501, dat `type` -> `format`
#    hernoemde, hoort daar 0 van bij. Daarom wordt de URL-set niet op de slug
#    gekozen maar op de VALUE-IDS van dat facet (pa.facet_watch_value_facet):
#    `/<slug>~<value-id>` met een value-id dat aantoonbaar in dit facet zit.
#    Ongefilterd zou een `type`-hernoeming 1.313 vreemde URL's hebben omgeleid.
#
# 2. DE LOCALE STAAT NIET IN HET EVENT. Elk label-event is per locale, maar de
#    payload draagt alleen Name/UrlSlug. De vier locales zijn alleen te scheiden
#    door de HUIDIGE slugs live op te halen (`GET /api/Facets/{id}`) en de keten
#    terug te lopen. Dat is geen detail: de wijzigingen van augustus landden op
#    nl-BE, niet op nl-NL -- facet 2931 heeft nl-NL `stijl_test` (nog steeds 200 op
#    beslist.nl) en nl-BE `woonstijl`. Wie de locale negeert zet een redirect op
#    .nl die de goede pagina wegduwt. Terugwaarts lopen vangt ook de flip-flops:
#    facet 2952 ging `opties` -> `met_matras_bed` -> `opties`, netto niets, en dat
#    hoort er niet als verhuizing in te staan.
_REDIRECT_COUNTRY = {"nl-NL": "NL", "nl-BE": "BE"}


def _facet_locale_slugs(facet_id):
    """locale -> huidige urlSlug, live. Geen cache: `facet_watch_facet_maincat`
    bewaart alleen de nl-NL-slug, en juist het onderscheid tussen de locales is
    hier de vraag."""
    try:
        f = _get(f"/api/Facets/{facet_id}")
    except requests.RequestException:
        return {}
    out = {}
    for l in f.get("labels") or []:
        loc, slug = l.get("locale"), _clean(l.get("urlSlug"))
        if loc and slug:
            out[loc] = slug
    return out


def _resolve_slug_move(events, final_slug):
    """Welke wijziging bracht deze locale op `final_slug`?

    Geeft `(old_slug, [events], None)`, of `(None, [], reden)` als het niet
    eenduidig is.

    Eén hop, met opzet. Een keten terugwandelen (`new` van de vorige stap is de
    `old` van de volgende) lijkt completer, maar de events dragen GEEN locale en de
    vier locales van een facet delen hun slugs: facet 2952 heeft nl-NL
    `opties -> met_matras_bed` en nl-BE `met_matras_bed -> opties`. Een wandeling
    pakt dan de wijziging van de ándere locale als volgende stap, en levert een
    redirect op die een werkende pagina wegduwt. Bij één hop kan dat niet: we
    vragen alleen wie op de huidige slug is uitgekomen.

    Meerdere kandidaten met een verschillende `old` (facet 4501 en-US: zowel
    `t_papier -> format` als `type -> format`) zijn niet te scheiden en worden
    gemeld in plaats van gekozen. Exacte duplicaten -- de audit log logt een
    wijziging soms twee keer -- geven hetzelfde antwoord en zijn dus geen conflict.
    """
    cands = [e for e in events if e["new"] == final_slug]
    if not cands:
        return None, [], None                       # deze locale is niet gewijzigd
    olds = {e["old"] for e in cands}
    if len(olds) > 1:
        return None, [], ("meerdere wijzigingen komen uit op '%s' (%s) — welke bij "
                          "deze locale hoort is niet uit het event te halen"
                          % (final_slug, ", ".join(sorted(olds))))
    old = olds.pop()
    if old == final_slug:
        return None, [], None                       # heen en terug, netto niets
    return old, cands, None


def get_moved_facets(days=30, with_url_counts=True):
    """Facetten waarvan de URL-slug is veranderd, per locale, met het aantal
    bestaande URL's dat erdoor van adres wisselt."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT e.audit_id, e.ts_utc, e.facet_id, e.actor,
                   e.changes->'UrlSlug'->>'Old'  AS old_slug,
                   e.changes->'UrlSlug'->>'New'  AS new_slug,
                   e.changes->'Name'->>'New'     AS new_name,
                   fm.facet_name,
                   (SELECT array_agg(m.name ORDER BY m.name)
                      FROM pa.facet_watch_maincats m
                     WHERE m.main_cat_id = ANY(fm.main_cat_ids)) AS main_cat_names
            FROM pa.facet_watch_events e
            LEFT JOIN pa.facet_watch_facet_maincat fm ON fm.facet_id = e.facet_id
            WHERE e.ts_utc > now() - (%s || ' days')::interval
              AND e.entity_name = 'Facet Label' AND e.action = 'UPDATE'
              AND 'UrlSlug' = ANY(e.changed_fields)
              AND e.changes->'UrlSlug'->>'Old' IS NOT NULL
              AND e.changes->'UrlSlug'->>'New' IS NOT NULL
            ORDER BY e.facet_id, e.ts_utc
        """, (days,))
        raw = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)

    by_facet = {}
    for r in raw:
        by_facet.setdefault(r["facet_id"], []).append(
            {"old": r["old_slug"], "new": r["new_slug"], "ts": r["ts_utc"],
             "actor": r["actor"], "new_name": r["new_name"]})

    rows, unresolved = [], []
    for fid, evs in by_facet.items():
        meta = next(r for r in raw if r["facet_id"] == fid)
        locales = _facet_locale_slugs(fid)
        if not locales:
            # Zonder de live labels is de locale niet te bepalen. Melden, niet gokken:
            # een redirect op de verkeerde locale duwt een werkende pagina weg.
            unresolved.append({"facet_id": fid, "facet_name": meta["facet_name"],
                               "events": len(evs),
                               "reason": "labels niet op te halen bij de Taxonomy API"})
            continue
        for locale, final_slug in sorted(locales.items()):
            old_slug, used, why = _resolve_slug_move(evs, final_slug)
            if why:
                unresolved.append({"facet_id": fid, "facet_name": meta["facet_name"],
                                   "locale": locale, "events": len(evs),
                                   "reason": why})
                continue
            if not old_slug:
                continue                      # niets veranderd, of heen en terug
            rows.append({
                "facet_id": fid,
                "facet_name": meta["facet_name"],
                "main_cat_names": meta["main_cat_names"] or [],
                "locale": locale,
                "old_slug": old_slug,
                "new_slug": final_slug,
                "first_change": min(e["ts"] for e in used),
                "last_change": max(e["ts"] for e in used),
                "actors": sorted({e["actor"] for e in used if e["actor"]}),
                "redirect_country": _REDIRECT_COUNTRY.get(locale),
                "affected_urls": None,
            })

    if with_url_counts and rows:
        counts = _count_affected_urls([(r["facet_id"], r["old_slug"]) for r in rows])
        for r in rows:
            r["affected_urls"] = counts.get((r["facet_id"], r["old_slug"]), 0)

    # Meeste te repareren URL's eerst; dat is de enige ordening die zegt waar het
    # werk zit. Bij gelijk aantal de nieuwste wijziging boven.
    rows.sort(key=lambda r: (-(r["affected_urls"] or 0),
                             -(r["last_change"].timestamp() if r["last_change"] else 0)))
    return {"moved": rows, "unresolved": unresolved, "days": days,
            "slug_events": len(raw)}


def _count_affected_urls(pairs):
    """(facet_id, old_slug) -> aantal pa.urls-rijen dat die slug MET een value-id van
    dat facet draagt. Zie valkuil 1 hierboven: op de slug alleen tellen is fout."""
    if not pairs:
        return {}
    conn = get_db_connection()
    cur = conn.cursor()
    out = {}
    try:
        for fid, slug in pairs:
            cur.execute("""
                SELECT count(*) AS n
                FROM pa.urls u
                WHERE (u.url LIKE '%%/' || %s || '~%%'
                       OR u.url LIKE '%%~~' || %s || '~%%')
                  AND EXISTS (
                        SELECT 1 FROM pa.facet_watch_value_facet vf
                         WHERE vf.facet_id = %s
                           AND (u.url LIKE '%%/'  || %s || '~' || vf.value_id || '%%'
                             OR u.url LIKE '%%~~' || %s || '~' || vf.value_id || '%%'))
            """, (slug, slug, fid, slug, slug))
            out[(fid, slug)] = (cur.fetchone() or {}).get("n", 0)
        return out
    finally:
        cur.close()
        return_db_connection(conn)


def build_moved_facet_redirects(facet_id, old_slug, new_slug, limit=5000):
    """De concrete oude -> nieuwe URL-paren voor één slug-wijziging.

    Het hernoemen én het HERSORTEREN komen uit
    `redirect_301_service.transform_and_sort_url`: de facetten in een `/c/`-pad staan
    alfabetisch op slug, dus `stijl_test` -> `woonstijl` verplaatst het facet naar
    achteren in datzelfde pad. Alleen de naam vervangen geeft een URL die de site
    zelf weer zou omleiden. Die sorteerregel stond er al voor de Redirect Generator
    en wordt hier hergebruikt in plaats van nagebouwd.

    De regels krijgen het VALUE-ID mee (`slug~123` -> `new~123`) en niet alleen de
    slug, zodat een URL die toevallig een gelijknamig facet van een ánder facet-id
    draagt niet meeverandert.
    """
    from backend.redirect_301_service import FacetRule, transform_and_sort_url

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT value_id FROM pa.facet_watch_value_facet
                       WHERE facet_id = %s""", (facet_id,))
        value_ids = [r["value_id"] for r in cur.fetchall()]
        if not value_ids:
            return {"pairs": [], "total": 0, "value_ids": 0,
                    "note": "Geen waarden van dit facet in de value-cache — "
                            "zonder value-ids is de URL-set niet af te bakenen."}

        cur.execute("""
            SELECT u.url
            FROM pa.urls u
            WHERE (u.url LIKE '%%/' || %s || '~%%' OR u.url LIKE '%%~~' || %s || '~%%')
              AND EXISTS (
                    SELECT 1 FROM unnest(%s::bigint[]) AS v(value_id)
                     WHERE u.url LIKE '%%/'  || %s || '~' || v.value_id || '%%'
                        OR u.url LIKE '%%~~' || %s || '~' || v.value_id || '%%')
            ORDER BY u.url
            LIMIT %s
        """, (old_slug, old_slug, value_ids, old_slug, old_slug, limit))
        urls = [r["url"] for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)

    rules = [FacetRule(old_facet=f"{old_slug}~{vid}", new_facet=f"{new_slug}~{vid}")
             for vid in value_ids]
    pairs, unchanged = [], 0
    for u in urls:
        new_url, changed = transform_and_sort_url(u, facet_rules=rules)
        if not changed or new_url == u:
            unchanged += 1
            continue
        pairs.append({"old": u, "new": new_url})
    return {"pairs": pairs, "total": len(pairs), "value_ids": len(value_ids),
            "scanned": len(urls), "unchanged": unchanged,
            "truncated": len(urls) >= limit}
