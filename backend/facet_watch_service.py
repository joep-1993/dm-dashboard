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

from backend.database import get_db_connection, return_db_connection

logger = logging.getLogger(__name__)

TAX_BASE = "http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl"
# Every GET is open internally; the header is what makes a read attributable in the
# audit log we are reading from. See memory taxonomy_api_user_header.
USER_HEADER = os.getenv("TAXONOMY_USER_NAME", "SEO_JOEP")
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
)

# Which entity names describe the FACET itself rather than one of its values. Used
# by the overview to separate "a new facet appeared" from "a value was added".
FACET_LEVEL = ("Facet", "Facet Label", "Category Facet", "Category Facet Setting")

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
"""

_session = None
_session_lock = threading.Lock()


def _sess():
    global _session
    with _session_lock:
        if _session is None:
            s = requests.Session()
            s.headers.update({"Accept": "application/json",
                              "X-User-Name": USER_HEADER})
            _session = s
        return _session


def _get(path, params=None, timeout=HTTP_TIMEOUT):
    r = _sess().get(f"{TAX_BASE}{path}", params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


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
                # rather than persisting an empty answer as fact.
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
    return facet_id, value_id, cat_id


def ingest(from_date=None, to_date=None, resolve_misses=True):
    """Pull audit events for the window, resolve them to main categories, upsert.

    Default window starts one day before the newest event already stored (or 30
    days back on an empty table). The deliberate overlap costs nothing — audit_id
    is the primary key — and covers an event written while the previous run was
    still paging.
    """
    _stop.clear()
    _set(status="running", phase="starting", events_seen=0, events_new=0,
         lookups=0, message="", started_at=time.time(), finished_at=None)
    run_id = None
    try:
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
        skip = 0
        while not _stop.is_set():
            page = _fetch_audit_page(_iso(frm), _iso(to), skip, AUDIT_PAGE)
            items = page.get("items") or []
            raw.extend(i for i in items if i.get("entityName") in FACET_ENTITIES)
            _inc("events_seen", len(items))
            skip += len(items)
            if len(items) < AUDIT_PAGE or skip >= (page.get("total") or 0):
                break
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
            if mcs:
                resolution = "resolved"
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
        _finish_run(run_id, "done" if not _stop.is_set() else "stopped", msg, new)
        _set(status="done", phase="done", message=msg, finished_at=time.time())
        return {"success": True, "events": len(raw), "written": new, "message": msg}
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
