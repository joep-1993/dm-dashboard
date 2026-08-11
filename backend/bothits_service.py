"""Bot Hits — query layer for the crawler-log dashboard.

Everything here reads the pa.bothits_* aggregates built by bothits_ingest. The
cube (pa.bothits_daily) answers the timeseries and split questions; the URL
tables answer "which pages", and are deliberately smaller than the cube's
coverage — see bothits_ingest for why.

Results are cached in-process for 5 minutes, matching seo_stats_service; the
dashboard's Refresh button passes force=True to bypass it.
"""
import logging
import threading
import time
from datetime import date, timedelta

from backend.database import get_db_connection, return_db_connection

logger = logging.getLogger(__name__)

CACHE_TTL = 300
_cache = {}
_cache_lock = threading.Lock()


def _cached(key, fn, force=False):
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and not force and now - hit[0] < CACHE_TTL:
            return hit[1]
    val = fn()
    with _cache_lock:
        _cache[key] = (now, val)
    return val


def _query(sql, params=None):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        return_db_connection(conn)


# pa.urls.main_cat_name is NULL on 968.503 of 1.031.796 rows (6.1% populated), so
# grouping on it silently answers for a sixteenth of the table. Derive the main
# category from the path instead — pa.urls is /c/-only, so segment 3 is always the
# maincat slug. The regexp folds the ~20 broken paths that carry a subcat slug in
# the maincat slot (/products/schoenen_430884/c/…) back onto their real maincat;
# it strips only at "_" followed by a digit, so tuin_accessoires survives intact.
MAIN_CAT_SQL = "regexp_replace(split_part(u.url, '/', 3), '_[0-9].*$', '')"


def _range(start_date, end_date):
    """Default to the last 30 days of data we actually have, not of wall time."""
    if start_date and end_date:
        return start_date, end_date
    rows = _query("SELECT min(log_date) AS lo, max(log_date) AS hi FROM pa.bothits_ingest")
    lo, hi = (rows[0]["lo"], rows[0]["hi"]) if rows else (None, None)
    if not hi:
        today = date.today()
        return str(today - timedelta(days=30)), str(today)
    end = end_date or str(hi)
    start = start_date or str(max(lo, date.fromisoformat(end) - timedelta(days=29)))
    return start, end


# ---------------------------------------------------------------------------
# URL-type: de zes buckets waarin de tool praat (Joep, 2026-08-11)
#
# De parser schrijft twaalf fijnere types weg (product, category_facet, search,
# list, sitemap, robots, info, …). Naar buiten toe zijn dat zes groepen, met exact
# de namen en de PRIORITEIT die seo_stats_service._urltype_case() gebruikt — /r/
# vóór /c/ vóór /p/ vóór /products/ — zodat een URL met zowel /r/ als /c/ in beide
# tools een R-url is.
#
# Bewust een mapping in de QUERYLAAG en niet in de parser: het ruwe type blijft in
# de cube staan, de 24 geladen logdatums hoeven niet opnieuw ingest, en de indeling
# is een presentatiekeuze die je later kunt bijstellen zonder de data aan te raken.
# Wat je ervoor inlevert: de mapping gaat van type naar bucket, niet van URL naar
# bucket, dus hij kan alleen zo fijn zijn als de parser al was. Voor deze zes
# valt dat samen — `search` IS /r/, `category_facet` IS /products/ + /c/.
URLTYPE_BUCKETS = [
    ("R-url",    ["search"]),
    ("C-url",    ["category_facet"]),
    ("PLP",      ["product", "product_legacy"]),
    ("Cat-url",  ["category", "category_legacy"]),
    ("Homepage", ["home"]),
    ("Overige",  ["list", "sitemap", "robots", "info", "other"]),
]
URLTYPE_ORDER = [name for name, _raw in URLTYPE_BUCKETS]
# bucket -> ruwe types, en de omgekeerde weg voor het filter
BUCKET_TO_RAW = {name: raw for name, raw in URLTYPE_BUCKETS}


def _urltype_case(alias="d"):
    """SQL CASE die het ruwe url_type op zijn bucket afbeeldt."""
    whens = " ".join(
        f"WHEN {alias}.url_type = ANY(ARRAY[{','.join(repr(t) for t in raw)}]) THEN {name!r}"
        for name, raw in URLTYPE_BUCKETS
    )
    return f"CASE {whens} ELSE 'Overige' END"


def _expand_urltype(value):
    """Bucketnamen -> ruwe types. Onbekende waarden blijven staan, zodat een
    directe API-call met een ruw type (bijv. url_type=product) blijft werken."""
    out = []
    for part in (value or "").split(","):
        part = part.strip()
        if not part:
            continue
        out.extend(BUCKET_TO_RAW.get(part, [part]))
    return out


# ---------------------------------------------------------------------------
# Filters shared by every endpoint
# ---------------------------------------------------------------------------
def _filters(host=None, bot_class=None, bot_family=None, url_type=None,
             known=None, alias="d"):
    """-> (sql_fragment, params). Applied against the cube or a join onto it."""
    sql, params = [], []
    if host:
        sql.append("h.host = ANY(%s)")
        params.append([x.strip() for x in host.split(",") if x.strip()])
    if bot_class:
        sql.append("b.bot_class = ANY(%s)")
        params.append([x.strip() for x in bot_class.split(",") if x.strip()])
    if bot_family:
        sql.append("b.bot_family = ANY(%s)")
        params.append([x.strip() for x in bot_family.split(",") if x.strip()])
    if url_type:
        sql.append(f"{alias}.url_type = ANY(%s)")
        params.append(_expand_urltype(url_type))
    if known == "known":
        sql.append(f"{alias}.is_known_url")
    elif known == "unknown":
        sql.append(f"NOT {alias}.is_known_url")
    return (" AND " + " AND ".join(sql) if sql else ""), params


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
def get_meta(force=False):
    """Everything the UI needs to build its filter controls."""
    def run():
        cov = _query("""
            SELECT min(log_date) AS first_day, max(log_date) AS last_day,
                   count(*) AS days,
                   count(*) FILTER (WHERE NOT is_complete) AS partial_days,
                   sum(raw_lines) AS raw_lines, sum(bot_lines) AS bot_lines
            FROM pa.bothits_ingest
        """)
        partial = _query("""
            SELECT log_date::text AS log_date, hours_present
            FROM pa.bothits_ingest WHERE NOT is_complete ORDER BY log_date
        """)
        return {
            "coverage": cov[0] if cov else {},
            "partial_days": partial,
            "hosts": [r["host"] for r in _query(
                "SELECT host FROM pa.bothits_host ORDER BY host")],
            "bots": _query("""
                SELECT bot_family, bot_class, is_tracked,
                       array_agg(bot_name ORDER BY bot_name) AS bot_names
                FROM pa.bothits_bot
                GROUP BY bot_family, bot_class, is_tracked
                ORDER BY bot_class, bot_family
            """),
            # De zes buckets in hun vaste volgorde, niet de ruwe DISTINCT: de
            # filterlijst moet dezelfde taal spreken als de grafiek, en een vaste
            # volgorde houdt de checkbox-lijst stabiel als een type een dag mist.
            "url_types": URLTYPE_ORDER,
        }
    return _cached("meta", run, force)


def get_daily(start_date=None, end_date=None, host=None, bot_class=None,
              bot_family=None, url_type=None, known=None, group_by="bot_class",
              force=False):
    """Per-day hits, split by the requested dimension.

    group_by is whitelisted rather than interpolated freely — it lands in the
    SQL directly.
    """
    start, end = _range(start_date, end_date)
    cols = {
        "bot_class": "b.bot_class",
        "bot_family": "b.bot_family",
        "bot_name": "b.bot_name",
        "url_type": _urltype_case("d"),
        "host": "h.host",
        "status_class": "d.status_class",
        "edge_result": "d.edge_result",
        "facet_depth": "d.facet_depth::text",
        "is_known_url": "CASE WHEN d.is_known_url THEN 'in pa.urls' ELSE 'not in pa.urls' END",
        # IP-verificatie tegen de gepubliceerde ranges van de operator. Zit hier
        # omdat 'failed' een tripwire is: zonder een splitsing in de grafiek is de
        # dag waarop iemand Googlebot gaat imiteren niet te zien.
        "verify_state": "d.verify_state",
        "none": "'all'",
    }
    col = cols.get(group_by, cols["bot_class"])
    key = ("daily", start, end, host, bot_class, bot_family, url_type, known, group_by)

    def run():
        frag, params = _filters(host, bot_class, bot_family, url_type, known)
        rows = _query(f"""
            SELECT d.log_date::text AS log_date,
                   {col} AS grp,
                   sum(d.hits)::bigint  AS hits,
                   sum(d.bytes)::bigint AS bytes
            FROM pa.bothits_daily d
            JOIN pa.bothits_host h ON h.host_id = d.host_id
            JOIN pa.bothits_bot  b ON b.bot_id  = d.bot_id
            WHERE d.log_date BETWEEN %s AND %s {frag}
            GROUP BY 1, 2
            ORDER BY 1, 3 DESC
        """, [start, end] + params)
        incomplete = {r["log_date"] for r in _query("""
            SELECT log_date::text AS log_date FROM pa.bothits_ingest
            WHERE NOT is_complete AND log_date BETWEEN %s AND %s
        """, (start, end))}
        return {"start_date": start, "end_date": end, "group_by": group_by,
                "rows": rows, "incomplete_days": sorted(incomplete)}
    return _cached(key, run, force)


def get_summary(start_date=None, end_date=None, host=None, bot_class=None,
                bot_family=None, url_type=None, known=None, force=False):
    """Totals per bot family + the url-type and crawl-waste breakdown."""
    start, end = _range(start_date, end_date)
    key = ("summary", start, end, host, bot_class, bot_family, url_type, known)

    def run():
        frag, params = _filters(host, bot_class, bot_family, url_type, known)
        args = [start, end] + params
        bots = _query(f"""
            SELECT b.bot_family, b.bot_class,
                   sum(d.hits)::bigint AS hits,
                   sum(d.bytes)::bigint AS bytes,
                   sum(d.hits) FILTER (WHERE d.is_known_url)::bigint AS hits_known,
                   sum(d.hits) FILTER (WHERE d.status_class='2xx')::bigint AS hits_2xx,
                   sum(d.hits) FILTER (WHERE d.status_class='3xx')::bigint AS hits_3xx,
                   sum(d.hits) FILTER (WHERE d.status_class='4xx')::bigint AS hits_4xx,
                   sum(d.hits) FILTER (WHERE d.status_class='5xx')::bigint AS hits_5xx,
                   sum(d.hits) FILTER (WHERE d.edge_result ILIKE 'Hit%%')::bigint AS cache_hits,
                   count(DISTINCT d.log_date) AS days
            FROM pa.bothits_daily d
            JOIN pa.bothits_host h ON h.host_id = d.host_id
            JOIN pa.bothits_bot  b ON b.bot_id  = d.bot_id
            WHERE d.log_date BETWEEN %s AND %s {frag}
            GROUP BY 1, 2 ORDER BY 3 DESC
        """, args)
        # Zes buckets, en in de VASTE volgorde van URLTYPE_BUCKETS — niet op
        # omvang. De donut en de filterlijst moeten dezelfde rij-orde hebben,
        # anders wisselt een segment van kleur zodra een type groeit.
        by_type = _query(f"""
            SELECT {_urltype_case('d')} AS url_type, sum(d.hits)::bigint AS hits,
                   sum(d.hits) FILTER (WHERE d.is_known_url)::bigint AS hits_known
            FROM pa.bothits_daily d
            JOIN pa.bothits_host h ON h.host_id = d.host_id
            JOIN pa.bothits_bot  b ON b.bot_id  = d.bot_id
            WHERE d.log_date BETWEEN %s AND %s {frag}
            GROUP BY 1
            ORDER BY array_position(%s::text[], {_urltype_case('d')})
        """, args + [URLTYPE_ORDER])
        by_depth = _query(f"""
            SELECT d.facet_depth, sum(d.hits)::bigint AS hits,
                   sum(d.hits) FILTER (WHERE d.is_known_url)::bigint AS hits_known
            FROM pa.bothits_daily d
            JOIN pa.bothits_host h ON h.host_id = d.host_id
            JOIN pa.bothits_bot  b ON b.bot_id  = d.bot_id
            WHERE d.log_date BETWEEN %s AND %s {frag}
            GROUP BY 1 ORDER BY 1
        """, args)
        by_host = _query(f"""
            SELECT h.host, sum(d.hits)::bigint AS hits
            FROM pa.bothits_daily d
            JOIN pa.bothits_host h ON h.host_id = d.host_id
            JOIN pa.bothits_bot  b ON b.bot_id  = d.bot_id
            WHERE d.log_date BETWEEN %s AND %s {frag}
            GROUP BY 1 ORDER BY 2 DESC
        """, args)
        total = sum(r["hits"] for r in bots) or 0
        known_hits = sum(r["hits_known"] or 0 for r in bots)

        # "Waste" has to be measured against the catalog URLs only. pa.urls
        # holds /c/ category+facet pages and nothing else, so every /p/ product
        # page is "not in pa.urls" by construction — counting those as waste
        # puts the number near 94% and means nothing. The real signal is what
        # share of CATEGORY-shaped crawling lands on facet permutations that
        # are not part of our indexable set.
        # by_type draagt nu buckets, dus de tegels rekenen daarop: C-url + Cat-url
        # zijn samen precies de oude CATALOG-set (category_facet + category +
        # category_legacy), en PLP is product + product_legacy.
        CATALOG = ("C-url", "Cat-url")
        catalog = [r for r in by_type if r["url_type"] in CATALOG]
        catalog_hits = sum(r["hits"] for r in catalog)
        catalog_known = sum(r["hits_known"] or 0 for r in catalog)
        product_hits = sum(r["hits"] for r in by_type if r["url_type"] == "PLP")
        return {
            "start_date": start, "end_date": end,
            "total_hits": total, "known_hits": known_hits,
            "product_hits": product_hits,
            "catalog_hits": catalog_hits, "catalog_known": catalog_known,
            "waste_pct": round((1 - catalog_known / catalog_hits) * 100, 1)
                         if catalog_hits else 0,
            "bots": bots, "by_url_type": by_type, "by_facet_depth": by_depth,
            "by_host": by_host,
        }
    return _cached(key, run, force)


def get_top_urls(start_date=None, end_date=None, host=None, bot_class=None,
                 bot_family=None, limit=100, main_cat=None, search=None,
                 force=False):
    """Most-crawled URLs that exist in pa.urls, with their bot split."""
    start, end = _range(start_date, end_date)
    limit = max(1, min(int(limit or 100), 1000))
    key = ("topurls", start, end, host, bot_class, bot_family, limit, main_cat, search)

    def run():
        sql, params = [], []
        if host:
            sql.append("h.host = ANY(%s)")
            params.append([x.strip() for x in host.split(",") if x.strip()])
        if bot_class:
            sql.append("b.bot_class = ANY(%s)")
            params.append([x.strip() for x in bot_class.split(",") if x.strip()])
        if bot_family:
            sql.append("b.bot_family = ANY(%s)")
            params.append([x.strip() for x in bot_family.split(",") if x.strip()])
        if main_cat:
            sql.append(f"{MAIN_CAT_SQL} = ANY(%s)")
            params.append([x.strip() for x in main_cat.split(",") if x.strip()])
        if search:
            sql.append("u.url ILIKE %s")
            params.append(f"%{search}%")
        frag = (" AND " + " AND ".join(sql)) if sql else ""
        return _query(f"""
            SELECT u.url, {MAIN_CAT_SQL} AS main_cat_name,
                   sum(t.hits)::bigint AS hits,
                   sum(t.n_2xx)::bigint AS n_2xx, sum(t.n_3xx)::bigint AS n_3xx,
                   sum(t.n_4xx)::bigint AS n_4xx, sum(t.n_5xx)::bigint AS n_5xx,
                   count(DISTINCT t.log_date) AS days,
                   count(DISTINCT b.bot_family) AS n_bots,
                   string_agg(DISTINCT b.bot_family, ', ' ORDER BY b.bot_family) AS bots
            FROM pa.bothits_url_daily t
            JOIN pa.urls u          ON u.url_id  = t.url_id
            JOIN pa.bothits_host h  ON h.host_id = t.host_id
            JOIN pa.bothits_bot  b  ON b.bot_id  = t.bot_id
            WHERE t.log_date BETWEEN %s AND %s {frag}
            GROUP BY u.url
            ORDER BY hits DESC
            LIMIT %s
        """, [start, end] + params + [limit])
    return _cached(key, run, force)


def get_top_waste(start_date=None, end_date=None, host=None, bot_family=None,
                  limit=100, force=False):
    """Most-crawled URLs that are NOT in pa.urls — where crawl budget leaks.

    Sourced from pa.bothits_unknown_daily, which keeps the top 500 per day per
    bot family. It is a daily top-N, so treat it as "the loudest offenders",
    not as an exhaustive ranking of the whole unknown tail.
    """
    start, end = _range(start_date, end_date)
    limit = max(1, min(int(limit or 100), 1000))
    key = ("topwaste", start, end, host, bot_family, limit)

    def run():
        sql, params = [], []
        if host:
            sql.append("h.host = ANY(%s)")
            params.append([x.strip() for x in host.split(",") if x.strip()])
        if bot_family:
            sql.append("b.bot_family = ANY(%s)")
            params.append([x.strip() for x in bot_family.split(",") if x.strip()])
        frag = (" AND " + " AND ".join(sql)) if sql else ""
        return _query(f"""
            SELECT w.url, w.url_type, w.facet_depth,
                   sum(w.hits)::bigint AS hits,
                   count(DISTINCT w.log_date) AS days,
                   string_agg(DISTINCT b.bot_family, ', ' ORDER BY b.bot_family) AS bots
            FROM pa.bothits_unknown_daily w
            JOIN pa.bothits_host h ON h.host_id = w.host_id
            JOIN pa.bothits_bot  b ON b.bot_id  = w.bot_id
            WHERE w.log_date BETWEEN %s AND %s {frag}
            GROUP BY w.url, w.url_type, w.facet_depth
            ORDER BY hits DESC LIMIT %s
        """, [start, end] + params + [limit])
    return _cached(key, run, force)


def get_url_detail(url, start_date=None, end_date=None, force=False):
    """Per-day, per-bot crawl history for one URL."""
    start, end = _range(start_date, end_date)
    key = ("urldetail", url, start, end)

    def run():
        return {
            "url": url,
            "rows": _query("""
                SELECT t.log_date::text AS log_date, b.bot_family, b.bot_name,
                       h.host, t.hits, t.n_2xx, t.n_3xx, t.n_4xx, t.n_5xx
                FROM pa.bothits_url_daily t
                JOIN pa.urls u         ON u.url_id  = t.url_id
                JOIN pa.bothits_host h ON h.host_id = t.host_id
                JOIN pa.bothits_bot  b ON b.bot_id  = t.bot_id
                WHERE u.url = %s AND t.log_date BETWEEN %s AND %s
                ORDER BY t.log_date, b.bot_family
            """, (url.rstrip("/"), start, end)),
        }
    return _cached(key, run, force)


def get_categories(start_date=None, end_date=None, host=None, bot_class=None,
                   bot_family=None, limit=100, force=False):
    """Crawl volume rolled up to main category — known URLs only."""
    start, end = _range(start_date, end_date)
    limit = max(1, min(int(limit or 100), 500))
    key = ("cats", start, end, host, bot_class, bot_family, limit)

    def run():
        sql, params = [], []
        if host:
            sql.append("h.host = ANY(%s)")
            params.append([x.strip() for x in host.split(",") if x.strip()])
        if bot_class:
            sql.append("b.bot_class = ANY(%s)")
            params.append([x.strip() for x in bot_class.split(",") if x.strip()])
        if bot_family:
            sql.append("b.bot_family = ANY(%s)")
            params.append([x.strip() for x in bot_family.split(",") if x.strip()])
        frag = (" AND " + " AND ".join(sql)) if sql else ""
        return _query(f"""
            SELECT nullif({MAIN_CAT_SQL}, '') AS main_cat_name,
                   sum(t.hits)::bigint AS hits,
                   count(DISTINCT t.url_id) AS urls_crawled
            FROM pa.bothits_url_daily t
            JOIN pa.urls u         ON u.url_id  = t.url_id
            JOIN pa.bothits_host h ON h.host_id = t.host_id
            JOIN pa.bothits_bot  b ON b.bot_id  = t.bot_id
            WHERE t.log_date BETWEEN %s AND %s {frag}
            GROUP BY 1 ORDER BY 2 DESC LIMIT %s
        """, [start, end] + params + [limit])
    return _cached(key, run, force)


def get_ingest_log(limit=200, force=False):
    def run():
        return _query("""
            SELECT log_date::text AS log_date, files, raw_lines, bot_lines,
                   known_rows, hours_present, is_complete, duration_s,
                   source_dirs, ingested_at::text AS ingested_at
            FROM pa.bothits_ingest ORDER BY log_date DESC LIMIT %s
        """, (limit,))
    return _cached(("ingestlog", limit), run, force)


def clear_cache():
    with _cache_lock:
        _cache.clear()
