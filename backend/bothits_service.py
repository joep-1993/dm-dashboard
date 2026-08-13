"""Bot Hits — query layer for the crawler-log dashboard.

Everything here reads pa.bothits_daily — the cube built by bothits_ingest, which
answers the timeseries and split questions.

Sinds 2026-08-13 leest deze laag GEEN URL-tabellen meer. De drie tabs die dat
deden (URL's, Crawl-verspilling, Categorieën) zijn eruit op verzoek van Joep, en
daarmee get_top_urls / get_top_waste / get_url_detail / get_categories. De ingest
vult pa.bothits_url_daily en pa.bothits_unknown_daily nog wél, dus de data loopt
door en die tabs terugzetten is puur frontend- plus endpoint-werk.

Alle queries tonen alleen GEVOLGDE bots (pa.bothits_bot.is_tracked) — zie
_filters() voor het waarom.

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


def _range(start_date, end_date):
    """Default to the last 30 days of data we actually have, not of wall time.

    Uit de CUBE en niet uit pa.bothits_ingest (2026-08-13). Die ledger is een
    PROCEStabel: hij zegt wat er geladen is, niet wat er te zien is. Het standaard
    herstelrecept — ledger-rijen weggooien om een herlaad te forceren — verschoof
    daardoor stil het venster waarop de tool opent. Tijdens de herlaad van 13 augustus
    stond max(log_date) even op 2026-06-09 en zou de tool op mei/juni zijn geopend
    zonder dat iets dat uitlegde. De cube is wat de gebruiker ziet, dus die hoort de
    grens te bepalen; min/max lopen over de primary key en zijn dus goedkoop.
    """
    if start_date and end_date:
        return start_date, end_date
    rows = _query("SELECT min(log_date) AS lo, max(log_date) AS hi FROM pa.bothits_daily")
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
    """-> (sql_fragment, params). Applied against the cube or a join onto it.

    b.is_tracked staat er ALTIJD in (Joep, 2026-08-13). Van de 31 families in de
    logs wil het dashboard er elf zien: de drie Google-bots en de grote
    AI-crawlers, plus Applebot omdat die de nummer twee is. De rest — Bing,
    Yandex, Social, SEO-tools, other-bot en een staart van scrapers met vier tot
    duizend hits — was ruis in de tabel en in de legenda.

    Via de vlag en niet via een lijst in de code: die vlag bestond al (de ingest
    zette hem voor other-bot / Monitoring / SEO-tools / Social om de per-URL-
    tabellen niet te laten ontploffen) en een bot terugzetten is nu één UPDATE in
    pa.bothits_bot in plaats van een deploy. De ingest schrijft hem alleen bij een
    nieuwe bot (ON CONFLICT DO NOTHING), dus handmatige wijzigingen blijven staan.

    LET OP na zo'n UPDATE: de cache hierboven is 5 minuten en weet niets van DB-state,
    dus de oude cijfers blijven tot zo lang staan. `POST /api/bothits/cache/clear` maakt
    hem meteen leeg (of de Refresh-knop, die force=True stuurt voor de grafieken). Een
    vlag-hash in de cachesleutel zou een extra query per verzoek kosten en dat is de
    remedie erger dan de kwaal — dit hoort in het runbook, niet in de hot path.

    Let op: dit filtert de CUBE, en de cube houdt de volledige aantallen. De
    tegels tellen dus 96,6% van de ruwe bot-hits — wat weg is, is bewust weg.
    """
    sql, params = ["b.is_tracked"], []
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
    # Geen `if sql else ""` meer (audit 2026-08-13): sql begint op ["b.is_tracked"],
    # dus de lege tak was aantoonbaar onbereikbaar en suggereerde een filterloos pad
    # dat niet bestaat.
    return " AND " + " AND ".join(sql), params


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
            # Alleen gevolgde bots, zodat de Bot-soort-checkboxen niet vragen om
            # klassen die nergens meer in de cijfers zitten (Joep, 2026-08-13).
            # is_tracked blijft in de SELECT staan: de kolom is nu de knop waarmee
            # je een familie terugzet, dus wie /meta leest moet kunnen zien dat er
            # gefilterd is.
            "bots": _query("""
                SELECT bot_family, bot_class, is_tracked,
                       array_agg(bot_name ORDER BY bot_name) AS bot_names
                FROM pa.bothits_bot
                WHERE is_tracked
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
    # Onbekende group_by valt terug op bot_class, maar het antwoord echode tot
    # 2026-08-13 de GEVRAAGDE naam terug — dus `?group_by=bot_familly` gaf een
    # bot_class-uitsplitsing met "bot_familly" erboven, en de cache bewaarde hem onder
    # de typefout. Nu noemt het antwoord de kolom die echt gebruikt is. Alle negen
    # opties die de UI aanbiedt staan in `cols`, dus voor de frontend verandert niets.
    group_by = group_by if group_by in cols else "bot_class"
    col = cols[group_by]
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


def _unknown_filters(host=None, bot_class=None, bot_family=None, url_type=None):
    """Filters voor pa.bothits_unknown_daily (alias `w`) -> (fragment, params).

    Apart van _filters(): die schrijft over de cube-alias `d` en kent is_known_url,
    wat deze tabel niet heeft. b.is_tracked staat er altijd in, anders tonen de
    URL-lijst en het paneel bots die de rest van de pagina niet meetelt.
    """
    sql, params = ["b.is_tracked"], []
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
        sql.append("w.url_type = ANY(%s)")
        params.append(_expand_urltype(url_type))
    return " AND " + " AND ".join(sql), params


def get_top_urls(start_date=None, end_date=None, host=None, bot_class=None,
                 bot_family=None, url_type=None, limit=250, force=False, q=None):
    """De meest gecrawlde URL's in de selectie.

    BRON: pa.bothits_unknown_daily — en LET OP, de reden daarvoor bestaat niet meer.

    Deze keuze is op 2026-08-13 gemaakt omdat pa.bothits_url_daily stil stond: elke
    ingest schreef known_rows = 0, dus praktisch élke gecrawlde URL viel in de
    "onbekende" tabel en die tabel WAS toen de lijst van meest gecrawlde URL's.

    Dat defect is diezelfde dag gerepareerd (expliciete fork-context, a2ee990), en
    daarmee is de rechtvaardiging vervallen. Gemeten 2026-08-13: url_daily staat op
    20.300.271 rijen t/m 2026-08-12, geen enkele ledgerdatum heeft nog known_rows = 0.
    Gevolg voor DEZE functie, en dat is nu een echte beperking in plaats van een
    tijdelijke: wat hier staat is alleen wat NIET in pa.urls zit. Op 2026-08-12 is dat
    41.427 van 3,39 mln bot-hits (1,2%), en de 191.108 hits op bekende /c/-URL's —
    precies de indexeerbare set — kunnen hier per constructie niet in voorkomen.
    Productpagina's evenmin: ingest.py:447 schrijft die nooit naar deze tabel.

    Terugzetten naar pa.bothits_url_daily is een ontwerpkeuze met een prijs, geen
    omzetting: gemeten 21,5s koud / 9,4s warm over 30 dagen, of 4,5s met de query
    omgebouwd (eerst aggregeren op url_id, dan pas pa.urls erbij voor de top 250).
    Zie cc1/TASKS.md — dit is fase 3 van de audit, niet iets om en passant te doen.

    Wat je ervoor inlevert, en dat moet de UI zeggen: die tabel bewaart per dag de top
    500 per bot-familie. Een URL die elke dag net onder die grens blijft, staat er niet
    in. Lees het als "de luidste veroorzakers", niet als een uitputtende ranglijst — de
    dagtotalen in het Overzicht kloppen wél volledig, want die komen uit de cube.

    Gemeten over de standaardselectie van 30 dagen: 85.307 unieke URL's, 2,25 mln hits.
    De top 50 draagt daarvan 53%, de top 250 62%, de top 1000 74% — de knik zit dus heel
    vroeg. Vandaar 250 als standaard: ruim voorbij de knik, rij #250 heeft nog 701 hits
    (~23 per dag) en een tabel van 250 rijen is nog te scannen en te sorteren.

    `q` is de zoekbox (Joep, 2026-08-13) en filtert in SQL, niet in de browser. Dat
    onderscheid is het hele punt: dit is een top-N van de selectie, dus een filter over
    de al geladen 250 rijen zou een URL die op plek 800 staat stilzwijgend verzwijgen.
    Nu is het "de top N van wat matcht".

    Twee keuzes in dat filter:
      * Substring via strpos(), geen LIKE. Bij ILIKE '%...%' zijn `%` en `_` wildcards
        die je dan moet escapen, en URL's zitten er vol mee — percent-encoding (%20)
        en underscores in facetwaarden zouden als "alles" gaan matchen. strpos() kent
        geen metatekens, dus wat je typt is wat je zoekt.
      * Meerdere woorden = AND, in willekeurige volgorde. Een pad als
        /c/wasmachines/siemens/ vind je zo met "wasmachines siemens"; bij een platte
        substring zou die spatie niets opleveren. Gemaximeerd op 6 termen, zodat een
        plakkerige zoekopdracht geen 50 strpos-en per rij wordt.

    Geen index om op te leunen (geen trigram op w.url), maar dat kost hier niets: de
    datumfilter snijdt eerst en ILIKE '%x%' had net zo goed geen index kunnen gebruiken.
    """
    start, end = _range(start_date, end_date)
    limit = max(1, min(int(limit or 250), 1000))
    terms = [t.lower() for t in (q or "").split()][:6]
    # Genormaliseerd in de cachesleutel, niet de ruwe string: "  Siemens" en "siemens"
    # zijn dezelfde query en horen dezelfde cache-entry te delen.
    key = ("topurls", start, end, host, bot_class, bot_family, url_type, limit,
           tuple(terms))

    def run():
        frag, params = _unknown_filters(host, bot_class, bot_family, url_type)
        if terms:
            frag += "".join(" AND strpos(lower(w.url), %s) > 0" for _ in terms)
            params = params + terms
        # Geen string_agg van de bot-families meer (Joep, 2026-08-13): die kolom is uit
        # de tabel en het uitklappaneel toont de verdeling nu als donut. Scheelt ook een
        # sort per groep in de query.
        return _query(f"""
            SELECT w.url, w.url_type, w.facet_depth,
                   sum(w.hits)::bigint AS hits,
                   count(DISTINCT w.log_date) AS days
            FROM pa.bothits_unknown_daily w
            JOIN pa.bothits_host h ON h.host_id = w.host_id
            JOIN pa.bothits_bot  b ON b.bot_id  = w.bot_id
            WHERE w.log_date BETWEEN %s AND %s {frag}
            GROUP BY w.url, w.url_type, w.facet_depth
            -- w.url als tie-break (audit 2026-08-13): op de standaardselectie zit rang
            -- 250 op 208 hits met drie rijen gelijk, dus zonder tweede sleutel kiest
            -- Postgres willekeurig welke twee je ziet en flapt de lijst tussen twee
            -- identieke verzoeken. De hits zelf veranderen niet.
            ORDER BY hits DESC, w.url LIMIT %s
        """, [start, end] + params + [limit])
    return _cached(key, run, force)


def get_url_detail(url, start_date=None, end_date=None, host=None, bot_class=None,
                   url_type=None, force=False):
    """Eén URL uitgesplitst: per bot-familie en per dag.

    Zelfde bron en dus dezelfde beperking als get_top_urls: dit is de dagelijkse top
    500 per bot-familie. Een dag waarop deze URL die grens niet haalde, levert géén
    rij — dus de dagreeks kan gaten hebben, en die gaten betekenen "niet in de top-500
    van die dag", niet "niet gecrawld". Daarom komt `days` mee: de frontend kan dan
    zeggen op hoeveel van de dagen in de selectie deze URL in beeld was.

    bot_family zit met opzet NIET in de parameters: dit paneel gaat over de verdeling
    over families, en die eerst wegfilteren zou de vraag beantwoorden met het antwoord.
    """
    start, end = _range(start_date, end_date)
    key = ("urldetail", url, start, end, host, bot_class, url_type)

    def run():
        frag, params = _unknown_filters(host, bot_class, None, url_type)
        args = [start, end, url] + params
        where = f"WHERE w.log_date BETWEEN %s AND %s AND w.url = %s {frag}"
        by_bot = _query(f"""
            SELECT b.bot_family, sum(w.hits)::bigint AS hits
            FROM pa.bothits_unknown_daily w
            JOIN pa.bothits_host h ON h.host_id = w.host_id
            JOIN pa.bothits_bot  b ON b.bot_id  = w.bot_id
            {where}
            GROUP BY 1 ORDER BY 2 DESC
        """, args)
        by_day = _query(f"""
            SELECT w.log_date::text AS log_date, sum(w.hits)::bigint AS hits
            FROM pa.bothits_unknown_daily w
            JOIN pa.bothits_host h ON h.host_id = w.host_id
            JOIN pa.bothits_bot  b ON b.bot_id  = w.bot_id
            {where}
            GROUP BY 1 ORDER BY 1
        """, args)
        return {
            "url": url, "start_date": start, "end_date": end,
            "hits": sum(r["hits"] for r in by_bot),
            "days": len(by_day),
            # Hoeveel dagen er DATA is in de selectie, zodat de frontend "17 van de 30"
            # kan zeggen zonder zelf datums te gaan rekenen. Uit de cube en niet uit
            # pa.bothits_ingest, om dezelfde reden als in _range(): die ledger raakt
            # rijen kwijt tijdens een herlaad en dan zou dit label liegen — precies in
            # de situatie waarin je hem het meest nodig hebt.
            "days_in_range": _query(
                "SELECT count(DISTINCT log_date) AS n FROM pa.bothits_daily "
                "WHERE log_date BETWEEN %s AND %s", (start, end))[0]["n"],
            "by_bot": by_bot, "by_day": by_day,
        }
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
