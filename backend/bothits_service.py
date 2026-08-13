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
# ruw type -> bucket. Eén bron voor beide richtingen, zodat de Type-kolom in de
# URL-tabel dezelfde woorden gebruikt als het URL-type-filter erboven (Joep,
# 2026-08-13). Die twee spraken elkaar tegen: het filter bood `C-url` en `PLP` aan
# terwijl de tabel `category_facet` en `product` toonde — één begrip, twee talen.
RAW_TO_BUCKET = {raw: name for name, raws in URLTYPE_BUCKETS for raw in raws}


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
             known=None, alias="d", has_url_type=True, has_known=True):
    """-> (sql_fragment, params). Applied against the cube or a join onto it.

    ÉÉN bouwer voor alle drie de feitentabellen (fase 4). Er stonden er drie naast
    elkaar — `_filters` voor de cube, `_unknown_filters` voor unknown_daily en het in
    fase 3 toegevoegde `_known_filters` voor url_daily — met woordelijk gelijke
    host/bot_class/bot_family-takken. Zo overleefde de blinde vlek van de URL-tab: een
    filter toevoegen aan de een deed stilletjes niets in de ander. De verschillen die er
    echt zijn, zijn nu vlaggen:
      * `alias`        — `d` voor de cube en url_daily, `w` voor unknown_daily;
      * `has_url_type` — url_daily draagt geen url_type (die rij is bewust smal);
      * `has_known`    — alleen de cube heeft is_known_url.

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
    if url_type and has_url_type:
        sql.append(f"{alias}.url_type = ANY(%s)")
        params.append(_expand_urltype(url_type))
    if has_known and known == "known":
        sql.append(f"{alias}.is_known_url")
    elif has_known and known == "unknown":
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
        # first_day/last_day uit de CUBE, de rest uit de ledger (fase 3). _range() legt
        # in zijn docstring uit waarom het venster niet aan pa.bothits_ingest mag hangen
        # — dat is een PROCEStabel, en het herstelrecept dat ledgerrijen weggooit
        # verschoof daarmee stil het venster waarop de tool opent. Die fix zat alleen in
        # _range(), terwijl de frontend de datumvelden hiermee vult en daarna ALTIJD
        # beide datums meestuurt, waardoor _range() meteen kortsluit en zijn eigen
        # bescherming nooit toepast. De hazard landde dus gewoon een niveau hoger.
        # raw_lines/bot_lines/partial_days blijven uit de ledger: dat gaat over het
        # laadproces, niet over wat er te zien is.
        cov = _query("""
            SELECT (SELECT min(log_date) FROM pa.bothits_daily) AS first_day,
                   (SELECT max(log_date) FROM pa.bothits_daily) AS last_day,
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
        # ALLEEN category-vormige URL's (fase 3). facet_depth() geeft 0 terug voor
        # álles zonder /c/ — productpagina's, de homepage, robots.txt, assets — en die
        # landden allemaal in de nul-balk. Gemeten over het standaardvenster van 30
        # dagen: depth 0 was 49.225.165 hits waarvan maar 7.682.976 (15,6%)
        # category-vormig, terwijl depth ≥1 voor 100% category is. De grafiek las dus
        # als "de meeste crawl gaat naar categoriepagina's zonder facetten", en in
        # werkelijkheid was 84% van die balk productverkeer dat per definitie geen
        # facetten HEEFT. Dit is dezelfde valkuil die waste_pct 25 regels lager al
        # ontwijkt met dezelfde drie url_types.
        by_depth = _query(f"""
            SELECT d.facet_depth, sum(d.hits)::bigint AS hits,
                   sum(d.hits) FILTER (WHERE d.is_known_url)::bigint AS hits_known
            FROM pa.bothits_daily d
            JOIN pa.bothits_host h ON h.host_id = d.host_id
            JOIN pa.bothits_bot  b ON b.bot_id  = d.bot_id
            WHERE d.log_date BETWEEN %s AND %s {frag}
              AND d.url_type IN ('category_facet', 'category', 'category_legacy')
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
        # waste_pct is BETEKENISLOOS zodra er op `known` gefilterd wordt (fase 3): de
        # noemer is dan al gefilterd op precies de eigenschap die de teller meet, dus
        # `known=known` geeft altijd 0,0 en `known=unknown` altijd 100,0 — de filter
        # bepaalt het antwoord, niet de data. None i.p.v. een zelfverzekerd getal, zodat
        # een lezer ziet dat de vraag in deze selectie niet te stellen is.
        waste = None if known else (
            round((1 - catalog_known / catalog_hits) * 100, 1) if catalog_hits else 0)
        return {
            "start_date": start, "end_date": end,
            "total_hits": total, "known_hits": known_hits,
            "product_hits": product_hits,
            "catalog_hits": catalog_hits, "catalog_known": catalog_known,
            "waste_pct": waste,
            "bots": bots, "by_url_type": by_type, "by_facet_depth": by_depth,
            "by_host": by_host,
        }
    return _cached(key, run, force)


# url_type/facet_depth voor de KNOWN-leg van get_top_urls. pa.bothits_url_daily draagt
# ze niet (die rij is bewust smal: alleen url_id + dimensies + tellers), dus ze worden
# hier uit pa.urls.url afgeleid.
#
# Dit is een VIERDE kopie van een vocabulaire dat al in de ingest, de service en de
# frontend staat, en dat is precies het driftrisico dat de audit noemt. Daarom
# geverifieerd in plaats van gehoopt: op 20.000 echte pa.urls-rijen geeft dit exact
# dezelfde uitkomst als url_type() en facet_depth() uit bothits_ingest.py — 0
# verschillen op beide. Verandert een van die twee functies, dan hoort deze mee te
# veranderen; de test staat in cc1/TASKS.md en is in tien regels te herhalen.
#
# pa.urls is voor 1.031.698 van de 1.031.796 rijen `/products/...`, dus de takken
# hieronder dekken de tabel ruimschoots; de rest valt in 'other', net als in de ingest.
_SQL_CTAIL = "btrim(substring(x.url from position('/c/' in x.url) + 3), '/')"
SQL_FACET_DEPTH = f"""
    CASE WHEN position('/c/' in x.url) = 0 OR {_SQL_CTAIL} = '' THEN 0
         ELSE (length({_SQL_CTAIL})
               - length(replace({_SQL_CTAIL}, '~~', ''))) / 2 + 1 END"""
SQL_URL_TYPE = """
    CASE WHEN x.url LIKE '/products/%%' AND position('/c/' in x.url) > 0
              THEN 'category_facet'
         WHEN x.url LIKE '/products/%%'   THEN 'category'
         WHEN x.url LIKE '/categories/%%' THEN 'category_legacy'
         WHEN x.url LIKE '/l/%%'          THEN 'list'
         WHEN x.url LIKE '/r/%%'          THEN 'search'
         WHEN x.url LIKE '/p/%%'          THEN 'product'
         ELSE 'other' END"""


def _known_filters(host=None, bot_class=None, bot_family=None):
    """Filters voor pa.bothits_url_daily (alias `d`). Geen url_type en geen
    is_known_url: die tabel draagt per definitie alleen URL's uit pa.urls, en het type
    wordt pas ná de aggregatie uit pa.urls afgeleid."""
    return _filters(host, bot_class, bot_family,
                    has_url_type=False, has_known=False)


def _unknown_filters(host=None, bot_class=None, bot_family=None, url_type=None):
    """Filters voor pa.bothits_unknown_daily (alias `w`). Die tabel heeft wel een
    url_type-kolom maar geen is_known_url — alles erin is per definitie onbekend."""
    return _filters(host, bot_class, bot_family, url_type,
                    alias="w", has_known=False)


def get_top_urls(start_date=None, end_date=None, host=None, bot_class=None,
                 bot_family=None, url_type=None, limit=250, force=False, q=None):
    """De meest gecrawlde URL's in de selectie.

    TWEE BRONNEN, samengevoegd en opnieuw gerankt (fase 3 van de audit, 2026-08-13):

      * pa.bothits_url_daily  -> de URL's die WEL in pa.urls staan (`source: pa.urls`)
      * pa.bothits_unknown_daily -> de staart daarbuiten (`source: onbekend`)

    Tot deze wijziging las hij alleen die tweede tabel. Dat was in augustus een keuze
    uit nood — url_daily stond stil omdat elke ingest known_rows = 0 schreef, dus viel
    praktisch élke gecrawlde URL in de "onbekende" bak en WAS dat de lijst. Die oorzaak
    is met de expliciete fork-context (a2ee990) verholpen, maar de querylaag ging niet
    mee. Gemeten op 2026-08-12: de tab zag 41.427 van 3,39 mln bot-hits (1,2%) en kon
    de 191.108 hits op bekende /c/-URL's — precies de indexeerbare set — per constructie
    niet tonen.

    Wat je nog steeds NIET ziet, en dat is nu de echte beperking:
      * Productpagina's. De ingest schrijft die nooit naar unknown_daily (ze zijn bijna
        uniek per hit, dus een top-N erover is ruis) en ze staan niet in pa.urls. Ze
        tellen wél volledig mee in het Overzicht, dat uit de cube komt.
      * Onbekende URL's die op geen enkele dag de top-500 per bot-familie haalden. De
        bekende kant is wél uitputtend geteld — de twee benen zijn dus niet symmetrisch,
        en `source` zegt per rij welke van de twee je leest.

    Snelheid: de bekende kant aggregeert eerst op url_id en haalt pa.urls er pas bij
    voor de top-N. De naïeve volgorde (eerst joinen) kost 21,5s koud over 30 dagen.

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
        # ---- been 1: de ONBEKENDE staart (pa.bothits_unknown_daily) ----------------
        frag, params = _unknown_filters(host, bot_class, bot_family, url_type)
        if terms:
            frag += "".join(" AND strpos(lower(w.url), %s) > 0" for _ in terms)
            params = params + terms
        # Geen string_agg van de bot-families meer (Joep, 2026-08-13): die kolom is uit
        # de tabel en het uitklappaneel toont de verdeling nu als donut. Scheelt ook een
        # sort per groep in de query.
        unknown = _query(f"""
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
        for r in unknown:
            r["source"] = "onbekend"

        # ---- been 2: de BEKENDE URL's (pa.bothits_url_daily) -----------------------
        kfrag, kparams = _known_filters(host, bot_class, bot_family)
        wanted = set(_expand_urltype(url_type)) if url_type else None
        # Eerst aggregeren op url_id, DAN pas pa.urls erbij voor alleen de top-N. De
        # naïeve volgorde (join eerst) kost 21,5s koud op 30 dagen omdat hij 1M
        # pa.urls-rijen aan 4,4M feitenrijen knoopt vóór er iets is weggegooid; zo
        # gaat de join over 250 rijen.
        # Zoektermen en een url_type-filter kunnen dat niet: die moeten vóór de
        # ranking bijten, anders krijg je "de top 250, en daaruit wat matcht" i.p.v.
        # "de top 250 van wat matcht". Dan eerst de url_id's opzoeken in pa.urls —
        # één scan over 1M smalle rijen — en daarmee de feitentabel op url_id
        # filteren, wat wél op de index kan.
        # Twee manieren om te knijpen, en welke goedkoop is hangt af van hoe SELECTIEF
        # het filter is:
        #   * een ZOEKTERM raakt een handvol URL's -> eerst de url_id's opzoeken en de
        #     feitentabel daarop filteren (die kan op de url_id-index). Gemeten 0,8s.
        #   * een URL-TYPE raakt bijna de hele tabel -> dat als id-lijst doorgeven
        #     betekent een ANY() met ~1 miljoen elementen, en dat kostte 37s. Zo'n
        #     filter hoort als predicaat in de aggregatie zelf, met de join erbij.
        extra, idparams = "", []
        if terms:
            idsql = ["SELECT x.url_id FROM pa.urls x WHERE true"]
            idp = []
            for t in terms:
                idsql.append("AND strpos(lower(x.url), %s) > 0")
                idp.append(t)
            if wanted:
                idsql.append(f"AND ({SQL_URL_TYPE}) = ANY(%s)")
                idp.append(list(wanted))
            pre_ids = [r["url_id"] for r in _query(" ".join(idsql), idp)]
            if not pre_ids:
                return sorted(unknown, key=lambda r: (-int(r["hits"]), r["url"]))[:limit]
            extra, idparams = " AND d.url_id = ANY(%s)", [pre_ids]
        elif wanted:
            extra = (f" AND EXISTS (SELECT 1 FROM pa.urls x WHERE x.url_id = d.url_id "
                     f"AND ({SQL_URL_TYPE}) = ANY(%s))")
            idparams = [list(wanted)]

        known = _query(f"""
            WITH agg AS (
                SELECT d.url_id, sum(d.hits)::bigint AS hits,
                       count(DISTINCT d.log_date) AS days
                FROM pa.bothits_url_daily d
                JOIN pa.bothits_host h ON h.host_id = d.host_id
                JOIN pa.bothits_bot  b ON b.bot_id  = d.bot_id
                WHERE d.log_date BETWEEN %s AND %s {kfrag}{extra}
                GROUP BY d.url_id
                ORDER BY hits DESC, d.url_id
                LIMIT %s
            )
            -- rtrim: pa.urls bewaart de trailing slash, de ingest bewaart het pad
            -- ZONDER. Beide benen moeten dezelfde vorm teruggeven, anders krijgt de
            -- frontend twee schrijfwijzen van hetzelfde pad en kan /url er niets mee.
            SELECT CASE WHEN rtrim(x.url, '/') = '' THEN '/'
                        ELSE rtrim(x.url, '/') END AS url,
                   {SQL_URL_TYPE} AS url_type,
                   {SQL_FACET_DEPTH} AS facet_depth,
                   a.hits, a.days
            FROM agg a JOIN pa.urls x ON x.url_id = a.url_id
            ORDER BY a.hits DESC, x.url
        """, [start, end] + kparams + idparams + [limit])
        for r in known:
            r["source"] = "pa.urls"

        # Samenvoegen en opnieuw ranken. Een URL kan niet in beide bakken zitten: de
        # ingest schrijft een hit óf naar url_daily (in pa.urls) óf naar unknown_daily.
        rows = sorted(known + unknown,
                      key=lambda r: (-int(r["hits"]), r["url"]))[:limit]
        # Type in dezelfde woorden als het filter erboven (Joep, 2026-08-13). De tabel
        # toonde het RUWE type (`category_facet`, `product`) terwijl Filters > URL-type
        # buckets aanbiedt (`C-url`, `PLP`) — één begrip in twee talen, op één scherm.
        # Het ruwe type blijft als `url_type_raw` meekomen: de frontend beslist daarmee
        # of een pad een trailing slash krijgt, en dát is een eigenschap van het ruwe
        # type, niet van de bucket.
        for r in rows:
            raw = r["url_type"]
            r["url_type_raw"] = raw
            r["url_type"] = RAW_TO_BUCKET.get(raw, "Overige")
        return rows
    return _cached(key, run, force)


def get_url_detail(url, start_date=None, end_date=None, host=None, bot_class=None,
                   url_type=None, force=False):
    """Eén URL uitgesplitst: per bot-familie en per dag.

    Volgt get_top_urls en leest dus ook uit twee tabellen (fase 3). Eerst pa.urls: staat
    de URL daarin, dan komt het detail uit pa.bothits_url_daily en is het UITPUTTEND —
    elke hit op elke dag. Zonder deze tak zou elke rij die sinds fase 3 uit de bekende
    kant komt een leeg paneel opleveren, want die URL's staan per definitie niet in de
    onbekende tabel.

    Zit hij er niet in, dan is de bron pa.bothits_unknown_daily, met de beperking die
    daarbij hoort: dat is de dagelijkse top-500 per bot-familie, dus de dagreeks kan
    gaten hebben en die gaten betekenen "die dag niet in de top-500", niet "niet
    gecrawld". `source` in het antwoord zegt welke van de twee je leest, en `days` +
    `days_in_range` laten de frontend "17 van de 30 dagen" tonen.

    bot_family zit met opzet NIET in de parameters: dit paneel gaat over de verdeling
    over families, en die eerst wegfilteren zou de vraag beantwoorden met het antwoord.
    """
    start, end = _range(start_date, end_date)
    key = ("urldetail", url, start, end, host, bot_class, url_type)

    def run():
        # Dezelfde last-wins-regel als URL_IDS in de ingest: bij twee pa.urls-rijen die
        # na het strippen van de trailing slash gelijk zijn, wint de hoogste url_id.
        hit = _query("SELECT url_id FROM pa.urls WHERE rtrim(url,'/') = %s "
                     "ORDER BY url_id DESC LIMIT 1", (url,))
        if hit:
            kfrag, kparams = _known_filters(host, bot_class, None)
            kargs = [start, end, hit[0]["url_id"]] + kparams
            kwhere = (f"WHERE d.log_date BETWEEN %s AND %s AND d.url_id = %s {kfrag}")
            by_bot = _query(f"""
                SELECT b.bot_family, sum(d.hits)::bigint AS hits
                FROM pa.bothits_url_daily d
                JOIN pa.bothits_host h ON h.host_id = d.host_id
                JOIN pa.bothits_bot  b ON b.bot_id  = d.bot_id
                {kwhere} GROUP BY 1 ORDER BY 2 DESC
            """, kargs)
            by_day = _query(f"""
                SELECT d.log_date::text AS log_date, sum(d.hits)::bigint AS hits
                FROM pa.bothits_url_daily d
                JOIN pa.bothits_host h ON h.host_id = d.host_id
                JOIN pa.bothits_bot  b ON b.bot_id  = d.bot_id
                {kwhere} GROUP BY 1 ORDER BY 1
            """, kargs)
            if by_bot:
                # n_2xx..n_5xx hadden tot fase 4 NUL lezers: alleen de DDL en de INSERT
                # noemden ze. Vier branches per bot-regel over ~500 mln regels en ~450 MB
                # tabel, voor niets. Ze zijn nuttig genoeg om aan te sluiten in plaats van
                # te schrappen — een per-URL 4xx-aandeel is precies wat je wil weten van
                # een pagina die crawlbudget kost.
                # `overig` is expliciet: de vier tellers zijn een SUBSET van hits, want
                # status_class() kent ook '0xx' (CloudFront logt sc-status 000 bij een
                # afgebroken verbinding). Zonder deze regel telt het paneel niet op en
                # lijkt dat een bug.
                st = _query(f"""
                    SELECT sum(d.n_2xx)::bigint AS n_2xx, sum(d.n_3xx)::bigint AS n_3xx,
                           sum(d.n_4xx)::bigint AS n_4xx, sum(d.n_5xx)::bigint AS n_5xx,
                           sum(d.hits)::bigint  AS hits
                    FROM pa.bothits_url_daily d
                    JOIN pa.bothits_host h ON h.host_id = d.host_id
                    JOIN pa.bothits_bot  b ON b.bot_id  = d.bot_id
                    {kwhere}
                """, kargs)[0]
                st["overig"] = (st["hits"] or 0) - sum(
                    st[k] or 0 for k in ("n_2xx", "n_3xx", "n_4xx", "n_5xx"))
                return {
                    "url": url, "start_date": start, "end_date": end,
                    "source": "pa.urls", "status": st,
                    "hits": sum(r["hits"] for r in by_bot), "days": len(by_day),
                    "days_in_range": _query(
                        "SELECT count(DISTINCT log_date) AS n FROM pa.bothits_daily "
                        "WHERE log_date BETWEEN %s AND %s", (start, end))[0]["n"],
                    "by_bot": by_bot, "by_day": by_day,
                }

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
            "source": "onbekend",
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
                   failed_files, expected_files,
                   source_dirs, ingested_at::text AS ingested_at
            FROM pa.bothits_ingest ORDER BY log_date DESC LIMIT %s
        """, (limit,))
    return _cached(("ingestlog", limit), run, force)


def clear_cache():
    with _cache_lock:
        _cache.clear()
