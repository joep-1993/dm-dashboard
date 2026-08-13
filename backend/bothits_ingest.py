"""Bot Hits — parse raw CloudFront access logs into the pa.bothits_* tables.

Reads gzipped CloudFront logs, classifies every request by crawler and URL
shape, and writes four aggregates per log date. Ingest is keyed on log_date and
is delete-then-insert, so re-processing a date (or dropping the same CloudFront
folder in twice) is safe.

Why the grain is what it is — all three numbers are measured, not guessed:

  * Full URL grain is ~154M rows over the 116 days of history we have.
  * Rolling that up to week or month saves almost nothing (1.05x): crawlers hit
    a largely DIFFERENT set of facet URLs every day, so each day contributes its
    own rows no matter how you bucket the date.
  * 86% of those rows are URLs absent from pa.urls — facet permutations the
    crawlers build themselves. Keeping URL detail only for known pa.urls brings
    the fact table down to ~21M rows.

So URL detail is kept for pa.urls members only (pa.bothits_url_daily), the
discarded tail stays fully countable in the cube via is_known_url + facet_depth
(pa.bothits_daily), and its loudest offenders are named per day in
pa.bothits_unknown_daily.

A second filter runs alongside it: only whitelisted crawler families get
per-URL rows (see UNTRACKED_FAMILIES). Worth being clear about what that buys —
on a measured day it removes 2% of URL rows, not 86%, because the volume is
Googlebot, OpenAI, Apple and Meta, all of which we keep. Its job is signal
quality and stopping the "other-bot" catch-all from growing without bound; the
pa.urls filter is what actually controls the table size.

CLI:
    python -m backend.bothits_ingest backfill [--src DIR] [--limit N] [--redo]
    python -m backend.bothits_ingest drop     [--src DIR]
    python -m backend.bothits_ingest status
"""
import argparse
import collections
import gzip
import heapq
import json
import logging
import multiprocessing
import os
import re
import shutil
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from urllib.parse import unquote

from psycopg2.extras import execute_values

from backend.database import get_db_connection, return_db_connection
from backend.bothits_verify import load as load_ip_ranges, verdict

logger = logging.getLogger(__name__)

# Historical archive: the 116 days already on disk. Folder names are DOWNLOAD
# dates, not log dates — folder "1-5-2026" holds logs for 2026-04-22..2026-05-01
# — so never trust the folder name, always read the date out of the filename.
BACKUP_DIR = os.getenv(
    "BOTHITS_BACKUP_DIR",
    "/mnt/c/Users/JoepvanSchagen/Downloads/claude/bothits_new/backup",
)
# Where new CloudFront exports get dropped for the scheduled ingest to pick up.
DROP_DIR = os.getenv(
    "BOTHITS_DROP_DIR",
    "/mnt/c/Users/JoepvanSchagen/Downloads/claude/bothits_drop",
)
KEEP_SOURCE = os.getenv("BOTHITS_KEEP_SOURCE", "") == "1"
# Hoe lang verwerkte bronbestanden in `_processed/` blijven staan (2026-08-13). Ze
# bleven er eeuwig, ~900 MB per logdatum, en op de dag dat dit erin ging stond er 30 GB
# — puur omdat niets ze ooit opruimde.
#
# 21 dagen en niet 7: de S3-bucket bewaart ~42 dagen, dus binnen deze termijn is een
# datum altijd nog opnieuw te downloaden ÉN nog lokaal te herladen zonder download. Dat
# tweede is precies waar het herstelpad van run_backfill --date op leunt, en dat wil je
# niet weggooien zolang een fout nog vers is. 0 = nooit opruimen.
STAGING_RETENTION_DAYS = int(os.getenv("BOTHITS_STAGING_RETENTION_DAYS", "21"))
MAX_WORKERS = int(os.getenv("BOTHITS_WORKERS", "12"))
# Named offenders kept per day per bot family among URLs missing from pa.urls.
TOP_UNKNOWN_PER_BOT = int(os.getenv("BOTHITS_TOP_UNKNOWN", "500"))

FILE_DATE_RX = re.compile(r"\.(\d{4}-\d{2}-\d{2})-(\d{2})\.")

# ---------------------------------------------------------------------------
# Crawler classification: (family, bot_class, pattern), first match wins.
#
# Ordering is load-bearing in two places:
#   * Google-AI and GoogleOther must be tested before the generic Googlebot
#     pattern, or "Google-Extended" gets swallowed by it.
#   * Applebot-Extended must be tested before Applebot, and meta-externalagent
#     before facebookexternalhit, for the same reason.
#
# Two vendors people expect to find here and won't, because they do not exist:
# Gemini has no crawler of its own (Google fetches with Google-Extended and
# Google-CloudVertexBot), and Copilot has none either (it rides on bingbot).
# ---------------------------------------------------------------------------
BOT_FAMILIES = [
    # --- AI: training corpora and answer-time retrieval ---------------------
    ("OpenAI",       "ai",     r"gptbot|chatgpt-user|oai-searchbot"),
    ("Anthropic",    "ai",     r"claudebot|claude-web|claude-user|claude-searchbot|anthropic-ai"),
    ("Perplexity",   "ai",     r"perplexitybot|perplexity-user"),
    ("Google-AI",    "ai",     r"google-extended|google-cloudvertexbot"),
    ("Apple-AI",     "ai",     r"applebot-extended"),
    ("Meta-AI",      "ai",     r"meta-externalagent|meta-externalfetcher"),
    ("ByteDance",    "ai",     r"bytespider|tiktokspider"),
    ("Amazon",       "ai",     r"amazonbot"),
    ("CommonCrawl",  "ai",     r"ccbot"),
    ("Mistral",      "ai",     r"mistralai-user"),
    ("Cohere",       "ai",     r"cohere-ai|cohere-training-data-crawler"),
    ("DuckAssist",   "ai",     r"duckassistbot"),
    ("You.com",      "ai",     r"youbot"),
    ("AllenAI",      "ai",     r"ai2bot"),
    ("Diffbot",      "ai",     r"diffbot"),
    ("Webz.io",      "ai",     r"omgilibot|omgili|webzio-extended"),
    ("Timpi",        "ai",     r"timpibot"),
    ("ImageSift",    "ai",     r"imagesiftbot"),
    # --- Classic search indexes --------------------------------------------
    ("GoogleOther",  "search", r"googleother"),
    ("Googlebot",    "search", r"googlebot|google-inspectiontool|storebot-google|"
                               r"google favicon|apis-google|adsbot-google|"
                               r"mediapartners-google"),
    ("Bing",         "search", r"bingbot|adidxbot|msnbot|bingpreview"),
    ("Apple",        "search", r"applebot"),
    ("DuckDuckGo",   "search", r"duckduckbot"),
    ("Yandex",       "search", r"yandex"),
    ("Baidu",        "search", r"baiduspider"),
    ("Petal",        "search", r"petalbot|aspiegel"),
    ("Naver",        "search", r"yeti/|naver"),
    ("Seznam",       "search", r"seznambot"),
    ("Sogou",        "search", r"sogou"),
    # --- Everything else ----------------------------------------------------
    ("Social",       "social", r"facebookexternalhit|facebookbot|twitterbot|"
                               r"linkedinbot|slackbot|whatsapp|discordbot|"
                               r"telegrambot|pinterest"),
    ("SEO-tools",    "seo-tool", r"ahrefsbot|semrushbot|mj12bot|dotbot|dataforseo|"
                                 r"screaming frog|sitebulb|seokicks|blexbot|barkrowler"),
    ("Monitoring",   "monitoring", r"uptimerobot|pingdom|newrelic|datadog|statuscake"),
    ("other-bot",    "other",  r"bot\b|crawler|spider|slurp|scrape|http-client|"
                               r"curl|wget|python-requests|go-http|java/|scrapy|"
                               r"firecrawl"),
]
BOT_RX = [(fam, cls, re.compile(p, re.I)) for fam, cls, p in BOT_FAMILIES]
BOT_CLASS = {fam: cls for fam, cls, _ in BOT_FAMILIES}
# Snelle afwijzing: de UNIE van alle patronen, opgebouwd uit dezelfde lijst zodat de
# twee niet uit elkaar kunnen lopen. Matcht deze niet, dan matcht geen enkel patroon —
# dus de uitkomst is identiek en we slaan 33 losse searches over. Ongeveer de helft van
# de logregels is geen bot, en die betaalden tot nu toe de volle rondgang.
ANY_BOT_RX = re.compile("|".join(f"(?:{p})" for _f, _c, p in BOT_FAMILIES), re.I)

# Longest-first so "Googlebot-Image" wins over "Googlebot" and
# "Applebot-Extended" over "Applebot".
CANON_NAMES = [
    "GPTBot", "ChatGPT-User", "OAI-SearchBot", "ClaudeBot", "Claude-Web",
    "Claude-User", "Claude-SearchBot", "PerplexityBot", "Perplexity-User",
    "Google-Extended", "Google-CloudVertexBot", "GoogleOther-Image",
    "GoogleOther-Video", "GoogleOther", "Googlebot-Image", "Googlebot-Video",
    "Googlebot-News", "Googlebot", "Storebot-Google", "Google-InspectionTool",
    "AdsBot-Google-Mobile", "AdsBot-Google", "Mediapartners-Google",
    "APIs-Google", "bingbot", "adidxbot", "msnbot", "BingPreview",
    "Applebot-Extended", "Applebot", "meta-externalagent",
    "meta-externalfetcher", "facebookexternalhit", "Twitterbot", "LinkedInBot",
    "Bytespider", "Amazonbot", "CCBot", "YandexBot", "YandexImages",
    "Baiduspider", "PetalBot", "SeznamBot", "DuckDuckBot", "DuckAssistBot",
    "MistralAI-User", "cohere-ai", "YouBot", "AI2Bot", "Diffbot", "Omgilibot",
    "Webzio-Extended", "Timpibot", "ImagesiftBot", "AhrefsBot", "SemrushBot",
    "MJ12bot", "DotBot",
]
NAME_RX = re.compile("(" + "|".join(
    sorted((re.escape(n) for n in CANON_NAMES), key=len, reverse=True)) + ")", re.I)
# The match is case-insensitive but re returns the text AS IT APPEARED in the
# user-agent, so "DiffBot" and "Diffbot" would become two dimension rows for
# one crawler. Fold the match back onto our own spelling.
CANON_BY_LOWER = {n.lower(): n for n in CANON_NAMES}

# ---------------------------------------------------------------------------
# UA -> (familie, botnaam), gememoïseerd (2026-08-13)
#
# Dit was de hot loop van de hele ingest: per LOGREGEL een unquote() plus tot 33
# regex-searches plus NAME_RX, over 6,9 mln regels per dag. En dat terwijl een
# logdatum maar een paar honderd unieke user-agents draagt — dezelfde string werd
# duizenden keren opnieuw ontleed.
#
# De memo zit op de RUWE (nog niet ge-unquote) string, want dat is wat zich herhaalt
# en het spaart de unquote ook uit. Semantiek blijft exact: dezelfde lijstvolgorde,
# dezelfde eerste-match-wint-regel, dezelfde CANON_BY_LOWER-terugvouwing. Bewust GEEN
# alternation met named groups als vervanging van de lus: die kiest de LEFTMOST match
# in de string en niet de eerste in lijstvolgorde, en dan zou een UA met een generieke
# 'bot\b' vóór een specifieke naam als other-bot eindigen.
#
# Grens erop omdat een spoofende client oneindig veel unieke UA's kan sturen; boven de
# grens blijft het gewoon werken, alleen zonder memo.
_UA_MEMO = {}
_UA_MEMO_MAX = int(os.getenv("BOTHITS_UA_MEMO_MAX", "50000"))


def classify_ua(raw_ua):
    """(familie, botnaam) of (None, None) voor de ruwe UA uit het logbestand."""
    hit = _UA_MEMO.get(raw_ua)
    if hit is not None:
        return hit
    ua = unquote(raw_ua)
    # Onvoorwaardelijk gezet (audit 2026-08-13). De unie-regex is uit dezelfde
    # patronen opgebouwd, dus "unie matcht maar geen enkel patroon" kan niet — geprobeerd
    # op alle 33 alternatieven en op 1.030 echte UA's, nul afwijkingen. Maar als die
    # invariant ooit breekt, gaf dit een UnboundLocalError in een worker en die sleept
    # via fut.result() de hele ingest mee. Een regel vangnet is goedkoper dan die dag.
    out = (None, None)
    if ANY_BOT_RX.search(ua):
        # De unie matchte, dus één van de patronen doet dat ook: fam wordt gezet.
        for name, _cls, rx in BOT_RX:
            if rx.search(ua):
                m = NAME_RX.search(ua)
                bot = CANON_BY_LOWER.get(m.group(1).lower(), m.group(1)) if m else name
                out = (name, bot)
                break
    if len(_UA_MEMO) < _UA_MEMO_MAX:
        _UA_MEMO[raw_ua] = out
    return out

# Families excluded from the per-URL tables. They still appear in the cube with
# full hit counts — this only stops an unbounded catch-all from generating URL
# rows. Override per bot afterwards via pa.bothits_bot.is_tracked.
#
# LET OP — die kolom heeft sinds 2026-08-13 een TWEEDE betekenis: bothits_service
# filtert er ook het hele dashboard op, want Joep wil alleen de drie Google-bots,
# Applebot en de grote AI-crawlers zien. In de DB staan daarom nog 16 families
# extra op false (Bing, Yandex, DuckDuckGo, Petal, Sogou, Baidu, Seznam, Naver,
# You.com, Cohere, Mistral, CommonCrawl, Diffbot, Apple-AI, Timpi, AllenAI).
#
# Die zestien staan hier met opzet NIET bij. Deze set bepaalt of er per-URL-rijen
# geschreven worden, en de URL-tabellen mogen hun volle breedte houden — het waren
# alleen de tabs die eruit gingen, niet de data. De DB-vlag is dus ruimer dan deze
# set; dat is geen inconsistentie maar het verschil tussen "wat we bewaren" en
# "wat we tonen". Wie een familie wil terugzetten in het dashboard doet dat met
# één UPDATE op pa.bothits_bot, niet hier.
UNTRACKED_FAMILIES = {"other-bot", "Monitoring", "SEO-tools", "Social"}

PRODUCTISH = {"product", "product_legacy"}
LEGACY_PRODUCT_RX = re.compile(r"^/[a-z0-9_-]+/d\d{6,}/")


def url_type(u):
    """Bucket a request path into the site's URL shapes."""
    if u.startswith("/p/"):
        return "product"
    if u.startswith("/products/"):
        return "category_facet" if "/c/" in u else "category"
    if u.startswith("/categories/"):
        return "category_legacy"
    if u.startswith("/sitemap"):
        return "sitemap"
    # Alleen "/": de aanroeper geeft `stem = unquote(...).rstrip("/") or "/"`, dus een
    # lege string bereikt deze functie niet. `or u == ""` stond hier als dode helft.
    if u == "/":
        return "home"
    if u.startswith("/robots.txt"):
        return "robots"
    if u.startswith("/info/") or u.startswith("/klantenservice"):
        return "info"
    if u.startswith("/r/"):
        return "search"
    if u.startswith("/l/"):
        return "list"
    if LEGACY_PRODUCT_RX.match(u):
        return "product_legacy"
    return "other"


def facet_depth(u):
    """Number of facet values in a /c/ URL; 0 for a plain category page."""
    if "/c/" not in u:
        return 0
    tail = u.split("/c/", 1)[1].strip("/")
    return 0 if not tail else tail.count("~~") + 1


def norm_host(h):
    h = (h or "-").lower()
    return h[4:] if h.startswith("www.") else h


# Welke domeinen de tellingen in mogen — een KEEP-list, geen skip-list (Joep,
# 2026-08-11). Elk domein dekt zichzelf plus zijn subdomeinen, dus "beslist.nl"
# houdt beslist.nl én shop.beslist.nl binnen.
#
# Een keep-list omdat er drie soorten hosts wegvielen die niets gemeen hebben:
# andere landen (beslist.be, shopcaddy.de en hun shop.*-varianten) en de
# CDN-distributies zelf (*.cloudfront.net, geen site maar de oorsprong). Met een
# skip-list moet je die alle drie blijven onderhouden en glipt een nieuwe
# distributie of markt er stil in; met een keep-list valt alles wat we niet
# expliciet willen automatisch af.
#
# Wat dit weghaalt, gemeten over de 23 geladen datums: 43.525.762 hits (45,08%),
# 790,6 GB uitgeserveerd verkeer en ~169 MB tabelruimte. Terugzetten staat in
# cc1/BOTHITS_PROCESS.md — het is deze env-var plus een re-ingest.
KEEP_DOMAINS = tuple(
    d.strip().lower()
    for d in os.getenv("BOTHITS_KEEP_DOMAINS", "beslist.nl").split(",")
    if d.strip()
)


# Voorgekookt uit KEEP_DOMAINS (fase 4). skip_host draait per logregel — ~500 mln per
# backfill — en bouwde daar elke keer `"." + d` op plus een generator. str.endswith neemt
# een tuple, dus dit is dezelfde test in C i.p.v. in een genexpr. Gemeten 0,162 -> 0,020
# µs per aanroep; uitkomst identiek.
_KEEP_SET = frozenset(KEEP_DOMAINS)
_KEEP_SUFFIXES = tuple("." + d for d in KEEP_DOMAINS)


def skip_host(h):
    """True als deze host niet onder KEEP_DOMAINS valt."""
    return not (h in _KEEP_SET or h.endswith(_KEEP_SUFFIXES))


def status_class(s):
    return (s[0] + "xx") if s and s[0:1].isdigit() else "?"


# ---------------------------------------------------------------------------
# pa.urls membership. Loaded once in the parent and inherited by the worker
# processes through fork, so the ~1M-entry dict is paged in once, not 12 times.
# ---------------------------------------------------------------------------
URL_IDS = {}


def load_url_ids():
    """url (no trailing slash) -> url_id for every row in pa.urls.

    Percent-ge-encodeerde rijen krijgen hun GEDECODEERDE vorm als extra sleutel
    (2026-08-13). De parser doet `unquote()` op het logpad, dus een pa.urls-rij die
    `%20` of `%C3%AB` in zijn url heeft kon nooit matchen: de lookup had de encoded
    vorm, de logregel de decoded. Gemeten 160 van 1.031.796 rijen (0,015%), dus het
    volume is klein — maar het is een gat dat je nooit ziet, want zulke URL's belanden
    stil in de onbekende bak.

    De letterlijke sleutel wint altijd: de alias komt er alleen bij als hij nog niet
    bestaat, zodat twee rijen die naar dezelfde string decoderen elkaar niet
    overschrijven en de exacte match onaangetast blijft.
    """
    global URL_IDS
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        # ORDER BY url_id (audit 2026-08-13): de dict-comprehension hieronder is
        # last-wins, en twee pa.urls-rijen kunnen na rstrip("/") dezelfde sleutel geven
        # (gemeten: 2 sleutels uit 4 rijen). Zonder vaste volgorde bepaalt Postgres welk
        # url_id overleeft, en dan herstelt een re-ingest níet exact dezelfde rijen —
        # wat BOTHITS_PROCESS.md wel belooft.
        cur.execute("SELECT url_id, url FROM pa.urls ORDER BY url_id")
        rows = cur.fetchall()
        cur.close()
    finally:
        return_db_connection(conn)
    URL_IDS = {r["url"].rstrip("/"): r["url_id"] for r in rows}
    exact = len(URL_IDS)
    for r in rows:
        u = r["url"].rstrip("/")
        if "%" not in u:
            continue
        dec = unquote(u).rstrip("/")
        if dec != u and dec not in URL_IDS:
            URL_IDS[dec] = r["url_id"]
    logger.info("pa.urls lookup loaded: %s urls (+%s gedecodeerde aliassen)",
                f"{exact:,}", f"{len(URL_IDS) - exact:,}")
    return URL_IDS


# De acht kolommen die de parser nodig heeft, in de volgorde waarin process_file ze
# uitpakt. Staan hier als lijst en niet als losse idx[...]-lookups zodat er precies
# één plek is die weet wat "leesbaar logbestand" betekent — en zodat een hernoemd
# veld een RuntimeError geeft in plaats van een dag met nul bots (audit 2026-08-13).
# x-host-header en niet cs(Host): de oude CSV-export liet die kolom vallen en werd
# daardoor onbruikbaar als bron, want dan weet je het domein niet meer.
REQUIRED_FIELDS = (
    "cs(User-Agent)", "x-host-header", "cs-uri-stem", "sc-status",
    "x-edge-result-type", "sc-bytes", "time-taken", "c-ip",
)


def process_file(path):
    """Parse one .gz log file.

    Returns (cube, known, unknown, raw_lines, bot_lines, failed) where
      cube    : (host, family, name, url_type, depth, is_known, status, edge,
                 verify_state)
                -> [hits, bytes, time_ms]
      known   : (url_id, host, family, name) -> [hits, bytes, n2, n3, n4, n5]
      unknown : (host, family, name, url_type, depth, url) -> hits
                (product pages excluded — they are near-unique per hit, so a
                 top-N over them is meaningless noise; the cube still counts them)
      failed  : 1 als het bestand niet volledig te lezen was, anders 0. ingest_date
                telt ze op en weigert de logdatum dan compleet te noemen — een
                afgebroken gzip gaf hiervoor stil een te korte dag (fase 2).
    """
    cube = collections.defaultdict(lambda: [0, 0, 0])
    known = collections.defaultdict(lambda: [0, 0, 0, 0, 0, 0])
    unknown = collections.Counter()
    raw_lines = 0
    bot_lines = 0
    bad_lines = 0
    failed = 0
    cols = None
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                if line[:1] == "#":
                    if line.startswith("#Fields:"):
                        names = line[len("#Fields:"):].split()
                        idx = {n: i for i, n in enumerate(names)}
                        # Namen één keer omzetten in posities (audit 2026-08-13), en
                        # hard falen als er één mist. Hiervóór stond er per regel een
                        # `except (IndexError, KeyError): continue`: hernoemt AWS een
                        # veld, dan gooide idx[...] op ELKE regel een KeyError, bleef
                        # raw_lines gewoon doortellen (die += staat ervóór) en werd
                        # bot_lines 0. Geen van beide tripwires ziet dat — die van
                        # ingest_date eist raw_lines == 0, en de known-URL-tripwire
                        # eist bot_lines > 100.000. Precies het gat waardoor een
                        # kapotte dag als een goede dag in de ledger belandt.
                        missing = [f for f in REQUIRED_FIELDS if f not in idx]
                        if missing:
                            raise RuntimeError(
                                f"{os.path.basename(path)}: CloudFront-log mist "
                                f"veld(en) {missing}. Kolommen in dit bestand: "
                                f"{names}. De parser kan deze datum niet lezen — "
                                f"pas REQUIRED_FIELDS/process_file aan en draai hem "
                                f"opnieuw. Niets weggeschreven."
                            )
                        cols = tuple(idx[f] for f in REQUIRED_FIELDS)
                    continue
                if cols is None:
                    continue
                raw_lines += 1
                p = line.rstrip("\n").split("\t")
                i_ua, i_host, i_stem, i_st, i_edge, i_bytes, i_taken, i_cip = cols
                try:
                    # RUW, niet ge-unquote: classify_ua() doet dat achter zijn memo.
                    raw_ua = p[i_ua]
                    host = norm_host(p[i_host])
                    raw_stem = p[i_stem]
                    st = p[i_st]
                    edge = p[i_edge] or "-"
                    nbytes = p[i_bytes]
                    taken = p[i_taken]
                    cip = p[i_cip]
                except IndexError:
                    # Alleen nog een te korte regel; de KeyError-tak is hierboven
                    # afgevangen. Geteld i.p.v. genegeerd, zodat een bestand dat
                    # massaal afgekapte regels bevat zichzelf meldt.
                    bad_lines += 1
                    continue
                # Vóór de bot-check, ná raw_lines: een host buiten KEEP_DOMAINS
                # verdwijnt uit alle tellingen, maar raw_lines blijft het aantal
                # regels in de logbestanden — dat is de volledigheidsmaat van de
                # ledger en moet op het bestand kloppen.
                if skip_host(host):
                    continue

                fam, bot = classify_ua(raw_ua)
                if fam is None:
                    continue
                bot_lines += 1
                tracked = fam not in UNTRACKED_FAMILIES

                # unquote() PAS hier (fase 4). Hij stond boven skip_host en
                # classify_ua, terwijl 55% van de regels non-bot is (gemeten op 159.887
                # echte regels) — dat is meer dan de helft van het decodeerwerk voor een
                # pad dat daarna wordt weggegooid. Verplaatsen kan omdat `stem` tot hier
                # nergens wordt gebruikt; de uitkomst is byte-voor-byte identiek.
                stem = unquote(raw_stem).rstrip("/") or "/"

                ut = url_type(stem)
                depth = facet_depth(stem)
                sc = status_class(st)
                url_id = URL_IDS.get(stem)

                try:
                    nb = int(nbytes)
                except ValueError:
                    nb = 0
                try:
                    tms = int(float(taken) * 1000)
                except ValueError:
                    tms = 0

                # IP-verificatie op de gepubliceerde ranges van de operator. Zit als
                # dimensie in de cube en NIET als filter: de spoof-graad is 0,4% van
                # de hits, dus wegfilteren verandert geen cijfer maar kost wel data.
                # 'failed' is daarmee een tripwire i.p.v. een stille correctie.
                vs = verdict(cip, fam)

                c = cube[(host, fam, bot, ut, depth, url_id is not None, sc, edge, vs)]
                c[0] += 1
                c[1] += nb
                c[2] += tms

                if not tracked:
                    continue
                if url_id is not None:
                    k = known[(url_id, host, fam, bot)]
                    k[0] += 1
                    k[1] += nb
                    # LET OP: deze vier zijn een SUBSET van k[0], geen partitie.
                    # status_class() geeft ook '0xx' terug — CloudFront logt sc-status
                    # 000 bij een afgebroken verbinding — en dat valt bewust in geen
                    # bucket. Gemeten: 31.107 hits in de cube, en 1.984 url_daily-rijen
                    # waar hits > n_2xx+n_3xx+n_4xx+n_5xx. NIET bij 2xx optellen: een
                    # afgebroken verbinding is geen geslaagde request. Wie de vier
                    # kolommen leest moet het verschil als "overig" tonen, en dat doet
                    # het URL-detailpaneel sinds fase 4.
                    if sc == "2xx":
                        k[2] += 1
                    elif sc == "3xx":
                        k[3] += 1
                    elif sc == "4xx":
                        k[4] += 1
                    elif sc == "5xx":
                        k[5] += 1
                elif ut not in PRODUCTISH:
                    unknown[(host, fam, bot, ut, depth, stem)] += 1
    except (EOFError, OSError, gzip.BadGzipFile) as exc:
        # Een afgebroken gzip-stream levert de regels op die er vóór de fout uit kwamen,
        # en die gingen SHORT mee naar de ledger terwijl de dag is_complete=true bleef.
        # Nu wordt het bestand als mislukt teruggegeven en weigert ingest_date de dag
        # compleet te noemen (fase 2). gzip.BadGzipFile is een subklasse van OSError en
        # staat er alleen voor de leesbaarheid bij.
        logger.warning("skipping unreadable %s (na %s regels): %s",
                       os.path.basename(path), f"{raw_lines:,}", exc)
        failed = 1
    # Twee dingen die hiervóór volledig stil waren.
    if cols is None:
        logger.warning("%s: geen #Fields:-header gevonden, 0 regels gelezen",
                       os.path.basename(path))
    elif bad_lines:
        logger.warning("%s: %s van %s regels te kort om te parsen",
                       os.path.basename(path), f"{bad_lines:,}", f"{raw_lines:,}")
    return dict(cube), dict(known), unknown, raw_lines, bot_lines, failed


# ---------------------------------------------------------------------------
# Dimension upserts
# ---------------------------------------------------------------------------
# verify_state is later toegevoegd (2026-08-11), dus de bestaande tabel moet
# meegroeien. Zelfde vorm als faq_v2_publisher's migratie: eerst een goedkope
# catalogus-check, want ALTER TABLE pakt een AccessExclusiveLock óók als er niets
# te doen valt, en dat deadlockt tegen een lopende ingest. Bestaande rijen krijgen
# 'unchecked' — eerlijk, want die zijn geladen vóór er verificatie was.
#
# hours_present en is_complete zijn hier op 2026-08-13 bijgekomen (audit). Die twee
# bestonden alleen in de LIVE database, met de hand ge-ALTERd en nergens vastgelegd:
# niet in scripts/bothits_schema.sql en niet hier. Wie de tabellen uit dat bestand
# opbouwde kreeg dus een ledger waar de ingest niet in kán schrijven — de INSERT
# faalt op UndefinedColumn ná een parse van ~30 s, en /meta, /daily en /ingest/log
# 500'en allemaal omdat de querylaag ze wél leest. Nu convergeert een bestaande
# installatie via ADD COLUMN IF NOT EXISTS en klopt het schemabestand als bron.
SCHEMA_MIGRATE = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'pa' AND table_name = 'bothits_daily'
           AND column_name = 'verify_state'
    ) THEN
        ALTER TABLE pa.bothits_daily
            ADD COLUMN verify_state text NOT NULL DEFAULT 'unchecked';
        ALTER TABLE pa.bothits_daily DROP CONSTRAINT bothits_daily_pkey;
        ALTER TABLE pa.bothits_daily ADD PRIMARY KEY
            (log_date, host_id, bot_id, url_type, facet_depth, is_known_url,
             status_class, edge_result, verify_state);
    END IF;

    -- Zelfde vorm en om dezelfde reden een catalogus-check en niet alleen
    -- ADD COLUMN IF NOT EXISTS: die variant pakt de AccessExclusiveLock óók als er
    -- niets te doen valt, en deze migratie loopt bij ELKE ingest_date().
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'pa' AND table_name = 'bothits_ingest'
           AND column_name = 'hours_present'
    ) THEN
        ALTER TABLE pa.bothits_ingest ADD COLUMN hours_present smallint;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'pa' AND table_name = 'bothits_ingest'
           AND column_name = 'is_complete'
    ) THEN
        -- DEFAULT true omdat elke rij die al bestond een volledige dag was; de
        -- ingest geeft de waarde daarna altijd expliciet mee.
        ALTER TABLE pa.bothits_ingest
            ADD COLUMN is_complete boolean NOT NULL DEFAULT true;
    END IF;

    -- failed_files / expected_files (fase 2 van de audit 2026-08-13). Bestaande rijen
    -- krijgen 0 en NULL: we weten van die runs niet hoeveel bestanden onleesbaar waren
    -- of wat S3 had, en dat eerlijk als "onbekend" laten staan is beter dan een nul die
    -- als bewijs van volledigheid gelezen kan worden.
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'pa' AND table_name = 'bothits_ingest'
           AND column_name = 'failed_files'
    ) THEN
        ALTER TABLE pa.bothits_ingest
            ADD COLUMN failed_files integer NOT NULL DEFAULT 0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'pa' AND table_name = 'bothits_ingest'
           AND column_name = 'expected_files'
    ) THEN
        ALTER TABLE pa.bothits_ingest ADD COLUMN expected_files integer;
    END IF;
END $$;
"""


def _ensure_schema(cur):
    cur.execute(SCHEMA_MIGRATE)


def _dim_ids(conn, hosts, bots):
    """Ensure host/bot dimension rows exist; return {host: id}, {(fam,name): id}."""
    cur = conn.cursor()
    if hosts:
        execute_values(
            cur,
            "INSERT INTO pa.bothits_host (host) VALUES %s ON CONFLICT (host) DO NOTHING",
            [(h,) for h in sorted(hosts)],
        )
    if bots:
        # DO NOTHING, not DO UPDATE: is_tracked is a user-editable flag and a
        # re-ingest must not stomp a manual override back to the default.
        execute_values(
            cur,
            "INSERT INTO pa.bothits_bot (bot_family, bot_name, bot_class, is_tracked) "
            "VALUES %s ON CONFLICT (bot_family, bot_name) DO NOTHING",
            [(f, n, BOT_CLASS.get(f, "other"), f not in UNTRACKED_FAMILIES)
             for f, n in sorted(bots)],
        )
    cur.execute("SELECT host_id, host FROM pa.bothits_host")
    host_ids = {r["host"]: r["host_id"] for r in cur.fetchall()}
    cur.execute("SELECT bot_id, bot_family, bot_name FROM pa.bothits_bot")
    bot_ids = {(r["bot_family"], r["bot_name"]): r["bot_id"] for r in cur.fetchall()}
    cur.close()
    return host_ids, bot_ids


# ---------------------------------------------------------------------------
# Worker initializer for spawn-context (Windows). Loads the lookup tables
# that fork would have inherited via copy-on-write. Each of the N workers
# calls this once at startup, so it's N database queries instead of 1 —
# the price of not having fork.
# ---------------------------------------------------------------------------
def _worker_init():
    load_url_ids()
    load_ip_ranges()


# ---------------------------------------------------------------------------
# Ingest one log date
# ---------------------------------------------------------------------------
def ingest_date(log_date, files, source_dirs="", n_hours=24, expected_files=None,
                dist_hours=None):
    """Parse every file for one log date and replace that date in the DB.

    n_hours is how many of the 24 hourly buckets the archive actually holds for
    this date. Five dates in the backfill are cut off mid-day (the last day of
    each download batch), and a partial day plotted next to full ones reads as
    a traffic collapse that never happened — so it is recorded, not smoothed.

    `expected_files` is wat S3 zei te hebben voor deze datum (uit het manifest dat
    bothits_s3.fetch() achterlaat) en is de ENIGE harde volledigheidsmaat die we hebben.
    None bij een backfill uit het lokale archief: daar is geen autoriteit om tegen te
    toetsen, dus dan valt de check terug op uren + leesbare bestanden.

    Waarom het bestandsAANTAL zelf geen maat is: complete dagen lopen legitiem van
    1.591 tot 4.969 bestanden (gemeten over 151 datums), dus een drempel daarop zegt
    niets. `raw_lines` is wél stabiel (5,6–7,9 mln/dag) maar dat weet je pas ná het
    parsen. Vandaar de vergelijking met S3's eigen key-listing.
    """
    t0 = time.time()
    logger.info("[%s] %s files (%s/24 hours)", log_date, f"{len(files):,}", n_hours)
    _warn_thin_distributions(log_date, dist_hours)

    # In de PARENT, vóór het forken — net als load_url_ids(). De workers erven de
    # opzoektabel dan via fork in plaats van de lijsten twaalf keer op te halen.
    # Een mislukte fetch is geen fout: verdict() geeft dan 'unchecked'.
    load_ip_ranges()

    cube = collections.defaultdict(lambda: [0, 0, 0])
    known = collections.defaultdict(lambda: [0, 0, 0, 0, 0, 0])
    unknown = collections.Counter()
    raw_lines = bot_lines = 0
    failed_files = 0
    done = 0

    # EXPLICIET forken, en niet op de default vertrouwen (2026-08-13). Dit was de
    # oorzaak van de kapotte `is_known_url`: `uvicorn --reload` start de app zelf via
    # een spawn-context, en een child erft die default. Een ProcessPoolExecutor in de
    # server SPAWNDE dus, waardoor elke worker de module opnieuw importeerde en met een
    # LEGE URL_IDS en IP_RANGES begon. Beide symptomen kwamen daaruit: known_rows = 0
    # (elke URL leek onbekend) en verify_state = 'unchecked' voor alles (geen ranges om
    # tegen te toetsen). Twee globals, één oorzaak.
    #
    # Zelfde code buiten de server draaide altijd goed — daar is de default fork — wat
    # verklaart waarom de backfill uit het archief wél klopt en alleen wat via de knop
    # is geladen kapot is. `process_file()` was nooit stuk.
    #
    # Vandaar de expliciete context: deze ingest deelt ~1M dict-entries met de workers
    # via copy-on-write en dat is niet optioneel. Op Windows bestaat fork niet —
    # daar gebruiken we spawn met een initializer die de opzoektabellen in elke
    # worker laadt. Duurder (12× DB-query), maar correct en onvermijdelijk.
    _can_fork = "fork" in multiprocessing.get_all_start_methods()
    if _can_fork:
        mp_ctx = multiprocessing.get_context("fork")
        pool_kwargs = dict(mp_context=mp_ctx)
    else:
        mp_ctx = multiprocessing.get_context("spawn")
        pool_kwargs = dict(mp_context=mp_ctx, initializer=_worker_init)
    with ProcessPoolExecutor(max_workers=min(MAX_WORKERS, os.cpu_count() or 4),
                             **pool_kwargs) as ex:
        futs = [ex.submit(process_file, f) for f in files]
        for fut in as_completed(futs):
            c, k, u, rl, bl, ff = fut.result()
            for key, v in c.items():
                t = cube[key]
                t[0] += v[0]; t[1] += v[1]; t[2] += v[2]
            for key, v in k.items():
                t = known[key]
                for i in range(6):
                    t[i] += v[i]
            unknown.update(u)
            raw_lines += rl
            bot_lines += bl
            failed_files += ff
            done += 1
            if done % 500 == 0:
                logger.info("[%s]   %s/%s files | cube=%s known=%s unknown=%s",
                            log_date, done, len(files), f"{len(cube):,}",
                            f"{len(known):,}", f"{len(unknown):,}")

    # Trim the unknown tail to the loudest offenders per bot family.
    per_family = collections.defaultdict(list)
    for key, hits in unknown.items():
        per_family[key[1]].append((hits, key))
    # heapq.nlargest i.p.v. een volledige sort (fase 4): de dict is ~1,1 mln entries en
    # er blijven 500 per familie over, dus een complete sort per familie is werk dat
    # meteen wordt weggegooid. De uitkomst is identiek — de (hits, key)-tuples zijn
    # totaal geordend, dus ties breken op dezelfde manier.
    trimmed = []
    for fam, rows in per_family.items():
        trimmed.extend(heapq.nlargest(TOP_UNKNOWN_PER_BOT, rows))

    hosts = {k[0] for k in cube}
    bots = {(k[1], k[2]) for k in cube}

    # Tripwire vóór de DB-write (2026-08-13). De bestaande tripwire hieronder eist
    # `bot_lines > 100_000`, dus juist het ergste geval glipt erdoor: een parse die
    # HELEMAAL niets oplevert. Gemeten: 2026-08-09 kreeg een ledgerregel met
    # files=2904, raw_lines=0, duration_s=0 en nul rijen in alle drie de tabellen —
    # "2904 bestanden verwerkt" in nul seconden. De worker-pool was stuk (de machine
    # kwam net uit host-slaap) en gaf lege resultaten terug; niets in de keten vond dat
    # verdacht, en de dag stond daarna als volledig in de ledger met is_complete=true.
    #
    # Een dag met bestanden MOET regels opleveren; een CloudFront-logbestand is nooit
    # leeg. Daarom hier hard stoppen in plaats van waarschuwen: een ontbrekende
    # ledgerregel laat de dag opnieuw oppakken, een geschreven regel met nullen
    # verstopt zich in de cijfers als een verkeersinstorting die nooit gebeurd is.
    if files and not raw_lines:
        raise RuntimeError(
            f"[{log_date}] parse leverde 0 regels uit {len(files)} bestanden — "
            f"vermoedelijk een kapotte worker-pool. Niets weggeschreven; "
            f"draai deze datum opnieuw."
        )

    # Deze tripwire stond tot 2026-08-13 ONDER de commit en was alleen een logregel
    # (fase 2). Dat is te laat op precies de dag dat hij nodig is: de datum staat dan al
    # in de ledger als volledig, en in run_drop is de eerstvolgende stap `_archive()` —
    # dus het bewijsmateriaal verhuist naar de stapel die _prune_archive later opruimt.
    # Nul bekende URL's op miljoenen bot-hits kan niet: pa.urls heeft ~1M rijen en
    # Googlebot crawlt categoriepagina's. Dit heeft 30 logdatums stil verpest omdat
    # niemand naar known_rows keek tot een grafiek er raar uitzag.
    if bot_lines > 100_000 and not known:
        raise RuntimeError(
            f"[{log_date}] TRIPWIRE: {bot_lines:,} bot-hits en NUL bekende URL's — de "
            f"pa.urls-lookup heeft de workers niet bereikt (URL_IDS={len(URL_IDS):,} in "
            f"de parent). Controleer of de pool forkt; onder een spawn-context begint "
            f"elke worker met een lege lookup. Niets weggeschreven; draai deze datum "
            f"opnieuw."
        )

    # Volledigheid uit DRIE voorwaarden (fase 2), waar het tot 2026-08-13 alleen
    # `n_hours >= 24` was:
    #   1. alle 24 uurbuckets aanwezig — vangt een halve dag in de dropfolder;
    #   2. geen onleesbaar bestand — een afgebroken gzip gaf hiervoor stil een te
    #      korte dag die als volledig in de ledger belandde;
    #   3. minstens zoveel bestanden als S3 zei te hebben — de enige harde maat, en
    #      alleen beschikbaar als er een fetch-manifest ligt (None bij een backfill
    #      uit het lokale archief; dan blijft het bij 1 en 2).
    # Expliciet NIET meegenomen: "elke distributie heeft 24 uur". Zie
    # _warn_thin_distributions() — dat is gemeten en levert valse negatieven op.
    short = expected_files is not None and len(files) < expected_files
    complete = bool(n_hours >= 24 and not failed_files and not short)
    if failed_files:
        logger.error("[%s] %s van %s bestanden waren niet volledig te lezen — de datum "
                     "wordt als INCOMPLEET geboekt en kan opnieuw", log_date,
                     failed_files, len(files))
    if short:
        logger.error("[%s] %s bestanden verwerkt maar S3 had er %s — de datum wordt als "
                     "INCOMPLEET geboekt en kan opnieuw", log_date, len(files),
                     expected_files)

    conn = get_db_connection()
    try:
        host_ids, bot_ids = _dim_ids(conn, hosts, bots)
        cur = conn.cursor()
        _ensure_schema(cur)
        # Delete-then-insert makes a re-run idempotent.
        for tbl in ("pa.bothits_daily", "pa.bothits_url_daily",
                    "pa.bothits_unknown_daily"):
            cur.execute(f"DELETE FROM {tbl} WHERE log_date = %s", (log_date,))

        execute_values(cur, """
            INSERT INTO pa.bothits_daily
              (log_date, host_id, bot_id, url_type, facet_depth, is_known_url,
               status_class, edge_result, verify_state, hits, bytes, sum_time_ms)
            VALUES %s
        """, [(log_date, host_ids[k[0]], bot_ids[(k[1], k[2])], k[3], k[4],
               k[5], k[6], k[7], k[8], v[0], v[1], v[2]) for k, v in cube.items()],
            page_size=5000)

        execute_values(cur, """
            INSERT INTO pa.bothits_url_daily
              (log_date, url_id, host_id, bot_id, hits, bytes, n_2xx, n_3xx, n_4xx, n_5xx)
            VALUES %s
        """, [(log_date, k[0], host_ids[k[1]], bot_ids[(k[2], k[3])],
               v[0], v[1], v[2], v[3], v[4], v[5]) for k, v in known.items()],
            page_size=5000)

        execute_values(cur, """
            INSERT INTO pa.bothits_unknown_daily
              (log_date, host_id, bot_id, url, url_type, facet_depth, hits)
            VALUES %s
        """, [(log_date, host_ids[k[0]], bot_ids[(k[1], k[2])], k[5], k[3], k[4], hits)
              for hits, k in trimmed], page_size=5000)

        dur = int(time.time() - t0)
        cur.execute("""
            INSERT INTO pa.bothits_ingest
              (log_date, files, raw_lines, bot_lines, known_rows, source_dirs,
               duration_s, hours_present, failed_files, expected_files,
               is_complete, ingested_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
            ON CONFLICT (log_date) DO UPDATE SET
              files=EXCLUDED.files, raw_lines=EXCLUDED.raw_lines,
              bot_lines=EXCLUDED.bot_lines, known_rows=EXCLUDED.known_rows,
              source_dirs=EXCLUDED.source_dirs, duration_s=EXCLUDED.duration_s,
              hours_present=EXCLUDED.hours_present,
              failed_files=EXCLUDED.failed_files,
              expected_files=EXCLUDED.expected_files,
              is_complete=EXCLUDED.is_complete, ingested_at=now()
        """, (log_date, len(files), raw_lines, bot_lines, len(known),
              source_dirs[:500], dur, n_hours, failed_files, expected_files,
              complete))
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)

    logger.info("[%s] done in %ss | raw=%s bot=%s | cube=%s url=%s unknown=%s%s",
                log_date, dur, f"{raw_lines:,}", f"{bot_lines:,}",
                f"{len(cube):,}", f"{len(known):,}", f"{len(trimmed):,}",
                "" if complete else "  [INCOMPLEET]")
    # De known-URL-tripwire stond hier, ná de commit, en alleen als logregel. Hij staat
    # nu vóór de write en gooit — zie daar.
    return {
        "log_date": str(log_date), "files": len(files), "raw_lines": raw_lines,
        "bot_lines": bot_lines, "known_rows": len(known),
        "unknown_rows": len(trimmed), "cube_rows": len(cube), "duration_s": dur,
        "failed_files": failed_files, "expected_files": expected_files,
        "is_complete": complete,
    }


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------
def scan_tree(root, include_archived=False):
    """-> {log_date: [paths]}, {log_date: set(hours)}, {log_date: set(dirs)},
          {log_date: {dist: set(hours)}}.

    Two properties of the archive this has to survive:

      * A log date can be split across download folders — folder "26-3-2026"
        runs back to 2026-02-14, so 2026-03-15/16 also live in "16-3-2026".
        Walking the whole tree and keying on the date parsed out of the
        FILENAME (never the folder name) is what makes a date whole again.
      * Some folders hold an uncompressed copy of every .gz next to it, same
        name minus the suffix. Taking only .gz drops those duplicates; all 1039
        of them were verified to have a .gz twin, so nothing is lost.

    Files are then deduplicated on basename, because a date spanning two
    folders can list the same CloudFront object twice and it would otherwise be
    counted twice.

    `include_archived` neemt `_processed/` mee (2026-08-13). Normaal blijft die map
    buiten beeld, anders zou elke run alles wat hij ooit verwerkte opnieuw oppakken.
    Maar het herstelrecept — ledger-rij weggooien en opnieuw verwerken — vond daardoor
    NIETS, want de bestanden staan juist daar; op 13 augustus moest ik ze handmatig
    terugverhuizen om één datum te kunnen herladen. Met deze vlag kan run_backfill met
    `--date` uit het archief lezen zonder eerst 900 MB per dag opnieuw te downloaden.
    """
    seen = {}
    hours = collections.defaultdict(set)
    dirs = collections.defaultdict(set)
    # Uren PER DISTRIBUTIE, als diagnose en niet als poort — zie de meting in
    # _warn_thin_distributions() voor waarom dat onderscheid belangrijk is.
    dist_hours = collections.defaultdict(lambda: collections.defaultdict(set))
    for dirpath, _dirnames, filenames in os.walk(root):
        if not include_archived and "_processed" in dirpath.split(os.sep):
            continue
        for fn in filenames:
            if not fn.endswith(".gz"):
                continue
            m = FILE_DATE_RX.search(fn)
            if not m:
                continue
            d = m.group(1)
            if fn not in seen:
                seen[fn] = (d, os.path.join(dirpath, fn))
                hours[d].add(m.group(2))
                # De distributie is het deel vóór de eerste punt:
                # 'E3QQH7GDBASLV1.2026-08-12-19.17a7c0f4.gz'
                dist_hours[d][fn.split(".", 1)[0]].add(m.group(2))
            dirs[d].add(os.path.basename(dirpath))
    by_date = collections.defaultdict(list)
    for d, path in seen.values():
        by_date[d].append(path)
    return by_date, hours, dirs, dist_hours


# Sidecar dat bothits_s3.fetch() per datum achterlaat: hoeveel keys S3 had en hoeveel
# downloads faalden. Een bestandje en geen returnwaarde omdat download en ingest twee
# fases zijn met de staging-map als enige koppeling — zo overleeft het aantal ook een
# crash tussen de twee, en weet een backfill uit het archief gewoon dat er niets ligt.
MANIFEST_DIR = "_manifest"


def _read_manifest(src, log_date):
    """-> (expected_files, download_failed) of (None, 0) als er geen manifest is."""
    p = os.path.join(src, MANIFEST_DIR, f"{log_date}.json")
    try:
        with open(p, encoding="utf-8") as f:
            m = json.load(f)
        return m.get("expected_files"), int(m.get("failed") or 0)
    except (OSError, ValueError):
        return None, 0


def _warn_thin_distributions(log_date, dist_hours):
    """Waarschuw als een distributie minder dan 24 uurbestanden heeft. GEEN poort.

    Dit was in de audit als harde eis voorgesteld — "een dag is pas compleet als élke
    distributie 24 uur heeft" — en de meting zegt dat dat fout is. Gemeten op de 21
    datums in de staging (2026-08-13): DRIE datums hebben een distributie met minder
    dan 24 uur (07-31: 22, 08-10: 23, 08-11: 23), en het is elke keer
    E14VW8EO449KG7 — de kleinste distributie, 139 bestanden per dag, dus ~5,8 per uur.
    De missende uren zijn 00, 02 en 19. Dat is geen verloren data maar een uur zonder
    één request: CloudFront schrijft dan geen bestand.

    Zou dit een poort zijn, dan viel 14% van de datums om als "incompleet" en zou
    run_drop ze daarna nooit meer oppakken — een verzonnen probleem met echte schade.
    De ENIGE betrouwbare volledigheidsmaat is wat S3 zelf zegt te hebben; die komt via
    expected_files uit het manifest van bothits_s3.fetch(). Dit hier is de zachte
    tegenhanger: het valt op in het log als er iets echt scheef staat.
    """
    thin = {d: sorted(hs) for d, hs in (dist_hours or {}).items() if len(hs) < 24}
    if not thin:
        return
    for dist, hs in sorted(thin.items()):
        missing = sorted({f"{h:02d}" for h in range(24)} - set(hs))
        logger.warning("[%s] distributie %s heeft %s/24 uur (mist %s) — normaal bij een "
                       "distributie met weinig verkeer, verdacht bij een grote",
                       log_date, dist, len(hs), ",".join(missing))


def already_ingested(with_completeness=False):
    """-> set(datums), of met `with_completeness` een {datum: is_complete}-dict.

    Die tweede vorm bestaat omdat "staat in de ledger" tot 2026-08-13 gelezen werd als
    "is klaar" (fase 2 van de audit). Dat is niet hetzelfde: run_backfill laadt partiële
    datums bewust wél, en er stonden er vijf in (03-26 17/24, 04-13 8/24, 04-21 8/24,
    05-01 9/24, 06-09 9/24). run_drop zag zo'n datum als klaar, archiveerde de
    bronbestanden en _prune_archive wiste ze na de retentie — en buiten het S3-venster
    van ~42 dagen is dat definitief. Nu mag een incomplete datum opnieuw.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT log_date, is_complete FROM pa.bothits_ingest")
        rows = cur.fetchall()
        cur.close()
        if with_completeness:
            return {str(r["log_date"]): bool(r["is_complete"]) for r in rows}
        return {str(r["log_date"]) for r in rows}
    finally:
        return_db_connection(conn)


def run_backfill(src=None, limit=None, redo=False, only=None):
    """Ingest every log date found under src that isn't loaded yet.

    Unlike the drop folder this does NOT skip partial days: the archive's five
    cut-off dates are all the history there is for them, so they are loaded and
    flagged rather than withheld.

    Met `only` (de `--date`-vlag) leest hij ook uit `_processed/` — dat is het
    herstelpad voor één datum (2026-08-13). Verwerkte bestanden staan daar, dus
    zonder die vlag zou een gerichte herlaad ze niet vinden en moest je 900 MB per
    dag opnieuw uit S3 halen. Alleen bij `only`, want een backfill zonder filter zou
    anders alles wat hij ooit verwerkte opnieuw oppakken.
    """
    src = src or BACKUP_DIR
    load_url_ids()
    by_date, hours, dirs, dist_hours = scan_tree(src, include_archived=bool(only))
    done = set() if redo else already_ingested()
    todo = sorted(d for d in by_date if d not in done)
    if only:
        # sorted() en niet de dict-volgorde van by_date (audit 2026-08-13): die volgt
        # os.walk, dus meerdere --date-vlaggen werden in willekeurige volgorde
        # verwerkt. En een gevraagde datum die niet in de boom staat leverde stil
        # "0 to do" op zonder te zeggen WELKE hij niet vond — juist op het herstelpad.
        todo = sorted(set(only) & set(by_date))
        absent = sorted(set(only) - set(by_date))
        if absent:
            logger.warning("backfill: gevraagde datum(s) niet gevonden onder %s: %s",
                           src, ", ".join(absent))
    if limit:
        todo = todo[:limit]
    logger.info("backfill: %s dates found, %s already ingested, %s to do",
                len(by_date), len(done & set(by_date)), len(todo))
    results, failed = [], []
    cancelled = False
    for i, d in enumerate(todo, 1):
        # Zelfde annuleergrens als run_drop: tussen twee logdatums (2026-08-13).
        #
        # NUANCE na de audit van 2026-08-13: in-process is deze tak niet te bereiken.
        # _cancel wordt alleen gezet door request_cancel(), die niets doet tenzij
        # _ingest_state["running"] aan staat, en dat zet alleen start_ingest_async —
        # wiens worker run_DROP aanroept, nooit run_backfill. De enige aanroeper van
        # run_backfill is main(), een eigen CLI-proces met eigen module-globals.
        # De guard blijft staan omdat hij correct is en meteen werkt zodra er een route
        # of een signal-handler op komt; hij doet vandaag alleen niets.
        if _cancel.is_set():
            logger.info("backfill: geannuleerd na %s van %s datums", i - 1, len(todo))
            cancelled = True
            break
        logger.info("=== %s/%s : %s ===", i, len(todo), d)
        try:
            exp, _dl_failed = _read_manifest(src, d)
            results.append(ingest_date(date.fromisoformat(d), by_date[d],
                                       ",".join(sorted(dirs[d])), len(hours[d]),
                                       expected_files=exp, dist_hours=dist_hours[d]))
        except Exception as exc:
            # Gefaalde datums werden alleen gelogd en verdwenen daarna volledig: ze
            # kwamen niet in `results`, dus de aanroeper kon "mislukt" niet van
            # "stond er niet" onderscheiden (audit 2026-08-13). Nu komen ze terug.
            logger.error("[%s] FAILED: %s", d, exc, exc_info=True)
            failed.append({"log_date": d, "reason": str(exc)})
    # Ook hier opruimen (audit 2026-08-13). _prune_archive hing alleen aan run_drop,
    # terwijl het werk juist via backfill loopt — het herstelpad, de 30-datum-herlaad,
    # de CLI. Gevolg gemeten op 13-08: 18 datummappen voorbij de 21-daagse grens,
    # 18 GB, terwijl de retentie er sinds 13-08 in zit en dacht zijn werk te doen.
    # Ná de datums, niet ertussen, om dezelfde reden als in run_drop.
    freed = _prune_archive(src)
    if freed:
        logger.info("backfill: %s MB staging opgeruimd", round(freed / 1e6))
    if failed:
        logger.error("backfill: %s van %s datums mislukt (%s)", len(failed), len(todo),
                     ", ".join(f["log_date"] for f in failed))
    # Zelfde vorm als run_drop (audit 2026-08-13). Hiervoor kwam er een plátte lijst
    # terug waarin een mislukte datum simpelweg ontbrak, dus "mislukt" en "stond er
    # niet" waren niet te onderscheiden. Geen enkele aanroeper las de oude
    # returnwaarde — main() gooide hem weg — dus dit breekt niets.
    return {"status": ("cancelled" if cancelled else
                       "failed" if failed and not results else
                       "partial" if failed else "ok"),
            "dir": src, "cancelled": cancelled, "processed": results,
            "failed": failed,
            "archive_freed_mb": round(freed / 1e6) if freed else 0}


def run_drop(src=None):
    """Ingest complete dates from the drop folder, then archive their files.

    A date is only ingested once all 24 hour-buckets are present, so a folder
    still being copied in doesn't get loaded as a half day and then silently
    left that way.

    Kijkt sinds 2026-08-13 ECHT niet meer in `_processed/`. De skip in scan_tree
    vergeleek de basename met "_processed" en sloeg daarmee alleen die map zelf over,
    niet de datum-submappen eronder — dus elke run schuimde het hele archief af
    (46.097 bestanden op het moment van meten, groeiend met ~2.900 per dag) en
    "archiveerde" datums die er al stonden nog een keer over zichzelf. Nu scant hij
    alleen wat nieuw is.
    Wat dat kost: een datum waarvan je de ledger-rij weggooit wordt niet meer
    stilzwijgend uit het archief herladen. Dat was per ongeluk het herstelpad; nu is
    het een expliciete: `python -m backend.bothits_ingest backfill --src <staging>
    --date 2026-08-12`.
    """
    src = src or DROP_DIR
    if not os.path.isdir(src):
        # Zelfde sleutels als de gewone uitgang, zodat een aanroeper niet hoeft te
        # weten via welke tak hij hier komt.
        return {"status": "no_drop_dir", "dir": src, "cancelled": False,
                "processed": [], "skipped": [], "failed": [],
                "archive_freed_mb": 0}
    load_url_ids()
    by_date, hours, dirs, dist_hours = scan_tree(src)
    # Mét volledigheid (fase 2): een datum die er wél staat maar incompleet is, mag
    # opnieuw. Dat is precies het geval waarin de oude code de bronbestanden opruimde
    # zonder ze ooit volledig gelezen te hebben.
    done = already_ingested(with_completeness=True)

    processed, skipped, failed = [], [], []
    cancelled = False
    for d in sorted(by_date):
        # Annuleergrens: tussen twee logdatums. Alles wat al geladen is blijft
        # staan en is geldig; de rest ligt nog in de dropfolder en wordt bij een
        # volgende run alsnog opgepakt.
        if _cancel.is_set():
            cancelled = True
            skipped.append({"log_date": d, "reason": "geannuleerd"})
            continue
        if len(hours[d]) < 24:
            skipped.append({"log_date": d, "reason": f"incomplete ({len(hours[d])}/24 hours)"})
            continue
        if done.get(d):                       # aanwezig ÉN compleet
            skipped.append({"log_date": d, "reason": "already ingested"})
            if not KEEP_SOURCE:
                _archive(src, d, by_date[d])
            continue
        if d in done:
            logger.info("[%s] staat in de ledger maar is INCOMPLEET — opnieuw verwerken", d)
        try:
            exp, _dl_failed = _read_manifest(src, d)
            res = ingest_date(date.fromisoformat(d), by_date[d],
                              ",".join(sorted(dirs[d])), len(hours[d]),
                              expected_files=exp, dist_hours=dist_hours[d])
            processed.append(res)
            # Alleen archiveren als de datum ook echt compleet is binnengekomen.
            # Archiveren is de opmaat naar _prune_archive, en dat wist definitief:
            # buiten het S3-venster van ~42 dagen is er geen tweede kopie.
            if not KEEP_SOURCE and res.get("is_complete"):
                _archive(src, d, by_date[d])
            elif not res.get("is_complete"):
                logger.warning("[%s] incompleet geladen — bronbestanden blijven in %s "
                               "staan zodat een volgende run ze kan aanvullen", d, src)
        except Exception as exc:
            # Naar `failed` en niet naar `skipped` (audit 2026-08-13). Een fout stond
            # hiervoor tussen "al geïngest" en "incomplete" in dezelfde lijst, en de
            # UI rendert die allemaal als hetzelfde neutrale "N overgeslagen".
            logger.error("[%s] FAILED: %s", d, exc, exc_info=True)
            failed.append({"log_date": d, "reason": str(exc)})
    # Ná de datums, niet ertussen: opruimen mag nooit concurreren met een ingest die
    # nog bestanden aan het lezen is.
    freed = _prune_archive(src)
    # Een run waarin álles omviel meldde "ok" (audit 2026-08-13) — het enige verschil
    # zat in `skipped`, en niemand keek daarin naar het woord "error". Nu zegt status
    # wat er gebeurde, zodat de nachtelijke taak erop kan afgaan.
    if cancelled:
        status = "cancelled"
    elif failed and not processed:
        status = "failed"
    elif failed:
        status = "partial"
    else:
        status = "ok"
    if failed:
        logger.error("run_drop: %s van %s datums mislukt (%s)", len(failed),
                     len(failed) + len(processed),
                     ", ".join(f["log_date"] for f in failed))
    return {"status": status, "dir": src,
            "cancelled": cancelled, "processed": processed, "skipped": skipped,
            "failed": failed,
            "archive_freed_mb": round(freed / 1e6) if freed else 0}


def _prune_archive(src, days=None):
    """Gooi verwerkte bronbestanden weg die ouder zijn dan de retentie. -> bytes vrij.

    Alleen mappen `_processed/<YYYY-MM-DD>/` waarvan de DATUM ouder is dan de grens —
    niet op bestandsmtime, want die zegt wanneer we hem downloadden en niet over welke
    dag hij gaat. Buiten `_processed` blijft alles staan; een half gedownloade dag in de
    staging-root is werk-in-uitvoering, geen afval.

    En sinds fase 2: NOOIT een datum wissen die niet als compleet in de ledger staat.
    Dit is het laatste punt waarop een bronbestand nog te redden is — daarna is er
    buiten het S3-venster van ~42 dagen geen tweede kopie meer. Een datum die incompleet
    is geladen hoort te blijven liggen tot hij is aangevuld, ook al is hij oud.
    """
    days = STAGING_RETENTION_DAYS if days is None else days
    root = os.path.join(src, "_processed")
    if not days or not os.path.isdir(root):
        return 0
    try:
        complete = already_ingested(with_completeness=True)
    except Exception as exc:
        # Geen DB? Dan niets wissen. Opruimen is nooit dringend genoeg om te doen
        # zonder te weten wat er al veilig binnen is.
        logger.warning("staging niet opgeruimd, ledger niet te lezen: %s", exc)
        return 0
    cutoff = date.today() - timedelta(days=days)
    freed = 0
    for name in sorted(os.listdir(root)):
        try:
            if date.fromisoformat(name) >= cutoff:
                continue
        except ValueError:
            continue                      # geen datummap: laat staan
        if not complete.get(name):
            logger.warning("staging %s NIET opgeruimd: staat niet als compleet in de "
                           "ledger (%s)", name,
                           "incompleet geladen" if name in complete else "niet geladen")
            continue
        path = os.path.join(root, name)
        # os.walk en niet listdir (audit 2026-08-13): rmtree wist de hele boom, maar de
        # oude som keek alleen naar bestanden in de bovenste laag. Een submap werd dus
        # wél verwijderd en niet meegeteld, waardoor archive_freed_mb in het
        # API-antwoord minder meldde dan er werkelijk weg was.
        size = 0
        for dirpath, _dirs, fnames in os.walk(path):
            for f in fnames:
                fp = os.path.join(dirpath, f)
                if os.path.isfile(fp):
                    size += os.path.getsize(fp)
        try:
            shutil.rmtree(path)
        except OSError as exc:
            logger.warning("kon %s niet opruimen: %s", path, exc)
            continue
        freed += size
        logger.info("staging opgeruimd: %s (%s MB, ouder dan %s dagen)",
                    name, round(size / 1e6), days)
    return freed


def _archive(src, log_date, files):
    dest = os.path.join(src, "_processed", log_date)
    os.makedirs(dest, exist_ok=True)
    for f in files:
        try:
            shutil.move(f, os.path.join(dest, os.path.basename(f)))
        except OSError as exc:
            logger.warning("could not archive %s: %s", f, exc)


# ---------------------------------------------------------------------------
# Background runner + daily schedule
#
# One lock guards both the Verwerk button and the timer, so a scheduled run
# firing while a manual one is in flight is a no-op instead of two passes over
# the same files. threading.Timer rather than APScheduler on purpose: it
# matches gsd_ll_service's daily job, and a stray second uvicorn holding its
# own APScheduler is exactly how the GSD low-linkage phantom runs happened.
# Auto-ingest stays off unless BOTHITS_AUTO_INGEST is set, so only the
# deliberate process runs it.
# ---------------------------------------------------------------------------
AUTO_INGEST = os.getenv("BOTHITS_AUTO_INGEST", "false").lower() == "true"
AUTO_INGEST_AT = os.getenv("BOTHITS_AUTO_INGEST_AT", "04:30")

_ingest_lock = threading.Lock()
_ingest_state = {"running": False, "started_at": None, "finished_at": None,
                 "result": None, "error": None, "trigger": None, "phase": None,
                 "fetch": None, "fetch_progress": None, "cancelling": False}
_timer = None

# Coöperatieve annulering. Geen thread kill: de worker kijkt zelf op veilige
# grenzen of de vlag staat. Die grenzen zijn met opzet grof gekozen — tussen
# bestanden tijdens de download, en tussen LOGDATUMS tijdens het verwerken.
# Middenin ingest_date() stoppen zou een halve dag in de cube achterlaten die
# daarna als "geïngest" telt; per datum stoppen laat de ledger kloppen, want een
# datum is dan óf helemaal geladen óf helemaal niet.
_cancel = threading.Event()


def request_cancel() -> bool:
    """Vraag de lopende run te stoppen. -> of er iets liep om te stoppen."""
    if not _ingest_state["running"]:
        return False
    _cancel.set()
    _ingest_state["cancelling"] = True
    logger.info("bothits ingest: annulering aangevraagd")
    return True


def cancel_requested() -> bool:
    return _cancel.is_set()


def ingest_state():
    return dict(_ingest_state)


def start_ingest_async(trigger="manual", on_done=None, src=None, before=None):
    """Run a drop-folder ingest on a worker thread. -> (started, state).

    `src` overrides the folder to scan, and `before` runs inside the worker before
    the ingest — that is how the S3 fetch hangs off this same call instead of
    growing its own thread and lock. Download and ingest under ONE lock matters:
    they touch the same files, so a nightly pass firing halfway through a download
    would otherwise ingest a date whose 24th hour is still arriving.

    `before` gets a progress callback and whatever it returns is published on the
    state as `fetch`, so the UI can show what was downloaded while the (much
    longer) parse phase runs.
    """
    if not _ingest_lock.acquire(blocking=False):
        return False, dict(_ingest_state)
    # Wissen ONDER de lock en vóór de worker start: een annulering van de vorige
    # run mag de volgende niet meteen weer afbreken.
    _cancel.clear()
    _ingest_state.update(running=True, error=None, result=None, trigger=trigger,
                         finished_at=None, fetch=None, fetch_progress=None,
                         cancelling=False,
                         phase="fetch" if before else "ingest",
                         started_at=datetime.now().isoformat(timespec="seconds"))

    def worker():
        try:
            if before:
                # De callback krijgt een zin voor `phase` en tellers voor
                # `fetch_progress`, waar de UI zijn balk op vult. `stats` is
                # optioneel zodat een `before` die alleen zinnen stuurt blijft werken.
                def on_progress(msg, stats=None):
                    _ingest_state["phase"] = f"fetch: {msg}"
                    _ingest_state["fetch_progress"] = stats

                _ingest_state["fetch"] = before(on_progress, cancel_requested)
                # Downloaden is meetbaar, parsen niet: de ingest heeft geen teller die
                # vooraf bekend is. De tellers gaan hier dus weg, zodat de UI van een
                # bepaalde balk naar een onbepaalde schakelt in plaats van op 100% te
                # blijven staan (UI_BLUEPRINT, status-bar-lifecycle regel 1).
                _ingest_state["fetch_progress"] = None
                _ingest_state["phase"] = "ingest"
            _ingest_state["result"] = run_drop(src)
        except Exception as exc:
            logger.error("bothits ingest (%s) failed: %s", trigger, exc, exc_info=True)
            _ingest_state["error"] = str(exc)
        finally:
            _ingest_state["phase"] = None
            _ingest_state["running"] = False
            # cancelling meteen terug op false (audit 2026-08-13). Hij bleef anders
            # tot de vólgende start_ingest_async op true staan, dus /ingest/status
            # bleef "annuleren…" melden over een run die al klaar was.
            _ingest_state["cancelling"] = False
            _ingest_state["finished_at"] = datetime.now().isoformat(timespec="seconds")
            _ingest_lock.release()
            if on_done:
                try:
                    on_done()
                except Exception:
                    logger.warning("bothits ingest on_done hook failed", exc_info=True)

    # De release zit ALLEEN in de finally van worker(), dus als de thread niet start
    # komt hij er nooit — dan houdt dit proces de lock voor altijd vast en meldt elke
    # volgende knop-klik én elke timer "er loopt al een ingest", terwijl er niets
    # loopt. Alleen te herstellen met een herstart van uvicorn. Thread.start() faalt
    # bij thread- of geheugenuitputting (RuntimeError: can't start new thread), en dat
    # is precies het soort dag waarop je de knop nodig hebt (audit 2026-08-13).
    try:
        threading.Thread(target=worker, daemon=True, name="bothits-ingest").start()
    except BaseException as exc:
        logger.error("bothits ingest (%s): worker-thread start mislukt: %s",
                     trigger, exc, exc_info=True)
        _ingest_state.update(running=False, cancelling=False, phase=None,
                             error=f"worker-thread start mislukt: {exc}",
                             finished_at=datetime.now().isoformat(timespec="seconds"))
        _ingest_lock.release()
        raise
    return True, dict(_ingest_state)


def _seconds_until(hhmm):
    from datetime import timedelta as _td
    now = datetime.now()
    try:
        hh, mm = (int(x) for x in hhmm.split(":"))
    except ValueError:
        hh, mm = 4, 30
    nxt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if nxt <= now:
        nxt += _td(days=1)
    return (nxt - now).total_seconds()


def _schedule_next(on_done=None):
    global _timer
    delay = _seconds_until(AUTO_INGEST_AT)
    _timer = threading.Timer(delay, _fire, kwargs={"on_done": on_done})
    _timer.daemon = True
    _timer.start()
    logger.info("bothits auto-ingest: next run in %.1f h (at %s)",
                delay / 3600, AUTO_INGEST_AT)


def _fire(on_done=None):
    try:
        started, _ = start_ingest_async("scheduled", on_done)
        if not started:
            logger.info("bothits auto-ingest: skipped, an ingest is already running")
    finally:
        _schedule_next(on_done)


def start_scheduler(on_done=None):
    """Arm the daily drop-folder ingest. Call from app startup."""
    if not AUTO_INGEST:
        logger.info("bothits auto-ingest disabled (set BOTHITS_AUTO_INGEST=true)")
        return
    if not os.path.isdir(DROP_DIR):
        os.makedirs(DROP_DIR, exist_ok=True)
    _schedule_next(on_done)


def stop_scheduler():
    global _timer
    if _timer:
        _timer.cancel()
        _timer = None


def status():
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*) AS days, min(log_date) AS first_day,
                   max(log_date) AS last_day, sum(raw_lines) AS raw_lines,
                   sum(bot_lines) AS bot_lines, sum(duration_s) AS total_s
            FROM pa.bothits_ingest
        """)
        st = dict(cur.fetchone())
        for tbl in ("bothits_daily", "bothits_url_daily", "bothits_unknown_daily"):
            cur.execute(f"SELECT count(*) AS n FROM pa.{tbl}")
            st[tbl] = cur.fetchone()["n"]
            cur.execute("SELECT pg_size_pretty(pg_total_relation_size(%s)) AS s",
                        (f"pa.{tbl}",))
            st[tbl + "_size"] = cur.fetchone()["s"]
        cur.close()
        return st
    finally:
        return_db_connection(conn)


def main():
    logging.basicConfig(
        level=logging.INFO, stream=sys.stdout,
        format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Bot Hits ingest")
    ap.add_argument("command", choices=["backfill", "drop", "status"])
    ap.add_argument("--src", default=None)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--redo", action="store_true",
                    help="re-ingest dates already present")
    ap.add_argument("--date", action="append", dest="dates",
                    help="only this log date (repeatable), implies --redo")
    a = ap.parse_args()

    if a.command == "backfill":
        # Exitcode volgt de status (audit 2026-08-13): een backfill waarin datums
        # omvielen eindigde met 0, dus een wrapper of scheduled task zag "gelukt".
        res = run_backfill(a.src, a.limit, a.redo, a.dates)
        print(f"  status                       {res['status']}")
        print(f"  verwerkt                     {len(res['processed'])}")
        print(f"  mislukt                      {len(res['failed'])}")
        for f in res["failed"]:
            print(f"    {f['log_date']}: {f['reason']}")
        return 1 if res["status"] in ("failed", "partial") else 0
    elif a.command == "drop":
        res = run_drop(a.src)
        print(f"  status                       {res['status']}")
        print(f"  verwerkt                     {len(res.get('processed', []))}")
        print(f"  overgeslagen                 {len(res.get('skipped', []))}")
        print(f"  mislukt                      {len(res.get('failed', []))}")
        for f in res.get("failed", []):
            print(f"    {f['log_date']}: {f['reason']}")
        return 1 if res["status"] in ("failed", "partial") else 0
    else:
        for k, v in status().items():
            print(f"  {k:<28} {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
