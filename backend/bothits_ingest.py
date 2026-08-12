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
import logging
import os
import re
import shutil
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime
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

# Families excluded from the per-URL tables. They still appear in the cube with
# full hit counts — this only stops an unbounded catch-all from generating URL
# rows. Override per bot afterwards via pa.bothits_bot.is_tracked.
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
    if u == "/" or u == "":
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


def skip_host(h):
    """True als deze host niet onder KEEP_DOMAINS valt."""
    return not any(h == d or h.endswith("." + d) for d in KEEP_DOMAINS)


def status_class(s):
    return (s[0] + "xx") if s and s[0:1].isdigit() else "?"


# ---------------------------------------------------------------------------
# pa.urls membership. Loaded once in the parent and inherited by the worker
# processes through fork, so the ~1M-entry dict is paged in once, not 12 times.
# ---------------------------------------------------------------------------
URL_IDS = {}


def load_url_ids():
    """url (no trailing slash) -> url_id for every row in pa.urls."""
    global URL_IDS
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT url_id, url FROM pa.urls")
        URL_IDS = {r["url"].rstrip("/"): r["url_id"] for r in cur.fetchall()}
        cur.close()
    finally:
        return_db_connection(conn)
    logger.info("pa.urls lookup loaded: %s urls", f"{len(URL_IDS):,}")
    return URL_IDS


def process_file(path):
    """Parse one .gz log file.

    Returns (cube, known, unknown, raw_lines, bot_lines) where
      cube    : (host, family, name, url_type, depth, is_known, status, edge,
                 verify_state)
                -> [hits, bytes, time_ms]
      known   : (url_id, host, family, name) -> [hits, bytes, n2, n3, n4, n5]
      unknown : (host, family, name, url_type, depth, url) -> hits
                (product pages excluded — they are near-unique per hit, so a
                 top-N over them is meaningless noise; the cube still counts them)
    """
    cube = collections.defaultdict(lambda: [0, 0, 0])
    known = collections.defaultdict(lambda: [0, 0, 0, 0, 0, 0])
    unknown = collections.Counter()
    raw_lines = 0
    bot_lines = 0
    idx = None
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                if line[:1] == "#":
                    if line.startswith("#Fields:"):
                        names = line[len("#Fields:"):].split()
                        idx = {n: i for i, n in enumerate(names)}
                    continue
                if idx is None:
                    continue
                raw_lines += 1
                p = line.rstrip("\n").split("\t")
                try:
                    ua = unquote(p[idx["cs(User-Agent)"]])
                    host = norm_host(p[idx["x-host-header"]])
                    stem = unquote(p[idx["cs-uri-stem"]]).rstrip("/") or "/"
                    st = p[idx["sc-status"]]
                    edge = p[idx["x-edge-result-type"]] or "-"
                    nbytes = p[idx["sc-bytes"]]
                    taken = p[idx["time-taken"]]
                    cip = p[idx["c-ip"]]
                except (IndexError, KeyError):
                    continue
                # Vóór de bot-check, ná raw_lines: een host buiten KEEP_DOMAINS
                # verdwijnt uit alle tellingen, maar raw_lines blijft het aantal
                # regels in de logbestanden — dat is de volledigheidsmaat van de
                # ledger en moet op het bestand kloppen.
                if skip_host(host):
                    continue

                fam = None
                for name, _cls, rx in BOT_RX:
                    if rx.search(ua):
                        fam = name
                        break
                if fam is None:
                    continue
                bot_lines += 1
                tracked = fam not in UNTRACKED_FAMILIES

                m = NAME_RX.search(ua)
                bot = CANON_BY_LOWER.get(m.group(1).lower(), m.group(1)) if m else fam
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
        logger.warning("skipping unreadable %s: %s", os.path.basename(path), exc)
    return dict(cube), dict(known), unknown, raw_lines, bot_lines


# ---------------------------------------------------------------------------
# Dimension upserts
# ---------------------------------------------------------------------------
# verify_state is later toegevoegd (2026-08-11), dus de bestaande tabel moet
# meegroeien. Zelfde vorm als faq_v2_publisher's migratie: eerst een goedkope
# catalogus-check, want ALTER TABLE pakt een AccessExclusiveLock óók als er niets
# te doen valt, en dat deadlockt tegen een lopende ingest. Bestaande rijen krijgen
# 'unchecked' — eerlijk, want die zijn geladen vóór er verificatie was.
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
# Ingest one log date
# ---------------------------------------------------------------------------
def ingest_date(log_date, files, source_dirs="", n_hours=24):
    """Parse every file for one log date and replace that date in the DB.

    n_hours is how many of the 24 hourly buckets the archive actually holds for
    this date. Five dates in the backfill are cut off mid-day (the last day of
    each download batch), and a partial day plotted next to full ones reads as
    a traffic collapse that never happened — so it is recorded, not smoothed.
    """
    t0 = time.time()
    logger.info("[%s] %s files (%s/24 hours)", log_date, f"{len(files):,}", n_hours)

    # In de PARENT, vóór het forken — net als load_url_ids(). De workers erven de
    # opzoektabel dan via fork in plaats van de lijsten twaalf keer op te halen.
    # Een mislukte fetch is geen fout: verdict() geeft dan 'unchecked'.
    load_ip_ranges()

    cube = collections.defaultdict(lambda: [0, 0, 0])
    known = collections.defaultdict(lambda: [0, 0, 0, 0, 0, 0])
    unknown = collections.Counter()
    raw_lines = bot_lines = 0
    done = 0

    with ProcessPoolExecutor(max_workers=min(MAX_WORKERS, os.cpu_count() or 4)) as ex:
        futs = [ex.submit(process_file, f) for f in files]
        for fut in as_completed(futs):
            c, k, u, rl, bl = fut.result()
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
            done += 1
            if done % 500 == 0:
                logger.info("[%s]   %s/%s files | cube=%s known=%s unknown=%s",
                            log_date, done, len(files), f"{len(cube):,}",
                            f"{len(known):,}", f"{len(unknown):,}")

    # Trim the unknown tail to the loudest offenders per bot family.
    per_family = collections.defaultdict(list)
    for key, hits in unknown.items():
        per_family[key[1]].append((hits, key))
    trimmed = []
    for fam, rows in per_family.items():
        rows.sort(reverse=True)
        trimmed.extend(rows[:TOP_UNKNOWN_PER_BOT])

    hosts = {k[0] for k in cube}
    bots = {(k[1], k[2]) for k in cube}

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
               duration_s, hours_present, is_complete, ingested_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
            ON CONFLICT (log_date) DO UPDATE SET
              files=EXCLUDED.files, raw_lines=EXCLUDED.raw_lines,
              bot_lines=EXCLUDED.bot_lines, known_rows=EXCLUDED.known_rows,
              source_dirs=EXCLUDED.source_dirs, duration_s=EXCLUDED.duration_s,
              hours_present=EXCLUDED.hours_present,
              is_complete=EXCLUDED.is_complete, ingested_at=now()
        """, (log_date, len(files), raw_lines, bot_lines, len(known),
              source_dirs[:500], dur, n_hours, n_hours >= 24))
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)

    logger.info("[%s] done in %ss | raw=%s bot=%s | cube=%s url=%s unknown=%s",
                log_date, dur, f"{raw_lines:,}", f"{bot_lines:,}",
                f"{len(cube):,}", f"{len(known):,}", f"{len(trimmed):,}")
    return {
        "log_date": str(log_date), "files": len(files), "raw_lines": raw_lines,
        "bot_lines": bot_lines, "known_rows": len(known),
        "unknown_rows": len(trimmed), "cube_rows": len(cube), "duration_s": dur,
    }


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------
def scan_tree(root):
    """-> {log_date: [paths]}, {log_date: set(hours)}, {log_date: set(dirs)}.

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
    """
    seen = {}
    hours = collections.defaultdict(set)
    dirs = collections.defaultdict(set)
    for dirpath, _dirnames, filenames in os.walk(root):
        if os.path.basename(dirpath) == "_processed":
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
            dirs[d].add(os.path.basename(dirpath))
    by_date = collections.defaultdict(list)
    for d, path in seen.values():
        by_date[d].append(path)
    return by_date, hours, dirs


def already_ingested():
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT log_date FROM pa.bothits_ingest")
        out = {str(r["log_date"]) for r in cur.fetchall()}
        cur.close()
        return out
    finally:
        return_db_connection(conn)


def run_backfill(src=None, limit=None, redo=False, only=None):
    """Ingest every log date found under src that isn't loaded yet.

    Unlike the drop folder this does NOT skip partial days: the archive's five
    cut-off dates are all the history there is for them, so they are loaded and
    flagged rather than withheld.
    """
    src = src or BACKUP_DIR
    load_url_ids()
    by_date, hours, dirs = scan_tree(src)
    done = set() if redo else already_ingested()
    todo = sorted(d for d in by_date if d not in done)
    if only:
        todo = [d for d in by_date if d in set(only)]
    if limit:
        todo = todo[:limit]
    logger.info("backfill: %s dates found, %s already ingested, %s to do",
                len(by_date), len(done & set(by_date)), len(todo))
    results = []
    for i, d in enumerate(todo, 1):
        logger.info("=== %s/%s : %s ===", i, len(todo), d)
        try:
            results.append(ingest_date(date.fromisoformat(d), by_date[d],
                                       ",".join(sorted(dirs[d])), len(hours[d])))
        except Exception as exc:
            logger.error("[%s] FAILED: %s", d, exc, exc_info=True)
    return results


def run_drop(src=None):
    """Ingest complete dates from the drop folder, then archive their files.

    A date is only ingested once all 24 hour-buckets are present, so a folder
    still being copied in doesn't get loaded as a half day and then silently
    left that way.
    """
    src = src or DROP_DIR
    if not os.path.isdir(src):
        return {"status": "no_drop_dir", "dir": src, "processed": []}
    load_url_ids()
    by_date, hours, dirs = scan_tree(src)
    done = already_ingested()

    processed, skipped = [], []
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
        if d in done:
            skipped.append({"log_date": d, "reason": "already ingested"})
            if not KEEP_SOURCE:
                _archive(src, d, by_date[d])
            continue
        try:
            processed.append(ingest_date(date.fromisoformat(d), by_date[d],
                                         ",".join(sorted(dirs[d])), len(hours[d])))
            if not KEEP_SOURCE:
                _archive(src, d, by_date[d])
        except Exception as exc:
            logger.error("[%s] FAILED: %s", d, exc, exc_info=True)
            skipped.append({"log_date": d, "reason": f"error: {exc}"})
    return {"status": "cancelled" if cancelled else "ok", "dir": src,
            "cancelled": cancelled, "processed": processed, "skipped": skipped}


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
            _ingest_state["finished_at"] = datetime.now().isoformat(timespec="seconds")
            _ingest_lock.release()
            if on_done:
                try:
                    on_done()
                except Exception:
                    logger.warning("bothits ingest on_done hook failed", exc_info=True)

    threading.Thread(target=worker, daemon=True, name="bothits-ingest").start()
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
        run_backfill(a.src, a.limit, a.redo, a.dates)
    elif a.command == "drop":
        print(run_drop(a.src))
    else:
        for k, v in status().items():
            print(f"  {k:<28} {v}")


if __name__ == "__main__":
    main()
