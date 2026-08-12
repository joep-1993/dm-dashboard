"""Bot Hits — haal CloudFront-logs uit S3 naar de dropfolder.

Dit is de stap die tot nu toe met de hand gebeurde: `~/projects/cloudfront-logs/
download_cloudfront_logs.py` naar `Downloads\\Cloudfront`, en dan met de verkenner
naar de dropfolder. De rest van de keten bestond al — `bothits_ingest.run_drop()`
groepeert op de datum uit de bestandsnaam, wacht tot alle 24 uur binnen zijn en is
idempotent per logdatum. Dit module levert alleen de bestanden aan.

DRIE DINGEN GEMETEN OP DE ECHTE BUCKET (2026-08-11), want ze bepalen het ontwerp:

1. **De keys staan onder `cloudfront/<DIST>.<YYYY-MM-DD>-<HH>.<hash>.gz`** — dus met
   een prefix per (distributie, datum) haal je precies één dag op. Het losse script
   scande de hele bucket per datum en filterde client-side; dat is ~230 pagina's per
   dag én het waadt door duizenden `export-2022-*`-objecten die niets met CloudFront
   te maken hebben. Hier: 6 gerichte lists per dag.
2. **Zes distributies**, gevonden met `Delimiter="."` op de prefix (één call, geeft de
   `CommonPrefixes`). Ze worden gecached maar niet hardcoded: komt er een zevende
   distributie bij, dan pikt de volgende ophaal hem op zonder codewijziging.
3. **De retentie is ~42 dagen** (oudste key 2026-06-30 gemeten op 08-11). Vragen om
   iets ouders levert stil nul bestanden op, dus `preview()` meldt expliciet welke
   datums S3 niet meer heeft — anders lijkt een lege ophaal op een kapotte knop.

Eén dag is ~2.900 bestanden en ~900 MB. Daarom: downloads parallel, overslaan wat er
al ligt (zelfde grootte), en een `preview()` die de UI het volume laat quoten vóór de
klik.

Waarom een eigen staging-map (`BOTHITS_S3_DIR`, default `~/bothits_s3`) en niet
`BOTHITS_DROP_DIR`: die default staat op `/mnt/c/…/Downloads/claude/…`, onder
OneDrive. 900 MB per dag door die sync trekken is precies de I/O-hang die eerder
WSL-reads liet vastlopen (memory `onedrive_wsl_file_hang`), en de dropfolder moet
juist een Windows-pad blijven voor het handmatige pad. `run_drop(src=…)` neemt de map
al als argument, dus beide bestaan naast elkaar.
"""
import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

from backend.database import get_db_connection, return_db_connection

logger = logging.getLogger(__name__)

BUCKET = os.getenv("BOTHITS_S3_BUCKET",
                   "production-projectstack-1hts6sh41-logbucketbucket-10tf48d8lt2pt")
# Trailing slash hoort erbij: de keys zijn `cloudfront/E14….2026-08-10-13.abc.gz`.
PREFIX = os.getenv("BOTHITS_S3_PREFIX", "cloudfront/")
REGION = os.getenv("BOTHITS_S3_REGION", os.getenv("AWS_REGION", "eu-west-1"))
S3_DIR = os.path.expanduser(os.getenv("BOTHITS_S3_DIR", "~/bothits_s3"))
WORKERS = int(os.getenv("BOTHITS_S3_WORKERS", "8"))

# Alleen de distributie-prefixen, niet de export-2022-*-rommel die naast de
# cloudfront/-map in dezelfde bucket staat.
DIST_RX = re.compile(r"^E[A-Z0-9]{9,}$")
FILE_DATE_RX = re.compile(r"\.(\d{4}-\d{2}-\d{2})-(\d{2})\.")

_client = None
_client_lock = threading.Lock()
_dists = None


class S3NotConfigured(RuntimeError):
    """Credentials ontbreken — een 400 waard, geen 500."""


def _creds():
    key = os.getenv("BOTHITS_S3_ACCESS_KEY_ID", os.getenv("AWS_ACCESS_KEY_ID", ""))
    secret = os.getenv("BOTHITS_S3_SECRET_ACCESS_KEY",
                       os.getenv("AWS_SECRET_ACCESS_KEY", ""))
    return key, secret


def is_configured():
    return all(_creds())


def client():
    """Eén gedeelde boto3-client. Thread-safe genoeg: boto3-clients zijn dat voor
    reads, en het aanmaken zit achter een lock zodat 8 workers er niet 8 bouwen."""
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                key, secret = _creds()
                if not (key and secret):
                    raise S3NotConfigured(
                        "BOTHITS_S3_ACCESS_KEY_ID / _SECRET_ACCESS_KEY niet gezet")
                import boto3  # lokaal: de rest van het dashboard hoeft boto3 niet
                from botocore.config import Config
                _client = boto3.client("s3", aws_access_key_id=key,
                                       aws_secret_access_key=secret,
                                       region_name=REGION,
                                       config=Config(
                                           connect_timeout=10,
                                           read_timeout=30,
                                           retries={"max_attempts": 2}))
    return _client


def distributions(force=False):
    """De distributie-ids onder PREFIX, uit de CommonPrefixes van één list-call."""
    global _dists
    if _dists is not None and not force:
        return _dists
    r = client().list_objects_v2(Bucket=BUCKET, Prefix=PREFIX, Delimiter=".")
    found = []
    for p in r.get("CommonPrefixes", []):
        # 'cloudfront/E14VW8EO449KG7.' -> 'E14VW8EO449KG7'
        name = p["Prefix"][len(PREFIX):].rstrip(".")
        if DIST_RX.match(name):
            found.append(name)
    if not found:
        raise RuntimeError(
            f"geen CloudFront-distributies gevonden onder s3://{BUCKET}/{PREFIX}")
    _dists = sorted(found)
    logger.info("bothits s3: %s distributies: %s", len(_dists), ",".join(_dists))
    return _dists


def list_date(log_date: str):
    """-> (keys, bytes, hours) voor één logdatum, over alle distributies."""
    pag = client().get_paginator("list_objects_v2")
    keys, total, hours = [], 0, set()
    for dist in distributions():
        for page in pag.paginate(Bucket=BUCKET,
                                 Prefix=f"{PREFIX}{dist}.{log_date}"):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith(".gz"):
                    continue
                m = FILE_DATE_RX.search(obj["Key"])
                # Prefix-match op de datumstring kan in theorie een langere datum
                # raken; de regex is de echte poort.
                if not m or m.group(1) != log_date:
                    continue
                keys.append((obj["Key"], obj["Size"]))
                total += obj["Size"]
                hours.add(m.group(2))
    return keys, total, sorted(hours)


def _ingested_dates():
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT log_date FROM pa.bothits_ingest")
        out = {str(r["log_date"]) for r in cur.fetchall()}
        cur.close()
        return out
    finally:
        return_db_connection(conn)


def target_dates(days: int):
    """De laatste `days` volledige dagen, nieuwste eerst.

    Vandaag valt er altijd buiten: die is per definitie nog niet 24 uur oud en de
    ingest zou hem toch weigeren. Zo betekent days=1 "gisteren", niet "een halve dag
    van vandaag die morgen opnieuw moet".
    """
    yesterday = date.today() - timedelta(days=1)
    return [(yesterday - timedelta(days=i)).isoformat() for i in range(days)]


def preview(days: int = 3):
    """Wat een ophaal van `days` dagen zou doen — zonder te downloaden.

    Voedt de confirm-dialog, en is de enige plek die eerlijk kan zeggen "S3 heeft
    deze datum niet meer" (retentie) versus "die hebben we al".
    """
    ingested = _ingested_dates()
    out = []
    files = total = 0
    for d in target_dates(days):
        keys, size, hours = list_date(d)
        if not keys:
            state = "niet_in_s3"          # buiten de retentie, of nog niets geschreven
        elif d in ingested:
            state = "al_geingest"
        elif len(hours) < 24:
            state = f"incompleet ({len(hours)}/24 uur)"
        else:
            state = "op_te_halen"
            files += len(keys)
            total += size
        out.append({"log_date": d, "files": len(keys), "bytes": size,
                    "hours": len(hours), "state": state})
    return {"days": days, "dates": out, "fetch_files": files,
            "fetch_bytes": total, "fetch_mb": round(total / 1024 / 1024, 1),
            "dest": S3_DIR, "bucket": BUCKET, "prefix": PREFIX,
            "distributions": distributions()}


def fetch(days: int = 3, dest: str = None, progress=None):
    """Download de ontbrekende dagen naar `dest`. -> stats-dict.

    Alleen datums met 24 uur die nog niet in de ledger staan; de rest wordt
    gerapporteerd, niet gedownload. Bestaat een bestand al met dezelfde grootte, dan
    wordt het overgeslagen — dat maakt een tweede klik na een afgebroken download
    goedkoop in plaats van een volledige herhaling.

    `progress(msg, stats)` wordt aangeroepen met een zin én een tellerdict
    (`files_done` / `files_total` / `bytes_done` / `bytes_total` / `log_date` /
    `date_index` / `date_total`). De tweede parameter is optioneel voor de aanroeper:
    oudere callbacks die alleen `msg` aannemen blijven werken zolang ze hem negeren.
    """
    dest = dest or S3_DIR
    os.makedirs(dest, exist_ok=True)
    ingested = _ingested_dates()
    stats = {"dest": dest, "dates": [], "downloaded": 0, "skipped": 0,
             "failed": 0, "bytes": 0}

    # EERST plannen, dan pas downloaden. De listing gebeurde vroeger in dezelfde lus
    # als de download, dus het totaal was pas bekend als de laatste datum al binnen
    # was — en een voortgangsbalk zonder noemer is geen balk. Dit kost geen extra
    # S3-calls, het is dezelfde list_date() een fase eerder.
    plan = []
    for d in reversed(target_dates(days)):     # oud -> nieuw, zodat de ingest volgt
        keys, size, hours = list_date(d)
        if not keys:
            stats["dates"].append({"log_date": d, "state": "niet_in_s3"})
            continue
        if d in ingested:
            stats["dates"].append({"log_date": d, "state": "al_geingest",
                                   "files": len(keys)})
            continue
        if len(hours) < 24:
            stats["dates"].append({"log_date": d, "files": len(keys),
                                   "state": f"incompleet ({len(hours)}/24 uur)"})
            continue
        plan.append((d, keys, hours, size))

    files_total = sum(len(k) for _, k, _, _ in plan)
    bytes_total = sum(s for _, _, _, s in plan)
    files_done = bytes_done = 0

    def report(msg, **extra):
        """Voortgang in twee vormen: een zin voor `phase` en tellers voor de balk.

        De balk loopt op BESTANDEN, niet op bytes: een bestand dat al op schijf staat
        levert 0 bytes op maar is wel een afgevinkte eenheid werk, en een balk die op
        bytes loopt staat tijdens een hervatte download stil terwijl er wel degelijk
        wordt doorgewerkt.
        """
        if progress:
            progress(msg, {"files_done": files_done, "files_total": files_total,
                           "bytes_done": bytes_done, "bytes_total": bytes_total,
                           **extra})

    for idx, (d, keys, hours, size) in enumerate(plan, 1):
        report(f"download {d}: {len(keys)} bestanden, {size / 1024 / 1024:.0f} MB",
               log_date=d, date_index=idx, date_total=len(plan))
        got = skip = fail = got_bytes = 0

        def one(item):
            key, ksize = item
            local = os.path.join(dest, os.path.basename(key))
            if os.path.exists(local) and os.path.getsize(local) == ksize:
                return "skip", 0
            try:
                client().download_file(BUCKET, key, local)
                return "ok", ksize
            except Exception as exc:
                logger.warning("bothits s3: %s mislukt: %s", key, exc)
                return "fail", 0

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for fut in as_completed(pool.submit(one, k) for k in keys):
                what, n = fut.result()
                if what == "ok":
                    got += 1
                    got_bytes += n
                elif what == "skip":
                    skip += 1
                else:
                    fail += 1
                files_done += 1
                bytes_done += n
                # Elke 25 bestanden, plus de laatste. De poller haalt dit hooguit
                # elke seconde op, dus per bestand rapporteren zou alleen de state
                # laten klapperen; elke 25 is bij 8 workers nog altijd meerdere
                # updates per seconde.
                if files_done % 25 == 0 or files_done == files_total:
                    report(f"download {d}: {got + skip}/{len(keys)} bestanden",
                           log_date=d, date_index=idx, date_total=len(plan))
        stats["dates"].append({"log_date": d, "files": len(keys), "hours": len(hours),
                               "downloaded": got, "skipped": skip, "failed": fail,
                               "state": "gedownload" if not fail else "deels_mislukt"})
        stats["downloaded"] += got
        stats["skipped"] += skip
        stats["failed"] += fail
        stats["bytes"] += got_bytes
        report(f"{d}: {got} nieuw, {skip} al aanwezig"
               + (f", {fail} mislukt" if fail else ""),
               log_date=d, date_index=idx, date_total=len(plan))

    stats["mb"] = round(stats["bytes"] / 1024 / 1024, 1)
    return stats
