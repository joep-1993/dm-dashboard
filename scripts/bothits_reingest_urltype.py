"""Her-ingest van Bot Hits-logdatums, per datum, om een classificatiefout te herstellen.

WAAROM DIT BESTAAT (2026-08-14). `url_type()` in bothits_ingest.py checkte `/r/` met een
`startswith` ná `/products/`, en de R-urls van beslist hebben de vorm
`/products/<cat>/r/<term>`. Ze werden dus als Cat-url of C-url geboekt: 118.140 hits in het
venster 07-15..08-13, tegen 180 die als R-url in de donut stonden. De classifier is gefikst,
maar `pa.bothits_daily` bewaart het RUWE type zonder de URL-tekst — dus de historie is alleen
te herstellen door de logdatums opnieuw te verwerken.

WAAROM PER DATUM EN NIET IN ÉÉN KEER. Het S3-venster is 40 bruikbare datums van samen ~39,5 GB.
`fetch()` legt alles plat in één map, dus in één keer downloaden betekent 40 GB op schijf
voordat er iets verwerkt is. Per datum: downloaden, ingesten, bronbestanden weggooien. Piek
~1 GB, en na elke datum is de winst al binnen — een afgebroken run laat dus een consistente
staat achter in plaats van een halve.

WAAROM DIT VEILIG IS OM OPNIEUW TE STARTEN. De ingest is delete-then-insert per `log_date`
(bothits_ingest.py, "Delete-then-insert makes a re-run idempotent"), dus een datum twee keer
verwerken telt niet dubbel. Een half gedownloade datum wordt niet geïngest: de ingest eist 24
volledige uren.

TWEE PATCHES DIE NODIG ZIJN, en waarom ze hier staan en niet in de module:
  * `_ingested_dates()` -> {} : fetch() slaat datums over die al compleet in de ledger staan,
    en dat zijn precies onze doeldatums. Dit is de enige manier om een her-download te forceren
    zonder de ledger te slopen.
  * `target_dates()` -> [datum] : fetch() neemt een AANTAL dagen, geen datum. Zo haalt hij er
    exact één per ronde.
Beide zijn eenmalig herstelgereedschap; ze horen niet in de nachtelijke run, want daar is het
overslaan van geladen datums juist de bedoeling.

Gebruik:
    venv/bin/python scripts/bothits_reingest_urltype.py 2026-07-05 2026-07-06 ...
    venv/bin/python scripts/bothits_reingest_urltype.py --window 40      # oud -> nieuw
"""
import glob
import logging
import os
import sys
import time
from datetime import date, timedelta
from multiprocessing import freeze_support

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("reingest")

import backend.bothits_s3 as s3
from backend.bothits_ingest import run_backfill
from backend.bothits_s3 import S3_DIR


def target_window(days):
    """De laatste `days` volledige dagen, oud -> nieuw, zodat de ingest chronologisch loopt."""
    y = date.today() - timedelta(days=1)
    return [(y - timedelta(days=i)).isoformat() for i in range(days)][::-1]


def wipe_sources(log_date):
    """Bronbestanden van één datum weg. CloudFront zet de datum in de bestandsnaam, en
    fetch() legt alles plat in S3_DIR neer — vandaar een glob op de datum en niet een map."""
    n = bytes_ = 0
    for p in glob.glob(os.path.join(S3_DIR, f"*{log_date}*")):
        if os.path.isfile(p):
            bytes_ += os.path.getsize(p)
            os.remove(p)
            n += 1
    return n, bytes_


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if "--window" in sys.argv:
        i = sys.argv.index("--window")
        dates = target_window(int(sys.argv[i + 1]))
    elif args:
        dates = sorted(args)
    else:
        print(__doc__)
        return 2

    # De ledgerfilter uitzetten: zonder dit downloadt fetch() niets, want al deze datums
    # staan er compleet in.
    s3._ingested_dates = lambda: {}

    log.info("=== her-ingest van %s datums: %s .. %s", len(dates), dates[0], dates[-1])
    ok, mislukt, gb = [], [], 0.0
    t_start = time.time()

    for idx, d in enumerate(dates, 1):
        t0 = time.time()
        s3.target_dates = lambda days, _d=d: [_d]          # fetch() pakt exact deze datum
        try:
            st = s3.fetch(1, progress=lambda msg, s=None: None)
            got = st["downloaded"]
            mb = st["bytes"] / 1e6
            gb += st["bytes"] / 1e9
            if not got and not glob.glob(os.path.join(S3_DIR, f"*{d}*")):
                state = next((x.get("state") for x in st["dates"] if x["log_date"] == d), "?")
                log.warning("[%s/%s] %s overgeslagen: niets gedownload (%s)",
                            idx, len(dates), d, state)
                mislukt.append((d, f"download: {state}"))
                continue
            log.info("[%s/%s] %s gedownload: %s bestanden, %.0f MB in %.0fs",
                     idx, len(dates), d, got, mb, time.time() - t0)

            t1 = time.time()
            res = run_backfill(src=S3_DIR, redo=True, only=[d])
            proc = res.get("processed", [])
            if res.get("status") in ("failed",) or not proc:
                reason = (res.get("failed") or [{}])[0].get("reason", res.get("status"))
                log.error("[%s/%s] %s INGEST MISLUKT: %s", idx, len(dates), d, reason)
                mislukt.append((d, f"ingest: {reason}"))
            else:
                r = proc[0]
                log.info("[%s/%s] %s geingest: bot=%s known=%s unknown=%s cube=%s in %.0fs",
                         idx, len(dates), d, f"{r['bot_lines']:,}", f"{r['known_rows']:,}",
                         f"{r['unknown_rows']:,}", f"{r['cube_rows']:,}", time.time() - t1)
                ok.append(d)
        except Exception as e:                                   # noqa: BLE001
            log.exception("[%s/%s] %s onverwachte fout", idx, len(dates), d)
            mislukt.append((d, f"exception: {e}"))
        finally:
            n, b = wipe_sources(d)
            log.info("[%s/%s] %s opgeruimd: %s bestanden, %.0f MB", idx, len(dates), d, n, b / 1e6)

    log.info("=== KLAAR in %.0f min: %s gelukt, %s mislukt, %.1f GB gedownload",
             (time.time() - t_start) / 60, len(ok), len(mislukt), gb)
    for d, why in mislukt:
        log.error("   MISLUKT %s: %s", d, why)
    return 1 if mislukt else 0


if __name__ == "__main__":
    freeze_support()
    sys.exit(main())
