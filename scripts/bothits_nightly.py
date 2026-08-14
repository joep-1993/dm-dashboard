"""Nachtelijke Bot Hits-ingest: S3 ophalen + verwerken. Zie
Downloads/claude/bothits_nachtelijke_ingest_PROMPT.txt voor de achtergrond.

Hervatbaar en idempotent:
  * een bestand dat er al met dezelfde grootte staat wordt niet opnieuw gehaald;
  * een logdatum die al in pa.bothits_ingest staat wordt overgeslagen;
  * een datum met minder dan 24 uur wordt NIET verwerkt (halve dag = geen dag).
"""
import logging
import os
import sys
from multiprocessing import freeze_support

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s %(levelname)s %(message)s")

from backend.bothits_ingest import run_drop
from backend.bothits_s3 import S3_DIR, fetch

log = logging.getLogger("nightly")

# 5 en niet 3: al geladen datums en al gedownloade bestanden worden
# overgeslagen, dus een ruimere blik kost bijna niets en haalt een gemiste
# nacht automatisch in.
DAYS = int(os.getenv("BOTHITS_NIGHTLY_DAYS", "5"))


def main():
    log.info("=== fase 1: downloaden (%s dagen terug)", DAYS)
    stats = fetch(DAYS, progress=lambda msg, s=None: log.info("%s", msg))
    log.info("download: %s nieuw, %s overgeslagen, %s mislukt, %.1f GB",
             stats["downloaded"], stats["skipped"], stats["failed"],
             stats["bytes"] / 1e9)
    for d in stats["dates"]:
        if d.get("state"):
            log.info("  %s: %s", d["log_date"], d["state"])

    log.info("=== fase 2: verwerken uit %s", S3_DIR)
    res = run_drop(S3_DIR)

    fout = 0
    for r in res.get("processed", []):
        verdacht = r["bot_lines"] > 100_000 and r["known_rows"] == 0
        fout += verdacht
        log.info("  %s bot=%s known=%s unknown=%s cube=%s %ss%s",
                 r["log_date"], f"{r['bot_lines']:,}", f"{r['known_rows']:,}",
                 f"{r['unknown_rows']:,}", f"{r['cube_rows']:,}", r["duration_s"],
                 "   <-- VERDACHT: NUL BEKENDE URLS" if verdacht else "")
    for s in res.get("skipped", []):
        log.info("  overgeslagen %s: %s", s["log_date"], s.get("reason"))
    # Mislukte datums staan sinds de audit van 2026-08-13 in een eigen lijst en niet meer
    # tussen de overgeslagen datums. Ze tellen mee in de exitcode: een nacht waarin de
    # ingest omviel moet de Windows-taak op 'mislukt' zetten, en dat deed hij niet — de
    # oude exitcode keek alleen naar de nul-bekende-URL's-tripwire, dus een run waarin
    # ELKE datum een exception gaf eindigde met 0.
    for f in res.get("failed", []):
        log.error("  MISLUKT %s: %s", f["log_date"], f.get("reason"))
    fout += len(res.get("failed", []))
    if res.get("archive_freed_mb"):
        log.info("staging opgeruimd: %s MB", res["archive_freed_mb"])

    log.info("=== KLAAR: status=%s (%s verwerkt, %s overgeslagen, %s mislukt)",
             res.get("status"), len(res.get("processed", [])),
             len(res.get("skipped", [])), len(res.get("failed", [])))
    sys.exit(1 if fout else 0)      # exitcode 1 => Windows-taak markeert 'm als mislukt


if __name__ == "__main__":
    freeze_support()
    main()
