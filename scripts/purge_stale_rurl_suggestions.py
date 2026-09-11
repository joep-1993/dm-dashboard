#!/usr/bin/env python3
"""Gooi niet-doorgevoerde voorstellen weg die een oudere engine-versie maakte.

WAAROM: `rurl_processed` is de "al gezien"-cache — staat een R-URL erin, dan slaat
elke volgende run hem over (`already_processed`). Dat is precies wat je wil zolang
het voorstel nog het beste is dat de engine kan. Maar het script dat de redirect
genereert (`backend/rurl_optimizer_v2/`) verandert bijna wekelijks, en een rij uit
V52 blijft door die cache staan alsof V70 er niets aan zou verbeteren. Wat je niet
hebt doorgevoerd is niets waard; wat je wél hebt doorgevoerd is een productieregel
en blijft af.

WAT HIJ WEGGOOIT: rijen die (a) NIET doorgevoerd zijn — geen enkele live redirect
op de bron, zie `rurl_push_tracking.LIVE_SQL` — en (b) een `processed_at` dragen van
vóór de cutoff. Die URL's komen daarmee terug in de pool en worden door de volgende
run met de huidige engine opnieuw beoordeeld.

WAT HIJ MET RUST LAAT: alles wat live staat, ongeacht ouderdom, en alles wat ná de
cutoff is gegenereerd.

De cutoff is de datum van de laatste commit op de engine-map; `--cutoff` overschrijft
hem met de hand. Vóór de delete gaat elke te verwijderen rij naar een CSV, zodat dit
terug te draaien is — het zijn 70% van de tabel.

Dry run by default; `--commit` schrijft.
"""
from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection, return_db_connection  # noqa: E402

ENGINE_DIR = "backend/rurl_optimizer_v2/"
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _last_engine_commit() -> tuple[str, str, str]:
    """(iso timestamp, short sha, subject) of the last commit touching the engine."""
    out = subprocess.check_output(
        ["git", "log", "-1", "--date=iso-strict", "--format=%ad\t%h\t%s", "--", ENGINE_DIR],
        cwd=REPO, text=True,
    ).strip()
    ts, sha, subject = out.split("\t", 2)
    return ts, sha, subject


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cutoff", help="ISO timestamp; default = last engine commit")
    ap.add_argument("--backup", help="CSV path for the deleted rows "
                                     "(default: purged_rurl_<ts>.csv next to this script)")
    ap.add_argument("--commit", action="store_true", help="actually delete")
    args = ap.parse_args()

    from backend import rurl_push_tracking as track
    track.ensure_columns()

    if args.cutoff:
        cutoff, sha, subject = args.cutoff, "-", "(handmatige cutoff)"
    else:
        cutoff, sha, subject = _last_engine_commit()
    print(f"cutoff : {cutoff}")
    print(f"engine : {sha} {subject}\n")

    where = f"NOT {track.LIVE_SQL} AND processed_at < %s"

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT COALESCE(reliability_tier, '?') AS tier,
                           count(*) FILTER (WHERE {where})                     AS purge,
                           count(*) FILTER (WHERE {track.LIVE_SQL})            AS live,
                           count(*) FILTER (WHERE NOT {track.LIVE_SQL}
                                              AND processed_at >= %s)          AS fresh
                    FROM rurl_processed GROUP BY 1 ORDER BY 1""",
                (cutoff, cutoff),
            )
            rows = cur.fetchall()
            tot_purge = sum(r["purge"] for r in rows)
            print(f"{'tier':<6}{'weg':>10}{'live (blijft)':>16}{'na cutoff (blijft)':>21}")
            for r in rows:
                print(f"{r['tier']:<6}{r['purge']:>10,}{r['live']:>16,}{r['fresh']:>21,}")
            print(f"{'TOT':<6}{tot_purge:>10,}"
                  f"{sum(r['live'] for r in rows):>16,}{sum(r['fresh'] for r in rows):>21,}")

            if not tot_purge:
                print("\nniets te doen.")
                return 0
            if not args.commit:
                print("\n(dry run — pass --commit to delete)")
                return 0

            # Backup BEFORE the delete, in the same transaction, so the CSV can
            # never describe a different set of rows than the one that goes.
            backup = args.backup or os.path.join(
                REPO, "scripts", f"purged_rurl_{datetime.now():%Y%m%d_%H%M%S}.csv")
            cur.execute(
                f"""SELECT original_url, redirect_url, reliability_tier, reliability_score,
                           match_type, reason, processed_at, push_status, push_detail,
                           push_run_id, push_attempted_at
                    FROM rurl_processed WHERE {where}""", (cutoff,))
            fetched = cur.fetchall()
            with open(backup, "w", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=list(fetched[0].keys()))
                w.writeheader()
                w.writerows(fetched)
            print(f"\nbackup: {backup} ({len(fetched):,} rijen)")

            cur.execute(f"DELETE FROM rurl_processed WHERE {where}", (cutoff,))
            deleted = cur.rowcount
            if deleted != len(fetched):
                conn.rollback()
                print(f"[abort] delete raakte {deleted:,} rijen, backup heeft er "
                      f"{len(fetched):,} — teruggedraaid, niets verwijderd.")
                return 1
        conn.commit()
        print(f"verwijderd: {deleted:,} rijen")
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)


if __name__ == "__main__":
    raise SystemExit(main())
