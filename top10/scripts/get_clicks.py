#!/usr/bin/env python3
"""Stap 2: outclicks per product uit Redshift (gratis).

Telt 90 dagen ``bt.eans_with_outclicks`` en vouwt variant-EAN's terug op de
master-EAN. De ranking vertrouwt clicks alleen boven een drempel: onder de 20
clicks is het verschil tussen twee producten ruis.

Gebruikt de Redshift-pool van de dashboard-backend, niet het losse
skill-script — dan werkt dit straks ongewijzigd vanuit een dashboardtool.

    python top10/scripts/get_clicks.py --topic airfryers
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from shared.topic import add_topic_arg, find_topic                     # noqa: E402

MIN_USABLE_CLICKS = 20


def main() -> int:
    ap = add_topic_arg(argparse.ArgumentParser(description=__doc__))
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--min-clicks", type=int, default=MIN_USABLE_CLICKS)
    args = ap.parse_args()
    topic = find_topic(args.topic)

    from backend.database import get_redshift_connection, return_redshift_connection

    master = topic.read_json("products_master.json")
    eans = sorted({e for p in master.values()
                   for e in ([p["ean"]] + (p.get("eans") or []))
                   if e and str(e).isdigit()})
    if not eans:
        print("geen numerieke EAN's in products_master.json", file=sys.stderr)
        return 1

    sql = """
        SELECT ean, SUM(outclicks) AS clicks, SUM(productviews) AS productviews
        FROM bt.eans_with_outclicks
        WHERE deleted_ind = 0
          AND date >= dateadd(day, -%s, current_date)
          AND ean IN %s
        GROUP BY ean
    """
    conn = get_redshift_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (args.days, tuple(eans)))
        rows = cur.fetchall()
        cur.close()
    finally:
        return_redshift_connection(conn)

    # SUM negeert NULL-rijen (3,5% van de tabel heeft outclicks NULL); een EAN
    # met uitsluitend NULL-rijen komt terug als None en telt dus als nul.
    # De pool levert dict-rijen; tuples opvangen we voor het geval dat verandert.
    def cell(row, key, idx):
        return row[key] if isinstance(row, dict) else row[idx]

    raw = {cell(r, "ean", 0): {"clicks": int(cell(r, "clicks", 1) or 0),
                               "productviews": int(cell(r, "productviews", 2) or 0)}
           for r in rows}

    per_master = {}
    for mean, p in master.items():
        variants = {mean} | {e for e in (p.get("eans") or []) if e}
        c = sum(raw.get(e, {}).get("clicks", 0) for e in variants)
        v = sum(raw.get(e, {}).get("productviews", 0) for e in variants)
        per_master[mean] = {"clicks": c, "productviews": v, "clicks_usable": c >= args.min_clicks}

    topic.write_json("clicks.json", per_master)
    usable = sum(1 for x in per_master.values() if x["clicks_usable"])
    print(f"{len(eans)} EAN's bevraagd, {len(raw)} met rijen, "
          f"{usable}/{len(per_master)} producten boven {args.min_clicks} clicks")
    return 0


if __name__ == "__main__":
    sys.exit(main())
