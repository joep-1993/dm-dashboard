#!/usr/bin/env python3
"""Stap 2b: commercieel bewijs per product uit Redshift (gratis, maar traag).

Twee routes, samen gecacht in ``data/tagdata.json``:

* **A — `bt.ean_score`**, op EAN. Het A-label dat Beslist zelf aan een product
  hangt, plus de subscores (bestseller, top-omzet, laagste prijs, levertijd).
* **B — `bt.revenue_per_product`**, op `productIdV3` (één rij per shop-aanbod).
  Dat zijn de pixelkolommen: sessies, bounces, transacties en shopomzet over
  30 en 365 dagen. Route B heeft `offers_top10.csv` nodig voor de koppeling
  EAN → productIdV3; zonder dat bestand draait alleen route A.

    python top10/scripts/get_tagdata.py --topic airfryers
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from shared.topic import add_topic_arg, find_topic                     # noqa: E402

PERIODS = (30, 365)
SCORE_COLS = ["ean_score_label", "totaal_ean_score", "total_shops_with_ean",
              "score_bestseller", "score_bestseller_100", "score_top_revenue",
              "score_lowest_price", "score_productscore", "score_deliverytimesort"]
PIXEL_COLS = ["is_affiliate_shop"] + [
    f"{c}_{p}" for p in PERIODS
    for c in ("outclicks", "session_starts", "bounce_session_starts",
              "transactions", "all_shop_revenue", "linked_cpc_shop_revenue")]


def num(v):
    if v in (None, "", "NaN", "nan"):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return v
    return int(f) if f == int(f) else round(f, 2)


def fetch(conn, sql: str, params: tuple, label: str) -> list[dict]:
    print(f"  {label} … (full table scan, dit duurt minuten)", flush=True)
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = [r if isinstance(r, dict) else dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close()
    print(f"  {label} → {len(rows)} rijen")
    return rows


def main() -> int:
    ap = add_topic_arg(argparse.ArgumentParser(description=__doc__))
    ap.add_argument("--limit", type=int, help="alleen de eerste N EAN's")
    args = ap.parse_args()
    topic = find_topic(args.topic)

    from backend.database import get_redshift_connection, return_redshift_connection

    master = topic.read_json("products_master.json")
    eans = [e for e in master if str(e).isdigit()]      # één sleutel kan een groupId zijn
    if args.limit:
        eans = eans[:args.limit]

    offers: dict[str, dict[str, str]] = {}
    csv_path = topic.file("offers_top10.csv")
    if csv_path.exists():
        for r in csv.DictReader(open(csv_path, encoding="utf-8")):
            if r.get("product_id_v3"):
                offers.setdefault(r["ean"], {})[r["product_id_v3"]] = r["shop_name"]
    pids = sorted({p for v in offers.values() for p in v})
    print(f"EAN's: {len(eans)} · aanbiedingen met productIdV3: {len(pids)} "
          f"(voor {len(offers)} EAN's)")

    conn = get_redshift_connection()
    try:
        # Route A. Deze tabel heeft ÉÉN RIJ PER SHOP-AANBOD (shop_id, country) —
        # geen rij per EAN. Zonder `deleted_ind = 0` en een landfilter krijg je
        # verwijderde aanbiedingen en Belgische winkels mee, en dán lijken de
        # scores elkaar tegen te spreken: voor EAN 0622356386951 leverde dat 20
        # rijen met 3 verschillende labels op, terwijl de vier actuele
        # NL-winkels het allemaal eens zijn (label A, score 7).
        # Elke historische versie draagt bovendien load_end_date = 9999-12-31,
        # dus "nog open" filtert op zichzelf niets weg.
        score_rows = fetch(conn, f"""
            select ean, load_start_date, shop_id, {", ".join(SCORE_COLS)}
            from bt.ean_score
            where ean in %s
              and load_end_date > current_date
              and deleted_ind = 0
              and country = 'nl'
        """, (tuple(eans),), "bt.ean_score")

        pixel_rows = fetch(conn, f"""
            select productidv3, reference_date, {", ".join(PIXEL_COLS)}
            from bt.revenue_per_product
            where productidv3 in %s
              and reference_date >= current_date - 3
              and country_code = 'nl'
        """, (tuple(pids),), "bt.revenue_per_product") if pids else []
    finally:
        return_redshift_connection(conn)

    by_ean: dict[str, list] = {}
    for r in score_rows:
        by_ean.setdefault(r["ean"], []).append(r)
    score = {}
    for ean, rows in by_ean.items():
        newest = max(r["load_start_date"] for r in rows)
        current = [r for r in rows if r["load_start_date"] == newest]
        # Eén rij per winkel: we nemen de best scorende, want dat is het aanbod
        # dat de bezoeker op de productpagina als eerste ziet.
        best = max(current, key=lambda r: num(r["totaal_ean_score"]) or 0)
        score[ean] = {k: num(best[k]) for k in SCORE_COLS}
        score[ean]["shops_scored"] = len({r["shop_id"] for r in current})
        # Hoeveel verschillende labels de winkels aan dit EAN gaven. > 1 betekent
        # dat de winkels het oneens zijn en het label dus niet los te lezen is.
        score[ean]["label_variants"] = len({r["ean_score_label"] for r in current})
        score[ean]["load_start_date"] = str(newest)

    newest_per_pid = {}
    for r in pixel_rows:
        key = r["productidv3"]
        if key not in newest_per_pid or r["reference_date"] > newest_per_pid[key]["reference_date"]:
            newest_per_pid[key] = r
    pid_to_ean = {p: e for e, v in offers.items() for p in v}
    pixel: dict[str, dict] = {}
    for pid, r in newest_per_pid.items():
        ean = pid_to_ean.get(pid)
        if not ean:
            continue
        blk = pixel.setdefault(ean, {"reports_sessions": 0, "offers": []})
        row = {"product_id_v3": pid, "shop_name": offers[ean][pid],
               **{k: num(r.get(k)) for k in PIXEL_COLS}}
        blk["offers"].append(row)
        # Een lege session_starts betekent "deze winkel rapporteert niets",
        # niet "geen verkeer". Daarom tellen we hoeveel er wél rapporteren.
        if row.get("session_starts_365"):
            blk["reports_sessions"] += 1

    for blk in pixel.values():
        blk["offers"].sort(key=lambda o: -(o.get("all_shop_revenue_365") or 0))
        for p in PERIODS:
            for c in ("outclicks", "session_starts", "transactions",
                      "all_shop_revenue", "linked_cpc_shop_revenue"):
                vals = [o.get(f"{c}_{p}") for o in blk["offers"]]
                vals = [v for v in vals if v is not None]
                blk[f"total_{c}_{p}"] = round(sum(vals), 2) if vals else None
        blk["offers_total"] = len(blk["offers"])

    topic.write_json("tagdata.json", {"ean_score": score, "tag": pixel})
    labels: dict[str, int] = {}
    for v in score.values():
        labels[v["ean_score_label"]] = labels.get(v["ean_score_label"], 0) + 1
    conflicted = sum(1 for v in score.values() if v["label_variants"] > 1)
    print(f"\nean_score: {len(score)} EAN's · labels {labels}"
          + (f" · {conflicted} waar winkels een ander label geven" if conflicted else ""))
    print(f"pixeldata: {len(pixel)} EAN's · "
          f"{sum(b['reports_sessions'] for b in pixel.values())} van "
          f"{sum(b['offers_total'] for b in pixel.values())} aanbiedingen rapporteren sessies")
    return 0


if __name__ == "__main__":
    sys.exit(main())
