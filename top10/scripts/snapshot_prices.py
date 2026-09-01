#!/usr/bin/env python3
"""Stap 4: prijzen en winkelaanbod vastleggen (gratis).

Levert de twee bestanden die de export nodig heeft en die geen enkel ander
script maakt:

* ``price_snapshots.jsonl`` — per product de laagste prijs op deze datum. Eén
  regel per meting, zodat herhaald draaien een prijsverloop opbouwt in plaats
  van de vorige meting te overschrijven.
* ``offers_top10.csv`` — per product alle winkels met prijs en link.

Twee endpoints, met opzet: ``products-by-ids`` doet 50 producten per call maar
geeft alleen het béste aanbod (één shop), terwijl ``/search/product`` alle
winkels geeft maar één product per call. Vandaar prijzen in batch en offers
alleen voor de producten die daadwerkelijk in een top-10 staan.

Bezorgkosten zitten niet in deze API (alleen ``deliveryCompanies``, de
vervoerder). ``delivery_cost`` is daarom altijd 0.0 — geen ontbrekende waarde
maar een niet-bestaande.

    python top10/scripts/snapshot_prices.py --topic airfryers
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import date
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared.topic import add_topic_arg, find_topic                     # noqa: E402

BY_IDS = "https://productsearch-v2.api.beslist.nl/search/products-by-ids"
SINGLE = "https://productsearch-v2.api.beslist.nl/search/product"
BATCH = 50


def offer_price(offer: dict) -> float | None:
    for key in ("salePrice", "regularPrice"):
        p = (offer.get(key) or {}).get("price")
        if p is not None:
            return float(p)
    return None


def snapshot_prices(topic, master: dict, session: requests.Session, today: str) -> int:
    ids = [str(p.get("groupId") or p["ean"]) for p in master.values()]
    by_id = {str(p.get("groupId") or p["ean"]): ean for ean, p in master.items()}
    lines, missing = [], 0
    for i in range(0, len(ids), BATCH):
        chunk = ids[i:i + BATCH]
        r = session.get(BY_IDS, params={"ids": ",".join(chunk), "countryLanguage": "nl-nl",
                                        "isBot": "false"}, timeout=60)
        r.raise_for_status()
        got = {str(p.get("groupId")): p for p in r.json().get("products") or []}
        for gid in chunk:
            p = got.get(gid)
            price = p.get("minPrice") if p else None
            if price is None:
                missing += 1
                continue
            lines.append({"ean": by_id[gid], "date": today, "price": float(price)})
        print(f"  prijzen {min(i + BATCH, len(ids))}/{len(ids)}")
        time.sleep(0.2)

    path = topic.file("price_snapshots.jsonl")
    with open(path, "a", encoding="utf-8") as f:                    # append: prijsverloop
        for line in lines:
            f.write(json.dumps(line, ensure_ascii=False) + "\n")
    print(f"{len(lines)} prijzen weggeschreven ({missing} zonder prijs) -> {path.name}")
    return len(lines)


def top10_eans(topic) -> list[str]:
    """EAN's uit de rank-bestanden; nog geen ranking = alle producten."""
    eans, files = [], sorted(topic.data.glob("rank_*.json"))
    for f in files:
        # rank_*.json noemt de lijst 'products'; 'top10' is pas de exportvorm.
        data = json.loads(f.read_text(encoding="utf-8"))
        for row in data.get("products") or data.get("top10") or []:
            if row.get("ean"):
                eans.append(str(row["ean"]))
    return sorted(set(eans))


def snapshot_offers(topic, master: dict, session: requests.Session, eans: list[str]) -> int:
    rows = []
    for i, ean in enumerate(eans, 1):
        p = master.get(ean)
        gid = str((p or {}).get("groupId") or ean)
        try:
            r = session.get(SINGLE, params={"groupId": gid, "countryLanguage": "nl-nl",
                                            "isBot": "false"}, timeout=40)
            r.raise_for_status()
            payload = r.json()
        except Exception as e:
            print(f"  [{i}/{len(eans)}] {ean}: FOUT {type(e).__name__}")
            continue
        product = payload.get("product") or (payload.get("products") or [{}])[0]
        for shop in product.get("shops") or []:
            for offer in shop.get("offers") or []:
                price = offer_price(offer)
                if price is None:
                    continue
                rows.append({"ean": ean, "shop_name": shop.get("name"), "price": price,
                             "delivery_cost": 0.0, "url": offer.get("url")})
        if i % 25 == 0 or i == len(eans):
            print(f"  offers {i}/{len(eans)}")
        time.sleep(0.15)

    path = topic.file("offers_top10.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ean", "shop_name", "price", "delivery_cost", "url"])
        w.writeheader()
        w.writerows(rows)
    print(f"{len(rows)} aanbiedingen over {len(eans)} producten -> {path.name}")
    return len(rows)


def main() -> int:
    ap = add_topic_arg(argparse.ArgumentParser(description=__doc__))
    ap.add_argument("--all-offers", action="store_true",
                    help="offers voor alle producten, niet alleen de top-10's")
    ap.add_argument("--skip-prices", action="store_true")
    ap.add_argument("--skip-offers", action="store_true")
    args = ap.parse_args()
    topic = find_topic(args.topic)

    master = topic.read_json("products_master.json")
    session = requests.Session()
    today = date.today().isoformat()

    if not args.skip_prices:
        snapshot_prices(topic, master, session, today)

    if not args.skip_offers:
        eans = sorted(master.keys()) if args.all_offers else top10_eans(topic)
        if not eans:
            print("nog geen rank_*.json — offers overgeslagen "
                  "(draai rank_top10.py eerst, of gebruik --all-offers)")
        else:
            snapshot_offers(topic, master, session, eans)
    return 0


if __name__ == "__main__":
    sys.exit(main())
