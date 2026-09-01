#!/usr/bin/env python3
"""Stap 1: kandidaat-producten per zoekterm ophalen (Search v2, gratis).

Leest de termen en categorie uit topic.json en schrijft
``products_per_term.json`` (per term een lijst) en ``products_master.json``
(ontdubbeld over alle termen, gesleuteld op EAN).

    python top10/scripts/collect_products.py --topic airfryers
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared.topic import add_topic_arg, find_topic                     # noqa: E402

SEARCH_API = "https://productsearch-v2.api.beslist.nl/search/products"


def norm(p: dict) -> dict:
    """Eén product terugbrengen tot wat de rest van de pijplijn gebruikt.

    Producten zonder EAN vallen terug op hun groupId: die sleutel is dus niet
    per definitie een EAN en niet per definitie 13 cijfers — behandel hem als
    ondoorzichtig.
    """
    eans = p.get("eans") or []
    hist = p.get("priceHistory") or []
    return {
        "ean": eans[0] if eans else p.get("groupId"),
        "eans": eans,
        "groupId": p.get("groupId"),
        "title": p.get("title"),
        "brand": p.get("brandName"),
        "minPrice": p.get("minPrice"),
        "priceHistory": hist[-30:] if isinstance(hist, list) else hist,
        "popularity": p.get("popularity"),
        "shopCount": p.get("shopCount"),
        "pimRating": p.get("pimRating"),
        "plpUrl": p.get("url") or p.get("plpUrl"),
    }


def fetch_term(topic, term: dict, session: requests.Session) -> list[dict]:
    params = topic.search_params(**(term.get("params") or {}))
    r = session.get(SEARCH_API, params=params, timeout=60)
    r.raise_for_status()
    return [norm(p) for p in r.json().get("products") or []]


def main() -> int:
    ap = add_topic_arg(argparse.ArgumentParser(description=__doc__))
    ap.add_argument("--sleep", type=float, default=0.3, help="pauze tussen calls")
    args = ap.parse_args()
    topic = find_topic(args.topic)

    if not topic.terms:
        print("geen zoektermen in topic.json", file=sys.stderr)
        return 1

    session = requests.Session()
    per_term, master = {}, {}
    for i, term in enumerate(topic.terms, 1):
        try:
            products = fetch_term(topic, term, session)
        except Exception as e:
            print(f"[{i}/{len(topic.terms)}] {term['term']}: FOUT {type(e).__name__}: {e}")
            per_term[term["term"]] = {"term": term["term"], "volume": term.get("volume"),
                                      "kind": term.get("kind"), "products": []}
            continue
        # Termmetadata meeschrijven: de ranking gebruikt volume en positie,
        # en dan hoeft die topic.json niet nog eens te openen.
        per_term[term["term"]] = {"term": term["term"], "volume": term.get("volume"),
                                  "kind": term.get("kind"), "products": products}
        for p in products:
            if p["ean"]:
                master.setdefault(p["ean"], p)
        print(f"[{i}/{len(topic.terms)}] {term['term']}: {len(products)} producten")
        time.sleep(args.sleep)

    topic.write_json("products_per_term.json", per_term)
    topic.write_json("products_master.json", master)
    print(f"\n{len(master)} unieke producten over {len(per_term)} termen "
          f"-> {topic.file('products_master.json').relative_to(topic.dir.parents[2])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
