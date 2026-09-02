#!/usr/bin/env python3
"""Stap 1: kandidaat-producten per zoekterm ophalen (Search v2, gratis).

Leest de termen en categorie uit topic.json en schrijft
``products_per_term.json`` (per term een lijst) en ``products_master.json``
(ontdubbeld over alle termen, gesleuteld op EAN).

    python top10/scripts/collect_products.py --topic airfryers
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared.topic import add_topic_arg, find_topic, slugify           # noqa: E402

SEARCH_API = "https://productsearch-v2.api.beslist.nl/search/products"
IMAGE_BASE = "https://hwimages.beslist.net/beslist-images/"


def norm(p: dict) -> dict:
    """Eén product terugbrengen tot wat de rest van de pijplijn gebruikt.

    Producten zonder EAN vallen terug op hun groupId: die sleutel is dus niet
    per definitie een EAN en niet per definitie 13 cijfers — behandel hem als
    ondoorzichtig.
    """
    eans = p.get("eans") or []
    hist = p.get("priceHistory") or []
    # De API geeft een sjabloonpad met een {size}-placeholder en zonder host.
    # De werkende combinatie is hwimages.beslist.net/beslist-images/ met F300 —
    # cdn.beslist.net geeft 403, ongeacht de maat.
    images = p.get("images") or []
    image = None
    if images and images[0].get("url"):
        image = IMAGE_BASE + images[0]["url"].replace("{size}", "F300")
    return {
        "image": image,
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


def excluded(topic) -> re.Pattern | None:
    """``exclude_title`` uit topic.json: producten die niet in de categorie horen.

    Een categorie bevat soms een kindcategorie met een ander product erin.
    Matrassen heeft Topdekmatrassen (9000187) als kind, dus een categorie-brede
    zoekopdracht levert toppers op — 31 van de 77 kandidaten, en dan zet de
    ranking een topper op 1 van "de 10 beste matrassen". De categoriescope kan
    dat niet oplossen (de Search API neemt één categorie) en zoektermen ook
    niet: Emma- en Tempur-toppers komen net zo goed mee op "beste matrassen".
    Daarom een expliciete lijst per topic, gematcht op de producttitel.
    """
    pats = topic.cfg.get("exclude_title") or []
    return re.compile("|".join(pats), re.I) if pats else None


def fetch_term(topic, term: dict, session: requests.Session,
               skip: re.Pattern | None = None) -> tuple[list[dict], int]:
    params = topic.search_params(**(term.get("params") or {}))
    r = session.get(SEARCH_API, params=params, timeout=60)
    r.raise_for_status()
    products = [norm(p) for p in r.json().get("products") or []]
    if not skip:
        return products, 0
    keep = [p for p in products if not skip.search(p.get("title") or "")]
    return keep, len(products) - len(keep)


def build_pages(per_term: dict) -> list[dict]:
    """Termen met een identieke productlijst zijn één pagina.

    Twee zoektermen die dezelfde twintig kandidaten opleveren, geven dezelfde
    top-10 met dezelfde kop — dat is duplicate content, en het kost twee keer
    een rank-call. Bij elektrische tandenborstels vielen vijf generieke termen
    ("… kopen", "… test", "… aanbieding", "beste …") zo op één pagina.

    Exact gelijke verzamelingen, geen gelijkenisdrempel: gemeten liggen de
    duplicaten op precies 1,00 en het eerstvolgende paar op 0,74, dus er valt
    niets te kiezen. De term met het hoogste zoekvolume wordt de primaire; de
    rest blijft als extra doelwoord aan de pagina hangen.
    """
    groups: dict[frozenset, list] = {}
    for term, info in per_term.items():
        if not info["products"]:
            continue
        key = frozenset(p["ean"] for p in info["products"])
        groups.setdefault(key, []).append(info)

    pages = []
    for infos in groups.values():
        infos.sort(key=lambda i: (-(i.get("volume") or 0), len(i["term"])))
        primary = infos[0]
        volumes = [i.get("volume") for i in infos if i.get("volume") is not None]
        pages.append({
            "slug": slugify(primary["term"]),
            "term": primary["term"],
            "display": primary.get("display") or primary["term"],
            "volume": primary.get("volume"),
            # Som van losse keywords die dezelfde pagina bedienen. Enkelvoud en
            # meervoud zijn eerder al samengevoegd, dus hier wordt niets dubbel
            # geteld — maar het blijven aparte zoekopdrachten, geen één getal
            # dat Keyword Planner zo teruggeeft.
            "volume_combined": sum(volumes) if volumes else None,
            "terms": [{"term": i["term"], "volume": i.get("volume"), "kind": i.get("kind")}
                      for i in infos],
            "products": primary["products"],
        })
    pages.sort(key=lambda p: -(p["volume_combined"] or 0))
    return pages


def main() -> int:
    ap = add_topic_arg(argparse.ArgumentParser(description=__doc__))
    ap.add_argument("--sleep", type=float, default=0.3, help="pauze tussen calls")
    args = ap.parse_args()
    topic = find_topic(args.topic)

    if not topic.terms:
        print("geen zoektermen in topic.json", file=sys.stderr)
        return 1

    session = requests.Session()
    skip = excluded(topic)
    if skip:
        print(f"uitgesloten op titel: {skip.pattern}")
    per_term, master, weg = {}, {}, 0
    for i, term in enumerate(topic.terms, 1):
        try:
            products, n_weg = fetch_term(topic, term, session, skip)
            weg += n_weg
        except Exception as e:
            print(f"[{i}/{len(topic.terms)}] {term['term']}: FOUT {type(e).__name__}: {e}")
            per_term[term["term"]] = {"term": term["term"], "volume": term.get("volume"),
                                      "kind": term.get("kind"), "products": []}
            continue
        # Termmetadata meeschrijven: de ranking gebruikt volume en positie,
        # en dan hoeft die topic.json niet nog eens te openen.
        per_term[term["term"]] = {"term": term["term"], "display": term.get("display"),
                                  "volume": term.get("volume"),
                                  "kind": term.get("kind"), "products": products}
        for p in products:
            if p["ean"]:
                master.setdefault(p["ean"], p)
        print(f"[{i}/{len(topic.terms)}] {term['term']}: {len(products)} producten"
              + (f" ({n_weg} uitgesloten)" if skip and n_weg else ""))
        time.sleep(args.sleep)

    topic.write_json("products_per_term.json", per_term)
    topic.write_json("products_master.json", master)

    pages = build_pages(per_term)
    topic.write_json("pages.json", pages)

    if skip:
        print(f"{weg} treffers uitgesloten op titel")
    print(f"\n{len(master)} unieke producten over {len(per_term)} termen "
          f"-> {topic.file('products_master.json').relative_to(topic.dir.parents[2])}")
    print(f"{len(pages)} pagina's na samenvoegen van termen met dezelfde productlijst:")
    for pg in pages:
        extra = [t["term"] for t in pg["terms"][1:]]
        print(f"  {pg['volume_combined'] or 0:>7}/mnd  Beste {pg['display']}")
        if extra:
            print(f"           ook voor: {', '.join(extra)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
