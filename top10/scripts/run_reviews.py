#!/usr/bin/env python3
"""Stap 3: één web-search-review per uniek product (kost geld).

Per product één call met internetzoekopdracht, resultaat gecacht in
``data/results/review__openai__<ean>.json``. Opnieuw draaien vult alleen gaten:
een bestaande review zonder ``error`` wordt overgeslagen. Dat maakt een
afgebroken run gratis te hervatten.

    python top10/scripts/run_reviews.py --topic airfryers --limit 2   # proef
    python top10/scripts/run_reviews.py --topic airfryers             # de rest
"""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared.llm_websearch import run                                   # noqa: E402
from shared.topic import add_topic_arg, find_topic                     # noqa: E402

PROMPT = """Je bent een onafhankelijke productrecensent voor een Nederlandse vergelijkingssite. Zoek op internet naar ervaringen, reviews en tests van PRECIES dit product (zelfde model/uitvoering, niet een variant):

Product: {title}
Merk: {brand}
EAN: {ean}

Schrijf op basis van wat je vindt een consumentenreview-samenvatting in het Nederlands, in markdown, met exact deze koppen:
## Oordeel in één zin
## Pluspunten
(3–5 bullets; concreet en productspecifiek, geen algemeenheden; noem per punt hoe vaak/door wie het genoemd wordt als je dat weet)
## Minpunten
(3–5 bullets; wees eerlijk over nadelen, ook als het product populair is)
## Voor wie wel / voor wie niet
## Wat kopers vaak vragen
(2–4 veelgestelde vragen met kort antwoord)
## Betrouwbaarheid van deze samenvatting
(hoeveel bronnen/reviews je hebt gevonden, of het om exact dit model ging, en welke twijfels je hebt)
## Bronnen
(genummerde lijst met de URL's die je gebruikt hebt)

Regels: verzin niets; als je iets niet kunt vinden, zeg dat. BELANGRIJK: noem NERGENS een prijs, en zeg nooit dat iets goedkoop/duur/de goedkoopste is — prijzen veranderen; schrijf prijs-neutraal. Geen marketingtaal. Maximaal ~350 woorden exclusief bronnen.
"""

KEEP = ("ean", "title", "brand", "minPrice", "plpUrl")


def job(topic, product: dict) -> tuple[str, str, float]:
    out = topic.results / f"review__openai__{product['ean']}.json"
    if out.exists():
        try:
            if json.loads(out.read_text(encoding="utf-8")).get("error") is None:
                return product["ean"], "cached", 0.0
        except json.JSONDecodeError:
            pass                                    # stukgelopen bestand: opnieuw doen
    res = run("openai", PROMPT.format(title=product["title"],
                                      brand=product["brand"] or "onbekend",
                                      ean=product["ean"]),
              model=topic.review_model)
    out.write_text(json.dumps({**{k: product.get(k) for k in KEEP}, "task": "review", **res},
                              indent=1, ensure_ascii=False), encoding="utf-8")
    cost = (res.get("cost_usd") or {}).get("total") or 0.0
    return product["ean"], ("ok" if res["error"] is None else f"FOUT {str(res['error'])[:120]}"), cost


def main() -> int:
    ap = add_topic_arg(argparse.ArgumentParser(description=__doc__))
    ap.add_argument("--limit", type=int, help="alleen de eerste N producten")
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()
    topic = find_topic(args.topic)

    products = list(topic.read_json("products_master.json").values())
    if args.limit:
        products = products[:args.limit]

    todo = [p for p in products
            if not (topic.results / f"review__openai__{p['ean']}.json").exists()]
    print(f"{len(products)} producten, {len(products) - len(todo)} al gecacht, "
          f"{len(todo)} te doen op {topic.review_model}")

    total, errors = 0.0, 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(job, topic, p): p for p in products}
        for i, f in enumerate(as_completed(futures), 1):
            ean, status, cost = f.result()
            total += cost
            errors += status.startswith("FOUT")
            print(f"[{i}/{len(products)}] {ean} {status}" + (f" ${cost:.4f}" if cost else ""))

    print(f"\nklaar. {errors} fouten." + (f" Nieuwe kosten ${total:.2f}." if total else
          " Kosten onbekend: geen tarief voor dit model in shared/pricing.json."))
    return 0


if __name__ == "__main__":
    sys.exit(main())
