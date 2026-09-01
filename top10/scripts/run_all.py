#!/usr/bin/env python3
"""De hele pijplijn met één commando.

De gratis stappen lopen door; vóór de eerste betaalde stap stopt hij met een
kostenraming. Dat is met opzet: het aantal producten — en daarmee de rekening —
hangt af van de categorie, en dat weet je pas ná het verzamelen. Met ``--yes``
loopt hij door tot en met het exportbestand.

    python top10/scripts/run_all.py "airfryers"              # tot de kostenraming
    python top10/scripts/run_all.py --topic airfryers --yes  # ook de betaalde stappen
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from shared.llm_websearch import cost_for                                # noqa: E402
from shared.taxonomy import load_tree, search                            # noqa: E402
from shared.topic import Topic, find_topic                               # noqa: E402

# Gemeten op de eerste echte reviews (Luna, airfryers): ~40k input waarvan ~4k
# uit de cache, ~2k output en 2 zoekopdrachten per product. Genoeg om een
# rekening vooraf te schatten; de echte kosten komen uit de export.
TYPICAL = {"input_tokens": 40000, "cached_tokens": 4400, "output_tokens": 2100}
TYPICAL_SEARCHES = 2


def run(script: str, *args: str) -> None:
    cmd = [sys.executable, str(HERE / script), *args]
    # flush: het subproces schrijft rechtstreeks naar de terminal, dus zonder
    # dit verschijnt de kop ná de uitvoer van de stap die hij aankondigt.
    print(f"\n\033[1m→ {script} {' '.join(args)}\033[0m", flush=True)
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise SystemExit(f"{script} is gestopt met code {result.returncode}")


def ensure_topic(args) -> Topic:
    """Bestaand topic pakken, of er één aanmaken als de naam eenduidig is."""
    if args.topic:
        return find_topic(args.topic)
    hits = search(args.category, load_tree())
    if not hits:
        raise SystemExit(f"geen categorie gevonden voor '{args.category}'")
    if len(hits) > 1 and hits[0]["score"] == hits[1]["score"]:
        print(f"'{args.category}' matcht meerdere categorieën even goed:")
        for h in hits[:8]:
            print(f"  id {h['id']:<9} {' > '.join(p for p in h['path'] if p)}")
        raise SystemExit("kies er één met: resolve_category.py <naam> --create <id>")
    node = hits[0]
    print(f"categorie: {' > '.join(p for p in node['path'] if p)} (id {node['id']})")
    run("resolve_category.py", args.category, "--create", str(node["id"]))
    return find_topic(node["name"])


def estimate(topic: Topic, n_todo: int) -> float | None:
    per = cost_for(topic.review_model, TYPICAL, TYPICAL_SEARCHES)["total"]
    return None if per is None else per * n_todo


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("category", nargs="?", help="categorienaam, bv. 'airfryers'")
    ap.add_argument("--topic", help="bestaand topic in plaats van een nieuwe categorie")
    ap.add_argument("--yes", action="store_true", help="ook de betaalde stappen draaien")
    ap.add_argument("--top", type=int, default=10, help="aantal zoektermen")
    ap.add_argument("--skip-facet", action="append", default=[],
                    help="facet uitsluiten als bron van zoektermen")
    ap.add_argument("--tagdata", action="store_true",
                    help="ook het A-label en de pixeldata ophalen (gratis, duurt minuten)")
    ap.add_argument("--max-products", type=int,
                    help="hoogstens zoveel producten reviewen (rem op de rekening)")
    args = ap.parse_args()
    if not args.category and not args.topic:
        ap.error("geef een categorienaam of --topic")

    topic = ensure_topic(args)

    # --- gratis ---
    kw = ["--topic", topic.slug, "--top", str(args.top), "--apply"]
    for f in args.skip_facet:
        kw += ["--skip-facet", f]
    run("keyword_research.py", *kw)
    run("collect_products.py", "--topic", topic.slug)
    run("get_clicks.py", "--topic", topic.slug)

    # --- de rekening, vóór de eerste betaalde stap ---
    topic = find_topic(topic.slug)                     # termen zijn net gewijzigd
    master = topic.read_json("products_master.json")
    done = {p.stem.rsplit("__", 1)[-1] for p in topic.results.glob("review__openai__*.json")}
    todo = [e for e in master if e not in done]
    if args.max_products:
        todo = todo[:args.max_products]
    guess = estimate(topic, len(todo))

    print(f"\n\033[1m{len(master)} producten, {len(done)} al gereviewd, {len(todo)} te doen"
          f" op {topic.review_model}\033[0m")
    print(f"geschatte kosten: {'$%.2f' % guess if guess is not None else 'onbekend — geen tarief in shared/pricing.json'}"
          + (f"  (ruw: ~${guess / len(todo):.3f} per product)" if guess and todo else ""))
    if not args.yes:
        print("\nNiets betaalds gedaan. Draai opnieuw met --yes om door te gaan"
              + (f", of met --max-products N om het af te toppen." if todo else "."))
        return 0

    # --- betaald ---
    reviews = ["--topic", topic.slug]
    if args.max_products:
        reviews += ["--limit", str(args.max_products)]
    run("run_reviews.py", *reviews)
    run("rank_top10.py", "--topic", topic.slug)

    # --- gratis afronding ---
    # snapshot_prices vóór get_tagdata: route B van de pixeldata koppelt op
    # productIdV3, en dat id komt uit offers_top10.csv.
    run("snapshot_prices.py", "--topic", topic.slug)
    if args.tagdata:
        run("get_tagdata.py", "--topic", topic.slug)
    run("export_top10_data.py", "--topic", topic.slug)
    print(f"\n\033[1mklaar.\033[0m Contractbestand: "
          f"{topic.data / 'export'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
