#!/usr/bin/env python3
"""Stap 0: categorienaam of beslist.nl-URL -> topic.json.

Zet de twee Search-API-parameters vast (mainCategory + category-slug) en zet een
eerste set zoektermen klaar. Zonder ``--create`` toont hij alleen kandidaten:
een verkeerde categorie kost verderop echt geld aan reviews, dus de keuze wordt
bevestigd voordat er iets wordt aangemaakt.

    python top10/scripts/resolve_category.py "airfryers"
    python top10/scripts/resolve_category.py "airfryers" --create 9005486
    python top10/scripts/resolve_category.py --url https://www.beslist.nl/products/... --create
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared import taxonomy as tx                                     # noqa: E402
from shared.topic import RANK_MODEL, REVIEW_MODEL, Topic, new_topic_dir  # noqa: E402

SEARCH_API = "https://productsearch-v2.api.beslist.nl/search/products"


def parse_filters(url: str) -> dict[str, list[str]]:
    """/c/type_elek_fiets~23791934~~kleur~123+456 -> {facet: [waarden]}.

    Een categorie-URL mag een facetfilter dragen; dan is dát de gevraagde
    scope ("Fatbikes" binnen Elektrische fietsen) en blijft elke zoekopdracht
    erbinnen. Scheidingstekens volgen de site: ~~ tussen facetten, ~ tussen
    facet en waarden, + tussen waarden.
    """
    parts = [p for p in url.split("?")[0].split("#")[0].split("/") if p]
    if "c" not in parts:
        return {}
    tail = parts[parts.index("c") + 1:]
    if not tail:
        return {}
    filters = {}
    for chunk in tail[0].split("~~"):
        bits = chunk.split("~")
        if len(bits) >= 2 and bits[0]:
            filters[bits[0]] = [v for v in "~".join(bits[1:]).split("+") if v]
    return filters


def facet_value_names(node: dict, filters: dict) -> dict[str, str]:
    """Namen van de gefilterde facetwaarden, voor het label van het topic."""
    if not filters:
        return {}
    try:
        r = requests.get(SEARCH_API, params={"mainCategory": str(node["root_id"]),
                                             "category": node["slug"],
                                             "countryLanguage": "nl-nl",
                                             "isBot": "false", "limit": "1"}, timeout=30)
        names = {}
        for facet in r.json().get("facets") or []:
            wanted = filters.get((facet.get("urlName") or "").lower())
            if not wanted:
                continue
            for v in facet.get("values") or []:
                if str(v.get("id")) in [str(w) for w in wanted]:
                    names[str(v["id"])] = v["facetValue"]
        return names
    except Exception:
        return {}


def product_count(node: dict, filters: dict | None = None) -> int | None:
    """Indicatief aantal producten in deze categorie (Search API ``total``)."""
    extra = {f"filters[{f}][{i}]": v for f, vals in (filters or {}).items()
             for i, v in enumerate(vals)}
    try:
        r = requests.get(SEARCH_API, params={"mainCategory": str(node["root_id"]),
                                             "category": node["slug"],
                                             "countryLanguage": "nl-nl",
                                             "isBot": "false", "limit": "1", **extra}, timeout=30)
        return r.json().get("total") if r.status_code == 200 else None
    except Exception:
        return None


def brands(node: dict, top: int = 8) -> list[str]:
    """Merken uit het merk-facet van de categorie, populairste eerst.

    Gratis merktermen ('philips airfryer'), zonder keyword-tool. Let op: een
    ongefilterde categorie-call geeft maar de top-8 facetwaarden terug, dus dit
    is bewust een korte lijst en geen volledige merkenlijst.
    """
    try:
        r = requests.get(SEARCH_API, params={"mainCategory": str(node["root_id"]),
                                             "category": node["slug"],
                                             "countryLanguage": "nl-nl",
                                             "isBot": "false", "limit": "1"}, timeout=30)
        for facet in r.json().get("facets") or []:
            # De API noemt dit veld 'urlName'/'name'; de skill-docs zeggen
            # 'label' en dat bestaat hier niet — vandaar urlName als sleutel.
            if (facet.get("urlName") or "").lower() == "merk" or (facet.get("name") or "").lower() == "merk":
                vals = sorted(facet.get("values") or [], key=lambda v: -(v.get("count") or 0))
                return [v["facetValue"] for v in vals[:top]]
    except Exception:
        pass
    return []


def seed_terms(label: str, brand_names: list[str]) -> list[dict]:
    """Startset zoektermen. ``volume`` is None tot het keyword-onderzoek draait."""
    noun = label.lower()
    singular = noun[:-1] if noun.endswith("s") and not noun.endswith("ss") else noun
    terms = [{"term": f"beste {singular}", "volume": None, "kind": "generic", "params": {}},
             {"term": f"{noun} kopen", "volume": None, "kind": "generic", "params": {}}]
    terms += [{"term": f"{b.lower()} {singular}", "volume": None, "kind": "brand",
               "params": {"query": f"{b} {singular}"}} for b in brand_names]
    return terms


def show(hits: list[dict], filters: dict | None = None) -> None:
    print(f"\n{len(hits)} kandidaat-categorieën:\n")
    if filters:
        print(f"  (met facetfilter {filters})\n")
    for h in hits:
        path = " > ".join(p for p in h["path"] if p)
        n = product_count(h, filters)
        print(f"  id {h['id']:<9} {path}")
        print(f"  {'':<12} slug={h['slug']}  main={h['root_id']}  producten≈{n if n is not None else '?'}\n")
    print("Kies er één:  --create <id>")


def create(node: dict, args, filters: dict | None = None) -> Topic:
    filters = filters or {}
    # Met een facetfilter is de facetwaarde de naam van het onderwerp: de
    # gebruiker vroeg om "Fatbikes", niet om "Elektrische fietsen".
    names = facet_value_names(node, filters)
    label = ", ".join(names.values()) if names else node["name"]
    brand_names = brands(node) if not args.no_brand_terms else []
    directory = new_topic_dir(label)
    cfg = {
        "slug": directory.name,
        "label": label,
        "created_at": date.today().isoformat(),
        "category": {"category_id": node["id"], "name": node["name"],
                     "path": [p for p in node["path"] if p] + ([label] if names else []),
                     "main_category": node["root_id"], "category": node["slug"]},
        "filters": filters,
        "country_language": "nl-nl",
        "sort": "popularity",
        "sort_direction": "desc",
        "limit_per_term": args.limit_per_term,
        "models": {"review": REVIEW_MODEL, "rank": RANK_MODEL},
        "terms": seed_terms(label, brand_names),
    }
    (directory / "topic.json").write_text(json.dumps(cfg, indent=1, ensure_ascii=False), encoding="utf-8")
    t = Topic(directory)
    print(f"\naangemaakt: {directory.relative_to(Path.cwd()) if str(directory).startswith(str(Path.cwd())) else directory}")
    print(f"  categorie : {' > '.join(cfg['category']['path'])} (id {node['id']})")
    print(f"  search    : mainCategory={node['root_id']} category={node['slug']}"
          + (f" filters={filters}" if filters else ""))
    print(f"  producten : ≈{product_count(node, filters)}")
    print(f"  modellen  : review={t.review_model}  rank={t.rank_model}")
    print(f"  termen    : {len(cfg['terms'])} ({sum(1 for x in cfg['terms'] if x['kind']=='brand')} merktermen)")
    for x in cfg["terms"]:
        print(f"      - {x['term']}")
    print("\nControleer de termenlijst in topic.json voordat je verder gaat.")
    return t


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("name", nargs="?", help="categorienaam, bv. 'airfryers'")
    ap.add_argument("--url", help="beslist.nl categorie-URL (eenduidig, geen giswerk)")
    ap.add_argument("--create", nargs="?", const=True, metavar="CATEGORY_ID",
                    help="maak het topic aan voor deze categorie-id")
    ap.add_argument("--limit-per-term", type=int, default=20)
    ap.add_argument("--no-brand-terms", action="store_true")
    ap.add_argument("--refresh", action="store_true", help="categorieboom opnieuw ophalen")
    args = ap.parse_args()

    if not args.name and not args.url:
        ap.error("geef een categorienaam of --url")

    tree = tx.load_tree(refresh=args.refresh)

    if args.url:
        node = tx.from_beslist_url(args.url, tree)
        if not node:
            print("geen categorie herkend in die URL", file=sys.stderr)
            return 1
        hits = [node]
    else:
        hits = tx.search(args.name, tree)
        if not hits:
            print(f"geen categorie gevonden voor '{args.name}'", file=sys.stderr)
            return 1

    filters = parse_filters(args.url or "")
    if not args.create:
        show(hits, filters)
        return 0

    if args.create is True:
        node = hits[0]
    else:
        node = next((h for h in hits if str(h["id"]) == str(args.create)), None) or tree.get(str(args.create))
        if not node:
            print(f"categorie-id {args.create} niet gevonden", file=sys.stderr)
            return 1
    create(node, args, filters)
    return 0


if __name__ == "__main__":
    sys.exit(main())
