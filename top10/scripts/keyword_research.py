#!/usr/bin/env python3
"""Stap 0b: zoekvolumes ophalen en daarmee de zoektermen kiezen (gratis).

Bouwt kandidaat-zoektermen uit de categorie zelf — de naam, de merken en de
facetwaarden ("dubbele airfryer", "6 personen airfryer") — en vraagt Google
Keyword Planner hoe vaak daar per maand op wordt gezocht. De termen met het
meeste volume komen in topic.json; de rest blijft in keywords.csv staan als
onderbouwing (en gaat als ``keyword_research`` mee in het exportbestand).

Dat de samengestelde termen soms krom zijn ("5 liter airfryer") is geen
probleem: krom taalgebruik heeft geen zoekvolume en valt daarmee vanzelf af.

    python top10/scripts/keyword_research.py --topic airfryers          # tonen
    python top10/scripts/keyword_research.py --topic airfryers --apply  # vastleggen
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from shared.topic import add_topic_arg, find_topic, singular            # noqa: E402

SEARCH_API = "https://productsearch-v2.api.beslist.nl/search/products"

# Winkel is geen zoekterm; merk behandelen we apart. De rest komt per categorie
# uit topic.json ("skip_facets") of van --skip-facet, want welk facet een
# intentieval is verschilt per categorie: bij airfryers is dat
# 'bereidingsprogramma' (recepten), bij tandenborstels 'type_tand' (dat bevat
# opzetborstels en flossers — andere producten).
BASE_SKIP_FACETS = {"winkel", "merk"}


def facet_values(topic, per_facet: int, skip: set[str]) -> list[tuple[str, str]]:
    """[(facet_urlName, waarde)] — kandidaat-modifiers uit de categorie."""
    r = requests.get(SEARCH_API, params=topic.search_params(limit="1"), timeout=30)
    r.raise_for_status()
    out = []
    for facet in r.json().get("facets") or []:
        name = (facet.get("urlName") or "").lower()
        if name in skip:
            continue
        vals = sorted(facet.get("values") or [], key=lambda v: -(v.get("count") or 0))
        for v in vals[:per_facet]:
            value = (v.get("facetValue") or "").strip()
            # Kale getallen ('2', '4', '6' uit 'Aantal borstels') leveren
            # onzintermen op als '2 elektrische tandenborstel'. Die hoeven we
            # niet aan Keyword Planner voor te leggen om te weten dat ze niets
            # zijn — anders dan taalfouten, die wél volume kunnen hebben.
            if not value or value.replace(",", "").replace(".", "").isdigit():
                continue
            out.append((name, value))
    return out


def brands(topic, top: int) -> list[str]:
    r = requests.get(SEARCH_API, params=topic.search_params(limit="1"), timeout=30)
    for facet in r.json().get("facets") or []:
        if (facet.get("urlName") or "").lower() == "merk":
            vals = sorted(facet.get("values") or [], key=lambda v: -(v.get("count") or 0))
            return [v["facetValue"] for v in vals[:top]]
    return []


def candidates(topic, n_brands: int, per_facet: int, skip: set[str]) -> dict[str, dict]:
    """{zoekterm: {kind, params}} — alles wat we aan Keyword Planner voorleggen."""
    noun = topic.label.lower()
    one = singular(topic.label)
    out: dict[str, dict] = {}

    for t in (f"beste {one}", f"beste {noun}", one, noun, f"{one} kopen", f"{noun} kopen",
              f"{one} test", f"{one} aanbieding"):
        out.setdefault(t, {"kind": "generic", "params": {}})

    for b in brands(topic, n_brands):
        for t in (f"{b.lower()} {one}", f"beste {b.lower()} {one}"):
            out.setdefault(t, {"kind": "brand", "params": {"query": t}})

    for facet_name, value in facet_values(topic, per_facet, skip):
        v = value.lower()
        # Waarden die het categoriewoord al bevatten ('Dubbele airfryer') zijn
        # zelf de zoekterm; de rest wordt ervoor geplakt ('6 personen airfryer').
        t = v if one in v else f"{v} {one}"
        out.setdefault(t, {"kind": f"facet:{facet_name}", "params": {"query": t}})
    return out


def lookup_volumes(keywords: list[str]) -> dict[str, int | None]:
    """{zoekterm: volume of None}. None = de API gaf geen rij terug.

    Bewust niet via ``get_search_volumes()``: die maakt van een ontbrekende rij
    een 0, en dan is 'niets teruggekregen' niet te onderscheiden van 'niemand
    zoekt hierop' — precies het verschil waarop we straks termen selecteren.
    """
    from backend.keyword_planner_service import (CUSTOMER_IDS, _get_client,       # noqa: N812
                                                 _query_search_volumes, clean_keyword,
                                                 validate_keyword)
    cleaned_to_original: dict[str, str] = {}
    for kw in keywords:
        c = clean_keyword(kw)
        if validate_keyword(c):
            cleaned_to_original.setdefault(c, kw)

    client = _get_client()
    raw = None
    for customer_id in CUSTOMER_IDS:
        raw = _query_search_volumes(client, list(cleaned_to_original), customer_id)
        if raw is not None:
            break
    if raw is None:
        raise SystemExit("Keyword Planner gaf geen resultaat: alle customer-ids uitgeput")

    volumes: dict[str, int | None] = {}
    for cleaned, original in cleaned_to_original.items():
        volumes[original] = raw.get(cleaned)          # None = niet teruggekomen
    return volumes


def dedupe_variants(rows: list[dict]) -> tuple[list[dict], list[tuple[str, str]]]:
    """Enkelvoud/meervoud-varianten samenvoegen.

    Keyword Planner telt 'beste airfryer' en 'beste airfryers' samen en geeft
    beide hetzelfde volume terug. Twee termen die dezelfde pagina opleveren
    kosten wel twee ranglijsten en twee keer copy, dus daar houden we er één
    van over: de variant met het hoogste volume, bij gelijkspel de kortste.
    """
    def key(term: str) -> str:
        return " ".join(w[:-1] if w.endswith("s") and len(w) > 3 else w for w in term.split())

    best: dict[str, dict] = {}
    dropped: list[tuple[str, str]] = []
    for r in rows:
        k = key(r["keyword"])
        cur = best.get(k)
        if cur is None:
            best[k] = r
            continue
        winner, loser = ((cur, r) if (cur["search_volume"], -len(cur["keyword"]))
                         >= (r["search_volume"], -len(r["keyword"])) else (r, cur))
        best[k] = winner
        dropped.append((loser["keyword"], winner["keyword"]))
    return list(best.values()), dropped


def main() -> int:
    ap = add_topic_arg(argparse.ArgumentParser(description=__doc__))
    ap.add_argument("--top", type=int, default=10, help="hoeveel termen in topic.json")
    ap.add_argument("--brands", type=int, default=8)
    ap.add_argument("--per-facet", type=int, default=3)
    ap.add_argument("--min-volume", type=int, default=10)
    ap.add_argument("--skip-facet", action="append", default=[], metavar="URLNAME",
                    help="facet uitsluiten als bron van termen, bv. bereidingsprogramma")
    ap.add_argument("--apply", action="store_true", help="topic.json en keywords.csv wegschrijven")
    args = ap.parse_args()
    topic = find_topic(args.topic)

    skip = BASE_SKIP_FACETS | {n.lower() for n in (topic.cfg.get("skip_facets") or [])} \
        | {n.lower() for n in args.skip_facet}
    if skip - BASE_SKIP_FACETS:
        print(f"facetten uitgesloten: {', '.join(sorted(skip - BASE_SKIP_FACETS))}")
    cand = candidates(topic, args.brands, args.per_facet, skip)
    print(f"{len(cand)} kandidaat-termen voor '{topic.label}', volumes ophalen…")
    volumes = lookup_volumes(list(cand))

    rows = [{"keyword": k, "search_volume": volumes.get(k), **cand[k]} for k in cand]
    known = [r for r in rows if r["search_volume"] is not None]
    unknown = [r for r in rows if r["search_volume"] is None]
    known, merged = dedupe_variants(known)
    known.sort(key=lambda r: -r["search_volume"])

    print(f"\n{len(known)} met volume, {len(unknown)} zonder rij van de API"
          + (f", {len(merged)} varianten samengevoegd" if merged else "") + "\n")
    for loser, winner in merged:
        print(f"  samengevoegd: '{loser}' -> '{winner}'")
    if merged:
        print()
    for r in known[:25]:
        mark = "  <-- gekozen" if r in known[:args.top] and r["search_volume"] >= args.min_volume else ""
        print(f"  {r['search_volume']:>7}/mnd  {r['keyword']:<40} [{r['kind']}]{mark}")
    if unknown:
        print(f"\n  zonder volume: {', '.join(r['keyword'] for r in unknown[:8])}"
              + (" …" if len(unknown) > 8 else ""))

    chosen = [r for r in known if r["search_volume"] >= args.min_volume][:args.top]
    if not chosen:
        print("\ngeen enkele term haalt de volumedrempel — verlaag --min-volume", file=sys.stderr)
        return 1

    if not args.apply:
        print(f"\n(niets gewijzigd; --apply legt deze {len(chosen)} termen vast)")
        return 0

    with open(topic.file("keywords.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["keyword", "search_volume", "kind"])
        w.writeheader()
        for r in rows:                      # álle onderzochte termen, ook de
                                            # samengevoegde varianten: dit is
                                            # de onderbouwing, niet de selectie
            w.writerow({"keyword": r["keyword"],
                        "search_volume": r["search_volume"] if r["search_volume"] is not None else "",
                        "kind": r["kind"]})

    was = {t["term"]: t for t in topic.terms}
    # 'display' is de paginatitel. Facetwaarden leveren soms kromme
    # samenstellingen op die wél zoekvolume hebben ('draadloos koptelefoon');
    # de zoekterm blijft dan staan zoals hij gezocht wordt, maar de kop niet.
    # Standaard gelijk aan de term; met de hand te corrigeren in topic.json.
    topic.cfg["terms"] = [{"term": r["keyword"], "display": r["keyword"],
                           "volume": r["search_volume"], "kind": r["kind"],
                           "params": r["params"],
                           **({"has_mockup_page": True} if was.get(r["keyword"], {}).get("has_mockup_page") else {})}
                          for r in chosen]
    topic.save()
    print(f"\n{len(chosen)} termen vastgelegd in topic.json, {len(rows)} keywords in keywords.csv")
    print("Let op: de termen zijn gewijzigd — draai collect_products.py opnieuw.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
