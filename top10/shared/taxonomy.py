"""Categorieboom van de Taxonomy API: ophalen, cachen, doorzoeken.

De Taxonomy API heeft geen tree-endpoint (``/api/Categories/tree`` bestaat niet
— dat wordt gelezen als een categorie-id en geeft 400) en ``/api/Categories``
geeft alleen de 32 roots. Eén ``GET /api/Categories/{id}`` levert steeds precies
één niveau kinderen. Vandaar een breedte-eerst-wandeling die de hele boom één
keer ophaalt en op schijf zet; daarna is opzoeken gratis.

De ``urlSlug`` uit de nl-NL-labels is exact de ``category``-parameter van de
Search API. De getallen daarin zijn NIET de categorie-id (Airfryers = id
9005486, slug ``..._19968037_23583843``), dus de slug wordt altijd overgenomen
en nooit samengesteld.
"""
from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from .topic import CACHE, slugify

TAXONOMY_API = "http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl"
LOCALE = "nl-NL"


def _label(node: dict, locale: str = LOCALE) -> tuple[str | None, str | None]:
    lab = next((x for x in node.get("labels") or [] if x.get("locale") == locale), {})
    return lab.get("name"), lab.get("urlSlug")


def _cache_path(locale: str) -> Path:
    CACHE.mkdir(parents=True, exist_ok=True)
    return CACHE / f"categories_{locale}.json"


def build_tree(locale: str = LOCALE, workers: int = 16, verbose: bool = True) -> dict:
    """Wandel de hele boom en geef een platte index {id: node} terug."""
    s = requests.Session()
    failed: list[int] = []

    def fetch(cid: int) -> dict:
        """Eén node ophalen. De API tikt onder parallelle load af en toe af op
        een read timeout; dat mag niet de hele wandeling weggooien, dus drie
        pogingen en anders een lege node (de tak wordt dan overgeslagen)."""
        for attempt in range(3):
            try:
                r = s.get(f"{TAXONOMY_API}/api/Categories/{cid}", timeout=30)
                r.raise_for_status()
                return r.json()
            except Exception:
                if attempt == 2:
                    failed.append(cid)
                    return {"id": cid, "subCategories": []}
                time.sleep(1.5 * (attempt + 1))
        return {"id": cid, "subCategories": []}

    roots = s.get(f"{TAXONOMY_API}/api/Categories", params={"locale": locale}, timeout=30).json()
    index: dict[str, dict] = {}
    frontier = []
    for r in roots:
        name, slug = _label(r, locale)
        index[str(r["id"])] = {"id": r["id"], "name": name, "slug": slug, "parent_id": None,
                               "root_id": r["id"], "path": [name], "level": 0,
                               "enabled": r.get("isEnabled", True)}
        frontier.append(r["id"])

    level = 0
    while frontier:
        level += 1
        with ThreadPoolExecutor(max_workers=workers) as ex:
            payloads = list(ex.map(fetch, frontier))
        nxt = []
        for parent_payload in payloads:
            parent = index[str(parent_payload["id"])]
            for child in parent_payload.get("subCategories") or []:
                name, slug = _label(child, locale)
                cid = str(child["id"])
                if cid in index:
                    continue
                index[cid] = {"id": child["id"], "name": name, "slug": slug,
                              "parent_id": parent["id"], "root_id": parent["root_id"],
                              "path": parent["path"] + [name], "level": level,
                              "enabled": child.get("isEnabled", True)}
                nxt.append(child["id"])
        if verbose:
            print(f"  niveau {level}: {len(nxt)} categorieën")
        frontier = nxt

    if failed and verbose:
        print(f"  let op: {len(failed)} categorieën niet opgehaald na 3 pogingen "
              f"(hun subcategorieën ontbreken): {failed[:10]}")

    _cache_path(locale).write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")
    return index


def load_tree(locale: str = LOCALE, refresh: bool = False, verbose: bool = True) -> dict:
    p = _cache_path(locale)
    if p.exists() and not refresh:
        return json.loads(p.read_text(encoding="utf-8"))
    if verbose:
        print("categorieboom ophalen (eenmalig, daarna gecacht)…")
    return build_tree(locale, verbose=verbose)


def search(term: str, tree: dict | None = None, limit: int = 12) -> list[dict]:
    """Kandidaat-categorieën voor vrije tekst, beste match eerst.

    Score: exacte naam > naam begint ermee > deelstring. Diepere categorieën
    winnen bij gelijke score, want "airfryers" hoort op de bladcategorie te
    landen en niet op de maincat die het woord toevallig ook bevat.
    """
    tree = tree if tree is not None else load_tree()
    want = slugify(term)
    hits = []
    for node in tree.values():
        name_slug = slugify(node.get("name") or "")
        if not name_slug:
            continue
        if name_slug == want:
            score = 3
        elif name_slug.startswith(want) or want.startswith(name_slug):
            score = 2
        elif want in name_slug or name_slug in want:
            score = 1
        else:
            continue
        hits.append({**node, "score": score})
    hits.sort(key=lambda h: (-h["score"], -h["level"], h["name"] or ""))
    return hits[:limit]


def by_slug(url_slug: str, tree: dict | None = None) -> dict | None:
    tree = tree if tree is not None else load_tree()
    return next((n for n in tree.values() if n.get("slug") == url_slug), None)


def from_beslist_url(url: str, tree: dict | None = None) -> dict | None:
    """https://www.beslist.nl/products/<main>/<category>/... -> categorie-node."""
    parts = [p for p in url.split("?")[0].split("#")[0].split("/") if p]
    if "products" not in parts:
        return None
    tail = parts[parts.index("products") + 1:]
    for candidate in reversed(tail[:2]):          # eerst de diepste van de twee
        node = by_slug(candidate, tree)
        if node:
            return node
    return None
