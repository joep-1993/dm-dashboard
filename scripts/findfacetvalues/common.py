# -*- coding: utf-8 -*-
"""Gedeelde helpers voor de findfacetvalues-pijplijn."""
import json
import os
import re
import unicodedata

import requests

TAXONOMY = "http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl"
SEARCH = "https://productsearch-v2.api.beslist.nl"
CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")


CACHE_TTL = 7 * 24 * 3600  # taxonomie verandert traag, maar niet nooit


def cache_get(name, url, params=None, ttl=CACHE_TTL):
    """GET met een bestandscache; verlopen of onleesbare cache wordt opnieuw opgehaald."""
    import time
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, name)
    if ttl and os.path.exists(path) and (time.time() - os.path.getmtime(path)) < ttl:
        try:
            return json.load(open(path, encoding="utf-8"))
        except Exception:
            pass
    r = requests.get(url, params=params, timeout=120)
    r.raise_for_status()
    d = r.json()
    json.dump(d, open(path, "w", encoding="utf-8"), ensure_ascii=False)
    return d


def fold(s):
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))


def norm(s):
    """Lowercase, accent-behoudend, alles wat geen letter/cijfer is wordt spatie."""
    s = (s or "").replace(" ", " ").lower()
    s = re.sub(r"[^a-z0-9À-ÿ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def kp_key(s):
    """Normalisatie zoals keyword_planner_service.clean_keyword, maar accent-veilig."""
    s = re.sub(r"[-_]", " ", fold(s))
    s = re.sub(r"[^a-zA-Z0-9\s]", "", s)
    return " ".join(s.split()).lower()


def nl_label(obj, field="name"):
    """Haal het nl-NL label uit een taxonomie-object."""
    for l in obj.get("labels") or []:
        if l.get("locale") == "nl-NL":
            v = l.get(field) or l.get("nameInColumn") or l.get("name")
            if v:
                return v
    for l in obj.get("labels") or []:
        v = l.get(field) or l.get("nameInColumn") or l.get("name")
        if v:
            return v
    return None


# --------------------------------------------------------------------------
# Categorie-resolutie
# --------------------------------------------------------------------------
def parse_category_url(url):
    """https://www.beslist.nl/products/klussen/klussen_486172_638250/ -> (klussen, klussen_486172_638250)"""
    m = re.search(r"/products/([^/]+)/([^/?#]+)", url)
    if not m:
        raise SystemExit(f"Kan geen categorie herkennen in URL: {url}")
    return m.group(1), m.group(2)


def main_category_id(slug):
    cats = cache_get("categories_top.json", f"{TAXONOMY}/api/Categories", {"locale": "nl-NL"})
    for c in cats:
        for l in c.get("labels") or []:
            if l.get("urlSlug") == slug:
                return c["id"], nl_label(c)
    raise SystemExit(f"Onbekende hoofdcategorie-slug: {slug}")


def resolve_category(url):
    """URL -> dict met main/leaf categorie-ids. De legacy-ids in de URL kent de Taxonomy API niet."""
    slug, url_name = parse_category_url(url)
    main_id, main_name = main_category_id(slug)
    d = requests.get(
        f"{SEARCH}/search/products",
        params={"mainCategory": main_id, "countryLanguage": "nl-nl", "isBot": "false", "limit": 1},
        timeout=120,
    ).json()
    if "categories" not in d:
        raise SystemExit(f"Search API gaf geen categorieën terug: {str(d)[:300]}")
    for c in d["categories"]:
        if c.get("urlName") == url_name:
            return {
                "main_category_id": main_id,
                "main_category_name": main_name,
                "category_id": c["id"],
                "category_name": c["name"],
                "url_name": url_name,
                "product_count": c.get("count"),
                "url": url,
            }
    raise SystemExit(f"Categorie '{url_name}' niet gevonden onder {main_name} ({main_id}).")


# --------------------------------------------------------------------------
# Facetten
# --------------------------------------------------------------------------
def all_facets():
    return cache_get("facets_all.json", f"{TAXONOMY}/api/Facets", {"locale": "nl-NL"})


def category_facets(category_id):
    r = requests.get(f"{TAXONOMY}/api/CategoryFacets", params={"categoryId": category_id}, timeout=60)
    r.raise_for_status()
    out = []
    for cf in r.json():
        fa = cf.get("facet") or {}
        out.append({
            "facet_id": cf.get("facetId"),
            "name": nl_label(fa),
            "is_hidden": cf.get("isHidden"),
            "is_enabled": fa.get("isEnabled"),
        })
    return out


def facet_values(facet_id):
    d = cache_get(f"facetvalues_{facet_id}.json", f"{TAXONOMY}/api/Facets/{facet_id}/values")
    items = d.get("items", d) if isinstance(d, dict) else d
    out = []
    for v in items:
        n = nl_label(v, "nameInColumn")
        if n:
            out.append(n)
    return out


def facets_named(name):
    """Alle facet-ids waarvan het nl-NL label (case-insensitief) gelijk is aan `name`."""
    want = norm(name)
    hits = []
    for f in all_facets():
        n = nl_label(f)
        if n and norm(n) == want:
            hits.append({"facet_id": f["id"], "name": n, "is_enabled": f.get("isEnabled")})
    return hits


# --------------------------------------------------------------------------
# Producten
# --------------------------------------------------------------------------
def fetch_products(main_id, category_id, max_products=3000, page=100, log=print):
    seen, offset, facets, total = {}, 0, None, None
    while offset < max_products:
        d = requests.get(
            f"{SEARCH}/search/products",
            params={"mainCategory": main_id, "category": category_id, "countryLanguage": "nl-nl",
                    "isBot": "false", "limit": page, "offset": offset},
            timeout=120,
        ).json()
        if facets is None:
            facets = d.get("facets")
        total = d.get("total", total)
        ps = d.get("products") or []
        if not ps:
            break
        for p in ps:
            seen[p["id"]] = p
        offset += page
        if offset % 500 == 0:
            log(f"  ... {len(seen)} producten opgehaald (API meldt total={total})")
    return list(seen.values()), facets, total
