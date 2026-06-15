"""
Keyword Redirect Service.

Enriches the keyword x category volume results (from category_keyword_service)
with, per category:
  - the Beslist category URL carrying a shop's "winkel" facet, and
  - the live number of products that shop has in that category,
and builds a redirect mapping (old search URL -> winkel-facet category URL).

Shop product counts come from the authoritative
  GET /shop-stats/{shopId}/category/{categoryId}
endpoint. The Search API's filters[winkel] shop filter is NOT used: it is
silently dropped for many categories (returns the global cap of 10000 with
non-shop products) and is non-deterministic between calls.

URLs follow the Beslist canonicalisation rule: /c/ URLs get NO trailing slash;
/r/ search URLs keep their trailing slash. /r/ keywords use underscores for
spaces (the resolver treats spaces/%20/+/_ as equivalent, so only the
underscore form needs to be generated).
"""

import logging
import re
import requests
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict, Any

from backend.beslist_rate_limit import productsearch_bucket
from backend.gsd_check_service import search_gsd

logger = logging.getLogger(__name__)

SEARCH_API = "https://productsearch-v2.api.beslist.nl"
TAX_API = "http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl"
LOCALE = "nl-NL"

_session = requests.Session()
# Process-global cat_id -> nl-NL urlSlug cache (slugs are effectively static).
_slug_cache: Dict[int, str] = {}


# ---------------------------------------------------------------------------
# Shop resolution
# ---------------------------------------------------------------------------

def resolve_shop(query: str) -> Dict[str, Any]:
    """Resolve a shop name or numeric id to {shop_id, shop_name, candidates}.

    Numeric input is treated as an exact shop_id. Otherwise a partial-match
    LIKE search runs (Redshift via gsd_check_service) and the best candidate is
    chosen: exact name > "<query>.nl" > name-starts-with > shortest name. The
    Redshift shop_id equals the Search-API winkel id (verified: Hema.nl=652149).
    """
    query = (query or "").strip()
    if not query:
        return {"shop_id": None, "shop_name": None, "candidates": []}

    if query.isdigit():
        sid = int(query)
        res = search_gsd(shop_ids=[sid]).get("results", [])
        name = res[0]["shop_name"] if res else None
        return {"shop_id": sid, "shop_name": name,
                "candidates": [{"shop_id": sid, "shop_name": name}]}

    res = search_gsd(shop_names=[query]).get("results", [])
    candidates = [{"shop_id": r["shop_id"], "shop_name": r["shop_name"]} for r in res]
    if not candidates:
        return {"shop_id": None, "shop_name": None, "candidates": []}

    ql = query.lower()

    def score(c: Dict[str, Any]):
        name = (c.get("shop_name") or "").lower()
        return (name == ql, name == f"{ql}.nl", name.startswith(ql), -len(name))

    best = max(candidates, key=score)
    return {"shop_id": best["shop_id"], "shop_name": best["shop_name"],
            "candidates": candidates}


# ---------------------------------------------------------------------------
# Shop product counts (authoritative shop-stats endpoint)
# ---------------------------------------------------------------------------

def _shop_main_counts(shop_id: int) -> Dict[int, int]:
    """cat_id -> shop product count, harvested from the per-main subtree calls.

    One call per top-level main category returns that main's whole subtree
    (depths 0-2) with counts, keyed by the 9xxxxxx taxonomy ids. Cheap bulk
    pass; gaps (e.g. deeper categories) are filled individually by the caller.
    """
    productsearch_bucket.acquire()
    try:
        mains = _session.get(f"{SEARCH_API}/shop-stats/{shop_id}",
                             params={"isBot": "false"}, timeout=30).json()
    except Exception as e:
        logger.warning("shop-stats main list failed for %s: %s", shop_id, e)
        return {}
    main_ids = [e["id"] for e in mains if e.get("depth") == 0]

    def fetch(mid: int) -> Dict[int, int]:
        productsearch_bucket.acquire()
        try:
            data = _session.get(f"{SEARCH_API}/shop-stats/{shop_id}/category/{mid}",
                                params={"isBot": "false"}, timeout=60).json()
        except Exception:
            return {}
        return {e["id"]: e["count"] for e in data
                if e.get("id") is not None and e.get("count") is not None}

    counts: Dict[int, int] = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for d in ex.map(fetch, main_ids):
            counts.update(d)
    return counts


def _shop_cat_count(shop_id: int, cat_id: int) -> int:
    """Authoritative shop product count for a single category id."""
    productsearch_bucket.acquire()
    try:
        data = _session.get(f"{SEARCH_API}/shop-stats/{shop_id}/category/{cat_id}",
                            params={"isBot": "false"}, timeout=30).json()
    except Exception:
        return 0
    for e in data:
        if e.get("id") == cat_id:
            return e.get("count", 0) or 0
    return 0


# ---------------------------------------------------------------------------
# Category URL building
# ---------------------------------------------------------------------------

def _url_slug(cat_id: int) -> str:
    """nl-NL urlSlug for a taxonomy category id (cached)."""
    if cat_id in _slug_cache:
        return _slug_cache[cat_id]
    slug = ""
    try:
        j = _session.get(f"{TAX_API}/api/Categories/{cat_id}", timeout=30).json()
        labels = j.get("labels", []) or []
        for loc in (LOCALE, "nl-BE"):
            for l in labels:
                if l.get("locale") == loc and l.get("urlSlug"):
                    slug = l["urlSlug"]
                    break
            if slug:
                break
        if not slug:
            slug = next((l.get("urlSlug") for l in labels if l.get("urlSlug")), "") or ""
    except Exception:
        slug = ""
    _slug_cache[cat_id] = slug
    return slug


def _build_url(cat_id: int, shop_id: int) -> Optional[str]:
    """https://www.beslist.nl/products/{main}/{slug}/c/winkel~{shop_id} (no trailing /)."""
    slug = _url_slug(cat_id)
    if not slug:
        return None
    main = re.sub(r"(_\d+)+$", "", slug)  # strip trailing numeric path ids -> main urlName
    if main == slug:  # depth-0 main category, no numeric suffix
        path = f"/products/{slug}/c/winkel~{shop_id}"
    else:
        path = f"/products/{main}/{slug}/c/winkel~{shop_id}"
    return f"https://www.beslist.nl{path}"


def _kw_to_old_path(keyword: str) -> str:
    """/products/r/{keyword}/ with spaces collapsed to underscores (keeps trailing /)."""
    keyword = " ".join(keyword.split())
    return "/products/r/" + keyword.replace(" ", "_") + "/"


# ---------------------------------------------------------------------------
# Enrichment + redirect building
# ---------------------------------------------------------------------------

def enrich_redirects(deepest_cat_results: List[dict], shop_id: int) -> Dict[str, Any]:
    """Add url + results to categories where the shop has products (results > 0)
    and build the deduplicated redirect mapping.

    Returns {"rows": [...], "redirects": [...], "stats": {...}}.
    """
    # maincat search-volume totals over ALL categories (matches category_volumes semantics)
    maincat_volumes: Dict[str, int] = {}
    for r in deepest_cat_results:
        mc = r.get("maincat", "")
        maincat_volumes[mc] = maincat_volumes.get(mc, 0) + (r.get("search_volume", 0) or 0)

    # 1) product counts: cheap per-main pass, then individual lookups for gaps
    counts = _shop_main_counts(shop_id)
    cat_ids = set()
    for r in deepest_cat_results:
        try:
            cat_ids.add(int(r.get("cat_id")))
        except (TypeError, ValueError):
            continue
    missing = [c for c in cat_ids if c not in counts]
    if missing:
        with ThreadPoolExecutor(max_workers=16) as ex:
            for cid, cnt in zip(missing, ex.map(lambda c: _shop_cat_count(shop_id, c), missing)):
                counts[cid] = cnt

    # 2) keep categories with products, warm the urlSlug cache for them
    kept = []
    for r in deepest_cat_results:
        try:
            cid = int(r.get("cat_id"))
        except (TypeError, ValueError):
            continue
        cnt = counts.get(cid, 0)
        if cnt and cnt > 0:
            kept.append((cid, cnt, r))
    with ThreadPoolExecutor(max_workers=24) as ex:
        list(ex.map(_url_slug, list({cid for cid, _, _ in kept})))

    # 3) build the data rows
    rows = []
    for cid, cnt, r in kept:
        url = _build_url(cid, shop_id)
        if not url:
            continue
        rows.append({
            "maincat": r.get("maincat", ""),
            "maincat_id": r.get("maincat_id", ""),
            "deepest_cat": r.get("deepest_cat", ""),
            "cat_id": r.get("cat_id", ""),
            "original_keyword": r.get("original_keyword", ""),
            "final_keyword": "; ".join(r.get("combinations") or []),
            "search_volume_deepest_cat": r.get("search_volume", 0),
            "search_volume_maincat": maincat_volumes.get(r.get("maincat", ""), 0),
            "url": url,
            "results": cnt,
        })
    rows.sort(key=lambda x: (x["maincat"], x["deepest_cat"]))

    # 4) redirects: explode final_keyword, dedupe "old" keeping the highest-result target
    best: Dict[str, tuple] = {}
    for row in rows:
        for kw in (s.strip() for s in str(row["final_keyword"]).split(";")):
            if not kw:
                continue
            old = _kw_to_old_path(kw)
            if old not in best or row["results"] > best[old][1]:
                best[old] = (row["url"], row["results"])
    redirects = [{"old": o, "new": best[o][0]} for o in sorted(best)]

    return {
        "rows": rows,
        "redirects": redirects,
        "stats": {
            "categories_with_products": len(rows),
            "total_redirects": len(redirects),
        },
    }
