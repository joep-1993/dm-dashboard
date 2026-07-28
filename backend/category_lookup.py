"""
Category lookup: URL slug -> (maincat name, deepest-cat name).

API-first, with cat_urls.csv as the offline fallback.

Why API-first. The CSV only ever contained the old ``slug_catid`` url form
(``huis_tuin_505064``), and newer categories are published under a bare readable
slug — Wandpanelen is id 9005645 with nl-NL urlSlug ``wandpanelen``. Those can
never match a ``slug_catid`` key, so lookup_category() missed, and callers fell
back to guessing a name off the API product's categories[] array by URL depth,
which lands on an ANCESTOR: /products/huis_tuin/wandpanelen/ was labelled
"Woonaccessoires". 352 URLs across 37 categories carried that fingerprint.
Nothing in the repo ever generated the CSV either — two commits in its whole
history, one of them a mojibake repair.

So the CSV is now a *cache of the API* rather than an independent source: every
successful walk rewrites it. That means the fallback is the last known good API
snapshot instead of a hand-made export of unknown age, and a new category stops
needing a manual file edit.

Ordering:
  1. in-memory slug map, rebuilt from Taxonomy API v2 (1h TTL)
  2. on a MISS, re-walk (rate-limited) and retry — a new category resolves
     without a restart
  3. only if the API is unreachable, read the CSV as it stands on disk

NB urlSlug is not a top-level field: it sits inside labels[] per locale, so it
has to be read off the ``nl-NL`` entry.
"""

import csv
import os
import threading
import time
from typing import Dict, Optional, Tuple

import requests

from backend.text_encoding import fix_mojibake

_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
CAT_URLS_CSV = os.path.join(_DATA_DIR, "cat_urls.csv")
MAINCAT_CSV = os.path.join(os.path.dirname(__file__), "maincat_mapping.csv")

TAX_BASE = "http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl"
TAX_HEADERS = {"X-User-Name": "SEO_JOEP", "Accept": "application/json"}
TAX_TIMEOUT = 30
LOCALE = "nl-NL"

# A full walk is ~1k requests, so it must not run per lookup.
API_TTL = 3600          # rebuild the map at most hourly
MISS_COOLDOWN = 600     # and on a miss, at most once every 10 min

# slug -> (maincat, deepest_cat)
_csv_map: Dict[str, Tuple[str, str]] = {}
_api_map: Dict[str, Tuple[str, str]] = {}
_api_map_at: float = 0.0
_last_walk_at: float = 0.0
_walk_in_flight: bool = False
_lock = threading.Lock()

# Reuse one session: the walk is many small requests to the same host.
_session = requests.Session()


# ---------------------------------------------------------------------------
# CSV (fallback + on-disk cache of the last successful walk)
# ---------------------------------------------------------------------------
def _load_csv() -> None:
    if _csv_map:
        return
    try:
        with open(CAT_URLS_CSV, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f, delimiter=";"):
                url_name = (row.get("url_name") or "").strip("/")
                # Defensive: cat_urls.csv has historically shipped with mojibaked
                # names (UTF-8 read as Latin-1 upstream), e.g. "PlissÃ©gordijnen".
                # Repair on load so generated titles never inherit it.
                if url_name:
                    _csv_map[url_name] = (
                        fix_mojibake(row.get("maincat") or ""),
                        fix_mojibake(row.get("deepest_cat") or ""),
                    )
        print(f"[CategoryLookup] CSV fallback: {len(_csv_map)} mappings")
    except Exception as e:
        print(f"[CategoryLookup] Failed to load cat_urls.csv: {e}")


def _write_csv(rows) -> None:
    """Rewrite cat_urls.csv from a successful walk. Atomic, and best-effort:
    the in-memory map is already usable, so a write failure must not break the
    lookup that triggered it."""
    try:
        os.makedirs(_DATA_DIR, exist_ok=True)
        tmp = CAT_URLS_CSV + ".tmp"
        with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["maincat", "deepest_cat", "url_name", "cat_id"])
            for maincat, cat_name, slug, cat_id in rows:
                w.writerow([maincat, cat_name, f"/{slug}/", cat_id])
        os.replace(tmp, CAT_URLS_CSV)
        _csv_map.clear()          # force a reload from the fresh file
        print(f"[CategoryLookup] cat_urls.csv rewritten from the API ({len(rows)} rows)")
    except Exception as e:
        print(f"[CategoryLookup] Could not rewrite cat_urls.csv: {e}")


# ---------------------------------------------------------------------------
# Taxonomy API v2
# ---------------------------------------------------------------------------
def _nl_label(obj: dict) -> dict:
    return next((l for l in (obj.get("labels") or []) if l.get("locale") == LOCALE), {})


def _maincat_roots():
    """[(maincat_name, maincat_id), ...] from maincat_mapping.csv."""
    out = []
    try:
        with open(MAINCAT_CSV, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f, delimiter=";"):
                name = fix_mojibake((row.get("maincat") or "").strip())
                mid = (row.get("maincat_id") or "").strip()
                if name and mid.isdigit():
                    out.append((name, int(mid)))
    except Exception as e:
        print(f"[CategoryLookup] Failed to read maincat_mapping.csv: {e}")
    return out


def _walk(parent_id: int, maincat: str, acc: list, seen: set, depth: int = 0) -> None:
    """Depth-first over subCategories, collecting (maincat, name, slug, id).

    ``seen`` guards against a cycle or a category reachable twice — the API is a
    graph in principle and an unguarded recursion would not terminate.
    """
    if depth > 8 or parent_id in seen:
        return
    seen.add(parent_id)
    try:
        r = _session.get(f"{TAX_BASE}/api/Categories/{parent_id}",
                         headers=TAX_HEADERS, params={"locale": LOCALE},
                         timeout=TAX_TIMEOUT)
        if r.status_code != 200:
            return
        for sub in r.json().get("subCategories") or []:
            if not sub.get("isEnabled", True):
                continue
            lab = _nl_label(sub)
            name, slug, cid = lab.get("name") or "", lab.get("urlSlug") or "", sub.get("id")
            if name and slug and cid:
                acc.append((maincat, fix_mojibake(name), slug, cid))
            if cid:
                _walk(cid, maincat, acc, seen, depth + 1)
    except Exception:
        # One bad branch must not lose the rest of the walk.
        pass


def _refresh_api_map() -> bool:
    """Rebuild the slug map from the API and rewrite the CSV. True on success.

    Returns False (leaving any existing map intact) when the API yields nothing,
    so an outage degrades to the previous snapshot instead of an empty map.
    """
    global _api_map, _api_map_at, _last_walk_at
    _last_walk_at = time.time()
    rows: list = []
    for maincat, mid in _maincat_roots():
        _walk(mid, maincat, rows, set())
    if not rows:
        print("[CategoryLookup] Taxonomy API returned nothing — keeping the CSV fallback")
        return False
    _api_map = {slug: (maincat, name) for maincat, name, slug, _ in rows}
    _api_map_at = time.time()
    print(f"[CategoryLookup] Slug map rebuilt from Taxonomy API v2: {len(_api_map)} categories")
    _write_csv(rows)
    return True


def _walk_in_background() -> None:
    """Kick off a rebuild without blocking the caller.

    A full walk measured ~168s against prod, so it can NEVER run inline: the
    first title generated after a restart (or after the TTL lapses) would stall
    for three minutes. The caller falls through to the CSV meanwhile, which is
    itself the previous walk's output, so cold-start answers are still current.
    """
    global _walk_in_flight
    with _lock:
        if _walk_in_flight or (_last_walk_at and (time.time() - _last_walk_at) < MISS_COOLDOWN):
            return
        _walk_in_flight = True

    def run():
        global _walk_in_flight
        try:
            _refresh_api_map()
        finally:
            with _lock:
                _walk_in_flight = False

    threading.Thread(target=run, daemon=True, name="category-slug-walk").start()


def _api_lookup(key: str) -> Optional[Tuple[str, str]]:
    with _lock:
        hit = _api_map.get(key)
        stale = not _api_map or (time.time() - _api_map_at) > API_TTL
    if hit and not stale:
        return hit
    # Cold, stale, or a miss: rebuild in the background and answer with what we
    # have. A slug the map has never seen therefore resolves on a LATER call
    # (or via the CSV now) rather than making this request wait for the walk.
    _walk_in_background()
    return hit


# ---------------------------------------------------------------------------
# Public
# ---------------------------------------------------------------------------
def lookup_category(main_category: Optional[str], category: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Look up category names from URL parts.

    Args:
        main_category: Main category URL part (e.g. "meubilair")
        category: Subcategory URL part — either the old ``slug_catid`` form
            ("meubilair_389369_389525") or a bare slug ("wandpanelen") — or None
            for top-level.

    Returns:
        (maincat_name, deepest_cat_name) e.g. ("Meubels", "Loveseats"),
        or (None, None) if neither the API nor the CSV knows the slug.
    """
    key = category if category else main_category
    if not key:
        return None, None

    hit = _api_lookup(key)
    if hit:
        return hit

    _load_csv()
    return _csv_map.get(key) or (None, None)


def refresh_now() -> dict:
    """Force a rebuild (used by tooling/tests). Returns a small status dict."""
    with _lock:
        ok = _refresh_api_map()
    return {"refreshed": ok, "categories": len(_api_map), "csv": CAT_URLS_CSV}


def stats() -> dict:
    return {
        "api_categories": len(_api_map),
        "api_age_seconds": int(time.time() - _api_map_at) if _api_map_at else None,
        "csv_mappings": len(_csv_map),
    }
