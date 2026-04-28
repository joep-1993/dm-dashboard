"""
Data loading module for Beslist.nl R-URL Optimizer.

Phase 1 rewrite (2026-04-24): sources category + facet data from the
Taxonomy v2 API and Search API v2 instead of Redshift. Cached to CSV
in data/cache so subsequent runs are instant.

The three DataFrames returned keep the same shape the matcher expects:
  - main_categories: cat_id, name, table_name
  - categories:      cat_id, url_name, display_name
  - facets:          facet_id, facet_name, facet_value_id, facet_value_name,
                     url, main_category_id, main_category_name
"""

from __future__ import annotations

import json
import logging
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
import config

logger = logging.getLogger(__name__)

TAXV2_BASE_URL = "http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl"
SEARCH_BASE_URL = "https://productsearch-v2.api.beslist.nl"
LOCALE = "nl-NL"
SEARCH_LOCALE = "nl-nl"
MAX_WORKERS = 12
HTTP_TIMEOUT = 30


def _fetch_json(url: str, retries: int = 2) -> dict | list:
    """GET JSON with a small retry. Raises on final failure."""
    last_exc: Exception | None = None
    for _ in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            last_exc = e
            time.sleep(0.3)
    raise last_exc  # type: ignore[misc]


def _pick_label(labels: list | None, locale: str = LOCALE) -> dict:
    """Pick the label for the requested locale; fall back to the first one."""
    if not labels:
        return {}
    for lab in labels:
        if lab.get("locale") == locale:
            return lab
    return labels[0]


class DataLoader:
    """Loads categories and facets from taxv2 + Search APIs with CSV caching."""

    def __init__(self, use_cache: bool = True):
        self.use_cache = use_cache
        self._tree_cache: dict | None = None
        self._facet_meta_cache: dict[int, str] | None = None

    # ------------------------------------------------------------------
    # Taxonomy v2 — category tree (BFS)
    # ------------------------------------------------------------------
    def _fetch_category_tree(self) -> dict:
        """
        Crawl /api/Categories/{id}?includeSubCategories=true in parallel.

        Returns dict with:
          id_to_name:      cat_id -> nl-NL display name
          id_to_url_slug:  cat_id -> nl-NL urlSlug
          id_to_parent:    cat_id -> parentId (None for roots)
          id_to_root:      cat_id -> root (main category) id
          root_ids:        list of root ids in tree order
        """
        if self._tree_cache is not None:
            return self._tree_cache

        t0 = time.time()
        roots = _fetch_json(f"{TAXV2_BASE_URL}/api/Categories?locale={LOCALE}")
        if not isinstance(roots, list):
            raise RuntimeError(f"Unexpected taxv2 response: {type(roots).__name__}")

        id_to_name: dict[int, str] = {}
        id_to_url_slug: dict[int, str] = {}
        id_to_parent: dict[int, int | None] = {}
        id_to_root: dict[int, int] = {}
        root_ids: list[int] = []

        for cat in roots:
            cid = cat.get("id")
            if cid is None:
                continue
            lab = _pick_label(cat.get("labels"))
            id_to_name[cid] = lab.get("name") or str(cid)
            id_to_url_slug[cid] = lab.get("urlSlug") or ""
            id_to_parent[cid] = cat.get("parentId")
            id_to_root[cid] = cid
            root_ids.append(cid)

        def fetch_detail(cid: int):
            url = f"{TAXV2_BASE_URL}/api/Categories/{cid}?includeSubCategories=true&includeFacets=false"
            try:
                return cid, _fetch_json(url)
            except Exception as e:
                return cid, {"__error__": str(e)}

        frontier = list(id_to_parent.keys())
        fetch_errors = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            while frontier:
                results = list(ex.map(fetch_detail, frontier))
                nxt: list[int] = []
                for cid, detail in results:
                    if "__error__" in detail:
                        fetch_errors += 1
                        continue
                    root = id_to_root.get(cid, cid)
                    for sub in detail.get("subCategories") or []:
                        sid = sub.get("id")
                        if sid is None or sid in id_to_parent:
                            continue
                        lab = _pick_label(sub.get("labels"))
                        id_to_name[sid] = lab.get("name") or str(sid)
                        id_to_url_slug[sid] = lab.get("urlSlug") or ""
                        id_to_parent[sid] = sub.get("parentId", cid)
                        id_to_root[sid] = root
                        nxt.append(sid)
                frontier = nxt

        logger.info(
            "Taxv2 BFS: %d cats crawled in %.1fs (errors=%d)",
            len(id_to_parent), time.time() - t0, fetch_errors,
        )
        self._tree_cache = {
            "id_to_name": id_to_name,
            "id_to_url_slug": id_to_url_slug,
            "id_to_parent": id_to_parent,
            "id_to_root": id_to_root,
            "root_ids": root_ids,
        }
        return self._tree_cache

    # ------------------------------------------------------------------
    # Taxonomy v2 — facet metadata (id -> urlSlug)
    # ------------------------------------------------------------------
    def _fetch_facet_meta(self) -> dict[int, str]:
        """Return mapping facet_id -> urlSlug (e.g. 1290 -> 'merk')."""
        if self._facet_meta_cache is not None:
            return self._facet_meta_cache

        t0 = time.time()
        out: dict[int, str] = {}
        # /api/Facets returns the full list as a bare array — skip/take are ignored.
        data = _fetch_json(f"{TAXV2_BASE_URL}/api/Facets")
        items = data.get("items") if isinstance(data, dict) else data
        for f in items or []:
            fid = f.get("id")
            if fid is None:
                continue
            lab = _pick_label(f.get("labels"))
            slug = lab.get("urlSlug")
            if slug:
                out[fid] = slug
        logger.info("Taxv2 facet metadata: %d facets in %.1fs", len(out), time.time() - t0)
        self._facet_meta_cache = out
        return out

    # ------------------------------------------------------------------
    # Public API — same shape as before
    # ------------------------------------------------------------------
    def load_main_categories(self) -> pd.DataFrame:
        """Return DataFrame: cat_id, name, table_name."""
        cache_path = config.CACHE_DIR / "main_categories.csv"
        if self.use_cache and cache_path.exists():
            return pd.read_csv(cache_path)

        tree = self._fetch_category_tree()
        rows = []
        for rid in tree["root_ids"]:
            slug = tree["id_to_url_slug"].get(rid, "")
            rows.append({
                "cat_id": rid,
                "name": tree["id_to_name"][rid],
                "table_name": slug,  # legacy column — use slug as stand-in
            })
        df = pd.DataFrame(rows)
        df.to_csv(cache_path, index=False)
        return df

    def load_categories(self) -> pd.DataFrame:
        """Return DataFrame: cat_id, url_name, display_name (all enabled subcats)."""
        cache_path = config.CACHE_DIR / "categories.csv"
        if self.use_cache and cache_path.exists():
            return pd.read_csv(cache_path)

        tree = self._fetch_category_tree()
        rows = []
        for cid, slug in tree["id_to_url_slug"].items():
            if not slug:
                continue
            # Skip roots (main categories) — old `tblcategories_online` only had subcats
            if tree["id_to_parent"][cid] is None:
                continue
            rows.append({
                "cat_id": cid,
                "url_name": slug,
                "display_name": tree["id_to_name"][cid],
            })
        df = pd.DataFrame(rows)
        df.to_csv(cache_path, index=False)
        return df

    def load_facets(self) -> pd.DataFrame:
        """
        Return DataFrame: facet_id, facet_name, facet_value_id, facet_value_name,
        url, main_category_id, main_category_name, category_id, category_url_slug, count.

        Only includes (cat, facet, value) combos that actually have products,
        as reported by the Search API.
        """
        cache_path = config.CACHE_DIR / "facets.csv"
        if self.use_cache and cache_path.exists():
            return pd.read_csv(cache_path)

        tree = self._fetch_category_tree()
        facet_meta = self._fetch_facet_meta()

        # Query every subcategory's facet counts via the Search API in parallel.
        sub_ids = [
            cid for cid, parent in tree["id_to_parent"].items()
            if parent is not None and tree["id_to_url_slug"].get(cid)
        ]
        logger.info("Fetching facets for %d subcategories via Search API...", len(sub_ids))

        def fetch_cat_facets(cid: int):
            slug = tree["id_to_url_slug"][cid]
            url = (
                f"{SEARCH_BASE_URL}/search/products"
                f"?category={urllib.parse.quote(slug)}"
                f"&countryLanguage={SEARCH_LOCALE}&isBot=false&limit=1"
            )
            try:
                data = _fetch_json(url)
                return cid, data.get("facets") or []
            except Exception as e:
                return cid, {"__error__": str(e)}

        rows: list[dict] = []
        errors = 0
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(fetch_cat_facets, cid): cid for cid in sub_ids}
            done = 0
            for fut in as_completed(futures):
                cid, result = fut.result()
                done += 1
                if done % 500 == 0:
                    logger.info("  ...%d/%d cats (%.1fs elapsed)",
                                done, len(sub_ids), time.time() - t0)
                if isinstance(result, dict) and "__error__" in result:
                    errors += 1
                    continue

                slug = tree["id_to_url_slug"][cid]
                root = tree["id_to_root"][cid]
                root_slug = tree["id_to_url_slug"].get(root, "")
                root_name = tree["id_to_name"].get(root, "")

                for f in result:
                    fid = f.get("id")
                    fname = facet_meta.get(fid) or f.get("label") or ""
                    if not fid or not fname:
                        continue
                    for v in f.get("values") or []:
                        vid = v.get("id")
                        if vid is None:
                            continue
                        rows.append({
                            "facet_id": fid,
                            "facet_name": fname,
                            "facet_value_id": vid,
                            "facet_value_name": v.get("facetValue") or "",
                            "url": f"/products/{root_slug}/{slug}/c/{fname}~{vid}",
                            "main_category_id": root,
                            "main_category_name": root_name,
                            "category_id": cid,
                            "category_url_slug": slug,
                            "count": v.get("count") or 0,
                        })

        logger.info(
            "Facet fetch complete: %d rows in %.1fs (errors=%d)",
            len(rows), time.time() - t0, errors,
        )
        df = pd.DataFrame(rows)
        df.to_csv(cache_path, index=False)
        return df

    def load_r_urls(self, filepath: str) -> pd.DataFrame:
        return pd.read_csv(filepath)

    def save_to_cache(self, df: pd.DataFrame, filename: str) -> Path:
        cache_path = config.CACHE_DIR / filename
        df.to_csv(cache_path, index=False)
        return cache_path

    def close(self):
        pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    loader = DataLoader(use_cache=False)
    mc = loader.load_main_categories()
    print(f"main_categories: {len(mc)} rows")
    print(mc.head())
    cats = loader.load_categories()
    print(f"\ncategories: {len(cats)} rows")
    print(cats.head())
    facets = loader.load_facets()
    print(f"\nfacets: {len(facets)} rows")
    print(facets.head())
