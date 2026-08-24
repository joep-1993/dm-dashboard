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
import os
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

# V61: de loader had geen enkele rem. 12 threads tegen een endpoint van ~250 ms is
# ~48 req/s, terwijl search_derived 20 QPS documenteert als de proces-globale cap
# die "met IT afgestemd moet worden" voordat je 'm verhoogt. Met de V60-tweede-pas
# erbij ging een rebuild van ~3,5k naar ~16,6k calls, en `start_optimize` trapt die
# rebuild automatisch af zodra de cache 7 dagen oud is — dus vlak voor een run van
# een gebruiker. Zelfde token bucket als de rest, env-tunebaar.
FACETS_QPS = float(os.getenv("RURL_FACETS_QPS", "20"))

# V60: an unfiltered /search/products call truncates every facet's value list to
# the N values with the most products. N is per facet, not exposed by the API and
# not settable by any query param (probed: facetValueLimit, facetLimit,
# maxFacetValues, … all ignored) — 8 for `ruimte`, 16 for `kleur`, 100 for `merk`.
# Everything below the cut simply isn't in the response, so the optimizer never
# saw e.g. ruimte~4945789 (Balkon, 21 products) in Opbergkasten and could never
# append it. Re-requesting the SAME category with a filter on that facet returns
# the facet's FULL value list, with counts identical to the unfiltered ones
# (the API computes a facet excluding its own filter), so a second pass repairs
# the cache without introducing a second, possibly disagreeing, source.
FACET_VALUE_REPROBE = True
# Brand/shop are deliberately left truncated. `winkel` is excluded from matching
# everywhere, and lifting `merk` off its 100-value cap would add thousands of
# tail brands per category — that changes brand matching (and the size of this
# cache) enough to deserve its own evaluation, and it isn't what the truncation
# actually costs us: the misses are attribute facets.
REPROBE_SKIP_FACETS = {"merk", "winkel"}


def _atomic_to_csv(df: pd.DataFrame, cache_path: Path) -> None:
    """Write a cache CSV atomically. A killed rebuild (OOM, disk full, the
    subprocess dying with uvicorn) used to leave a truncated file that pandas
    parses happily — and because the service's staleness test is mtime-based,
    that half-file reads as 0 days old and the 7-day auto-refresh will not
    repair it. Write beside the target, then rename: the previous good cache
    survives any crash before the rename."""
    tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, cache_path)


def _fetch_json(url: str, retries: int = 2, bucket=None) -> dict | list:
    """GET JSON with a small retry. Raises on final failure.

    `bucket` paces the call against FACETS_QPS; a rebuild fans out over 12
    threads and would otherwise hammer the live Search API."""
    last_exc: Exception | None = None
    for _ in range(retries + 1):
        try:
            if bucket is not None:
                bucket.acquire()
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
        # Gedeeld over alle threads van deze loader, zodat pass 1 en de V60
        # tweede pas samen onder FACETS_QPS blijven.
        from src.search_derived import _TokenBucket
        self._bucket = _TokenBucket(FACETS_QPS)
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
                return cid, _fetch_json(url, bucket=self._bucket)
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
        _atomic_to_csv(df, cache_path)
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
        _atomic_to_csv(df, cache_path)
        return df

    def _search_category_facets(self, slug: str, filter_facet: str = "",
                                filter_value=None) -> list:
        """Return the `facets` block for a category. With `filter_facet` set,
        THAT facet's value list comes back complete instead of truncated to its
        top-N (see FACET_VALUE_REPROBE); every other facet in the response is
        narrowed to the filtered product set and must be ignored."""
        params = {
            "category": slug,
            "countryLanguage": SEARCH_LOCALE,
            "isBot": "true",
            "limit": "1",
        }
        if filter_facet and filter_value is not None:
            params[f"filters[{filter_facet}][0]"] = str(filter_value)
        url = f"{SEARCH_BASE_URL}/search/products?{urllib.parse.urlencode(params)}"
        data = _fetch_json(url, bucket=getattr(self, '_bucket', None))
        return data.get("facets") or []

    def _reprobe_truncated_facet_values(self, pair_values: dict, pair_ctx: dict,
                                        facet_meta: dict) -> list[dict]:
        """V60 second pass: refetch the (category, facet) pairs whose value list
        looks truncated and return the rows the first pass could not see.

        A facet's cap is invisible per response, so it's derived from the data:
        the cap can only be the highest value count that facet reaches across all
        categories, and a pair sitting at that number is exactly the pair that
        may have been cut off. Pairs below it showed everything they had. That
        over-probes facets which are simply small (a 3-value facet is "at its
        max" in every category) — those calls just come back with nothing new,
        which is the cheap side of the trade.
        """
        max_per_facet: dict[int, int] = {}
        for (_cid, fid), vids in pair_values.items():
            if len(vids) > max_per_facet.get(fid, 0):
                max_per_facet[fid] = len(vids)

        candidates = []
        for pair, vids in pair_values.items():
            fname = pair_ctx[pair]["facet_name"]
            # The filter param takes the facet's url slug. When the first pass
            # fell back to the response label we don't have one, so we can't
            # address the facet and have to leave the pair as it is.
            if not vids or fname in REPROBE_SKIP_FACETS:
                continue
            if facet_meta.get(pair[1]) != fname:
                continue
            if len(vids) >= max_per_facet.get(pair[1], 0):
                candidates.append(pair)

        if not candidates:
            return []
        logger.info("V60: re-probing %d of %d (category, facet) pairs for "
                    "truncated value lists...", len(candidates), len(pair_values))

        def probe(pair):
            ctx = pair_ctx[pair]
            # Any value of this facet unlocks the full list; take the lowest id
            # so a rebuild issues byte-identical requests.
            seed = min(pair_values[pair])
            try:
                return pair, self._search_category_facets(
                    ctx["slug"], ctx["facet_name"], seed)
            except Exception as e:
                return pair, {"__error__": str(e)}

        extra: list[dict] = []
        errors = 0
        error_samples: dict[str, int] = {}
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(probe, p) for p in candidates]
            done = 0
            for fut in as_completed(futures):
                pair, result = fut.result()
                done += 1
                if done % 2000 == 0:
                    logger.info("  ...%d/%d pairs (%.1fs elapsed)",
                                done, len(candidates), time.time() - t0)
                if isinstance(result, dict) and "__error__" in result:
                    errors += 1
                    msg = str(result["__error__"])[:120]
                    error_samples[msg] = error_samples.get(msg, 0) + 1
                    continue

                cid, fid = pair
                ctx = pair_ctx[pair]
                known = pair_values[pair]
                for f in result:
                    if f.get("id") != fid:
                        continue
                    for v in f.get("values") or []:
                        vid = v.get("id")
                        if vid is None or vid in known:
                            continue
                        known.add(vid)
                        extra.append({
                            "facet_id": fid,
                            "facet_name": ctx["facet_name"],
                            "facet_value_id": vid,
                            "facet_value_name": v.get("facetValue") or "",
                            "url": f"/products/{ctx['root_slug']}/{ctx['slug']}"
                                   f"/c/{ctx['facet_name']}~{vid}",
                            "main_category_id": ctx["root"],
                            "main_category_name": ctx["root_name"],
                            "category_id": cid,
                            "category_url_slug": ctx["slug"],
                            "count": v.get("count") or 0,
                        })

        logger.info("V60 re-probe complete: +%d values on %d pairs in %.1fs "
                    "(errors=%d)", len(extra), len(candidates),
                    time.time() - t0, errors)
        # A failed probe silently leaves a pair truncated, which is invisible in
        # the output — so say what went wrong, not just how often.
        for msg, n in sorted(error_samples.items(), key=lambda kv: -kv[1])[:5]:
            logger.warning("  probe error x%d: %s", n, msg)
        return extra

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
            try:
                return cid, self._search_category_facets(tree["id_to_url_slug"][cid])
            except Exception as e:
                return cid, {"__error__": str(e)}

        rows: list[dict] = []
        # V60 bookkeeping: which values we saw per (category, facet), plus the
        # context needed to build a row for a value the second pass discovers.
        pair_values: dict[tuple[int, int], set] = {}
        pair_ctx: dict[tuple[int, int], dict] = {}
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
                    pair = (cid, fid)
                    pair_ctx[pair] = {
                        "facet_name": fname,
                        "slug": slug,
                        "root": root,
                        "root_slug": root_slug,
                        "root_name": root_name,
                    }
                    seen = pair_values.setdefault(pair, set())
                    for v in f.get("values") or []:
                        vid = v.get("id")
                        if vid is None:
                            continue
                        seen.add(vid)
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

        if FACET_VALUE_REPROBE:
            rows.extend(self._reprobe_truncated_facet_values(
                pair_values, pair_ctx, facet_meta))

        # Deterministic row order: rows were appended in thread-completion
        # (as_completed) order, so a cache rebuild could reshuffle them and flip
        # "first matching row wins" tie-breaks downstream. Sort by a stable key
        # so the cached CSV is reproducible run-to-run.
        rows.sort(key=lambda r: (r["category_id"], r["facet_id"], r["facet_value_id"]))
        df = pd.DataFrame(rows)
        _atomic_to_csv(df, cache_path)
        return df

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
