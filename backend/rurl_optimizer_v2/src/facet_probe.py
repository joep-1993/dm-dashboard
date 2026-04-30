"""V29: facet-probe rescue extension.

For AND-mode keywords (V28 already established a dominant deepest_cat),
probe candidate facet values via filter queries to find one that covers
>= MIN_FACET_COVERAGE of the keyword's result set. Append the winning
facet to the redirect URL so the user lands on a narrowly-targeted page
instead of a deepest_cat-only page.

Cache: separate table `facet_probe_cache` in the same SQLite DB used by
search_derived. derive_facet() is read-only; prefetch_facet_probes()
populates the cache (sequentially throttled by the shared _TokenBucket).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests

from src.search_derived import (
    SEARCH_BASE_URL, COUNTRY_LANG, TIMEOUT,
    SEARCH_QPS, MAX_PREFETCH_WORKERS,
    _CACHE_DB_PATH, _normalize, _is_fresh, _cache_get, _TokenBucket,
)

logger = logging.getLogger(__name__)

# Tunables
MIN_FACET_COVERAGE = 0.6       # winning value must cover this fraction of base T
MIN_VALUE_PRODUCTS = 5          # skip facet values with fewer products subcat-wide
MAX_CANDIDATES_PER_PAIR = 15    # hard cap on probes per pair → bounds API cost

# Facet names that aren't useful for routing — operational / commercial
# attributes that don't help the user pick a category-narrowed page.
# Blacklist (rather than whitelist) so new facet names introduced by the
# taxonomy team automatically participate unless explicitly excluded.
FACET_BLACKLIST = {
    "winkel",            # already filtered by facet_id=1, kept here for safety
    "voorraad",          # stock status
    "leverbaarheid",
    "levertijd",
    "bezorging",
    "bezorgtijd",
    "verzending",
    "garantie",
    "prijs",
    "prijsklasse",
    "korting",
    "actie",
    "aanbieding",
    "betaling",
    "betaalmethode",
    "retour",
    "uitvoering",        # too generic — usually doesn't narrow well
    "conditie",          # new vs used — not a navigational signal we want
    "conditie_systemen",
}

_FACETS_CACHE: Optional[pd.DataFrame] = None


def _facets_df() -> pd.DataFrame:
    global _FACETS_CACHE
    if _FACETS_CACHE is None:
        path = Path(__file__).parent.parent / "data" / "cache" / "facets.csv"
        _FACETS_CACHE = pd.read_csv(path)
    return _FACETS_CACHE


def _connect(readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        uri = f"file:{_CACHE_DB_PATH}?mode=ro"
        return sqlite3.connect(uri, uri=True, timeout=5)
    conn = sqlite3.connect(_CACHE_DB_PATH, timeout=10)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS facet_probe_cache (
            maincat TEXT NOT NULL,
            keyword TEXT NOT NULL,
            payload TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (maincat, keyword)
        )
        """
    )
    return conn


def _probe_get(mn: str, kn: str) -> Optional[dict]:
    if not _CACHE_DB_PATH.exists():
        return None
    try:
        c = _connect(readonly=True)
    except sqlite3.OperationalError:
        return None
    try:
        cur = c.execute(
            "SELECT payload, fetched_at FROM facet_probe_cache WHERE maincat=? AND keyword=?",
            (mn, kn),
        )
        row = cur.fetchone()
        if not row or not _is_fresh(row[1]):
            return None
        return json.loads(row[0])
    except sqlite3.OperationalError:
        return None
    finally:
        c.close()


def _probe_put(mn: str, kn: str, payload: dict) -> None:
    c = _connect(readonly=False)
    try:
        c.execute(
            "INSERT OR REPLACE INTO facet_probe_cache (maincat, keyword, payload, fetched_at) "
            "VALUES (?, ?, ?, ?)",
            (mn, kn, json.dumps(payload), datetime.now(timezone.utc).isoformat()),
        )
        c.commit()
    finally:
        c.close()


def _probe_one(category_slug: str, keyword: str, base_total: int,
               facet_name: str, value_id: int) -> Optional[float]:
    """One filter-probe API call. Returns coverage = filtered_total / base_total
    or None on error."""
    params = {
        "category": category_slug,
        "query": keyword,
        "countryLanguage": COUNTRY_LANG,
        "isBot": "true",
        "limit": "1",
        "trackTotalHits": "true",
        f"filters[{facet_name}][0]": str(value_id),
    }
    url = f"{SEARCH_BASE_URL}/search/products?{urllib.parse.urlencode(params)}"
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        c = data.get("total") or 0
        return (c / base_total) if base_total else 0.0
    except Exception as e:
        logger.debug(f"probe failed for {facet_name}={value_id}: {e}")
        return None


def _facet_id_to_name() -> dict:
    """Build facet_id → facet_name slug map from cached facets.csv."""
    fdf = _facets_df()
    return dict(zip(fdf["facet_id"].astype(int), fdf["facet_name"]))


def _check_surfaced(v28_payload: dict, base_total: int,
                    id_to_name: dict) -> Optional[tuple]:
    """V29 step 1: see if the V28 base call already surfaced a dominant
    facet value (≥ MIN_FACET_COVERAGE). If yes, no probe API calls
    needed — return the winner directly. Returns (cov, count, name, vid,
    vname) or None.
    """
    surfaced = v28_payload.get("surfaced_facets") or []
    best = None
    for f in surfaced:
        fid = f.get("facet_id")
        facet_name = id_to_name.get(fid)
        if not facet_name or facet_name.lower() in FACET_BLACKLIST:
            continue
        for vid, vname, count in (f.get("values") or []):
            if count is None or count <= 0:
                continue
            cov = count / base_total if base_total else 0
            if cov < MIN_FACET_COVERAGE:
                continue
            cand = (round(cov, 3), int(count), facet_name, int(vid), vname or "")
            if best is None or cand > best:
                best = cand
    return best


def _do_probe(maincat: str, keyword: str, v28_payload: dict,
              bucket: _TokenBucket) -> dict:
    """Find the best facet value for this (maincat, keyword) pair.

    Two-stage:
      Stage 1 — check surfaced_facets in the V28 base-call response (no
                API calls). If a value covers ≥ MIN_FACET_COVERAGE, win.
      Stage 2 — probe filtered candidates from facets.csv via per-facet
                /search/products?filters[…] calls. Caps at
                MAX_CANDIDATES_PER_PAIR to bound API cost.

    Returns the dict to cache. mode ∈ {match, match_from_response,
    no_match, no_candidates, no_probe, error}.
    """
    if v28_payload.get("mode") != "and":
        return {"mode": "no_probe", "reason": "v28_not_and"}
    base_total = v28_payload.get("total") or 0
    dom_slug = v28_payload.get("dom_cat_url_slug")
    if not dom_slug or base_total <= 0:
        return {"mode": "no_probe", "reason": "no_dom_cat"}

    id_to_name = _facet_id_to_name()

    # Stage 1: free win from already-surfaced facets in the base response.
    surfaced_best = _check_surfaced(v28_payload, base_total, id_to_name)
    if surfaced_best is not None:
        coverage, value_count, facet_name, value_id, value_name = surfaced_best
        return {
            "mode": "match_from_response",
            "facet_name": facet_name,
            "value_id": value_id,
            "value_name": value_name,
            "coverage": coverage,
            "value_count": value_count,
            "candidates_probed": 0,
        }

    # Stage 2: candidates from cached facets.csv, then API probes.
    fdf = _facets_df()
    cands = fdf[fdf["category_url_slug"] == dom_slug]
    if cands.empty:
        return {"mode": "no_candidates", "reason": "no_facets_for_subcat"}
    min_count = max(MIN_VALUE_PRODUCTS, int(base_total * MIN_FACET_COVERAGE))
    cands = cands[
        (cands["facet_id"] != 1)
        & (cands["count"] >= min_count)
        & (~cands["facet_name"].str.lower().isin(FACET_BLACKLIST))
    ]
    cands = cands.sort_values("count", ascending=False).head(MAX_CANDIDATES_PER_PAIR)
    if cands.empty:
        return {"mode": "no_candidates", "reason": "filter_empty",
                "min_count_required": min_count}

    best = None  # (coverage, value_count, facet_name, value_id, value_name)
    n_probes = 0
    for _, row in cands.iterrows():
        bucket.acquire()
        cov = _probe_one(dom_slug, keyword, base_total,
                         row["facet_name"], int(row["facet_value_id"]))
        n_probes += 1
        if cov is None or cov < MIN_FACET_COVERAGE:
            continue
        cand = (round(cov, 3), int(row["count"]),
                row["facet_name"], int(row["facet_value_id"]),
                row["facet_value_name"])
        if best is None or cand > best:
            best = cand

    if best is None:
        return {"mode": "no_match", "candidates_probed": n_probes,
                "candidates_considered": int(len(cands))}

    coverage, value_count, facet_name, value_id, value_name = best
    return {
        "mode": "match",
        "facet_name": facet_name,
        "value_id": value_id,
        "value_name": value_name,
        "coverage": coverage,
        "value_count": value_count,
        "candidates_probed": n_probes,
    }


def derive_facet(maincat: str, keyword: str) -> dict:
    """Cache-only read. Returns the cached probe payload (with `mode`),
    or {} if uncached.
    """
    if not maincat or not keyword:
        return {}
    mn, kn = _normalize(maincat, keyword)
    return _probe_get(mn, kn) or {}


def prefetch_facet_probes(pairs: Iterable[tuple[str, str]],
                          qps: float = SEARCH_QPS,
                          max_workers: int = MAX_PREFETCH_WORKERS,
                          verbose: bool = True) -> dict:
    """For every pair where V28's cache says mode=and with a dominant
    deepest_cat, probe candidate facet values and cache the winner.
    Pairs without a usable V28 result are noted but skipped.
    """
    seen: set[tuple[str, str]] = set()
    todo: list[tuple[str, str, str, str, dict]] = []
    hits = skipped_no_v28 = skipped_no_dom = 0
    for maincat, keyword in pairs:
        if not maincat or not keyword:
            continue
        mn, kn = _normalize(maincat, keyword)
        if (mn, kn) in seen:
            continue
        seen.add((mn, kn))
        if _probe_get(mn, kn) is not None:
            hits += 1
            continue
        v28 = _cache_get(mn, kn)
        if v28 is None:
            skipped_no_v28 += 1
            continue
        if v28.get("mode") != "and" or not v28.get("dom_cat_url_slug"):
            skipped_no_dom += 1
            # Cache "no probe needed" so future runs skip it for free.
            _probe_put(mn, kn, {"mode": "no_probe"})
            continue
        todo.append((maincat, keyword, mn, kn, v28))

    n_workers = min(max_workers, MAX_PREFETCH_WORKERS,
                    max(1, int(round(qps / 4)))) or 1
    bucket = _TokenBucket(qps)

    if verbose:
        # Average ~8 probes per pair (filter typically yields 5–15).
        eta = int(len(todo) * 8 / max(qps, 0.01))
        print(f"[V29 facet-probe] hits: {hits}, skipped (no V28): {skipped_no_v28}, "
              f"skipped (no dom_cat): {skipped_no_dom}, to probe: {len(todo)} "
              f"at {qps} QPS / {n_workers} workers (~{eta}s)")

    probed = errors = 0
    lock = threading.Lock()

    def _worker(item):
        maincat, keyword, mn, kn, v28 = item
        try:
            payload = _do_probe(maincat, keyword, v28, bucket)
            _probe_put(mn, kn, payload)
            return True
        except Exception as e:
            logger.debug(f"probe worker error: {e}")
            try:
                _probe_put(mn, kn, {"mode": "error", "reason": str(e)[:80]})
            except Exception:
                pass
            return False

    if todo:
        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            futs = [ex.submit(_worker, item) for item in todo]
            for f in as_completed(futs):
                ok = False
                try:
                    ok = f.result()
                except Exception:
                    pass
                with lock:
                    probed += 1
                    if not ok:
                        errors += 1
                    if verbose and probed % 50 == 0:
                        print(f"[V29 facet-probe]   {probed}/{len(todo)} done "
                              f"(errors so far: {errors})")

    if verbose:
        print(f"[V29 facet-probe] done: hits={hits} probed={probed} errors={errors}")

    return {"hits": hits, "probed": probed, "errors": errors,
            "skipped_no_v28": skipped_no_v28, "skipped_no_dom": skipped_no_dom,
            "total_unique_pairs": len(seen)}
