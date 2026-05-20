"""V28: Search-derived rescue layer with disk-backed cache + prefetch.

Architecture
------------
1. **Disk cache** (SQLite). Keyed by `(maincat, keyword_normalized)`. Stores
   the full search-derived result so a row that's been seen before — in
   any prior run — never re-hits the API.

2. **Prefetch pass**. Before the parallel matcher runs, the entrypoint
   collects every unique `(maincat, keyword)` pair from the input URLs,
   filters out the ones already in cache, and fetches the rest
   sequentially at SEARCH_QPS calls/sec. Throttled by a simple sleep —
   we run from a single process, so no cross-process token bucket needed.

3. **Cache-only lookups during matching**. The worker pool calls
   `derive_redirect()` which reads from cache only and never hits the API.
   This guarantees we don't blow past the rate limit even with many
   workers.

Tunable knobs (all module constants):
    SEARCH_QPS              global rate cap during the prefetch step.
    CACHE_TTL_DAYS          stale entries are re-fetched.
    AND_MODE_TOTAL_THRESHOLD  AND-mode classifier (real total, not capped).
    DOMINANCE_THRESHOLD     fraction of products that must agree on cat.

API params used: isBot=true (skips A/B experiments + personalisation),
trackTotalHits=true (uncaps `total` so AND vs fallback is bimodal).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
import urllib.parse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

import requests

logger = logging.getLogger(__name__)

SEARCH_BASE_URL = "https://productsearch-v2.api.beslist.nl"
COUNTRY_LANG = "nl-nl"
LIMIT = 50
TIMEOUT = 10

# Tunables — adjust here if IT clears a different QPS or you want fresher data.
# 20 QPS matches the process-global cap enforced by backend/beslist_rate_limit.py
# for the FastAPI service. rurl_optimizer_v2 runs as a subprocess (no shared
# in-memory bucket), so it mirrors the cap here. The prefetch is parallelised
# across MAX_PREFETCH_WORKERS threads — adding workers above the cap has no
# effect because the local _TokenBucket below paces all requests.
SEARCH_QPS = 20.0
MAX_PREFETCH_WORKERS = 20
CACHE_TTL_DAYS = 7
AND_MODE_TOTAL_THRESHOLD = 10000
# V31: raised from 0.60 to 0.75. At 60% dominance the "dominant category" is
# often noise — e.g. /r/elektrische_sigaretten/ landed on Kapperstassen at
# 60% via incidental product-description hits. Above 75% the signal is
# strong enough that the guess is usually right.
DOMINANCE_THRESHOLD = 0.75

# V31: bump when _classify's output shape changes. Cached payloads with a
# missing or older schema_version are ignored by _cache_get so the next run
# re-fetches them with the new classifier. Previously, fallback-mode rows
# stored only {"mode": "fallback", "total": N} and never produced a dom_cat,
# which blocked the facet-probe pipeline for niche queries.
SCHEMA_VERSION = 2

_CACHE_DB_PATH = Path(__file__).parent.parent / "data" / "cache" / "search_derived.sqlite"
_CACHE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def _connect(readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        # File-URI form gives us read-only access without taking a write lock,
        # which matters when many worker processes open the cache at once.
        uri = f"file:{_CACHE_DB_PATH}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5)
    else:
        conn = sqlite3.connect(_CACHE_DB_PATH, timeout=10)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS search_cache (
                maincat TEXT NOT NULL,
                keyword TEXT NOT NULL,
                payload TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                PRIMARY KEY (maincat, keyword)
            )
            """
        )
    return conn


def _normalize(maincat: str, keyword: str) -> tuple[str, str]:
    return (maincat or "").strip().lower(), " ".join((keyword or "").lower().split())


def _is_fresh(fetched_at_iso: str) -> bool:
    try:
        ts = datetime.fromisoformat(fetched_at_iso)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except Exception:
        return False
    return datetime.now(timezone.utc) - ts < timedelta(days=CACHE_TTL_DAYS)


def _fetch_live(maincat: str, keyword: str) -> Optional[dict]:
    params = {
        "category": maincat,
        "query": keyword,
        "countryLanguage": COUNTRY_LANG,
        "isBot": "true",
        "limit": str(LIMIT),
        "trackTotalHits": "true",
    }
    url = f"{SEARCH_BASE_URL}/search/products?{urllib.parse.urlencode(params)}"
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.debug(f"V28 fetch failed for ({maincat}, {keyword!r}): {e}")
        return None


def _classify(api_resp: Optional[dict]) -> dict:
    """Boil an API response down to the small dict we cache.

    V28: uses the response's `categories` array — which carries per-category
    product counts across the *entire* result set — instead of sampling the
    top-N products. Top-N sampling can mislead when the API ranks broader
    cats first (e.g. "senioren huistelefoon" returned 7/10 Mobiele telefoons
    in the sample but the true split is 139 Huistelefoons / 3 Mobiele).

    V31: never short-circuit on `total >= AND_MODE_TOTAL_THRESHOLD`. The
    search API switches to OR-fallback when AND-matching produces fewer
    products than `limit`, and in that mode `total` becomes the whole-cat
    OR count (millions). But the `categories[]` array still reports the
    true AND-match counts per category, so we can recover a usable dom_cat
    for niche queries like "hoesloze dekbedden" (17 AND-matches, 6.9M OR
    total). Mode is reported as "fallback_with_dom_cat" when that recovery
    path fires, so downstream callers can tell the two apart.
    """
    if api_resp is None:
        return {"schema_version": SCHEMA_VERSION, "mode": "error", "total": None}
    total = api_resp.get("total") or 0
    products = api_resp.get("products") or []
    if not products:
        return {"schema_version": SCHEMA_VERSION, "mode": "empty", "total": total}

    is_fallback = total >= AND_MODE_TOTAL_THRESHOLD
    out = {"schema_version": SCHEMA_VERSION,
           "mode": "fallback" if is_fallback else "and",
           "total": total}

    # V29: Capture surfaced facets[] so the facet_probe layer can read
    # value counts directly from this response without extra API calls.
    # Slim shape: list of {facet_id, values: [(value_id, value_name, count), ...]}.
    # facet_name is not in the response — joined via cached facets.csv at probe time.
    surfaced = []
    for f in (api_resp.get("facets") or []):
        fid = f.get("id")
        if fid is None or fid == 1:  # skip winkel
            continue
        vals = []
        for v in (f.get("values") or []):
            vid = v.get("id")
            if vid is None:
                continue
            vals.append([int(vid), v.get("facetValue") or "", int(v.get("count") or 0)])
        if vals:
            surfaced.append({"facet_id": int(fid), "values": vals})
    if surfaced:
        out["surfaced_facets"] = surfaced

    cats_resp = api_resp.get("categories") or []
    if cats_resp:
        # Pick the deepest depth that has at least one category — that's
        # the "leaf" level for this query — then pick the cat with the
        # highest count among siblings at that depth.
        max_depth = max((c.get("depth") or 0) for c in cats_resp)
        leaf_cats = [c for c in cats_resp if (c.get("depth") or 0) == max_depth]
        leaf_cats.sort(key=lambda c: -(c.get("count") or 0))
        sum_at_leaf = sum((c.get("count") or 0) for c in leaf_cats) or 1
        top = leaf_cats[0]
        share = (top.get("count") or 0) / sum_at_leaf
        out.update({
            "dom_cat_id": top.get("id"),
            "dom_cat_name": top.get("name", ""),
            "dom_cat_url_slug": top.get("urlName", ""),
            "dom_cat_share": round(share, 2),
            "dom_cat_count": top.get("count") or 0,
            "dom_cat_depth": max_depth,
        })
        return out

    # Fallback path (older cache entries / unusual responses): sample the
    # returned products.
    rows = []
    for p in products:
        cats = p.get("categories") or []
        if cats:
            c = cats[-1]
            rows.append((c.get("id"), c.get("name", ""), c.get("urlName", "")))
    if rows:
        counter = Counter(rows)
        (cat_id, cat_name, cat_slug), count = counter.most_common(1)[0]
        share = count / len(rows)
        out.update({
            "dom_cat_id": cat_id,
            "dom_cat_name": cat_name,
            "dom_cat_url_slug": cat_slug,
            "dom_cat_share": round(share, 2),
        })
    return out


def _cache_get(maincat_norm: str, keyword_norm: str) -> Optional[dict]:
    if not _CACHE_DB_PATH.exists():
        return None
    try:
        conn = _connect(readonly=True)
    except sqlite3.OperationalError:
        return None
    try:
        cur = conn.execute(
            "SELECT payload, fetched_at FROM search_cache WHERE maincat=? AND keyword=?",
            (maincat_norm, keyword_norm),
        )
        row = cur.fetchone()
        if not row or not _is_fresh(row[1]):
            return None
        payload = json.loads(row[0])
        # V31: ignore entries written under an older classifier schema so
        # the next run re-fetches them with the new logic (e.g. old rows
        # cached `{mode: fallback, total: N}` with no dom_cat — those need
        # re-classifying via categories[]).
        if payload.get("schema_version") != SCHEMA_VERSION:
            return None
        return payload
    finally:
        conn.close()


def _cache_put(maincat_norm: str, keyword_norm: str, payload: dict) -> None:
    conn = _connect(readonly=False)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO search_cache (maincat, keyword, payload, fetched_at) "
            "VALUES (?, ?, ?, ?)",
            (maincat_norm, keyword_norm, json.dumps(payload),
             datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def _build_redirect_url(maincat: str, classified: dict) -> Optional[str]:
    # V31: also build a redirect URL for fallback responses where the
    # categories[] breakdown recovered a dominant cat. That's the niche-
    # query case (e.g. "hoesloze dekbedden" — total reads 6.9M in OR-mode,
    # but only 17 products genuinely match and they all sit in one cat).
    if classified.get("mode") not in ("and", "fallback"):
        return None
    share = classified.get("dom_cat_share")
    slug = classified.get("dom_cat_url_slug")
    if share is None or slug is None or share < DOMINANCE_THRESHOLD:
        return None
    return f"https://www.beslist.nl/products/{maincat}/{slug}/"


def derive_redirect(maincat: str, keyword: str) -> dict:
    """Cache-only lookup. Returns a result dict shaped like the cached payload
    plus a `redirect_url` when AND-mode dominance is reached. When the cache
    has no entry (or it's stale), returns {'mode': 'uncached', ...}.
    Workers MUST NOT trigger live API calls — that's the prefetch's job.
    """
    if not maincat or not keyword:
        return {"mode": "skipped", "total": None}
    mn, kn = _normalize(maincat, keyword)
    cached = _cache_get(mn, kn)
    if cached is None:
        return {"mode": "uncached", "total": None}
    out = dict(cached)
    rurl = _build_redirect_url(maincat, cached)
    if rurl:
        out["redirect_url"] = rurl
    return out


class _TokenBucket:
    """Reserve-slot rate limiter. Each acquire() atomically claims the next
    interval; concurrent threads sleep outside the lock so the realised
    throughput converges to exactly `qps` even under high worker counts.
    """

    def __init__(self, qps: float):
        self._lock = threading.Lock()
        self._interval = 1.0 / qps if qps > 0 else 0.0
        self._next_slot = 0.0

    def acquire(self) -> None:
        if self._interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self._interval
        wait = slot - time.monotonic()
        if wait > 0:
            time.sleep(wait)


def prefetch_pairs(pairs: Iterable[tuple[str, str]],
                   qps: float = SEARCH_QPS,
                   max_workers: int = MAX_PREFETCH_WORKERS,
                   verbose: bool = True) -> dict:
    """Concurrently fetch every (maincat, keyword) pair that isn't already
    cached fresh. A shared TokenBucket caps global throughput at `qps`
    regardless of `max_workers`. We hard-cap workers at MAX_PREFETCH_WORKERS
    so a misconfigured `qps` can't spawn an unbounded pool.

    Returns counts of {hits, fetched, errors, total_unique_pairs}.
    """
    seen: set[tuple[str, str]] = set()
    todo: list[tuple[str, str, str, str]] = []  # (maincat, keyword, mn, kn)
    hits = 0
    for maincat, keyword in pairs:
        if not maincat or not keyword:
            continue
        mn, kn = _normalize(maincat, keyword)
        if (mn, kn) in seen:
            continue
        seen.add((mn, kn))
        if _cache_get(mn, kn) is not None:
            hits += 1
            continue
        todo.append((maincat, keyword, mn, kn))

    # Cap workers so we never exceed the global limit AND never overshoot
    # what the rate cap can actually feed. With response time ~0.2s, one
    # worker sustains ~5 QPS, so qps/5 is the useful upper bound.
    desired = min(MAX_PREFETCH_WORKERS, max(1, int(round(qps / 4))))
    n_workers = min(max_workers, MAX_PREFETCH_WORKERS, desired) or 1
    bucket = _TokenBucket(qps)

    if verbose:
        eta = int(len(todo) / max(qps, 0.01))
        print(f"[V28 prefetch] cache hits: {hits}, to fetch: {len(todo)} "
              f"at {qps} QPS / {n_workers} workers (~{eta}s)")

    fetched = 0
    errors = 0
    fetched_lock = threading.Lock()

    def _worker(item):
        maincat, keyword, mn, kn = item
        bucket.acquire()
        api = _fetch_live(maincat, keyword)
        classified = _classify(api)
        _cache_put(mn, kn, classified)
        return api is not None

    if todo:
        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            futs = [ex.submit(_worker, item) for item in todo]
            for f in as_completed(futs):
                ok = False
                try:
                    ok = f.result()
                except Exception as e:
                    logger.debug(f"V28 prefetch worker error: {e}")
                with fetched_lock:
                    fetched += 1
                    if not ok:
                        errors += 1
                    if verbose and fetched % 100 == 0:
                        print(f"[V28 prefetch]   {fetched}/{len(todo)} fetched "
                              f"(errors so far: {errors})")

    if verbose:
        print(f"[V28 prefetch] done: hits={hits} fetched={fetched} errors={errors}")

    return {"hits": hits, "fetched": fetched, "errors": errors,
            "total_unique_pairs": len(seen)}
