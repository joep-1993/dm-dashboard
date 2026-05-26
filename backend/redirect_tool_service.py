"""Redirect Tool — talks to redirect.api.beslist.nl and persists runs.

Key behavior:
- The redirect API's `url_redirect` table has a UNIQUE index across both fromUrl and
  toUrl, so a URL can't be both at once. Preflight rewrites each input row's `new`
  value to the terminal target if the URL is already a fromUrl in the DB (i.e.,
  "flattens" the chain client-side so the POST doesn't 500).
- URL variants (literal space, underscore, %20) are treated as equivalent for
  matching purposes — many real URLs exist under multiple forms.
- The homepage (`/`, empty, `/index`, `/index.html`) is hard-blocked as a fromUrl.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import requests

from backend.database import get_db_connection, return_db_connection

logger = logging.getLogger(__name__)

# In-memory submit task registry. Keyed by short hex task_id; the frontend
# polls /submit-status/{id} every ~500ms to drive the progress bar.
# Tasks are kept after completion so the final poll picks up the result;
# capped at 100 entries so a misbehaving client can't leak unbounded memory.
_SUBMIT_TASKS: dict[str, dict[str, Any]] = {}
_SUBMIT_LOCK = threading.Lock()
_SUBMIT_TASKS_MAX = 100

# Preflight task registry (mirrors submit) — preflight makes one HTTP call
# to redirect.api.beslist.nl per row (chain-flatten check), so it benefits
# from the same progress-bar treatment.
_PREVIEW_TASKS: dict[str, dict[str, Any]] = {}
_PREVIEW_LOCK = threading.Lock()
_PREVIEW_TASKS_MAX = 100

REDIRECT_API = "https://redirect.api.beslist.nl"
HTTP_TIMEOUT = 30
LIST_PAGE_SIZE = 50

# When a preflight batch exceeds this row count, switch from per-row HTTP
# lookups to a one-shot prefetch of the entire redirect table. The full
# table is ~820k rows / ~150MB JSON / ~200MB RAM as a Python dict — heavy,
# but pays for itself once we'd otherwise be making >>3× as many HTTP calls.
# Below the threshold, the existing per-row path is faster (no warmup cost).
PREFETCH_THRESHOLD = 5000
PREFETCH_PAGE_SIZE = 5000  # upstream API supports this; ~900KB per page
PREFETCH_WORKERS = 8       # parallel page fetches; the API tolerates this fine

# Shared session for connection pooling. Re-uses the underlying TCP+TLS
# connection across calls, saving ~50ms per call on the second+ request
# to the redirect API (no new handshake). Material at 1000-row scale
# where we'd otherwise re-handshake hundreds of times sequentially.
_HTTP = requests.Session()

DEFAULT_COUNTRY = "nl"
ALLOWED_STATUS_CODES = {301, 302, 303, 307, 308}
DEFAULT_STATUS_CODE = 301

# Paths that resolve to the homepage and must never be redirected
HOMEPAGE_PATHS = {"", "/", "/index", "/index.html"}


# ---------------------------------------------------------------------------
# URL handling
# ---------------------------------------------------------------------------

def strip_domain(url: str) -> str:
    """Return a /-prefixed path for any URL form (full URL, bare hostname, path)."""
    if not url:
        return ""
    s = url.strip()
    if s.startswith("/"):
        return s
    if "://" in s:
        parsed = urllib.parse.urlparse(s)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        return path
    # bare hostname like www.beslist.nl/foo
    if "/" in s and ("." in s.split("/")[0]):
        return "/" + s.split("/", 1)[1]
    return "/" + s


def normalize_path(path: str) -> str:
    """Decode %-escapes and strip whitespace, but keep spaces/underscores as-is."""
    if not path:
        return ""
    return urllib.parse.unquote(path.strip())


def equiv_key(path: str) -> str:
    """Canonical comparison key — treats space/underscore/%20 as identical."""
    return normalize_path(path).replace("_", " ")


def url_variants(path: str) -> list[str]:
    """Generate matching variants (space-form, underscore-form, %20-form)."""
    p = normalize_path(path)
    if not p:
        return []
    space = p.replace("_", " ")
    underscore = p.replace(" ", "_")
    percent = p.replace(" ", "%20")
    # Order matters: prefer the original-decoded form first so resolver hits the
    # exact stored value when possible.
    seen, out = set(), []
    for v in (p, space, underscore, percent):
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def is_homepage(path: str) -> bool:
    # Accept either path or full URL — strip domain first so callers can't bypass
    # the safety block by passing 'https://www.beslist.nl'.
    p = normalize_path(strip_domain(path))
    return p in HOMEPAGE_PATHS or p.rstrip("/") == ""


def _has_multivalue_facet(url: str) -> bool:
    # `+` is the multi-value separator inside a facet segment, which lives
    # after `/c/` in the path. Before `/c/` (search query in `/products/r/…`
    # or `/products/k/…`) `+` is a normal character and must not trigger the
    # multi-value skip.
    if not url or "+" not in url:
        return False
    p = normalize_path(strip_domain(url))
    idx = p.find("/c/")
    if idx == -1:
        return False
    return "+" in p[idx + 3:]


# ---------------------------------------------------------------------------
# Redirect API client
# ---------------------------------------------------------------------------

def _resolve_one(url: str, country: str = DEFAULT_COUNTRY) -> dict | None:
    try:
        r = _HTTP.get(
            f"{REDIRECT_API}/api/redirect",
            params={"searchterm": url, "country": country},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("totalRecords", 0) > 0 and data.get("data"):
            return data["data"][0]
    except Exception as exc:
        logger.warning("resolver call failed for %s: %s", url, exc)
    return None


def check_url_is_fromUrl(path: str, country: str = DEFAULT_COUNTRY) -> dict | None:
    """Return {url, statusCode, matched_variant} if `path` is a fromUrl in the DB."""
    for variant in url_variants(path):
        hit = _resolve_one(variant, country)
        if hit:
            return {**hit, "matched_variant": variant}
    return None


def check_url_incoming(path: str, max_pages: int = 2) -> list[dict]:
    """Find redirects whose toUrl matches any variant of `path`.

    `max_pages=2` covers the vast majority of real URLs (most distinctive
    substrings have well under 100 matches in the table). Increase only if
    you have URLs whose substring is intentionally non-distinctive.
    """
    variants = url_variants(path)
    if not variants:
        return []
    # Use the most-distinctive substring for urlContains — strip leading/trailing
    # slashes and pick a no-space variant to avoid query-string ambiguity.
    search = next((v for v in variants if " " not in v), variants[0]).strip("/")
    target_keys = {equiv_key(v) for v in variants}

    seen_ids: set[int] = set()
    matches: list[dict] = []
    for page in range(max_pages):
        try:
            r = _HTTP.get(
                f"{REDIRECT_API}/api/redirects",
                params={
                    "limit": LIST_PAGE_SIZE,
                    "offset": page * LIST_PAGE_SIZE,
                    "urlContains": search,
                },
                timeout=HTTP_TIMEOUT,
            )
            r.raise_for_status()
            data = r.json().get("data", [])
        except Exception as exc:
            logger.warning("incoming list call failed: %s", exc)
            break
        if not data:
            break
        for row in data:
            rid = row.get("id")
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            if equiv_key(row.get("toUrl", "")) in target_keys:
                matches.append(row)
        if len(data) < LIST_PAGE_SIZE:
            break
    return matches


def delete_redirect_by_fromurl(from_url: str) -> tuple[int, Any]:
    """DELETE /api/redirect?fromUrl=<url>. Per the API's Swagger spec
    (https://redirect.api.beslist.nl/swagger.json), delete is by fromUrl
    query param — not by id in the path. Returns (status_code, body).
    The body may be empty on success; we never raise from here so the
    caller can decide whether a 404 (rule already gone) is fatal or fine.
    """
    r = _HTTP.delete(
        f"{REDIRECT_API}/api/redirect",
        params={"fromUrl": from_url},
        timeout=HTTP_TIMEOUT,
    )
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text[:500]}
    return r.status_code, body


def post_redirect(from_url: str, to_url: str, country: str, status_code: int) -> tuple[int, Any]:
    r = _HTTP.post(
        f"{REDIRECT_API}/api/redirect",
        json=[{
            "fromUrl": from_url,
            "toUrl": to_url,
            "country": country,
            "statusCode": status_code,
        }],
        timeout=HTTP_TIMEOUT,
    )
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text[:500]}
    return r.status_code, body


# ---------------------------------------------------------------------------
# Preflight + submit
# ---------------------------------------------------------------------------

def preflight_rows(
    rows: list[dict],
    task: dict | None = None,
    fromurl_index: dict | None = None,
    tourl_index: dict | None = None,
) -> dict:
    """For each row, normalize, flatten chains, mark skips. Pure read-only.

    When `task` is provided, update its counters after every row so the
    /preview-status/{id} endpoint can drive a progress bar — preflight is
    O(n) HTTP calls (one chain-flatten check per row against the redirect
    API), so a 1000-row run takes minutes.

    When `fromurl_index` / `tourl_index` are provided (built by
    `build_redirect_index`), per-row lookups are served from in-memory
    dicts instead of calling redirect.api.beslist.nl per row. Used by
    `start_preflight` once the batch exceeds PREFETCH_THRESHOLD rows.
    """
    use_index = fromurl_index is not None and tourl_index is not None

    # Per-preflight memoization. Many batches share URLs across rows (a
    # single popular `new` URL is often the target of many rows). Caching
    # the resolver + incoming-list responses by canonical key avoids
    # repeating the same lookup. Cleared every preflight so changes the
    # user makes between runs are picked up fresh.
    _fromurl_cache: dict[tuple[str, str], dict | None] = {}
    _incoming_cache: dict[str, list[dict]] = {}
    _cache_lock = threading.Lock()

    def _lookup_fromurl_in_index(url: str) -> dict | None:
        """Mirror check_url_is_fromUrl using the prefetched index."""
        for variant in url_variants(url):
            hit = fromurl_index.get(equiv_key(variant))
            if hit:
                return {**hit, "matched_variant": variant}
        return None

    def _lookup_incoming_in_index(url: str) -> list[dict]:
        """Mirror check_url_incoming using the prefetched index."""
        variants = url_variants(url)
        if not variants:
            return []
        target_keys = {equiv_key(v) for v in variants}
        seen_ids: set[int] = set()
        matches: list[dict] = []
        for k in target_keys:
            for row in tourl_index.get(k, []):
                rid = row.get("id")
                if rid in seen_ids:
                    continue
                seen_ids.add(rid)
                matches.append(row)
        return matches

    def _cached_fromurl(url: str, country: str) -> dict | None:
        key = (equiv_key(url), country)
        with _cache_lock:
            if key in _fromurl_cache:
                return _fromurl_cache[key]
        # Fetch outside the lock — IO; another thread may double-fetch
        # in a race, which is fine (idempotent + bounded by row count).
        if use_index:
            result = _lookup_fromurl_in_index(url)
        else:
            result = check_url_is_fromUrl(url, country)
        with _cache_lock:
            _fromurl_cache.setdefault(key, result)
            return _fromurl_cache[key]

    def _cached_incoming(url: str) -> list[dict]:
        key = equiv_key(url)
        with _cache_lock:
            if key in _incoming_cache:
                return _incoming_cache[key]
        if use_index:
            result = _lookup_incoming_in_index(url)
        else:
            result = check_url_incoming(url)
        with _cache_lock:
            _incoming_cache.setdefault(key, result)
            return _incoming_cache[key]

    # Per-row pipeline: pure local normalization, then up to 3 API calls
    # (fromUrl-check on old, fromUrl-check on new, incoming-list on old).
    # Returns the processed `item` plus a small stats dict so the caller
    # can fold per-row counter updates after each result lands.
    def _process_one(raw: dict) -> tuple[dict, dict]:
        old = strip_domain(str(raw.get("old", "")))
        new = strip_domain(str(raw.get("new", "")))
        country = (str(raw.get("country") or "").strip() or DEFAULT_COUNTRY).lower()
        if country not in {"nl", "be"}:
            country = DEFAULT_COUNTRY
        try:
            sc = int(str(raw.get("statuscode") or DEFAULT_STATUS_CODE).strip())
        except (ValueError, TypeError):
            sc = DEFAULT_STATUS_CODE
        if sc not in ALLOWED_STATUS_CODES:
            sc = DEFAULT_STATUS_CODE
        label = str(raw.get("label") or "").strip()

        item: dict[str, Any] = {
            "input_old": old,
            "input_new": new,
            "final_new": new,
            "country": country,
            "statusCode": sc,
            "label": label,
            "skip_reason": None,
            "flatten_from": None,
        }
        stats = {"flattened": 0, "skipped_home": 0}

        if not old or not new:
            item["skip_reason"] = "missing URL"
            return item, stats
        if is_homepage(old):
            item["skip_reason"] = "homepage (blocked)"
            stats["skipped_home"] = 1
            return item, stats
        # Multi-value facet URLs (e.g. `kleur~504026+504028`) are no-indexed
        # on Beslist. `+` is the multi-value separator within a facet, but
        # only inside the facet segment (after `/c/`). Before `/c/` (e.g. a
        # search query like `/products/r/test_1+test_2/`) `+` is allowed.
        if _has_multivalue_facet(old) or _has_multivalue_facet(new):
            item["skip_reason"] = "multi-value facet (no-index)"
            return item, stats

        existing = _cached_fromurl(old, country)
        if existing:
            existing_url = existing.get("url") or ""
            item["existing_target"] = existing_url
            if equiv_key(existing_url) == equiv_key(item["final_new"]):
                item["skip_reason"] = "URL already redirected"
                item["already_correct"] = True
            else:
                item["skip_reason"] = "source has existing rule"
                item["existing_id"] = existing.get("id")
            return item, stats

        hit = _cached_fromurl(new, country)
        if hit:
            item["final_new"] = hit["url"]
            item["flatten_from"] = new
            stats["flattened"] = 1

        if equiv_key(item["final_new"]) == equiv_key(old):
            item["skip_reason"] = "self-redirect"
            return item, stats

        incoming = _cached_incoming(old)
        if incoming:
            item["incoming_rules"] = [
                {
                    "id": r.get("id"),
                    "fromUrl": r.get("fromUrl"),
                    "country": r.get("country") or country,
                    "statusCode": r.get("statusCode") or DEFAULT_STATUS_CODE,
                    "self_after_rewire": equiv_key(r.get("fromUrl") or "")
                        == equiv_key(item["final_new"]),
                }
                for r in incoming
            ]

        # Target-conflict detection. The redirect API's url_UNIQUE index
        # is country-aware: POSTing country='nl' to a toUrl that's already
        # the target of an existing country='nl, be' rule fails with a
        # duplicate-key error. Empirical: per-country values are treated
        # as equivalent to 'nl, be' for routing purposes (we confirmed via
        # the API that POST country='nl, be' to such a target succeeds and
        # creates a real new row). So when `final_new` is already a toUrl
        # in the table, auto-upgrade the row's country to match — the
        # user-facing semantics ("redirect X to Y") are preserved, only
        # the API-quirk country field changes.
        incoming_to_new = _cached_incoming(item["final_new"])
        if incoming_to_new:
            # Pick the country of the first existing rule (the API tends
            # to use 'nl, be' uniformly; pick whatever is there).
            existing_country = (incoming_to_new[0].get("country") or "").strip()
            if existing_country and existing_country != item["country"]:
                item["country_upgraded_from"] = item["country"]
                item["country"] = existing_country
        return item, stats

    # Parallel pass. 24 workers + connection-pooled session + URL cache
    # gives ~3× the throughput of the previous 8-worker default at the
    # cost of a few extra concurrent sockets — calls are HTTPS-I/O bound,
    # so threads work well (no GIL contention on syscalls). When the
    # caller has prefetched the redirect index, _cached_fromurl /
    # _cached_incoming become in-memory dict lookups; the worker count
    # then mostly governs how fast Python iterates per-row logic.
    WORKERS = 24
    processed: list[dict | None] = [None] * len(rows)
    flattened = 0
    skipped_home = 0
    completed = 0
    counter_lock = threading.Lock()

    def _bump_task():
        if task is None:
            return
        with _PREVIEW_LOCK:
            task["processed"] = completed
            task["flattened"] = flattened
            task["skipped"] = sum(1 for p in processed
                                  if p is not None and p.get("skip_reason"))

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        future_to_idx = {
            pool.submit(_process_one, raw): i for i, raw in enumerate(rows)
        }
        for fut in as_completed(future_to_idx):
            idx = future_to_idx[fut]
            try:
                item, stats = fut.result()
            except Exception as exc:
                logger.exception("preflight row %s failed", idx)
                item = {
                    "input_old": str(rows[idx].get("old", "")),
                    "input_new": str(rows[idx].get("new", "")),
                    "final_new": str(rows[idx].get("new", "")),
                    "country": DEFAULT_COUNTRY,
                    "statusCode": DEFAULT_STATUS_CODE,
                    "label": str(rows[idx].get("label") or ""),
                    "skip_reason": f"preflight error: {exc}",
                    "flatten_from": None,
                }
                stats = {"flattened": 0, "skipped_home": 0}
            with counter_lock:
                processed[idx] = item
                flattened += stats["flattened"]
                skipped_home += stats["skipped_home"]
                completed += 1
            _bump_task()

    # All slots are now populated (init was [None]*n; every index gets
    # assigned in the loop above). Narrow the type for downstream code.
    processed = [p for p in processed if p is not None]

    # Intra-batch dup check. The redirect DB's url_UNIQUE index is on the
    # fromUrl column — many rules can share a toUrl but only one rule per
    # fromUrl is allowed. So within a single submit batch, two rows that
    # share a fromUrl will see the SECOND one fail with a url_UNIQUE
    # SQLSTATE 23000. Catch that here and tag the duplicates so they
    # never reach the API. We pick the first occurrence as the "winner"
    # (kept submittable) and mark every later occurrence as skipped.
    seen_from: dict[str, int] = {}  # equiv_key(fromUrl) -> first processed-index
    for idx, p in enumerate(processed):
        if p.get("skip_reason"):
            continue
        k = equiv_key(p.get("input_old") or "")
        if not k:
            continue
        if k in seen_from:
            first_idx = seen_from[k]
            p["skip_reason"] = "duplicate in batch"
            p["duplicate_of_index"] = first_idx
        else:
            seen_from[k] = idx

    # Cross-batch check: a toUrl that equals another submittable row's
    # fromUrl would form a chain *inside* the batch — and after the
    # first row is POSTed, that toUrl-now-a-fromUrl will block any
    # subsequent insert that uses it as a fromUrl. Surface those too.
    submittable_froms = {equiv_key(p.get("input_old") or ""): idx
                         for idx, p in enumerate(processed)
                         if not p.get("skip_reason")}
    for idx, p in enumerate(processed):
        if p.get("skip_reason"):
            continue
        k = equiv_key(p.get("final_new") or "")
        if k and k in submittable_froms and submittable_froms[k] != idx:
            p["skip_reason"] = "target = batch source"
            p["chains_into_index"] = submittable_froms[k]

    return {
        "processed": processed,
        "stats": {
            "total": len(rows),
            "flattened": flattened,
            "skipped_home": skipped_home,
            "submittable": sum(1 for p in processed if not p["skip_reason"]),
        },
    }


# ---------------------------------------------------------------------------
# Async preflight (drives the Upload progress bar)
# ---------------------------------------------------------------------------

def _prune_preview_tasks() -> None:
    if len(_PREVIEW_TASKS) <= _PREVIEW_TASKS_MAX:
        return
    completed = [
        (tid, t) for tid, t in _PREVIEW_TASKS.items()
        if t.get("status") in ("completed", "failed")
    ]
    completed.sort(key=lambda kv: kv[1].get("finished_at") or "")
    for tid, _ in completed[: len(_PREVIEW_TASKS) - _PREVIEW_TASKS_MAX]:
        _PREVIEW_TASKS.pop(tid, None)


def build_redirect_index(task: dict | None = None) -> tuple[dict, dict]:
    """Paginate the full /api/redirects table into two in-memory indices:

    * `fromurl_index`: equiv_key(fromUrl) -> {url=toUrl, statusCode, id, country}
      (shape mirrors what `check_url_is_fromUrl` returns to its callers)
    * `tourl_index`: equiv_key(toUrl) -> [list of full rule dicts]
      (used by the equivalent of `check_url_incoming`)

    Fetches pages in parallel (PREFETCH_WORKERS) with a small look-ahead
    window so we keep the API pipelined without DoSing it. Stops when a
    page comes back short. Total table is ~820k rows so this takes
    roughly 10–20s with PREFETCH_PAGE_SIZE=5000 and 8 workers.

    When `task` is provided, updates `task["prefetch_rows"]` so the
    frontend can show a "Prefetching redirect table…" progress message
    before per-row preflight kicks in.
    """
    fromurl_index: dict[str, dict] = {}
    tourl_index: dict[str, list[dict]] = {}

    def _fetch_page(offset: int) -> list[dict]:
        r = _HTTP.get(
            f"{REDIRECT_API}/api/redirects",
            params={"limit": PREFETCH_PAGE_SIZE, "offset": offset},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        return r.json().get("data", []) or []

    def _ingest(rows: list[dict]) -> None:
        for row in rows:
            from_key = equiv_key(row.get("fromUrl") or "")
            to_key = equiv_key(row.get("toUrl") or "")
            if from_key:
                # Last-write-wins. The API enforces fromUrl uniqueness so
                # duplicate keys should never happen in practice.
                fromurl_index[from_key] = {
                    "url": row.get("toUrl"),
                    "statusCode": row.get("statusCode"),
                    "id": row.get("id"),
                    "country": row.get("country"),
                }
            if to_key:
                tourl_index.setdefault(to_key, []).append({
                    "id": row.get("id"),
                    "fromUrl": row.get("fromUrl"),
                    "toUrl": row.get("toUrl"),
                    "country": row.get("country"),
                    "statusCode": row.get("statusCode"),
                })

    # Window-based parallel fetch. We don't know the total upfront, so
    # fire a window of N pages, ingest them, and stop the moment a page
    # comes back short (the last partial page marks end-of-table). This
    # may over-fetch up to (WORKERS-1) pages past the end, which is fine
    # — those pages return empty arrays quickly.
    offset = 0
    rows_loaded = 0
    while True:
        window_offsets = [offset + i * PREFETCH_PAGE_SIZE
                          for i in range(PREFETCH_WORKERS)]
        with ThreadPoolExecutor(max_workers=PREFETCH_WORKERS) as pool:
            future_to_off = {pool.submit(_fetch_page, off): off
                             for off in window_offsets}
            window_results: dict[int, list[dict]] = {}
            for fut in as_completed(future_to_off):
                off = future_to_off[fut]
                try:
                    window_results[off] = fut.result()
                except Exception as exc:
                    logger.warning("prefetch page offset=%s failed: %s", off, exc)
                    window_results[off] = []
        # Ingest in offset order for deterministic last-write-wins.
        short_page_seen = False
        for off in window_offsets:
            data = window_results.get(off, [])
            _ingest(data)
            rows_loaded += len(data)
            if len(data) < PREFETCH_PAGE_SIZE:
                short_page_seen = True
        if task is not None:
            with _PREVIEW_LOCK:
                task["prefetch_rows"] = rows_loaded
        if short_page_seen:
            break
        offset += PREFETCH_WORKERS * PREFETCH_PAGE_SIZE

    logger.info(
        "redirect-tool prefetch done: %s rules, %s unique fromUrls, %s unique toUrls",
        rows_loaded, len(fromurl_index), len(tourl_index),
    )
    return fromurl_index, tourl_index


def start_preflight(rows: list[dict]) -> str:
    """Kick off preflight in a daemon thread; the frontend polls
    /preview-status/{task_id} every ~500ms to drive a progress bar.

    For batches > PREFETCH_THRESHOLD rows, prefetches the full redirect
    table once up front so per-row lookups stay in-memory. Below the
    threshold, the existing per-row HTTP path is faster (no warmup cost).
    """
    task_id = uuid.uuid4().hex[:12]
    total = len(rows)
    use_prefetch = total > PREFETCH_THRESHOLD
    task: dict[str, Any] = {
        "task_id": task_id,
        "status": "running",
        "phase": "prefetch" if use_prefetch else "preflight",
        "started_at": datetime.utcnow().isoformat(),
        "total": total,
        "processed": 0,
        "flattened": 0,
        "skipped": 0,
        "prefetch_rows": 0,
    }
    with _PREVIEW_LOCK:
        _PREVIEW_TASKS[task_id] = task
        _prune_preview_tasks()

    def _runner():
        try:
            fromurl_index = tourl_index = None
            if use_prefetch:
                fromurl_index, tourl_index = build_redirect_index(task=task)
                with _PREVIEW_LOCK:
                    task["phase"] = "preflight"
            result = preflight_rows(
                rows, task=task,
                fromurl_index=fromurl_index,
                tourl_index=tourl_index,
            )
            with _PREVIEW_LOCK:
                task["status"] = "completed"
                task["finished_at"] = datetime.utcnow().isoformat()
                task["result"] = result
        except Exception as exc:
            logger.exception("redirect-tool preflight task %s failed", task_id)
            with _PREVIEW_LOCK:
                task["status"] = "failed"
                task["finished_at"] = datetime.utcnow().isoformat()
                task["error"] = str(exc)

    threading.Thread(target=_runner, daemon=True).start()
    return task_id


def get_preflight_status(task_id: str) -> dict | None:
    with _PREVIEW_LOCK:
        task = _PREVIEW_TASKS.get(task_id)
        return dict(task) if task else None


def _raw_error_text(body: Any) -> str:
    """Best-effort extract a readable message from a failing API body.
    The redirect API sometimes returns a dict, sometimes a [dict] array."""
    if isinstance(body, dict):
        return str(body.get("message") or body.get("error") or body)[:500]
    if isinstance(body, list) and body and isinstance(body[0], dict):
        first = body[0]
        return str(first.get("message") or first.get("error") or first)[:500]
    return str(body)[:500]


# Pattern: ``Duplicate entry '<url>' for key 'url_redirect.url_UNIQUE'``.
# The captured URL is whatever value tripped the unique index — typically
# the value of `fromUrl` we just tried to insert (rule already exists),
# but the same index also fires when our `toUrl` happens to match an
# existing row's `url` column (rare).
_DUP_URL_RE = re.compile(
    r"Duplicate entry '([^']+)' for key 'url_redirect\.url_UNIQUE'"
)


def explain_submit_failure(item: dict, body: Any) -> dict:
    """Add a human-readable explanation + the existing rule's target (when
    we can fetch it) to a failed submit row. Always also keeps the raw
    message so power users can still see the SQLSTATE."""
    raw = _raw_error_text(body)
    out: dict[str, Any] = {"raw_message": raw, "friendly_message": None,
                          "existing_target": None}

    m = _DUP_URL_RE.search(raw)
    if not m:
        # Unknown failure mode — just surface the raw message.
        return out
    offending_raw = m.group(1)
    # MySQL truncates the duplicate-key value at ~64 chars in the error
    # message, so `offending_raw` may be a prefix of the real URL. If we
    # can match it as a prefix of input_old or final_new, recover the
    # full URL for display.
    offending_key = equiv_key(offending_raw)
    old_full = item.get("input_old") or ""
    new_full = item.get("final_new") or ""
    candidates = []
    if equiv_key(old_full).startswith(offending_key):
        candidates.append(old_full)
    if equiv_key(new_full).startswith(offending_key) and new_full not in candidates:
        candidates.append(new_full)
    if len(candidates) == 1:
        offending = candidates[0]
    elif len(candidates) >= 2:
        # Both URLs share the truncated prefix — MySQL can't tell which
        # one actually collided. Show both so the user can investigate.
        offending = " OR ".join(candidates)
    else:
        offending = offending_raw

    # Resolve the existing rule (if any) so the user can see what their old
    # URL currently maps to. Best-effort; we ignore failures here.
    existing_target = None
    try:
        hit = check_url_is_fromUrl(offending, item.get("country") or DEFAULT_COUNTRY)
        if hit:
            existing_target = hit.get("url")
    except Exception as exc:
        logger.debug("explain_submit_failure: lookup failed: %s", exc)

    out["existing_target"] = existing_target

    if equiv_key(offending) == equiv_key(item.get("input_old") or ""):
        # Keep the message a short tag; the frontend appends the target
        # separately from `existing_target` so the Run detail Note column
        # renders this row the same as preflight-skipped rows with the
        # same condition.
        msg = "source has existing rule"
    elif equiv_key(offending) == equiv_key(item.get("final_new") or ""):
        msg = f"duplicate URL: {offending}"
    else:
        msg = f"duplicate URL: {offending}"
    out["friendly_message"] = msg
    return out


def submit_rows(processed: list[dict], task: dict | None = None,
               replace_existing: bool = False) -> dict:
    """POST one row at a time so we get per-row pass/fail.

    When `task` is provided, update its counters after every row so the
    /submit-status/{id} endpoint can drive a progress bar. Counters live in
    {processed, success, failed, skipped}; total is set by the caller.

    When `replace_existing` is True, rows whose old URL already has a
    redirect rule (preflight tagged them with `existing_id`) are NOT
    skipped — instead the submitter DELETEs the existing rule first, then
    POSTs the new one. This is the explicit "overwrite" flow.
    """
    success = 0
    failed = 0
    skipped = 0
    per_row: list[dict] = []

    for item in processed:
        # Override the preflight skip for replaceable rows when the
        # caller opted in. The original skip_reason is preserved as
        # `replaced_skip_reason` so the audit trail shows why we acted.
        is_replaceable = (
            replace_existing
            and item.get("existing_id")
            and item.get("skip_reason") == "source has existing rule"
        )
        if item.get("skip_reason") and not is_replaceable:
            skipped += 1
            per_row.append({**item, "status": "skipped", "api_response": None})
        else:
            replaced_target = None
            replace_error: dict | None = None
            if is_replaceable:
                # The redirect API deletes by fromUrl (per Swagger spec);
                # input_old IS the fromUrl of the existing rule we want
                # to remove.
                try:
                    del_code, del_body = delete_redirect_by_fromurl(item["input_old"])
                except Exception as exc:
                    replace_error = {"error": f"DELETE existing rule failed: {exc}"}
                else:
                    # Treat 2xx and 404 as "rule no longer in the way"
                    # (404 = someone else already deleted it).
                    if 200 <= del_code < 300 or del_code == 404:
                        replaced_target = item.get("existing_target")
                    else:
                        replace_error = {
                            "error": f"DELETE returned {del_code}",
                            "delete_body": del_body,
                        }

            # Incoming-rewire pass: for each rule whose toUrl currently
            # equals our `old`, repoint its toUrl to our `new` so we don't
            # leave a chain and so the DB's url_UNIQUE index lets us insert
            # the new rule. DELETE + re-POST preserves fromUrl/country/
            # statusCode; the re-POST is skipped when it would self-redirect
            # (fromUrl == final_new), the DELETE is enough in that case.
            rewire_errors: list[dict] = []
            rewired_count = 0
            if replace_error is None and item.get("incoming_rules"):
                for inc in item["incoming_rules"]:
                    inc_from = inc.get("fromUrl")
                    rid = inc.get("id")
                    if not inc_from:
                        continue
                    try:
                        del_code, del_body = delete_redirect_by_fromurl(inc_from)
                    except Exception as exc:
                        rewire_errors.append({
                            "incoming_id": rid,
                            "incoming_from": inc_from,
                            "error": f"DELETE failed: {exc}",
                        })
                        continue
                    if not (200 <= del_code < 300 or del_code == 404):
                        rewire_errors.append({
                            "incoming_id": rid,
                            "error": f"DELETE returned {del_code}",
                            "body": del_body,
                        })
                        continue
                    if inc.get("self_after_rewire"):
                        # fromUrl == new — no point re-posting; the DELETE
                        # is the right action (the chain collapses to nothing).
                        rewired_count += 1
                        continue
                    try:
                        rew_code, rew_body = post_redirect(
                            inc["fromUrl"], item["final_new"],
                            inc.get("country") or item.get("country"),
                            inc.get("statusCode") or DEFAULT_STATUS_CODE,
                        )
                    except Exception as exc:
                        rewire_errors.append({
                            "incoming_id": rid,
                            "incoming_from": inc.get("fromUrl"),
                            "error": f"re-POST failed: {exc}",
                        })
                        continue
                    if 200 <= rew_code < 300:
                        rewired_count += 1
                    else:
                        rewire_errors.append({
                            "incoming_id": rid,
                            "incoming_from": inc.get("fromUrl"),
                            "error": f"re-POST returned {rew_code}",
                            "body": rew_body,
                        })

            if replace_error is not None:
                failed += 1
                per_row.append({
                    **item, "status": "fail", "api_response": replace_error,
                    "raw_message": _raw_error_text(replace_error),
                    "friendly_message": "replace failed: DELETE error",
                })
            elif rewire_errors and rewired_count == 0:
                # Couldn't rewire any incoming rule — bail without posting
                # the main rule (it would still fail on url_UNIQUE).
                failed += 1
                err_body = {"error": "incoming-rewire failed",
                            "details": rewire_errors[:5]}
                per_row.append({
                    **item, "status": "fail", "api_response": err_body,
                    "raw_message": _raw_error_text(err_body),
                    "friendly_message": (
                        f"rewire failed ({len(item.get('incoming_rules') or [])} "
                        "incoming rules)"
                    ),
                })
            else:
                try:
                    code, body = post_redirect(
                        item["input_old"], item["final_new"],
                        item["country"], item["statusCode"],
                    )
                except Exception as exc:
                    failed += 1
                    body = {"error": str(exc)}
                    per_row.append({
                        **item, "status": "fail", "api_response": body,
                        **explain_submit_failure(item, body),
                    })
                else:
                    if 200 <= code < 300:
                        success += 1
                        out_row = {**item, "status": "ok", "api_response": body}
                        if replaced_target:
                            out_row["replaced_target"] = replaced_target
                            out_row["skip_reason"] = None  # was overridden
                        if rewired_count:
                            out_row["rewired_count"] = rewired_count
                        if rewire_errors:
                            out_row["rewire_errors"] = rewire_errors
                        per_row.append(out_row)
                    else:
                        failed += 1
                        per_row.append({
                            **item, "status": "fail", "api_response": body,
                            "rewired_count": rewired_count,
                            "rewire_errors": rewire_errors,
                            **explain_submit_failure(item, body),
                        })

        if task is not None:
            with _SUBMIT_LOCK:
                task["processed"] = len(per_row)
                task["success"] = success
                task["failed"] = failed
                task["skipped"] = skipped

    return {"success": success, "failed": failed, "per_row": per_row}


# ---------------------------------------------------------------------------
# Async submit (drives the frontend progress bar)
# ---------------------------------------------------------------------------

def _prune_submit_tasks() -> None:
    """Drop the oldest completed tasks if we exceed the cap."""
    if len(_SUBMIT_TASKS) <= _SUBMIT_TASKS_MAX:
        return
    completed = [
        (tid, t) for tid, t in _SUBMIT_TASKS.items()
        if t.get("status") in ("completed", "failed")
    ]
    completed.sort(key=lambda kv: kv[1].get("finished_at") or "")
    for tid, _ in completed[: len(_SUBMIT_TASKS) - _SUBMIT_TASKS_MAX]:
        _SUBMIT_TASKS.pop(tid, None)


def start_submit(processed: list[dict], label: str, input_method: str,
                replace_existing: bool = False) -> str:
    """Kick off submission in a daemon thread; return the task_id immediately.

    Counters update after every row so the frontend's poll loop can drive
    a real progress bar (matches the FAQ pattern). On completion the final
    result + run_id land on the task dict for the closing poll to pick up.
    """
    task_id = uuid.uuid4().hex[:12]
    total = len(processed)
    # When replace_existing is on, the "skipped because old already has a
    # rule" rows count as submittable for stats too — they'll be DELETE+POST'd.
    def _will_submit(p: dict) -> bool:
        if not p.get("skip_reason"):
            return True
        return bool(
            replace_existing
            and p.get("existing_id")
            and p.get("skip_reason") == "source has existing rule"
        )
    preflight = {
        "stats": {
            "total": total,
            "flattened": sum(1 for p in processed if p.get("flatten_from")),
            "skipped_home": sum(
                1 for p in processed
                if p.get("skip_reason") == "homepage (blocked)"
            ),
            "submittable": sum(1 for p in processed if _will_submit(p)),
        }
    }

    task: dict[str, Any] = {
        "task_id": task_id,
        "status": "running",
        "started_at": datetime.utcnow().isoformat(),
        "total": total,
        "processed": 0,
        "success": 0,
        "failed": 0,
        "skipped": 0,
        "stats": preflight["stats"],
    }
    with _SUBMIT_LOCK:
        _SUBMIT_TASKS[task_id] = task
        _prune_submit_tasks()

    def _runner():
        try:
            result = submit_rows(processed, task=task, replace_existing=replace_existing)
            run_id = save_run(label, input_method, preflight, result)
            with _SUBMIT_LOCK:
                task["status"] = "completed"
                task["finished_at"] = datetime.utcnow().isoformat()
                task["result"] = {
                    "run_id": run_id,
                    "success": result["success"],
                    "failed": result["failed"],
                    "stats": preflight["stats"],
                }
        except Exception as exc:
            logger.exception("redirect-tool submit task %s failed", task_id)
            with _SUBMIT_LOCK:
                task["status"] = "failed"
                task["finished_at"] = datetime.utcnow().isoformat()
                task["error"] = str(exc)

    threading.Thread(target=_runner, daemon=True).start()
    return task_id


def get_submit_status(task_id: str) -> dict | None:
    with _SUBMIT_LOCK:
        task = _SUBMIT_TASKS.get(task_id)
        return dict(task) if task else None


# ---------------------------------------------------------------------------
# Run persistence
# ---------------------------------------------------------------------------

def save_run(label: str, input_method: str, preflight: dict, result: dict) -> int:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO redirect_tool_runs
               (label, input_method, total_rows, flattened, skipped_home, success, failed, results)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               RETURNING id""",
            (
                label or None,
                input_method,
                preflight["stats"]["total"],
                preflight["stats"]["flattened"],
                preflight["stats"]["skipped_home"],
                result["success"],
                result["failed"],
                json.dumps(result["per_row"]),
            ),
        )
        new_id = cur.fetchone()["id"]
        conn.commit()
        return new_id
    finally:
        return_db_connection(conn)


def list_runs(limit: int = 100) -> list[dict]:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, created_at, label, input_method, total_rows, flattened,
                      skipped_home, success, failed
               FROM redirect_tool_runs ORDER BY created_at DESC LIMIT %s""",
            (limit,),
        )
        rows = cur.fetchall()
        for r in rows:
            r["created_at"] = r["created_at"].isoformat()
        return rows
    finally:
        return_db_connection(conn)


def get_run(run_id: int) -> dict | None:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, created_at, label, input_method, total_rows, flattened,
                      skipped_home, success, failed, results
               FROM redirect_tool_runs WHERE id = %s""",
            (run_id,),
        )
        row = cur.fetchone()
        if row:
            row["created_at"] = row["created_at"].isoformat()
        return row
    finally:
        return_db_connection(conn)


def delete_run(run_id: int) -> bool:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM redirect_tool_runs WHERE id = %s", (run_id,))
        deleted = cur.rowcount
        conn.commit()
        return deleted > 0
    finally:
        return_db_connection(conn)
