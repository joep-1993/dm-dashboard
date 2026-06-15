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
# Coverage over a tiny base is statistically meaningless: when the dom_cat has
# only 1-2 AND-matches for the keyword (common when the base call OR-fell-back
# and dom_cat_count is tiny), any facet on those products scores ~100% by luck
# (e.g. "ivermectine" → Wormenkuur dom_cat_count=1 → dier~Paarden, 1/1=100%).
# Require this many base products before a COVERAGE win is trusted. Keyword
# matches bypass it — the user explicitly named the value, so 1 product is fine.
MIN_COVERAGE_BASE_TOTAL = 5
# V31: raised from 15 to 50. The candidates list is sorted by raw subcat-wide
# count desc, but niche-specific facet values often sit deep in the tail
# (e.g. "Zonder overtrek" ranks ~#32 of 190 in huis_tuin_505062_505149).
# Early-stop below caps the cost when an obvious winner shows up early.
MAX_CANDIDATES_PER_PAIR = 50
EARLY_STOP_COVERAGE = 0.9       # stop probing once a value covers ≥ this

# Bump when the probe SELECTION logic changes so cached picks from older
# logic are ignored and re-derived. v2: keyword↔value-name match priority,
# generic-attribute coverage suppression, and the Stage 1.5 live subcat
# keyword probe. v3: Stage-1 coverage-winner now requires cov <= 1.0 (rejects
# OR-fallback / maincat-vs-subcat scope-mismatch inflation like the 212%
# doelgroep pick), and demographic facets (doelgroep_*/leeftijd_*/geslacht_*)
# are treated as generic attributes. v4: generic-attribute detection is now
# prefix/suffix based, so category-qualified attribute slugs (kleurtint,
# kleur_*, materiaal_*, maat_*, *kleur) are suppressed too — not just the bare
# names. Without this, stale picks (e.g. kleurtint 'Koper' for "pellets")
# would linger in the cache. v5: stijl_*/dier_* added to the generic-attribute
# families, and coverage wins now require base_total >= MIN_COVERAGE_BASE_TOTAL
# (kills 1/1 flukes like "ivermectine" → dier~Paarden). v6: search_derived
# SCHEMA_VERSION bumped to 4 (greedy dom_cat) — base_total/dom_slug change, so
# re-derive probes against the new dom_cats rather than reusing stale picks.
PROBE_SCHEMA_VERSION = 6

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

# Generic ATTRIBUTE facets carry little navigational intent when the user
# didn't actually search for the value. A material/colour/size/weight value
# that wins purely on coverage is usually noise (e.g. "fontein wc" →
# materiaal~Keramiek, or "pellets" → kleurtint~Koper). Such facets are
# appended ONLY when they're a keyword match (the kw_best branch); as a pure
# coverage winner they're skipped so the redirect stays at the bare dom_cat.
# Deliberately EXCLUDES type_* / eigenschap_* / o_* facets — those carry intent
# even via coverage (e.g. "hoesloze dekbedden" → eigenschap_beddengoed "Zonder
# overtrek").
#
# Matched by PREFIX, because each family has many category-qualified slugs
# (kleur, kleurtint, kleur_glazen_zb, materiaal_sieraad, maat_mode_bovenkleding,
# formaat_tv, …) — enumerating bare names alone (the old behaviour) let every
# qualified variant slip through and win on coverage. Demographic/audience
# facets (doelgroep_*, leeftijd_*, geslacht_*, dier_* = target-animal) are the
# same class — nearly every product carries one (e.g. "humor" →
# doelgroep_feestkleding 'Volwassenen' 212%; "mini gps-tracker" →
# dier_dierenbenodigdheden 'Honden' 92%) — as is decor style (stijl_*, e.g.
# "vogelgeluiden" → stijl_woonaccessoires 'Modern' 86%). All folded in here.
_GENERIC_ATTRIBUTE_PREFIXES = (
    "kleur", "materiaal", "maat", "gewicht", "formaat", "stijl",
    "doelgroep", "leeftijd", "geschikte_leeftijd", "geslacht", "dier",
)
# A few colour slugs carry 'kleur' as a suffix rather than a prefix
# (goudkleur, haarkleur, subkleur).
_GENERIC_ATTRIBUTE_SUFFIXES = ("kleur",)


def _is_generic_attribute_facet(facet_name: str) -> bool:
    n = (facet_name or "").lower()
    return (n.startswith(_GENERIC_ATTRIBUTE_PREFIXES)
            or n.endswith(_GENERIC_ATTRIBUTE_SUFFIXES))


# ── Keyword ↔ facet-value-name matching ──────────────────────────────────
# When the search query literally names a facet value (e.g. query
# "ketoconazol shampoo" → value "Ketoconazol"), that value should win
# regardless of product coverage — the user explicitly asked for it. The
# old behaviour ranked purely by coverage, so a lexically-unrelated but
# higher-coverage value ("Anti-roos", 4/4) beat the exact keyword match
# ("Ketoconazol", 2/4, below the 0.6 floor) and the match was discarded.
import re as _re

_TOKEN_RE = _re.compile(r"[a-z0-9]+")


def _stem(tok: str) -> str:
    """Light Dutch-plural stem: drop a trailing 's' then a trailing 'e'
    (e.g. 'shampoos' → 'shampoo', 'kleuren' stays). Mirrors the leftover-
    token stemming used by the V31 consumer in main_parallel_v2."""
    tok = tok.lower()
    if len(tok) > 3 and tok.endswith("s"):
        tok = tok[:-1]
    if len(tok) > 3 and tok.endswith("e"):
        tok = tok[:-1]
    return tok


def _tokens(text: str) -> set:
    return {_stem(t) for t in _TOKEN_RE.findall((text or "").lower())}


def _value_matches_keyword(keyword: str, value_name: str) -> bool:
    """True when every (stemmed) token of the facet value name is present in
    the (stemmed) keyword tokens — i.e. the query explicitly mentions this
    value. 'Ketoconazol' ⊆ {'ketoconazol','shampoo'} → True;
    'Anti roos' ⊄ {'ketoconazol','shampoo'} → False."""
    vtoks = _tokens(value_name)
    if not vtoks:
        return False
    return vtoks <= _tokens(keyword)


def _tok_links(vtok: str, ktok: str) -> bool:
    """Looser token link than equality: a value token "links" to a keyword
    token if they're equal OR one is a >=4-char prefix of the other. This
    bridges the cases pure set-membership misses, e.g. value 'Thuis'
    (ut_voetbalshirt) ↔ query token 'thuisshirt', or 'EK' ↔ 'ek'. The
    >=4 floor keeps short noise ('L', 'M') from prefix-matching."""
    if vtok == ktok:
        return True
    if len(vtok) >= 4 and ktok.startswith(vtok):
        return True
    if len(ktok) >= 4 and vtok.startswith(ktok):
        return True
    return False


def _value_consistent_with_keyword(keyword: str, value_name: str) -> bool:
    """Like _value_matches_keyword but uses _tok_links instead of strict set
    membership, so 'Thuis' ⊂ 'thuisshirt' and '122/128' ↔ '122-128' count.
    Every token of the value name must link to SOME keyword token — partial
    junk ('Nederlands Duitsland') is still rejected because 'duitsland'
    links to nothing in the query."""
    vtoks = _tokens(value_name)
    if not vtoks:
        return False
    ktoks = _tokens(keyword)
    return all(any(_tok_links(vt, kt) for kt in ktoks) for vt in vtoks)


# Size/quantity facets are excluded from multi-facet assembly: per-value
# pages (maat=XL, gewicht=…) churn in and out of stock far faster than the
# product-type / fanshop / colour axes, so pinning a redirect to one size is
# brittle. Matched as PREFIXES because the real slugs are category-qualified
# (e.g. 'maat_mode_bovenkleding', 'maat_schoenen'). Colour (kleur) is
# deliberately NOT here — it's stable enough and the user often searches it
# explicitly (e.g. "oranje").
_MULTI_FACET_SIZE_PREFIXES = ("maat", "gewicht", "formaat", "lengte", "inhoud",
                              "schoenmaat", "sz_")


def _is_size_facet(facet_name: str) -> bool:
    return (facet_name or "").lower().startswith(_MULTI_FACET_SIZE_PREFIXES)

# Intent-bearing facet prefixes/names sort to the FRONT of an assembled /c/
# URL; attribute facets (kleur, materiaal, …) trail. Order only affects the
# URL's readability/canonical shape, not whether it resolves.
_MULTI_FACET_INTENT_PREFIXES = ("type_", "ut_", "o_", "eigenschap_", "soort_")
_MULTI_FACET_INTENT_NAMES = {"fanshop", "merk"}


def _extract_multi_facets(api_facets: Optional[list], keyword: str) -> list[dict]:
    """From a subcat-level search response's facets[], pick the single
    top-count keyword-consistent value of each intent-bearing facet.

    Returns an ordered list of {facet_name, value_id, value_name, count}.
    Used by the V33 multi-facet rescue: a query that spans several axes
    (fanshop + merk + product-type + colour) can't be represented by the
    single appended facet the V29 probe picks, so we assemble one value per
    axis the query actually names.
    """
    ktoks = _tokens(keyword)
    picks: list[dict] = []
    for f in (api_facets or []):
        fname = (f.get("urlName") or "").lower()
        if (not fname or fname == "winkel"
                or fname in FACET_BLACKLIST
                or _is_size_facet(fname)):
            continue
        # Among keyword-consistent values of THIS facet, prefer the one that
        # covers the most query intent — by tokens covered, then by the
        # longest token covered, then by product count. Picking by raw count
        # alone mis-selects when several values match: for "nederlands elftal
        # ... ek '88", fanshop value 'EK' has more products than 'Nederlands
        # Elftal', but only 'Nederlands Elftal' represents the long token
        # 'nederlands' that the reject guard cares about.
        best = None       # (n_covered, max_len_covered, count)
        best_meta = None  # (value_id, value_name, count)
        for v in (f.get("values") or []):
            vid = v.get("id")
            vname = v.get("facetValue") or ""
            cnt = int(v.get("count") or 0)
            if vid is None or cnt <= 0:
                continue
            if not _value_consistent_with_keyword(keyword, vname):
                continue
            covered = {kt for kt in ktoks
                       if any(_tok_links(vt, kt) for vt in _tokens(vname))}
            rank = (len(covered), max((len(t) for t in covered), default=0), cnt)
            if best is None or rank > best:
                best = rank
                best_meta = (int(vid), vname, cnt)
        if best_meta:
            vid, vname, cnt = best_meta
            picks.append({"facet_name": fname, "value_id": vid,
                          "value_name": vname, "count": cnt})

    def _prio(d: dict) -> int:
        n = d["facet_name"]
        if n in _MULTI_FACET_INTENT_NAMES or n.startswith(_MULTI_FACET_INTENT_PREFIXES):
            return 0
        return 1

    picks.sort(key=lambda d: (_prio(d), -d["count"]))
    return picks


def _extract_size_facet(api_facets: Optional[list], keyword: str) -> Optional[dict]:
    """V34: when the query names a size (XL, 122-128, …), resolve it against
    the dom_cat's maat_* facet values from the search response. Returns
    {facet_name, value_id, value_name} or None. Kept SEPARATE from
    _extract_multi_facets so size is opt-in at assembly time — per-size pages
    churn in/out of stock, so we collect the match but only append it when
    the caller explicitly wants size honoured."""
    from src.size_tokens import extract_sizes, match_size_value
    sizes = extract_sizes(keyword)
    if not sizes:
        return None
    for f in (api_facets or []):
        fname = (f.get("urlName") or "").lower()
        if not _is_size_facet(fname):
            continue
        values = [(v.get("id"), v.get("facetValue") or "")
                  for v in (f.get("values") or []) if int(v.get("count") or 0) > 0]
        hit = match_size_value(sizes, values)
        if hit:
            vid, vname = hit
            return {"facet_name": fname, "value_id": int(vid), "value_name": vname}
    return None


def _fetch_subcat_facets(dom_slug: str, keyword: str,
                         bucket: "_TokenBucket") -> Optional[list]:
    """One throttled subcat-level query → the response's facets[] list (or
    None on any error). Shared by the multi-facet and size extractors so a
    rescued pair costs a single extra call."""
    if not dom_slug or not keyword:
        return None
    bucket.acquire()
    try:
        params = {
            "category": dom_slug, "query": keyword,
            "countryLanguage": COUNTRY_LANG, "isBot": "true", "limit": "1",
        }
        url = f"{SEARCH_BASE_URL}/search/products?{urllib.parse.urlencode(params)}"
        r = requests.get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        return (r.json() or {}).get("facets")
    except Exception as e:
        logger.debug(f"subcat facet fetch failed ({dom_slug}, {keyword!r}): {e}")
        return None


def _derive_multi_facets(dom_slug: str, keyword: str,
                         bucket: "_TokenBucket") -> list[dict]:
    """Convenience wrapper: fetch + _extract_multi_facets. Returns [] on any
    error (the multi-facet rescue is strictly additive — a failure just
    leaves the existing single-facet / reject behaviour in place)."""
    return _extract_multi_facets(_fetch_subcat_facets(dom_slug, keyword, bucket), keyword)


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
        payload = json.loads(row[0])
        # Ignore picks cached under older selection logic so they re-derive.
        if payload.get("probe_schema") != PROBE_SCHEMA_VERSION:
            return None
        return payload
    except sqlite3.OperationalError:
        return None
    finally:
        c.close()


def _probe_put(mn: str, kn: str, payload: dict) -> None:
    payload = {**payload, "probe_schema": PROBE_SCHEMA_VERSION}
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
        cov = (c / base_total) if base_total else 0.0
        # V31: when the AND-match for (keyword × filter) is small, the search
        # API switches to OR-fallback and returns an inflated `total` (often
        # millions). Coverage > 1.0 is impossible in a real AND-restricted
        # subset, so treat that as the OR-fallback signal and reject the
        # candidate. Without this, non-covering facets like materiaal=Katoen
        # come back with bogus coverage like 1345.3 and beat the real winner.
        if cov > 1.0:
            return None
        return cov
    except Exception as e:
        logger.debug(f"probe failed for {facet_name}={value_id}: {e}")
        return None


def _facet_id_to_name() -> dict:
    """Build facet_id → facet_name slug map from cached facets.csv."""
    fdf = _facets_df()
    return dict(zip(fdf["facet_id"].astype(int), fdf["facet_name"]))


def _check_surfaced(v28_payload: dict, base_total: int,
                    id_to_name: dict, keyword: str = "") -> Optional[tuple]:
    """V29 step 1: see if the V28 base call already surfaced a usable facet
    value, without any probe API calls. Returns (cov, count, name, vid,
    vname, is_keyword_match) or None.

    Two priorities:
      1. Keyword match — a surfaced value whose NAME the query literally
         mentions (e.g. 'ketoconazol shampoo' → 'Ketoconazol') wins outright,
         regardless of coverage, as long as it has ≥1 product (so the target
         page isn't empty). Among several keyword matches, the highest
         coverage/count wins.
      2. Coverage — otherwise the highest-coverage value that clears
         MIN_FACET_COVERAGE, as before.
    """
    surfaced = v28_payload.get("surfaced_facets") or []
    kw_best = None     # keyword-name match (any coverage > 0)
    cov_best = None    # coverage winner (≥ MIN_FACET_COVERAGE)
    for f in surfaced:
        fid = f.get("facet_id")
        facet_name = id_to_name.get(fid)
        if not facet_name or facet_name.lower() in FACET_BLACKLIST:
            continue
        for vid, vname, count in (f.get("values") or []):
            if count is None or count <= 0:
                continue
            cov = count / base_total if base_total else 0
            if keyword and _value_matches_keyword(keyword, vname or ""):
                cand = (round(cov, 3), int(count), facet_name, int(vid), vname or "", True)
                if kw_best is None or cand > kw_best:
                    kw_best = cand
            # Coverage-winner branch. Require a real, in-scope fraction: a
            # surfaced count > base_total means either the base call OR-fell-
            # back, or the count is maincat-wide while base_total is only the
            # dominant subcat (dom_cat_count) — both inflate coverage past
            # 100% (e.g. 1485/700 = 212%). Stage 2's _probe_one rejects the
            # same signal via `cov > 1.0`; mirror it here so the bogus ratio
            # can't win. Also require a non-trivial base (MIN_COVERAGE_BASE_TOTAL)
            # so a 1/1 fluke can't score 100%. A literal keyword match (kw_best,
            # above) is unaffected by both guards.
            if (MIN_FACET_COVERAGE <= cov <= 1.0
                    and base_total >= MIN_COVERAGE_BASE_TOTAL
                    and not _is_generic_attribute_facet(facet_name)):
                cand = (round(cov, 3), int(count), facet_name, int(vid), vname or "", False)
                if cov_best is None or cand > cov_best:
                    cov_best = cand
    return kw_best or cov_best


def _subcat_keyword_facet(dom_slug: str, keyword: str, bucket: _TokenBucket) -> Optional[tuple]:
    """Live subcat-level facet lookup for a keyword match.

    The maincat-level V28 query frequently OR-fallbacks (total in the
    millions) and surfaces only merk/winkel, hiding niche facet values that
    ARE present at the subcategory level — e.g. ingr_shamp 'Ketoconazol'
    (added 2026-04-20, also missing from the facets.csv snapshot). A single
    subcat-level query surfaces them. Returns (facet_name, value_id,
    value_name, count) for the best keyword-matching surfaced value, or None.
    One throttled API call.
    """
    bucket.acquire()
    try:
        params = {
            "category": dom_slug, "query": keyword,
            "countryLanguage": COUNTRY_LANG, "isBot": "true", "limit": "1",
        }
        url = f"{SEARCH_BASE_URL}/search/products?{urllib.parse.urlencode(params)}"
        r = requests.get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception as e:
        logger.debug(f"subcat keyword probe failed ({dom_slug}, {keyword!r}): {e}")
        return None
    best = None  # (count, facet_name, value_id, value_name)
    for f in (data.get("facets") or []):
        fname = (f.get("urlName") or "").lower()
        if not fname or fname == "winkel" or fname in FACET_BLACKLIST:
            continue
        for v in (f.get("values") or []):
            vid = v.get("id")
            vname = v.get("facetValue") or ""
            cnt = int(v.get("count") or 0)
            if vid is None or cnt <= 0:
                continue
            if _value_matches_keyword(keyword, vname):
                cand = (cnt, fname, int(vid), vname)
                if best is None or cand > best:
                    best = cand
    if best:
        cnt, fname, vid, vname = best
        return fname, vid, vname, cnt
    return None


def _do_probe(maincat: str, keyword: str, v28_payload: dict,
              bucket: _TokenBucket) -> dict:
    """Single-facet probe (see _do_probe_inner) plus the V33 multi_facets
    list. The multi-facet assembly is cached alongside the single pick so
    the cache-only worker can fall back to it when its single appended facet
    would be hard-rejected for dropping a long product token."""
    res = _do_probe_inner(maincat, keyword, v28_payload, bucket)
    dom_slug = v28_payload.get("dom_cat_url_slug")
    if dom_slug and res.get("mode") != "no_probe":
        try:
            facets = _fetch_subcat_facets(dom_slug, keyword, bucket)
            res["multi_facets"] = _extract_multi_facets(facets, keyword)
            size = _extract_size_facet(facets, keyword)
            if size:
                res["size_facet"] = size
        except Exception as e:
            logger.debug(f"multi-facet attach failed ({maincat}, {keyword!r}): {e}")
    return res


def _do_probe_inner(maincat: str, keyword: str, v28_payload: dict,
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
    if v28_payload.get("mode") not in ("and", "fallback"):
        return {"mode": "no_probe", "reason": "v28_not_and_or_fallback"}
    dom_slug = v28_payload.get("dom_cat_url_slug")
    # V31: in fallback mode, `total` is the OR-mode whole-cat count (millions);
    # the real AND-match count is `dom_cat_count`. In AND mode they're roughly
    # equal for narrow queries, but `dom_cat_count` is the strictly correct
    # base for facet-coverage math either way, since we filter within dom_cat.
    base_total = v28_payload.get("dom_cat_count") or v28_payload.get("total") or 0
    if not dom_slug or base_total <= 0:
        return {"mode": "no_probe", "reason": "no_dom_cat"}

    id_to_name = _facet_id_to_name()

    # Stage 1: free win from already-surfaced facets in the base response.
    surfaced_best = _check_surfaced(v28_payload, base_total, id_to_name, keyword)
    if surfaced_best is not None:
        coverage, value_count, facet_name, value_id, value_name, is_kw = surfaced_best
        return {
            "mode": "match_from_response",
            "facet_name": facet_name,
            "value_id": value_id,
            "value_name": value_name,
            "coverage": coverage,
            "value_count": value_count,
            "keyword_match": bool(is_kw),
            "candidates_probed": 0,
        }

    # Stage 1.5: live subcat-level keyword probe. Stage 1 only sees the
    # maincat-level surfaced facets (often just merk/winkel after an
    # OR-fallback) and Stage 2 only sees the facets.csv snapshot — so a
    # niche value the user literally searched for (e.g. ingr_shamp
    # 'Ketoconazol') is invisible to both. One subcat-level query surfaces
    # it. Gated on a leftover query token (>=4 chars, not in the dom_cat
    # name) so we don't add an API call for queries the category already
    # covers, and only runs because Stage 1 returned nothing above.
    dom_name = v28_payload.get("dom_cat_name", "")
    dom_toks = _tokens(dom_name)
    leftover = [w for w in _tokens(keyword) if len(w) >= 4 and w not in dom_toks]
    if leftover:
        kw_hit = _subcat_keyword_facet(dom_slug, keyword, bucket)
        if kw_hit:
            fname, vid, vname, cnt = kw_hit
            return {
                "mode": "match_from_response",
                "facet_name": fname,
                "value_id": vid,
                "value_name": vname,
                "coverage": round((cnt / base_total) if base_total else 0, 3),
                "value_count": cnt,
                "keyword_match": True,
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
        & (~cands["facet_name"].str.lower().isin(FACET_BLACKLIST))
    ].copy()
    # Flag candidates whose value name the query literally mentions. These
    # keyword matches bypass the subcat-wide count floor (a niche value like
    # "Ketoconazol" can be rare subcat-wide yet be exactly what was searched)
    # and sort to the front so they're probed first.
    cands["_kwmatch"] = cands["facet_value_name"].apply(
        lambda n: _value_matches_keyword(keyword, str(n)))
    cands = cands[cands["_kwmatch"] | (cands["count"] >= min_count)]
    cands = cands.sort_values(["_kwmatch", "count"], ascending=[False, False]) \
                 .head(MAX_CANDIDATES_PER_PAIR)
    if cands.empty:
        return {"mode": "no_candidates", "reason": "filter_empty",
                "min_count_required": min_count}

    kw_best = None   # keyword-name match with live coverage > 0
    cov_best = None  # coverage winner (≥ MIN_FACET_COVERAGE)
    n_probes = 0
    for _, row in cands.iterrows():
        is_kw = bool(row["_kwmatch"])
        bucket.acquire()
        cov = _probe_one(dom_slug, keyword, base_total,
                         row["facet_name"], int(row["facet_value_id"]))
        n_probes += 1
        if cov is None or cov <= 0:
            continue
        cand = (round(cov, 3), int(row["count"]),
                row["facet_name"], int(row["facet_value_id"]),
                row["facet_value_name"])
        if is_kw:
            # A keyword match only needs ≥1 matching product, not the 0.6
            # coverage floor — the user explicitly searched for this value.
            if kw_best is None or cand > kw_best:
                kw_best = cand
            break  # candidates are sorted kw-first; first live kw match wins
        if cov < MIN_FACET_COVERAGE:
            continue
        # Coverage over a tiny base is noise — a 1/1 fluke scores 100%. Only
        # keyword matches (handled above) are trusted at a tiny base.
        if base_total < MIN_COVERAGE_BASE_TOTAL:
            continue
        # Skip generic-attribute facets (kleur/materiaal/maat/…) that win
        # purely on coverage — appending them to a non-keyword-matched query
        # is noise (e.g. "fontein wc" → materiaal~Keramiek).
        if _is_generic_attribute_facet(row["facet_name"]):
            continue
        if cov_best is None or cand > cov_best:
            cov_best = cand
        # V31: early-stop on a very confident match to keep the per-pair
        # API cost low. Without this, raising MAX_CANDIDATES to 50 would
        # be 3× slower for the easy cases too.
        if cov >= EARLY_STOP_COVERAGE:
            break

    best = kw_best or cov_best
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
        "keyword_match": best is kw_best,
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
        # V31: also probe `fallback` rows when V28 recovered a dom_cat from
        # the categories[] breakdown. Without this, niche queries (where the
        # search API switches to OR-fallback at limit=50) never trigger the
        # facet-coverage path even though the dom_cat itself is reliable.
        if v28.get("mode") not in ("and", "fallback") or not v28.get("dom_cat_url_slug"):
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
