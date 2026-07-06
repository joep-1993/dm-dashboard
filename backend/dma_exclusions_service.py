"""
DMA Exclusions Service
======================

Excludes a single product (item id) from Beslist's DMA / Shopping (PLA) campaigns
by adding a negative product_item_id UNIT to the listing-group (product partition)
tree, and re-enables it later by removing that negative and pruning the tree back.

Given only a product item id we resolve its bid category from
`shopping_performance_view` (the campaigns/ad-groups it actually serves in, plus
its custom labels). From that we build the target set:

  * the category trio  PLA/<category>_a / _b / _c
  * PLA/Amazon bestsellers
  * PLA/APlus            (per-category ad group, matched on CL0 = deepest-cat-id)

Three tree shapes, two underlying operations (verified live 2026-06-25):

  bestsellers : CL0='amazon bestsellers' is already an item_id SUBDIVISION ->
                append one negative item_id UNIT.
  category    : the biddable CL3-OTHERS UNIT (bid set) must be converted to a
                SUBDIVISION holding item_id-OTHERS (positive, original bid) +
                the negative item_id.
  aplus       : same convert-the-biddable-leaf op, leaf = the INDEX0=<cl0> unit
                under the INDEX1='aplus' subdivision of the category's ad group.

Re-enable removes the negative and, where we created the subdivision, collapses
it back to the original biddable UNIT (the user's "remove & prune" choice).

Writes go through Google Ads atomic mutates. Nothing here runs unless the router
calls apply()/enable(); preview()/lookup() are strictly read-only.
"""
import os
import re
import heapq
import json
import time
import logging
import threading
import urllib.parse
import urllib.request
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.api_core import exceptions as google_exceptions

from backend.database import get_db_connection, return_db_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MCC_CUSTOMER_ID = "3011145605"
ACCOUNTS = {
    "NL": "3800751597",
    "BE": "9920951707",
}

BESTSELLERS_CAMPAIGN = "PLA/Amazon bestsellers"
APLUS_CAMPAIGN = "PLA/APlus"
APLUS_CATEGORY_INDEX = "INDEX0"  # APlus subdivides on CL0 = deepest-cat-id
CL3_INDEX = "INDEX3"             # category campaigns: leaf dimension is shop (CL3)

# Matches "PLA/<category>_a" / "_b" / "_c" but NOT the named campaigns above.
_CATEGORY_RE = re.compile(r"^PLA/(?P<cat>.+)_(?P<tier>[abc])$")

# OOS (out-of-stock) crawl-override monitor — feeds the exclusion candidate list.
OOS_BASE = "https://googlemc-suc.bva-apps.aks.private.beslist.nl/api/v1/overrides"
# DMA aggregated feed prefixes every offer id with this; suffix is the GTIN/EAN.
DMA_ITEM_PREFIX = "nl-nl-gold-"

# --- scan caches -----------------------------------------------------------
# Re-scans are frequent and the underlying data is slow-moving (GA metrics are a
# rolling 30-day window), so cache the expensive per-EAN GA / ES lookups
# in-process with a short TTL — a warm re-scan within the window skips the
# network work entirely. Per-process; clears on restart. Only successful fetches
# are cached (never transient failures).
_SCAN_CACHE_TTL = 1800  # seconds (30 min)
_SCAN_CACHE_MAX = 20000  # bound each scan cache; evict oldest ~10% past this
_CACHE_MISS = object()
_GA_CACHE: Dict[str, tuple] = {}     # ean -> (agg|None, ts)   None = queried, not live in DMA
_ES_CACHE: Dict[str, tuple] = {}     # norm_ean -> (headline_offer dict, ts)


def _cache_get(store: dict, key):
    ent = store.get(key)
    if ent is not None and (time.monotonic() - ent[1]) < _SCAN_CACHE_TTL:
        return ent[0]
    return _CACHE_MISS


def _cache_put(store: dict, key, value) -> None:
    # Long-lived uvicorn process: without a bound these caches retain every EAN
    # ever scanned. Evict the oldest ~10% when full (amortized: once per ~2000 puts).
    if len(store) >= _SCAN_CACHE_MAX and key not in store:
        for k in [k for k, _ in heapq.nsmallest(_SCAN_CACHE_MAX // 10, store.items(),
                                                 key=lambda kv: kv[1][1])]:
            store.pop(k, None)
    store[key] = (value, time.monotonic())

# ---------------------------------------------------------------------------
# Headline-offer check (Elasticsearch product index)
# ---------------------------------------------------------------------------
# An OOS EAN that serves in DMA is only worth excluding if it IS the product's
# *headline* (bestOffer) offer — the one the gold/DMA ad actually advertises and
# the PLP lands on. Apparel/footwear products carry one EAN per size variant; the
# monitor flags individual variant EANs, but the gold ad rides the headline
# variant. If a non-headline variant is OOS while the headline is a different
# in-stock variant/shop, excluding the EAN would needlessly kill a live, buyable
# ad. So we cross-check each candidate against the product search index and only
# treat it as a real exclusion when the OOS EAN == the headline offer's EAN.
ES_URL = "https://elasticsearch-job-cluster-eck-v9.beslist.nl"
# Products are spread across one index per maincat; wildcard covers them all.
ES_INDEX = "product_search_v4_nl-nl_*"

# Reuse one keep-alive session — a cold TLS handshake is ~3.5 s, a warm query ~30 ms.
_es_session = requests.Session()
_es_session.mount("https://", HTTPAdapter(
    pool_connections=1, pool_maxsize=16, pool_block=True,
))


def _norm_ean(ean: str) -> str:
    """ES stores EANs zero-padded to 13 chars; retail systems strip leading
    zeros. Pad so "12345" matches "0000000012345"."""
    e = str(ean or "").strip()
    return e.zfill(13) if 0 < len(e) < 13 else e


BESLIST_BASE = "https://www.beslist.nl"


def _plp_url(plp: Optional[str]) -> Optional[str]:
    """ES stores plpUrl as a relative path (/p/...); make it absolute."""
    if not plp:
        return None
    return plp if plp.startswith("http") else BESLIST_BASE + plp


def _norm_shop(name: Optional[str]) -> Optional[str]:
    """Match shop names across sources: drop a trailing |COUNTRY, lowercase."""
    if not name:
        return None
    return name.split("|", 1)[0].strip().lower()


def headline_offer(ean: str) -> Dict[str, Any]:
    """Cached wrapper around _headline_offer_uncached (TTL; skips transient ES
    errors so a blip isn't cached for the whole window)."""
    n = _norm_ean(ean)
    hit = _cache_get(_ES_CACHE, n)
    if hit is not _CACHE_MISS:
        return hit
    res = _headline_offer_uncached(ean)
    if res.get("status") != "error":
        _cache_put(_ES_CACHE, n, res)
    return res


def _headline_offer_uncached(ean: str) -> Dict[str, Any]:
    """Resolve the headline (bestOffer) for the product carrying this EAN.

    Returns {status, headline_ean, headline_shop, headline_stock, plp_url,
    shop_stock} where status:
      match       — the OOS EAN *is* the headline offer's EAN (safe to exclude)
      differs     — the headline is a different EAN/shop (do NOT exclude)
      no_headline — product found but no bestOffer (can't confirm)
      not_found   — no product in ES for this EAN (likely gone)
      error       — ES lookup failed (transient; don't fail-closed on it)
    shop_stock maps _norm_shop(name) -> max in-stock count seen for that shop in
    beslist's index. (Only plp_url is consumed now — apply() uses it to link the
    Saved-list item; the verdict fields are legacy but harmless.)
    """
    n = _norm_ean(ean)
    q = {"query": {"term": {"eans": n}}, "size": 10, "_source": ["shops", "plpUrl"]}
    try:
        r = _es_session.post(f"{ES_URL}/{ES_INDEX}/_search", json=q, timeout=20)
        r.raise_for_status()
        hits = r.json().get("hits", {}).get("hits", [])
    except Exception as e:  # noqa: BLE001
        logger.warning("ES headline lookup failed for %s: %s", ean, e)
        return {"status": "error", "headline_ean": None, "headline_shop": None,
                "headline_stock": None, "plp_url": None, "shop_stock": {}}

    if not hits:
        return {"status": "not_found", "headline_ean": None, "headline_shop": None,
                "headline_stock": None, "plp_url": None, "shop_stock": {}}

    # An EAN can resolve to several productidv3 docs; collect every offer together
    # with the PLP url of the doc it came from, and tally per-shop stock.
    #
    # We do NOT trust ES's `bestOffer` flag to name the headline shop: once the
    # cheapest offer goes out of stock the flag keeps pointing at it (stock=0),
    # while the live PLP promotes the cheapest *in-stock* offer instead. So we
    # replicate the site's choice — cheapest in-stock total price — and only fall
    # back to the flagged/first offer when nothing is in stock.
    offers = []          # (offer, shop, plp)
    shop_stock: Dict[str, int] = {}
    for h in hits:
        src = h.get("_source", {})
        plp = src.get("plpUrl")
        for shop in src.get("shops", []) or []:
            nm = _norm_shop(shop.get("name"))
            for off in shop.get("offers", []) or []:
                offers.append((off, shop, plp))
                st = off.get("stock")
                if nm and isinstance(st, (int, float)):
                    shop_stock[nm] = max(shop_stock.get(nm, 0), int(st))

    if not offers:
        return {"status": "no_headline", "headline_ean": None, "headline_shop": None,
                "headline_stock": None, "shop_stock": shop_stock,
                "plp_url": _plp_url(hits[0].get("_source", {}).get("plpUrl"))}

    # Restrict to offers for the exact EAN we looked up (the PLP is per-GTIN); a
    # doc's other variant offers don't sit on this product's headline. Fall back
    # to the whole pool if the EAN isn't carried as a per-offer field.
    matching = [t for t in offers if t[0].get("ean") and _norm_ean(t[0]["ean"]) == n]
    pool = matching or offers

    def _total_price(o):
        # Effective item price (sale beats regular) + delivery; None sorts last.
        sp = o.get("salePrice")
        rp = (o.get("regularPrice") or {}).get("price")
        price = sp if isinstance(sp, (int, float)) and sp > 0 else rp
        if not isinstance(price, (int, float)):
            return float("inf")
        deliv = o.get("deliveryCost")
        return price + (deliv if isinstance(deliv, (int, float)) else 0.0)

    def _rank(item):
        off = item[0]
        st = off.get("stock")
        in_stock = isinstance(st, (int, float)) and st > 0
        # in-stock first, then cheapest total, then ES bestOffer as a price tiebreak.
        return (0 if in_stock else 1, _total_price(off), 0 if off.get("bestOffer") else 1)

    off, shop, plp = min(pool, key=_rank)
    st = off.get("stock")
    return {
        # "match": the live headline is the EAN we looked up and it's in stock —
        # i.e. a customer still sees this exact offer available. "differs": the
        # headline moved (different EAN or now out of stock).
        "status": "match" if (matching and isinstance(st, (int, float)) and st > 0) else "differs",
        "headline_ean": off.get("ean"),
        "headline_shop": shop.get("name"),
        "headline_stock": off.get("stock"),
        "plp_url": _plp_url(plp),
        "shop_stock": shop_stock,
    }


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

# A GoogleAdsClient owns a gRPC channel + OAuth credentials; building one forces
# a token refresh on first use. The config is market-agnostic (customer_id is
# passed per-query, never baked in), so memoize a single process-wide instance
# rather than rebuilding it on every lookup/resolve/apply/enable. The client's
# channel is thread-safe, so sharing it across the ThreadPoolExecutor fan-outs
# below is safe.
_CLIENT: Optional[GoogleAdsClient] = None


def _get_client() -> GoogleAdsClient:
    global _CLIENT
    if _CLIENT is None:
        config = {
            "developer_token": os.environ.get("GOOGLE_DEVELOPER_TOKEN", ""),
            "refresh_token": os.environ.get("GOOGLE_REFRESH_TOKEN", ""),
            "client_id": os.environ.get("GOOGLE_CLIENT_ID", ""),
            "client_secret": os.environ.get("GOOGLE_CLIENT_SECRET", ""),
            "login_customer_id": os.environ.get("GOOGLE_LOGIN_CUSTOMER_ID", MCC_CUSTOMER_ID),
            "use_proto_plus": True,
        }
        _CLIENT = GoogleAdsClient.load_from_dict(config)
    return _CLIENT


def _customer_id(market: str) -> str:
    cid = ACCOUNTS.get((market or "").upper())
    if not cid:
        raise ValueError(f"Unknown market {market!r}; expected one of {list(ACCOUNTS)}")
    return cid


# Per-ad-group write lock. Two exclusions/re-enables that touch the SAME ad group
# must not mutate its criterion tree concurrently (they'd read a stale tree and
# race: lost subdivisions / CONCURRENT_MODIFICATION). Bulk paths serialize per
# ad group with this. Each write holds exactly one lock at a time (one ad group),
# so there is no lock-ordering / deadlock concern.
_AD_GROUP_LOCKS: Dict[str, threading.Lock] = {}
_AD_GROUP_LOCKS_GUARD = threading.Lock()


def _ad_group_lock(ad_group_id) -> threading.Lock:
    key = str(ad_group_id)
    lk = _AD_GROUP_LOCKS.get(key)
    if lk is None:
        with _AD_GROUP_LOCKS_GUARD:
            lk = _AD_GROUP_LOCKS.get(key)
            if lk is None:
                lk = threading.Lock()
                _AD_GROUP_LOCKS[key] = lk
    return lk


# ---------------------------------------------------------------------------
# Listing-tree reading
# ---------------------------------------------------------------------------

def _read_tree(client: GoogleAdsClient, customer_id: str, ad_group_id: str) -> Dict[str, dict]:
    """Return {resource_name: node} for every LISTING_GROUP criterion in an ad group."""
    ga = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          ad_group_criterion.resource_name,
          ad_group_criterion.criterion_id,
          ad_group_criterion.listing_group.type,
          ad_group_criterion.listing_group.parent_ad_group_criterion,
          ad_group_criterion.listing_group.case_value.product_custom_attribute.index,
          ad_group_criterion.listing_group.case_value.product_custom_attribute.value,
          ad_group_criterion.listing_group.case_value.product_item_id.value,
          ad_group_criterion.negative,
          ad_group_criterion.cpc_bid_micros
        FROM ad_group_criterion
        WHERE ad_group_criterion.ad_group = 'customers/{customer_id}/adGroups/{ad_group_id}'
          AND ad_group_criterion.type = 'LISTING_GROUP'
          AND ad_group_criterion.status != 'REMOVED'
    """
    nodes: Dict[str, dict] = {}
    for row in ga.search(customer_id=customer_id, query=query):
        agc = row.ad_group_criterion
        lg = agc.listing_group
        # defensive: skip any criterion whose listing-group type didn't resolve
        if lg.type_.name not in ("SUBDIVISION", "UNIT"):
            continue
        cv = lg.case_value
        which = cv._pb.WhichOneof("dimension")
        dim = None
        index = None
        value = None
        item_id = None
        if which == "product_item_id":
            dim = "item_id"
            item_id = cv.product_item_id.value  # "" => OTHERS
        elif which == "product_custom_attribute":
            dim = "custom_attr"
            index = cv.product_custom_attribute.index.name  # INDEX0..INDEX4
            value = cv.product_custom_attribute.value        # "" => OTHERS
        nodes[agc.resource_name] = {
            "resource": agc.resource_name,
            "criterion_id": agc.criterion_id,
            "type": lg.type_.name,  # SUBDIVISION | UNIT
            "parent": lg.parent_ad_group_criterion or None,
            "dim": dim,
            "index": index,
            "value": value,
            "item_id": item_id,
            "negative": bool(agc.negative),
            "bid": int(agc.cpc_bid_micros or 0),
        }
    return nodes


def _children(nodes: Dict[str, dict], parent_resource: str) -> List[dict]:
    return [n for n in nodes.values() if n["parent"] == parent_resource]


def _ad_group_cpc(client: GoogleAdsClient, customer_id: str, ad_group_id: str) -> int:
    """Ad group's default cpc_bid_micros (fallback bid for new biddable units)."""
    ga = client.get_service("GoogleAdsService")
    q = (f"SELECT ad_group.cpc_bid_micros FROM ad_group "
         f"WHERE ad_group.id = {ad_group_id}")
    for row in ga.search(customer_id=customer_id, query=q):
        return int(row.ad_group.cpc_bid_micros or 0)
    return 0


# ---------------------------------------------------------------------------
# Category resolution from a bare item id
# ---------------------------------------------------------------------------

def lookup(item_id: str, market: str) -> Dict[str, Any]:
    """Resolve category + serving campaigns for an item id (READ-ONLY)."""
    item_id = (item_id or "").strip()
    if not item_id:
        raise ValueError("item_id is required")
    client = _get_client()
    customer_id = _customer_id(market)
    ga = client.get_service("GoogleAdsService")

    query = f"""
        SELECT
          campaign.name, ad_group.name,
          segments.product_item_id,
          segments.product_custom_attribute0, segments.product_custom_attribute3,
          segments.product_type_l1, segments.product_type_l2,
          metrics.impressions
        FROM shopping_performance_view
        WHERE segments.product_item_id = {item_id!r}
          AND segments.date DURING LAST_30_DAYS
    """
    serving: List[dict] = []
    try:
        for row in ga.search(customer_id=customer_id, query=query):
            serving.append({
                "campaign": row.campaign.name,
                "ad_group": row.ad_group.name,
                "cl0": row.segments.product_custom_attribute0,
                "shop": row.segments.product_custom_attribute3,
                "type_l1": row.segments.product_type_l1,
                "type_l2": row.segments.product_type_l2,
            })
    except GoogleAdsException as e:
        raise RuntimeError(f"Google Ads query failed: {e.error.code().name}") from e

    # Collect candidates across the category-matching rows, then pick
    # deterministically (lowest sorted) — serving-row order is arbitrary, so
    # last-writer-wins gave a non-stable cl0/shop for multi-category products.
    cat_candidates: List[str] = []
    cl0_candidates = set()
    shop_candidates = set()
    for r in serving:
        m = _CATEGORY_RE.match(r["campaign"])
        if m:
            cat_candidates.append(m.group("cat"))
            if r["cl0"] and r["cl0"].isdigit():
                cl0_candidates.add(r["cl0"])
            if r["shop"]:
                shop_candidates.add(r["shop"])
    category = _pick_category(cat_candidates)
    cl0 = min(cl0_candidates) if cl0_candidates else None
    shop = min(shop_candidates) if shop_candidates else None
    # fall back to any serving row's shop (deterministic)
    if shop is None:
        all_shops = {r["shop"] for r in serving if r["shop"]}
        shop = min(all_shops) if all_shops else None

    res = {
        "item_id": item_id,
        "market": market.upper(),
        "found": bool(serving),
        "category": category,
        "cl0": cl0,
        "shop": shop,
        "serving_campaigns": sorted({r["campaign"] for r in serving}),
        "note": (
            None if serving else
            "No serving rows in the last 30 days; category cannot be resolved "
            "from Google Ads (Merchant Center fallback not yet enabled)."
        ),
    }
    # Warm the resolution cache so a follow-up resolve_targets/apply on the same
    # id (the normal preview -> apply flow) skips this ~6s shopping_performance
    # query. Short TTL + graceful fallback in _cached_lookup keep it fresh.
    _cache_resolution(res)
    return res


def _pick_category(cands: List[str]) -> Optional[str]:
    """Prefer a real product-category trio (PLA/Koffiezetapparaten_a) over a
    "<shop> store" allow-list campaign (PLA/Koffie store_a): the store campaign
    also matches _CATEGORY_RE and would otherwise shadow the real category,
    leaving the product excluded only via APlus/bestsellers (its CL3-OTHERS leaf
    is negative, so the trio gets skipped). Store campaigns are deferred."""
    # Pick deterministically (lowest sorted), matching the min() used for cl0/shop
    # in lookup(): serving-row order is arbitrary, so [0] of the raw list gave a
    # non-stable category (hence a different PLA/<cat>_a/_b/_c set) across runs.
    non_store = sorted(c for c in cands if not c.lower().endswith(" store"))
    return (non_store or sorted(cands) or [None])[0]


# Resolution cache: lookup() runs a ~6s shopping_performance_view query, and it
# dominates each exclusion. oos_scan already does ONE batched query over all
# candidates, so it pre-populates this cache; resolve_targets then skips the
# per-EAN lookup. Short TTL + graceful fallback to lookup() keep it safe/fresh.
_RES_CACHE: Dict[tuple, tuple] = {}
_RES_TTL = 1800.0  # 30 min
_RES_MAX = 5000    # bound the cache; evict the oldest entries past this

def _cache_resolution(res: dict) -> None:
    if len(_RES_CACHE) >= _RES_MAX:
        for k in [k for k, _ in heapq.nsmallest(_RES_MAX // 10, _RES_CACHE.items(),
                                                key=lambda kv: kv[1][1])]:
            _RES_CACHE.pop(k, None)
    _RES_CACHE[(res["market"], res["item_id"])] = (res, time.monotonic())

def _cached_lookup(item_id: str, market: str) -> dict:
    key = (market.upper(), (item_id or "").strip())
    hit = _RES_CACHE.get(key)
    if hit and time.monotonic() - hit[1] < _RES_TTL:
        return hit[0]
    return lookup(item_id, market)


# ---------------------------------------------------------------------------
# Target discovery (which campaigns/ad-groups/nodes to touch)
# ---------------------------------------------------------------------------

def _find_campaigns(client, customer_id, like_patterns: List[str]) -> List[dict]:
    ga = client.get_service("GoogleAdsService")
    out: List[dict] = []
    seen = set()
    for pat in like_patterns:
        esc = pat.replace("'", "\\'")
        q = (
            "SELECT campaign.id, campaign.name, campaign.status FROM campaign "
            f"WHERE campaign.status != 'REMOVED' AND campaign.name LIKE '{esc}'"
        )
        for row in ga.search(customer_id=customer_id, query=q):
            if row.campaign.id in seen:
                continue
            seen.add(row.campaign.id)
            out.append({
                "campaign_id": str(row.campaign.id),
                "campaign_name": row.campaign.name,
                "status": row.campaign.status.name,
            })
    return out


def _aplus_adgroups_for_cl0(client, customer_id, campaign_id, cl0) -> List[dict]:
    """Find the APlus ad group(s) whose tree carries an INDEX0=<cl0> node.

    One campaign-scoped criterion query instead of scanning all ~1400 ad groups.
    A deepest-cat-id is unique to its category, so this returns a single ad group.
    """
    ga = client.get_service("GoogleAdsService")
    esc = str(cl0).replace("'", "\\'")
    q = (
        "SELECT ad_group.id, ad_group.name FROM ad_group_criterion "
        f"WHERE campaign.id = {campaign_id} AND ad_group_criterion.type = 'LISTING_GROUP' "
        "AND ad_group_criterion.status != 'REMOVED' "
        f"AND ad_group_criterion.listing_group.case_value.product_custom_attribute.index = '{APLUS_CATEGORY_INDEX}' "
        f"AND ad_group_criterion.listing_group.case_value.product_custom_attribute.value = '{esc}'"
    )
    out: Dict[str, dict] = {}
    for r in ga.search(customer_id=customer_id, query=q):
        out[str(r.ad_group.id)] = {"ad_group_id": str(r.ad_group.id), "ad_group_name": r.ad_group.name}
    return list(out.values())


def _ad_groups(client, customer_id, campaign_id) -> List[dict]:
    ga = client.get_service("GoogleAdsService")
    q = (
        "SELECT ad_group.id, ad_group.name, ad_group.status FROM ad_group "
        f"WHERE ad_group.campaign = 'customers/{customer_id}/campaigns/{campaign_id}' "
        "AND ad_group.status != 'REMOVED'"
    )
    return [
        {"ad_group_id": str(r.ad_group.id), "ad_group_name": r.ad_group.name,
         "status": r.ad_group.status.name}
        for r in ga.search(customer_id=customer_id, query=q)
    ]


def _node_summary(node: Optional[dict]) -> Optional[dict]:
    if not node:
        return None
    return {k: node[k] for k in ("resource", "type", "dim", "index", "value", "item_id", "negative", "bid")}


def _leaf_for_category(nodes: Dict[str, dict]) -> Optional[dict]:
    """The CL3-OTHERS node (the catch-all shop bucket products serve under)."""
    for n in nodes.values():
        if n["dim"] == "custom_attr" and n["index"] == CL3_INDEX and (n["value"] or "") == "":
            return n
    return None


def _leaf_for_aplus(nodes: Dict[str, dict], cl0: str) -> Optional[dict]:
    """The INDEX0=<cl0> category node inside an APlus ad group."""
    for n in nodes.values():
        if n["dim"] == "custom_attr" and n["index"] == APLUS_CATEGORY_INDEX and (n["value"] or "") == cl0:
            return n
    return None


def _bestsellers_subdiv(nodes: Dict[str, dict]) -> Optional[dict]:
    for n in nodes.values():
        if (n["dim"] == "custom_attr" and n["index"] == "INDEX0"
                and (n["value"] or "") == "amazon bestsellers" and n["type"] == "SUBDIVISION"):
            return n
    return None


def _negative_item_children(nodes: Dict[str, dict], subdiv_resource: str) -> List[dict]:
    return [n for n in _children(nodes, subdiv_resource)
            if n["dim"] == "item_id" and (n["item_id"] or "") != "" and n["negative"]]


def _existing_negative(nodes: Dict[str, dict], subdiv_resource: str, item_id: str) -> Optional[dict]:
    for n in _children(nodes, subdiv_resource):
        if n["dim"] == "item_id" and (n["item_id"] or "") == item_id and n["negative"]:
            return n
    return None


def _build_target(item_id, kind, campaign, ad_group, nodes, leaf) -> dict:
    """Assemble a per-ad-group target with the planned action, given its leaf node.

    Pure: works off the already-read `nodes`/`leaf`; no Google Ads client needed.
    """
    t = {
        "kind": kind,
        "campaign_id": campaign["campaign_id"],
        "campaign_name": campaign["campaign_name"],
        "ad_group_id": ad_group["ad_group_id"],
        "ad_group_name": ad_group["ad_group_name"],
        "leaf": _node_summary(leaf),
        "action": None,
        "leaf_role": None,
        "original_bid": None,
        "already_excluded": False,
        "skip_reason": None,
    }
    if leaf is None:
        t["action"] = "skip"
        t["skip_reason"] = "target node not found in tree"
        return t

    if leaf["type"] == "SUBDIVISION":
        # already subdivided -> only safe to append if it splits on item_id
        children = _children(nodes, leaf["resource"])
        item_children = [c for c in children if c["dim"] == "item_id"]
        if not item_children:
            split = children[0]["index"] if children else "?"
            t["leaf_role"] = "other_subdivision"
            t["action"] = "skip"
            t["skip_reason"] = f"leaf subdivides on {split}, not item_id (unsupported topology)"
        else:
            t["leaf_role"] = "item_subdivision"
            if _existing_negative(nodes, leaf["resource"], item_id):
                t["action"] = "skip"
                t["already_excluded"] = True
                t["skip_reason"] = "item already excluded here"
            else:
                t["action"] = "append_negative"
    elif leaf["negative"]:
        # allow-list tree: the OTHERS bucket is EXCLUDED and specific shops are
        # included. The product serves via an included CL3=shop leaf, not here;
        # converting this negative bucket would wrongly start serving it. Skip.
        t["leaf_role"] = "negative_unit"
        t["action"] = "skip"
        t["skip_reason"] = "leaf is an excluded (negative) bucket — allow-list tree, not auto-excludable"
    else:
        # biddable UNIT -> must convert to a subdivision first
        t["leaf_role"] = "biddable_unit"
        t["original_bid"] = leaf["bid"]
        t["action"] = "subdivide_and_exclude"
    return t


def resolve_targets(item_id: str, market: str, campaign_filter: Optional[str] = None,
                    resolution: Optional[dict] = None) -> Dict[str, Any]:
    """Discover every ad group + node to touch (READ-ONLY). Returns plan."""
    item_id = (item_id or "").strip()
    client = _get_client()
    customer_id = _customer_id(market)
    res = resolution or _cached_lookup(item_id, market)
    cf = (campaign_filter or "").strip().lower()

    # The three branches below are independent READ-ONLY discovery passes
    # (~13 sequential Google Ads queries total dominate the latency). They have
    # no data dependency on each other, and within the category branch each
    # ad-group tree read is independent too — so run them concurrently. All
    # mutations still happen later, sequentially, in apply(), so there's no race.
    def _category_branch():
        out, warn = [], []
        if not res.get("category"):
            return out, ["Category not resolved; skipping category trio + APlus."]
        cat = res["category"]
        camps = _find_campaigns(client, customer_id, [f"PLA/{cat}_%"])
        trio = [c for c in camps
                if (m := _CATEGORY_RE.match(c["campaign_name"])) and m.group("cat") == cat]
        if not trio:
            warn.append(f"No PLA/{cat}_a/_b/_c campaigns found.")
        pairs = [(c, ag) for c in trio
                 for ag in _ad_groups(client, customer_id, c["campaign_id"])]

        def _build(pair):
            c, ag = pair
            nodes = _read_tree(client, customer_id, ag["ad_group_id"])
            leaf = _leaf_for_category(nodes)
            return _build_target(item_id, "category", c, ag, nodes, leaf)

        if pairs:
            with ThreadPoolExecutor(max_workers=min(8, len(pairs))) as ex:
                out = list(ex.map(_build, pairs))
        return out, warn

    def _bestsellers_branch():
        # Guard: the bestsellers campaign is a flat per-item-id list, so this
        # used to run for ANY id — including a bogus/never-served one, whose
        # append then fails because the id isn't in the tree. Only attempt it
        # when the item actually resolved (a real bestseller exclusion still has
        # a serving row in PLA/Amazon bestsellers, so found is True there).
        if not res.get("found"):
            return [], ["Item id not resolved (no serving history); skipping Amazon bestsellers."]
        out = []
        for c in _find_campaigns(client, customer_id, [BESTSELLERS_CAMPAIGN]):
            for ag in _ad_groups(client, customer_id, c["campaign_id"]):
                nodes = _read_tree(client, customer_id, ag["ad_group_id"])
                leaf = _bestsellers_subdiv(nodes)
                out.append(_build_target(item_id, "bestsellers", c, ag, nodes, leaf))
        return out, []

    def _aplus_branch():
        # needs cl0 to pick the right per-category ad group
        if not res.get("cl0"):
            return [], (["CL0 (deepest-cat-id) not resolved; skipping APlus."]
                        if res.get("category") else [])
        out, warn = [], []
        cl0 = res["cl0"]
        for c in _find_campaigns(client, customer_id, [APLUS_CAMPAIGN]):
            ag_rows = _aplus_adgroups_for_cl0(client, customer_id, c["campaign_id"], cl0)
            if not ag_rows:
                warn.append(f"No APlus ad group found for category id {cl0}.")
            for ag in ag_rows:
                nodes = _read_tree(client, customer_id, ag["ad_group_id"])
                leaf = _leaf_for_aplus(nodes, cl0)
                out.append(_build_target(item_id, "aplus", c, ag, nodes, leaf))
        return out, warn

    targets: List[dict] = []
    warnings: List[str] = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        for fut in [ex.submit(b) for b in (_category_branch, _bestsellers_branch, _aplus_branch)]:
            t, w = fut.result()
            targets.extend(t)
            warnings.extend(w)

    if cf:
        targets = [t for t in targets if cf in t["campaign_name"].lower()]

    return {
        "item_id": item_id,
        "market": market.upper(),
        "resolution": res,
        "campaign_filter": campaign_filter or None,
        "targets": targets,
        "warnings": warnings,
        "actionable": sum(1 for t in targets if t["action"] in ("append_negative", "subdivide_and_exclude")),
    }


def preview(item_id: str, market: str, shop: Optional[str] = None,
            campaign_filter: Optional[str] = None) -> Dict[str, Any]:
    """Dry-run: exactly what apply() would change. No writes."""
    plan = resolve_targets(item_id, market, campaign_filter)
    plan["shop"] = shop or plan["resolution"].get("shop")
    # Resolve the live headline (bestOffer) shop so the UI can show it alongside
    # the DMA-feed shop (plan["shop"], from product_custom_attribute3). They
    # legitimately differ when the cheapest/in-stock offer has moved to another
    # shop since the gold item was built. Best-effort: an ES blip must never fail
    # the preview.
    item_id_s = (item_id or "").strip()
    ean = item_id_s[len(DMA_ITEM_PREFIX):] if item_id_s.startswith(DMA_ITEM_PREFIX) else item_id_s
    try:
        ho = headline_offer(ean)
        plan["headline_shop"] = ho.get("headline_shop")
        plan["plp_url"] = ho.get("plp_url")
    except Exception:  # noqa: BLE001
        plan["headline_shop"] = None
        plan["plp_url"] = None
    plan["dry_run"] = True
    return plan


# ---------------------------------------------------------------------------
# Mutations
# ---------------------------------------------------------------------------

class _Temp:
    """Per-mutate temporary-id generator (negative ints)."""
    def __init__(self):
        self.n = 0

    def path(self, client, customer_id, ad_group_id) -> str:
        self.n -= 1
        return client.get_service("AdGroupCriterionService").ad_group_criterion_path(
            customer_id, str(ad_group_id), str(self.n))


def _unit_op(client, customer_id, ad_group_id, temp, parent_resource, *,
             item_id_value=None, custom_attr=None, negative=False, bid=None):
    """Build a create-UNIT AdGroupCriterionOperation."""
    op = client.get_type("AdGroupCriterionOperation")
    cr = op.create
    cr.resource_name = temp.path(client, customer_id, ad_group_id)
    cr.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
    if bid and not negative:
        cr.cpc_bid_micros = bid
    lg = cr.listing_group
    lg.type_ = client.enums.ListingGroupTypeEnum.UNIT
    lg.parent_ad_group_criterion = parent_resource
    if item_id_value is not None:
        if item_id_value != "":
            lg.case_value.product_item_id.value = item_id_value
        else:
            # item-id OTHERS: touch the message but set no value
            client.copy_from(lg.case_value.product_item_id, client.get_type("ProductItemIdInfo"))
    elif custom_attr is not None:
        lg.case_value.product_custom_attribute.index = client.enums.ProductCustomAttributeIndexEnum[custom_attr["index"]]
        if custom_attr["value"]:
            lg.case_value.product_custom_attribute.value = custom_attr["value"]
    if negative:
        cr.negative = True
    return op, cr.resource_name


def _subdiv_op(client, customer_id, ad_group_id, temp, parent_resource, *, custom_attr):
    op = client.get_type("AdGroupCriterionOperation")
    cr = op.create
    cr.resource_name = temp.path(client, customer_id, ad_group_id)
    cr.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
    lg = cr.listing_group
    lg.type_ = client.enums.ListingGroupTypeEnum.SUBDIVISION
    lg.parent_ad_group_criterion = parent_resource
    lg.case_value.product_custom_attribute.index = client.enums.ProductCustomAttributeIndexEnum[custom_attr["index"]]
    if custom_attr["value"]:
        lg.case_value.product_custom_attribute.value = custom_attr["value"]
    return op, cr.resource_name


def _remove_op(client, resource_name):
    op = client.get_type("AdGroupCriterionOperation")
    op.remove = resource_name
    return op


def _apply_one_target(client, customer_id, item_id, target) -> dict:
    """Execute a single target's planned write. Returns reversal metadata.

    Holds the ad group's write lock for the whole read+mutate so concurrent
    exclusions touching the same ad group can't race on its criterion tree.
    """
    agc = client.get_service("AdGroupCriterionService")
    ad_group_id = target["ad_group_id"]
    rev = dict(target)  # carry kind/campaign/ad_group/leaf for enable
    leaf = target["leaf"]

    with _ad_group_lock(ad_group_id):
        temp = _Temp()
        if target["action"] == "append_negative":
            op, neg_res = _unit_op(client, customer_id, ad_group_id, temp,
                                   leaf["resource"], item_id_value=item_id, negative=True)
            resp = agc.mutate_ad_group_criteria(customer_id=customer_id, operations=[op])
            rev["created_subdivision"] = False
            rev["negative_resource"] = resp.results[0].resource_name
            rev["status"] = "excluded"
            return rev

        if target["action"] == "subdivide_and_exclude":
            # Atomic: remove biddable leaf, create subdivision in its place, add
            # item-id OTHERS (positive, original bid) + the negative item id.
            ca = {"index": leaf["index"], "value": leaf["value"] or ""}
            # parent is the leaf's parent in the live tree
            nodes = _read_tree(client, customer_id, ad_group_id)
            live_leaf = nodes.get(leaf["resource"])
            if live_leaf is None:
                raise RuntimeError("leaf node disappeared before apply")
            parent_resource = live_leaf["parent"]
            # The biddable OTHERS unit needs a bid. Use the LIVE leaf's own bid (not
            # the possibly-stale snapshot); if it inherits (0), fall back to the ad
            # group's default CPC so a manual-CPC ad group doesn't reject the new
            # unit (cpc_bid_micros REQUIRED). None is correct for auto-bidding groups.
            bid = live_leaf["bid"] or _ad_group_cpc(client, customer_id, ad_group_id) or None

            ops = []
            ops.append(_remove_op(client, leaf["resource"]))
            sub_op, sub_res = _subdiv_op(client, customer_id, ad_group_id, temp, parent_resource, custom_attr=ca)
            ops.append(sub_op)
            others_op, others_res = _unit_op(client, customer_id, ad_group_id, temp, sub_res,
                                             item_id_value="", negative=False, bid=bid)
            ops.append(others_op)
            neg_op, neg_res = _unit_op(client, customer_id, ad_group_id, temp, sub_res,
                                       item_id_value=item_id, negative=True)
            ops.append(neg_op)
            resp = agc.mutate_ad_group_criteria(customer_id=customer_id, operations=ops)
            # resp.results order matches ops order: [remove, subdiv, others, negative]
            rev["created_subdivision"] = True
            rev["subdivision_resource"] = resp.results[1].resource_name
            rev["others_resource"] = resp.results[2].resource_name
            rev["negative_resource"] = resp.results[3].resource_name
            rev["leaf_custom_attr"] = ca
            rev["original_bid"] = live_leaf["bid"]
            rev["status"] = "excluded"
            return rev

    rev["status"] = "skipped"
    return rev


def _persist_apply(*, item_id, market, shop, resolution, campaign_filter,
                   results, errors, warnings, plp_url, source,
                   headline_shop=None) -> Dict[str, Any]:
    """Compute the exclusion status, persist the record, build the API result.

    Shared tail of the single-item apply() and the bulk oos_exclude() path so
    both produce identical records and response shapes.
    """
    applied = [r for r in results if r.get("result") == "excluded"]
    # excluded = all actionable targets succeeded; partial = some applied but
    # others errored (don't hide the failures); failed = a real error with nothing
    # applied; already_excluded = no error, the item was already excluded wherever
    # actionable; noop = no error, nothing actionable at all (e.g. every leaf is an
    # allow-list negative bucket, or category unresolved). The last two used to be
    # mislabelled "failed", prompting needless retries.
    if applied and not errors:
        status = "excluded"
    elif applied:
        status = "partial"
    elif errors:
        status = "failed"
    elif any(r.get("already_excluded") for r in results):
        status = "already_excluded"
    else:
        status = "noop"
    record_id = _save_record(
        item_id=item_id, market=market.upper(), shop=shop,
        category=resolution.get("category"), cl0=resolution.get("cl0"),
        campaign_filter=campaign_filter,
        status=status,
        plp_url=plp_url,
        headline_shop=headline_shop,
        targets=applied,
        last_result={"applied": len(applied), "errors": errors, "warnings": warnings},
        source=source,
    )
    return {
        "id": record_id,
        "item_id": item_id,
        "market": market.upper(),
        "applied": len(applied),
        "skipped": sum(1 for r in results if r.get("result") == "skipped"),
        "errors": errors,
        "warnings": warnings,
        "targets": results,
    }


def apply(item_id: str, market: str, shop: Optional[str] = None,
          campaign_filter: Optional[str] = None, source: str = "manual") -> Dict[str, Any]:
    """Apply the exclusion live, then persist it for later re-enable."""
    item_id = (item_id or "").strip()
    client = _get_client()
    customer_id = _customer_id(market)
    plan = resolve_targets(item_id, market, campaign_filter)
    shop = shop or plan["resolution"].get("shop")

    # Resolve the product PLP url (best-effort) so the Saved list can link the item
    # id. It's an independent ES call with no bearing on the mutates, so run it
    # concurrently with the write wave instead of serially after it.
    ean = item_id[len(DMA_ITEM_PREFIX):] if item_id.startswith(DMA_ITEM_PREFIX) else item_id

    def _headline_lookup():
        try:
            return headline_offer(ean)
        except Exception:  # noqa: BLE001 - never fail an apply over the ES lookup
            return {}

    results: List[dict] = []
    errors: List[dict] = []
    # Skipped targets are pure bookkeeping (no I/O); keep them out of the pool.
    actionable: List[dict] = []
    for t in plan["targets"]:
        if t["action"] in ("append_negative", "subdivide_and_exclude"):
            actionable.append(t)
        else:
            results.append({**t, "result": "skipped", "reason": t.get("skip_reason")})

    def _do(t):
        # Each target is a distinct ad group and _apply_one_target gets its own
        # service handle, so these writes have no shared state and can run in
        # parallel. (Cross-item bulk parallelism is NOT safe — see resolve/apply
        # docs — but within one item the ad groups are disjoint.)
        try:
            rev = _apply_one_target(client, customer_id, item_id, t)
            return ("ok", {**rev, "result": "excluded"})
        except Exception as e:  # noqa: BLE001 - surface per-target failures, keep going
            logger.exception("apply failed for %s", t.get("campaign_name"))
            return ("err", {"campaign_name": t["campaign_name"],
                            "ad_group_id": t["ad_group_id"], "error": str(e)})

    with ThreadPoolExecutor(max_workers=min(8, len(actionable) + 1)) as ex:
        headline_fut = ex.submit(_headline_lookup)
        for status, payload in ex.map(_do, actionable):
            (results if status == "ok" else errors).append(payload)
        headline = headline_fut.result() or {}
        plp_url = headline.get("plp_url")
        headline_shop = headline.get("headline_shop")

    return _persist_apply(
        item_id=item_id, market=market, shop=shop, resolution=plan["resolution"],
        campaign_filter=campaign_filter, results=results, errors=errors,
        warnings=plan["warnings"], plp_url=plp_url, headline_shop=headline_shop,
        source=source)


def enable(record_id: int) -> Dict[str, Any]:
    """Re-enable a previously-excluded product: remove negatives and prune."""
    rec = _get_record(record_id)
    if rec is None:
        raise ValueError(f"Exclusion #{record_id} not found")
    if rec["status"] == "enabled":
        return {"id": record_id, "status": "already_enabled", "reverted": 0}

    client = _get_client()
    customer_id = _customer_id(rec["market"])
    item_id = rec["item_id"]

    def _revert_one(t):
        # Distinct ad groups within one item run in parallel; the per-ad-group
        # lock also serializes this against any other item re-enabling the SAME
        # ad group (bulk oos_reenable), so the fresh tree read below reflects the
        # prior item's removal — that's what lets the sole-negative collapse work.
        ad_group_id = t["ad_group_id"]
        agc = client.get_service("AdGroupCriterionService")
        try:
            with _ad_group_lock(ad_group_id):
                nodes = _read_tree(client, customer_id, ad_group_id)
                # locate the subdivision the item lives under
                if t.get("created_subdivision"):
                    subdiv = nodes.get(t.get("subdivision_resource")) or _relocate_subdiv(nodes, t)
                else:
                    subdiv = nodes.get(t["leaf"]["resource"]) or _relocate_subdiv(nodes, t)
                if subdiv is None:
                    return ("err", {"campaign_name": t["campaign_name"], "error": "subdivision not found (already changed?)"})

                neg = _existing_negative(nodes, subdiv["resource"], item_id)
                ops = []
                if neg:
                    ops.append(_remove_op(client, neg["resource"]))

                if t.get("created_subdivision"):
                    # collapse only if our item was the sole negative AND there are no
                    # other (hand-added) positive item leaves under the subdivision —
                    # removing the subdivision would destroy them. The empty-value
                    # OTHERS bucket we created doesn't count.
                    other_negs = [n for n in _negative_item_children(nodes, subdiv["resource"])
                                  if (n["item_id"] or "") != item_id]
                    other_positives = [n for n in _children(nodes, subdiv["resource"])
                                       if n["dim"] == "item_id" and not n["negative"]
                                       and (n["item_id"] or "") not in ("", item_id)]
                    if not other_negs and not other_positives:
                        # remove item-id OTHERS + the subdivision, recreate biddable UNIT
                        others = [n for n in _children(nodes, subdiv["resource"])
                                  if n["dim"] == "item_id" and (n["item_id"] or "") == ""]
                        for o in others:
                            ops.append(_remove_op(client, o["resource"]))
                        ops.append(_remove_op(client, subdiv["resource"]))
                        temp = _Temp()
                        ca = t.get("leaf_custom_attr") or {
                            "index": t["leaf"]["index"], "value": t["leaf"]["value"] or ""}
                        bid = t.get("original_bid") or _ad_group_cpc(client, customer_id, ad_group_id) or None
                        unit_op, _ = _unit_op(client, customer_id, ad_group_id, temp, subdiv["parent"],
                                              custom_attr=ca, negative=False, bid=bid)
                        ops.append(unit_op)

                if ops:
                    agc.mutate_ad_group_criteria(customer_id=customer_id, operations=ops)
                return ("ok", {"campaign_name": t["campaign_name"], "ad_group_id": ad_group_id})
        except Exception as e:  # noqa: BLE001
            logger.exception("enable failed for %s", t.get("campaign_name"))
            return ("err", {"campaign_name": t["campaign_name"], "error": str(e)})

    reverted: List[dict] = []
    errors: List[dict] = []
    targets = rec.get("targets") or []
    if targets:
        with ThreadPoolExecutor(max_workers=min(8, len(targets))) as ex:
            for status, payload in ex.map(_revert_one, targets):
                (reverted if status == "ok" else errors).append(payload)

    new_status = "enabled" if not errors else "partial"
    _update_status(record_id, new_status, {"reverted": len(reverted), "errors": errors})
    return {"id": record_id, "status": new_status, "reverted": len(reverted), "errors": errors}


def _relocate_subdiv(nodes, target):
    """Best-effort: re-find the subdivision the item lives under by its dimension."""
    kind = target["kind"]
    if kind == "bestsellers":
        return _bestsellers_subdiv(nodes)
    if kind == "category":
        n = _leaf_for_category(nodes)
        return n if n and n["type"] == "SUBDIVISION" else None
    if kind == "aplus":
        ca = target.get("leaf_custom_attr") or {}
        val = ca.get("value") or (target["leaf"].get("value") if target.get("leaf") else None)
        if val:
            n = _leaf_for_aplus(nodes, val)
            return n if n and n["type"] == "SUBDIVISION" else None
    return None


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

_TABLE_READY = False


def _ensure_table():
    global _TABLE_READY
    if _TABLE_READY:
        return
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dma_exclusions (
                    id              SERIAL PRIMARY KEY,
                    item_id         TEXT NOT NULL,
                    market          TEXT NOT NULL,
                    shop            TEXT,
                    category        TEXT,
                    cl0             TEXT,
                    campaign_filter TEXT,
                    status          TEXT NOT NULL DEFAULT 'excluded',
                    targets         JSONB,
                    last_result     JSONB,
                    source          TEXT DEFAULT 'manual',
                    created_at      TIMESTAMP DEFAULT now(),
                    applied_at      TIMESTAMP DEFAULT now(),
                    enabled_at      TIMESTAMP
                )
            """)
            cur.execute("ALTER TABLE dma_exclusions ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'manual'")
            cur.execute("ALTER TABLE dma_exclusions ADD COLUMN IF NOT EXISTS plp_url TEXT")
            # Live headline (bestOffer) shop from the ES product index — distinct
            # from `shop` (the DMA feed's product_custom_attribute3). Stored so the
            # Saved list can show both and flag mismatches.
            cur.execute("ALTER TABLE dma_exclusions ADD COLUMN IF NOT EXISTS headline_shop TEXT")
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS dma_exclusions_item_market_uniq
                ON dma_exclusions (item_id, market)
            """)
        conn.commit()
        _TABLE_READY = True
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)


def _save_record(*, item_id, market, shop, category, cl0, campaign_filter,
                 status, targets, last_result, source="manual", plp_url=None,
                 headline_shop=None) -> int:
    _ensure_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dma_exclusions
                    (item_id, market, shop, category, cl0, campaign_filter,
                     status, targets, last_result, source, plp_url, headline_shop,
                     applied_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                ON CONFLICT (item_id, market) DO UPDATE SET
                    shop = EXCLUDED.shop,
                    category = EXCLUDED.category,
                    cl0 = EXCLUDED.cl0,
                    campaign_filter = EXCLUDED.campaign_filter,
                    status = EXCLUDED.status,
                    targets = EXCLUDED.targets,
                    last_result = EXCLUDED.last_result,
                    source = EXCLUDED.source,
                    plp_url = COALESCE(EXCLUDED.plp_url, dma_exclusions.plp_url),
                    headline_shop = COALESCE(EXCLUDED.headline_shop, dma_exclusions.headline_shop),
                    applied_at = now(),
                    enabled_at = NULL
                RETURNING id
            """, (item_id, market, shop, category, cl0, campaign_filter, status,
                  json.dumps(targets), json.dumps(last_result), source, plp_url,
                  headline_shop))
            rid = cur.fetchone()["id"]
        conn.commit()
        return rid
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)


def _get_record(record_id: int) -> Optional[dict]:
    _ensure_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM dma_exclusions WHERE id = %s", (record_id,))
            return cur.fetchone()
    finally:
        return_db_connection(conn)


def _update_status(record_id: int, status: str, result_patch: dict):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE dma_exclusions
                SET status = %s,
                    enabled_at = CASE WHEN %s = 'enabled' THEN now() ELSE enabled_at END,
                    last_result = COALESCE(last_result, '{}'::jsonb) || %s::jsonb
                WHERE id = %s
            """, (status, status, json.dumps(result_patch), record_id))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)


def cleanup_enabled(market: str) -> Dict[str, Any]:
    """Delete resolved (status='enabled') records for a market — bookkeeping only;
    they're already reverted in Google Ads, so this just clears history rows."""
    _ensure_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM dma_exclusions WHERE market = %s AND status = 'enabled'",
                        (market.upper(),))
            deleted = cur.rowcount
        conn.commit()
        return {"market": market.upper(), "deleted": deleted}
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db_connection(conn)


def list_exclusions() -> List[dict]:
    _ensure_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, item_id, market, shop, category, cl0, campaign_filter,
                       status, source, plp_url, headline_shop, created_at,
                       applied_at, enabled_at,
                       COALESCE(jsonb_array_length(targets), 0) AS target_count
                FROM dma_exclusions
                ORDER BY applied_at DESC NULLS LAST, id DESC
            """)
            rows = cur.fetchall()
        for r in rows:
            for k in ("created_at", "applied_at", "enabled_at"):
                if r.get(k):
                    r[k] = r[k].isoformat()
        return rows
    finally:
        return_db_connection(conn)


def backfill_headline_shops(only_missing: bool = True) -> Dict[str, Any]:
    """Populate `headline_shop` for existing saved exclusions from the live ES
    index — a one-shot fix for rows created before the column existed.

    Caveat: headline_offer() returns the *current* bestOffer, so a backfilled
    value reflects the headline as of the backfill, not as of the original
    exclusion. That's the best we can reconstruct (ES keeps no history), and it's
    still the honest "who holds the headline now vs the feed shop" comparison.

    Best-effort per row: an ES miss (or a product gone from the index) leaves the
    row unchanged rather than overwriting a good value with None. `only_missing`
    (default) skips rows that already carry a headline shop.
    """
    _ensure_table()
    rows = list_exclusions()
    todo = [r for r in rows if not (only_missing and r.get("headline_shop"))]
    if not todo:
        return {"scanned": len(rows), "eligible": 0, "updated": 0, "unresolved": 0}

    def _resolve(r):
        iid = r["item_id"]
        ean = iid[len(DMA_ITEM_PREFIX):] if iid.startswith(DMA_ITEM_PREFIX) else iid
        try:
            return r["id"], headline_offer(ean).get("headline_shop")
        except Exception:  # noqa: BLE001 - one bad row must not sink the batch
            return r["id"], None

    with ThreadPoolExecutor(max_workers=16) as ex:
        resolved = list(ex.map(_resolve, todo))

    updates = [(hs, rid) for rid, hs in resolved if hs]
    if updates:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE dma_exclusions SET headline_shop = %s WHERE id = %s",
                    updates)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            return_db_connection(conn)

    return {"scanned": len(rows), "eligible": len(todo),
            "updated": len(updates), "unresolved": len(todo) - len(updates)}


def exclusion_targets(record_id: int) -> Dict[str, Any]:
    """The campaigns/ad-groups a saved exclusion touched: the successful
    `targets` plus any per-target `errors` from the last apply/enable run
    (so the UI can flag which campaigns it failed on)."""
    _ensure_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT item_id, targets, last_result FROM dma_exclusions WHERE id = %s",
                        (record_id,))
            row = cur.fetchone()
        if not row:
            raise ValueError(f"exclusion {record_id} not found")
        last_result = row.get("last_result") or {}
        return {
            "targets": row.get("targets") or [],
            "errors": last_result.get("errors") or [],
        }
    finally:
        return_db_connection(conn)


# ---------------------------------------------------------------------------
# OOS (out-of-stock) integration
#
# The OOS crawl-override monitor lists offers Google flagged out of stock that
# are still live on beslist.nl. Its product_id_v3 is an opaque per-shop key and
# does NOT match DMA; the bridge is the GTIN -> DMA item id `nl-nl-gold-<gtin>`
# (verified). We expose the actionable candidates (live in DMA, with their 30d
# spend/clicks/conversions so the operator can avoid pulling profitable items),
# let them exclude a selected set, and re-enable EANs that have recovered. The
# monitor's `exclude-eans` list is authoritative (cheapest + still-live + Google-
# OOS + confirmed fresh), so recovery is pure set-membership: an excluded EAN that
# has dropped off the current list is safe to put back on. See oos_recovered().
# ---------------------------------------------------------------------------

def _exclude_eans(market: str) -> Dict[str, Any]:
    """Fetch the monitor's authoritative exclude list for a market.

    Single source of truth (replaces the old /oos-eans + /by-eans pair): every
    EAN returned is guaranteed to be the cheapest, still-live offer on beslist
    that Google currently flags out of stock, confirmed within ~2 days. Anything
    that fails one of those never appears, so "not on the list" safely means "put
    it back on".

    Returns {healthy, as_of, count, eans}. `healthy` False means the monitor's
    snapshot is stale/degraded — callers MUST NOT act on an EAN's *absence*
    (i.e. must not re-enable) when the list can't be trusted.
    """
    country = (market or "NL").upper()
    qs = urllib.parse.urlencode({"country": country})
    url = f"{OOS_BASE}/exclude-eans?{qs}"
    with urllib.request.urlopen(url, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return {"healthy": bool(data.get("healthy")),
            "as_of": data.get("as_of"),
            "count": data.get("count"),
            "eans": list(data.get("eans") or [])}


def _campaign_family(name: str) -> str:
    if name == BESTSELLERS_CAMPAIGN:
        return "bestsellers"
    if name == APLUS_CAMPAIGN:
        return "aplus"
    if _CATEGORY_RE.match(name):
        return "category"
    return "other"


# Transient Google Ads server-side errors — safe to retry the read-only query.
_GA_TRANSIENT = (
    google_exceptions.InternalServerError,
    google_exceptions.ServiceUnavailable,
    google_exceptions.DeadlineExceeded,
    google_exceptions.TooManyRequests,
)


def _ga_search_rows(ga, customer_id: str, query: str, attempts: int = 3) -> list:
    """Run a GAQL search and materialise its rows, retrying transient 5xx/timeout
    errors with backoff — a single batch 500 shouldn't crash a whole multi-batch
    OOS scan (which now walks many batches to reach the headline-match limit)."""
    for n in range(1, attempts + 1):
        try:
            return list(ga.search(customer_id=customer_id, query=query))
        except _GA_TRANSIENT as e:
            if n == attempts:
                raise
            logger.warning("GA search transient error (attempt %d/%d): %s", n, attempts, e)
            time.sleep(1.5 * n)
    # unreachable with attempts >= 1, but never silently return None (callers
    # iterate the result, so None would raise an unhelpful TypeError downstream).
    raise ValueError(f"_ga_search_rows called with attempts={attempts} (< 1)")


# How many GA batch queries to run concurrently. shopping_performance_view is
# slow (~25s/batch), so this is the scan's dominant cost; a handful of parallel
# searches cuts the GA phase ~N× without tripping GA's concurrency limits.
_GA_BATCH_CONCURRENCY = 6
_OOS_BATCH = 200  # EANs per GA query (product_item_id IN (...))


def _ga_batch_agg(ga, customer_id: str, batch: List[str]) -> Dict[str, dict]:
    """Return item_id -> aggregated 30d metrics for the live-in-DMA EANs in this
    batch. Each EAN lives in exactly one batch, so the agg fully aggregates it.
    Cached per-EAN (TTL): only the uncached EANs are actually queried, and EANs
    queried-but-not-live are cached as None so a re-scan skips them too."""
    fresh: Dict[str, dict] = {}
    to_query = []
    for e in batch:
        hit = _cache_get(_GA_CACHE, e)
        if hit is _CACHE_MISS:
            to_query.append(e)
        elif hit is not None:
            fresh[DMA_ITEM_PREFIX + e] = hit   # cached live
        # hit is None -> cached not-live, skip
    if not to_query:
        return fresh

    ids = [DMA_ITEM_PREFIX + e for e in to_query]
    q = (
        "SELECT segments.product_item_id, campaign.name, "
        "segments.product_custom_attribute0, segments.product_custom_attribute3, "
        "metrics.clicks, metrics.impressions, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        "FROM shopping_performance_view "
        f"WHERE segments.product_item_id IN ({','.join(repr(x) for x in ids)}) "
        "AND segments.date DURING LAST_30_DAYS"
    )
    agg: Dict[str, dict] = {}
    for row in _ga_search_rows(ga, customer_id, q):
        iid = row.segments.product_item_id
        m = row.metrics
        a = agg.setdefault(iid, {"clicks": 0, "impr": 0, "cost": 0,
                                 "conv": 0.0, "conv_value": 0.0, "campaigns": set(),
                                 "cl0s": set(), "shops": set()})
        a["clicks"] += m.clicks
        a["impr"] += m.impressions
        a["cost"] += m.cost_micros
        a["conv"] += m.conversions
        a["conv_value"] += m.conversions_value
        if row.campaign.name:
            a["campaigns"].add(row.campaign.name)
        cl0 = row.segments.product_custom_attribute0
        if cl0 and cl0.isdigit():
            a["cl0s"].add(cl0)
        if row.segments.product_custom_attribute3:
            a["shops"].add(row.segments.product_custom_attribute3)
    # cache results for every queried EAN (live -> agg, not-live -> None)
    for e in to_query:
        _cache_put(_GA_CACHE, e, agg.get(DMA_ITEM_PREFIX + e))
    fresh.update(agg)
    return fresh


def _build_oos_candidate(market: str, iid: str, a: dict, excluded: set) -> dict:
    """Build one live-in-DMA candidate from its aggregated serving metrics, and
    pre-cache the resolution so a follow-up exclude skips the ~6s lookup()."""
    camps = sorted(a["campaigns"])
    fams = {_campaign_family(c) for c in camps}
    cat = _pick_category([m.group("cat") for c in camps if (m := _CATEGORY_RE.match(c))])
    cl0 = min(a["cl0s"]) if a["cl0s"] else None      # deterministic (sorted), not row-order
    shop = min(a["shops"]) if a["shops"] else None
    _cache_resolution({
        "item_id": iid, "market": market.upper(), "found": True,
        "category": cat, "cl0": cl0, "shop": shop,
        "serving_campaigns": camps, "note": None,
    })
    return {
        "item_id": iid,
        "ean": iid[len(DMA_ITEM_PREFIX):],
        "category": cat,
        "shop": shop,
        "plp_url": None,       # filled in by oos_scan for the final (capped) candidate set
        "headline_shop": None,  # same — the live bestOffer shop from ES
        "campaigns": camps,
        "uncovered_campaigns": sorted(c for c in camps if _campaign_family(c) == "other"),
        "fully_covered": "other" not in fams,
        "clicks": int(a["clicks"]),
        "impressions": int(a["impr"]),
        "cost_eur": round(a["cost"] / 1e6, 2),
        "conversions": round(a["conv"], 1),
        "conv_value_eur": round(a["conv_value"], 2),
        "already_excluded": iid in excluded,
    }


def oos_scan(market: str, limit: Optional[int] = None) -> Dict[str, Any]:
    """Return OOS EANs that are live in DMA, with 30d clicks/spend/conversions
    and the campaigns they serve in. READ-ONLY (never re-enables).

    The monitor's exclude-eans list guarantees every EAN on it is the cheapest,
    still-live offer on beslist that Google currently flags OOS (confirmed within
    ~2 days), so every OOS EAN that is live in DMA is a safe exclusion candidate —
    no per-EAN headline re-verification is needed. `limit` caps how many candidates
    to collect: OOS EANs are scanned in GA batches, stopping once `limit` live-in-DMA
    candidates are found (most OOS EANs aren't live in DMA, so the prefix scanned is
    typically many × the limit). None scans the whole list.
    """
    snap = _exclude_eans(market)
    eans = snap["eans"]
    oos_total = len(eans)
    client = _get_client()
    customer_id = _customer_id(market)
    ga = client.get_service("GoogleAdsService")

    # which item ids are already excluded (so the UI can disable them)
    excluded = {r["item_id"] for r in list_exclusions()
                if r["market"] == market.upper() and r["status"] in ("excluded", "partial")}

    # Run the slow GA serving queries in concurrent waves, collecting live-in-DMA
    # candidates until `limit` is reached. Pipeline: prefetch wave N+1's GA while
    # wave N is turned into candidates. At most one wave's GA is in flight at a time.
    batches = [eans[i:i + _OOS_BATCH] for i in range(0, len(eans), _OOS_BATCH)]
    waves = [batches[w:w + _GA_BATCH_CONCURRENCY]
             for w in range(0, len(batches), _GA_BATCH_CONCURRENCY)]
    candidates: List[dict] = []
    scanned = 0
    ga_pool = ThreadPoolExecutor(max_workers=_GA_BATCH_CONCURRENCY)
    try:
        def submit_wave(wv):  # submit a wave's batches as GA futures (non-blocking)
            return [ga_pool.submit(_ga_batch_agg, ga, customer_id, b) for b in wv]

        pending = submit_wave(waves[0]) if waves else []
        for wi, wv in enumerate(waves):
            aggs = [f.result() for f in pending]            # gather this wave's GA
            scanned += sum(len(b) for b in wv)
            candidates.extend(_build_oos_candidate(market, iid, a, excluded)
                              for agg in aggs for iid, a in agg.items())
            if limit and len(candidates) >= limit:
                break                                       # don't prefetch a wave we won't use
            # Prefetch the NEXT wave only once we know we still need it — keeps the
            # GA pipeline full on a full scan, but on a capped scan avoids kicking
            # off (and blocking on) a whole extra wave that cancel_futures can't stop.
            pending = submit_wave(waves[wi + 1]) if wi + 1 < len(waves) else []
    finally:
        ga_pool.shutdown(wait=False, cancel_futures=True)   # drop any outstanding prefetch

    # Every live-in-DMA EAN is a safe exclusion candidate now (the monitor's list
    # guarantees cheapest + still-live + Google-OOS + fresh), so just rank by 30d
    # spend and cap to `limit`.
    candidates.sort(key=lambda c: c["cost_eur"], reverse=True)
    if limit:
        candidates = candidates[:limit]

    # Enrich the final (capped) set with the product's PLP url. This is a separate
    # source from the GA scan — the ES headline lookup — so it's fetched only for
    # the candidates we actually return, in parallel (cached, warm ~30ms each).
    if candidates:
        def _headline_safe(c):
            # A single ES hiccup must not discard the whole (minutes-long) GA scan.
            try:
                return headline_offer(c["ean"])
            except Exception:  # noqa: BLE001
                return {}
        with ThreadPoolExecutor(max_workers=16) as es_pool:
            hos = es_pool.map(_headline_safe, candidates)
            for c, ho in zip(candidates, hos):
                ho = ho or {}
                c["plp_url"] = ho.get("plp_url")
                c["headline_shop"] = ho.get("headline_shop")

    return {
        "market": market.upper(),
        "healthy": snap["healthy"],
        "as_of": snap["as_of"],
        "oos_total": oos_total,
        "scanned": scanned,
        "live_in_dma": len(candidates),
        "totals": {
            "clicks": sum(c["clicks"] for c in candidates),
            "cost_eur": round(sum(c["cost_eur"] for c in candidates), 2),
            "conversions": round(sum(c["conversions"] for c in candidates), 1),
            "conv_value_eur": round(sum(c["conv_value_eur"] for c in candidates), 2),
        },
        "candidates": candidates,
    }


def _resolve_ad_group_target(client, customer_id, item_id, ref_target, cl0) -> dict:
    """Re-read ONE ad group's tree and rebuild its target fresh.

    Used when serializing several items through the same ad group: an earlier
    item may have converted the leaf (biddable UNIT -> item-id SUBDIVISION), so a
    later item must re-resolve to append its negative under the new subdivision
    instead of trying to subdivide the (now-gone) unit again. Mirrors the per-ad-
    group work resolve_targets() does inline, reusing the same leaf finders.
    """
    ad_group_id = ref_target["ad_group_id"]
    nodes = _read_tree(client, customer_id, ad_group_id)
    kind = ref_target["kind"]
    if kind == "category":
        leaf = _leaf_for_category(nodes)
    elif kind == "bestsellers":
        leaf = _bestsellers_subdiv(nodes)
    elif kind == "aplus":
        leaf = _leaf_for_aplus(nodes, cl0) if cl0 else None
    else:
        leaf = None
    campaign = {"campaign_id": ref_target["campaign_id"], "campaign_name": ref_target["campaign_name"]}
    ad_group = {"ad_group_id": ad_group_id, "ad_group_name": ref_target["ad_group_name"]}
    return _build_target(item_id, kind, campaign, ad_group, nodes, leaf)


def oos_exclude(item_ids: List[str], market: str) -> Dict[str, Any]:
    """Exclude a selected list of OOS item ids (source-tagged 'oos').

    The monitor's exclude list is authoritative — every EAN a Scan surfaced is
    guaranteed to be the cheapest, still-live, Google-OOS headline offer — so the
    selected ids are excluded directly, with no server-side re-verification.

    Fast path (vs the old per-item serial apply() loop): resolve every item's
    targets concurrently (read-only, safe), then execute grouped BY AD GROUP.
    Distinct ad groups run in parallel; items sharing an ad group run serially
    (the 2nd+ re-resolved against a fresh tree) so the shared bestsellers/APlus/
    category trees never race. Each item's live mutation is identical to what the
    old sequential apply() produced — only ordering/concurrency changed.
    """
    client = _get_client()
    customer_id = _customer_id(market)
    # Dedup while preserving order; drop blanks.
    seen: "OrderedDict[str, None]" = OrderedDict()
    for iid in item_ids:
        iid = (iid or "").strip()
        if iid:
            seen.setdefault(iid, None)
    ids = list(seen.keys())
    if not ids:
        return {"market": market.upper(), "processed": 0, "results": []}

    # --- Phase A: resolve every item concurrently (READ-ONLY). resolve_targets
    # hits _RES_CACHE warmed by oos_scan, so this is cheap after a scan. ----------
    def _resolve(iid):
        try:
            return iid, resolve_targets(iid, market), None
        except Exception as e:  # noqa: BLE001
            logger.exception("oos_exclude resolve failed for %s", iid)
            return iid, None, str(e)

    with ThreadPoolExecutor(max_workers=min(16, len(ids))) as ex:
        resolved = list(ex.map(_resolve, ids))

    per_item: "OrderedDict[str, dict]" = OrderedDict()
    groups: Dict[str, list] = defaultdict(list)  # ad_group_id -> [(iid, target, cl0)]
    for iid, plan, err in resolved:
        pi = {"plan": plan, "resolve_error": err, "results": [], "errors": []}
        per_item[iid] = pi
        if plan is None:
            continue
        cl0 = plan["resolution"].get("cl0")
        for t in plan["targets"]:
            if t["action"] in ("append_negative", "subdivide_and_exclude"):
                groups[t["ad_group_id"]].append((iid, t, cl0))
            else:
                pi["results"].append({**t, "result": "skipped", "reason": t.get("skip_reason")})

    # --- Phase B: execute ad groups concurrently, items serial within a group ----
    def _run_group(items):
        out = []  # (iid, kind, payload) with kind in {"ok","err","skip"}
        for i, (iid, ref_t, cl0) in enumerate(items):
            try:
                # 1st item's resolved target is still fresh (nothing has mutated
                # this ad group yet); 2nd+ re-resolve to see the new subdivision.
                t = ref_t if i == 0 else _resolve_ad_group_target(client, customer_id, iid, ref_t, cl0)
                if t["action"] in ("append_negative", "subdivide_and_exclude"):
                    rev = _apply_one_target(client, customer_id, iid, t)
                    out.append((iid, "ok", {**rev, "result": "excluded"}))
                else:
                    out.append((iid, "skip", {**t, "result": "skipped", "reason": t.get("skip_reason")}))
            except Exception as e:  # noqa: BLE001
                logger.exception("oos_exclude apply failed for %s / %s", iid, ref_t.get("campaign_name"))
                out.append((iid, "err", {"campaign_name": ref_t["campaign_name"],
                                         "ad_group_id": ref_t["ad_group_id"], "error": str(e)}))
        return out

    if groups:
        with ThreadPoolExecutor(max_workers=min(16, len(groups))) as ex:
            for grp_out in ex.map(_run_group, list(groups.values())):
                for iid, kind, payload in grp_out:
                    per_item[iid]["errors" if kind == "err" else "results"].append(payload)

    # --- PLP enrichment (best-effort, warm ES from scan), parallel --------------
    live_ids = [iid for iid, pi in per_item.items() if pi["plan"] is not None]

    def _headline(iid):
        ean = iid[len(DMA_ITEM_PREFIX):] if iid.startswith(DMA_ITEM_PREFIX) else iid
        try:
            return iid, headline_offer(ean)
        except Exception:  # noqa: BLE001 - never fail an exclude over the ES lookup
            return iid, {}

    headlines: Dict[str, dict] = {}
    if live_ids:
        with ThreadPoolExecutor(max_workers=16) as ex:
            headlines = dict(ex.map(_headline, live_ids))

    # --- Phase C: persist per item + build response -----------------------------
    results = []
    for iid, pi in per_item.items():
        if pi["plan"] is None:
            results.append({"item_id": iid, "error": pi["resolve_error"] or "resolution failed"})
            continue
        plan = pi["plan"]
        ho = headlines.get(iid) or {}
        out = _persist_apply(
            item_id=iid, market=market, shop=plan["resolution"].get("shop"),
            resolution=plan["resolution"], campaign_filter=None,
            results=pi["results"], errors=pi["errors"],
            warnings=plan["warnings"], plp_url=ho.get("plp_url"),
            headline_shop=ho.get("headline_shop"), source="oos")
        results.append({"item_id": iid, "id": out["id"], "applied": out["applied"],
                        "errors": out["errors"]})
    return {"market": market.upper(), "processed": len(results), "results": results}


def oos_recovered(market: str) -> List[dict]:
    """OOS-sourced exclusions that are safe to re-enable.

    The monitor's `exclude-eans` list is authoritative: it holds exactly the EANs
    that are still the cheapest, still-live, Google-OOS headline offer. So this is
    pure set-membership over our currently-excluded EANs (a DMA exclusion is
    GTIN-level, ``nl-nl-gold-<ean>``):
      * EAN still on the list -> still the served OOS headline -> KEEP excluded
      * EAN absent from list  -> back in stock / a rival is now cheaper / gone from
                                 beslist -> safe to re-enable

    Guard: if the monitor reports the snapshot is not ``healthy``, an EAN's absence
    can't be trusted (the list may be stale/degraded), so re-enable NOTHING rather
    than risk restoring a still-OOS ad. Re-enabling is otherwise the safe direction
    — a product that has genuinely gone won't serve anyway.
    """
    country = market.upper()
    rows = [r for r in list_exclusions()
            if r["market"] == country and r.get("source") == "oos"
            and r["status"] in ("excluded", "partial")]
    if not rows:
        return []
    snap = _exclude_eans(market)
    if not snap["healthy"]:
        logger.warning("oos_recovered[%s]: monitor snapshot not healthy (as_of=%s); "
                       "re-enabling nothing", country, snap.get("as_of"))
        return []
    active = {_norm_ean(e) for e in snap["eans"]}
    out: List[dict] = []
    kept = 0
    for r in rows:
        iid = r["item_id"]
        ean = iid[len(DMA_ITEM_PREFIX):] if iid.startswith(DMA_ITEM_PREFIX) else iid
        if _norm_ean(ean) in active:
            kept += 1  # still the served OOS headline -> keep excluded
        else:
            out.append(r)  # dropped off the list -> recovered / gone -> re-enable
    logger.info("oos_recovered[%s]: %d still OOS headline (kept), %d to re-enable",
                country, kept, len(out))
    return out


def oos_reenable(market: str) -> Dict[str, Any]:
    """Re-enable every recovered OOS exclusion for a market.

    enable() calls run concurrently: each is independent, and _revert_one holds
    the per-ad-group lock, so two records that share an ad group (e.g. Amazon
    bestsellers) serialize on it and each reads a fresh tree — the sole-negative
    collapse stays correct — while records on disjoint ad groups run in parallel.
    """
    recovered = oos_recovered(market)
    if not recovered:
        return {"market": market.upper(), "recovered": 0, "results": []}

    def _one(r):
        try:
            return enable(r["id"])
        except Exception as e:  # noqa: BLE001
            logger.exception("oos_reenable failed for %s", r["id"])
            return {"id": r["id"], "error": str(e)}

    with ThreadPoolExecutor(max_workers=min(8, len(recovered))) as ex:
        results = list(ex.map(_one, recovered))
    return {"market": market.upper(), "recovered": len(recovered), "results": results}
