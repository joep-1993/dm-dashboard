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
import json
import time
import logging
import urllib.parse
import urllib.request
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
# A 'match' whose Google crawl-OOS verdict hasn't been refreshed in this many
# days is cautioned (kept selectable, but not auto-picked by Select-all): the
# crawl may be stale and the offer already back in stock. Tune as needed.
CRAWL_STALE_DAYS = 3

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
    """Resolve the headline (bestOffer) for the product carrying this EAN.

    Returns {status, headline_ean, headline_shop, headline_stock, plp_url,
    shop_stock} where status:
      match       — the OOS EAN *is* the headline offer's EAN (safe to exclude)
      differs     — the headline is a different EAN/shop (do NOT exclude)
      no_headline — product found but no bestOffer (can't confirm)
      not_found   — no product in ES for this EAN (likely gone)
      error       — ES lookup failed (transient; don't fail-closed on it)
    shop_stock maps _norm_shop(name) -> max in-stock count seen for that shop in
    beslist's index (used to veto a stale crawl-OOS match — see _es_shop_instock).
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

    # An EAN can resolve to several productidv3 docs; collect every bestOffer
    # together with the PLP url of the doc it came from, and tally per-shop stock.
    headlines = []
    shop_stock: Dict[str, int] = {}
    for h in hits:
        src = h.get("_source", {})
        plp = src.get("plpUrl")
        for shop in src.get("shops", []) or []:
            nm = _norm_shop(shop.get("name"))
            for off in shop.get("offers", []) or []:
                if off.get("bestOffer"):
                    headlines.append((off, shop, plp))
                st = off.get("stock")
                if nm and isinstance(st, (int, float)):
                    shop_stock[nm] = max(shop_stock.get(nm, 0), int(st))

    if not headlines:
        return {"status": "no_headline", "headline_ean": None, "headline_shop": None,
                "headline_stock": None, "shop_stock": shop_stock,
                "plp_url": _plp_url(hits[0].get("_source", {}).get("plpUrl"))}

    # Prefer a headline whose EAN equals the OOS EAN — that's a confirmed match.
    match = next(((o, s, p) for (o, s, p) in headlines
                  if o.get("ean") and _norm_ean(o["ean"]) == n), None)
    off, shop, plp = match or headlines[0]
    return {
        "status": "match" if match else "differs",
        "headline_ean": off.get("ean"),
        "headline_shop": shop.get("name"),
        "headline_stock": off.get("stock"),
        "plp_url": _plp_url(plp),
        "shop_stock": shop_stock,
    }


def _headline_offers(eans: List[str]) -> Dict[str, Dict[str, Any]]:
    """headline_offer() for many EANs concurrently (pool caps real ES load)."""
    eans = list(dict.fromkeys(eans))  # dedupe, order-preserving
    if not eans:
        return {}
    with ThreadPoolExecutor(max_workers=16) as ex:
        return dict(zip(eans, ex.map(headline_offer, eans)))


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

def _get_client() -> GoogleAdsClient:
    config = {
        "developer_token": os.environ.get("GOOGLE_DEVELOPER_TOKEN", ""),
        "refresh_token": os.environ.get("GOOGLE_REFRESH_TOKEN", ""),
        "client_id": os.environ.get("GOOGLE_CLIENT_ID", ""),
        "client_secret": os.environ.get("GOOGLE_CLIENT_SECRET", ""),
        "login_customer_id": os.environ.get("GOOGLE_LOGIN_CUSTOMER_ID", MCC_CUSTOMER_ID),
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config)


def _customer_id(market: str) -> str:
    cid = ACCOUNTS.get((market or "").upper())
    if not cid:
        raise ValueError(f"Unknown market {market!r}; expected one of {list(ACCOUNTS)}")
    return cid


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

    return {
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


def _pick_category(cands: List[str]) -> Optional[str]:
    """Prefer a real product-category trio (PLA/Koffiezetapparaten_a) over a
    "<shop> store" allow-list campaign (PLA/Koffie store_a): the store campaign
    also matches _CATEGORY_RE and would otherwise shadow the real category,
    leaving the product excluded only via APlus/bestsellers (its CL3-OTHERS leaf
    is negative, so the trio gets skipped). Store campaigns are deferred."""
    non_store = [c for c in cands if not c.lower().endswith(" store")]
    return (non_store or cands or [None])[0]


# Resolution cache: lookup() runs a ~6s shopping_performance_view query, and it
# dominates each exclusion. oos_scan already does ONE batched query over all
# candidates, so it pre-populates this cache; resolve_targets then skips the
# per-EAN lookup. Short TTL + graceful fallback to lookup() keep it safe/fresh.
_RES_CACHE: Dict[tuple, tuple] = {}
_RES_TTL = 1800.0  # 30 min
_RES_MAX = 5000    # bound the cache; evict the oldest entries past this

def _cache_resolution(res: dict) -> None:
    if len(_RES_CACHE) >= _RES_MAX:
        for k in [k for k, _ in sorted(_RES_CACHE.items(), key=lambda kv: kv[1][1])[:_RES_MAX // 10]]:
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


def _build_target(client, customer_id, item_id, kind, campaign, ad_group, nodes, leaf) -> dict:
    """Assemble a per-ad-group target with the planned action, given its leaf node."""
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
            return _build_target(client, customer_id, item_id, "category", c, ag, nodes, leaf)

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
                out.append(_build_target(client, customer_id, item_id, "bestsellers", c, ag, nodes, leaf))
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
                out.append(_build_target(client, customer_id, item_id, "aplus", c, ag, nodes, leaf))
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
    """Execute a single target's planned write. Returns reversal metadata."""
    agc = client.get_service("AdGroupCriterionService")
    ad_group_id = target["ad_group_id"]
    temp = _Temp()
    rev = dict(target)  # carry kind/campaign/ad_group/leaf for enable
    leaf = target["leaf"]

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


def apply(item_id: str, market: str, shop: Optional[str] = None,
          campaign_filter: Optional[str] = None, source: str = "manual") -> Dict[str, Any]:
    """Apply the exclusion live, then persist it for later re-enable."""
    item_id = (item_id or "").strip()
    client = _get_client()
    customer_id = _customer_id(market)
    plan = resolve_targets(item_id, market, campaign_filter)
    shop = shop or plan["resolution"].get("shop")

    results: List[dict] = []
    errors: List[dict] = []
    for t in plan["targets"]:
        if t["action"] not in ("append_negative", "subdivide_and_exclude"):
            results.append({**t, "result": "skipped", "reason": t.get("skip_reason")})
            continue
        try:
            rev = _apply_one_target(client, customer_id, item_id, t)
            results.append({**rev, "result": "excluded"})
        except Exception as e:  # noqa: BLE001 - surface per-target failures, keep going
            logger.exception("apply failed for %s", t.get("campaign_name"))
            errors.append({"campaign_name": t["campaign_name"],
                           "ad_group_id": t["ad_group_id"], "error": str(e)})

    applied = [r for r in results if r.get("result") == "excluded"]
    # Resolve the product PLP url (best-effort) so the Saved list can link the item id.
    ean = item_id[len(DMA_ITEM_PREFIX):] if item_id.startswith(DMA_ITEM_PREFIX) else item_id
    try:
        plp_url = headline_offer(ean).get("plp_url")
    except Exception:  # noqa: BLE001 - never fail an apply over the PLP lookup
        plp_url = None
    # excluded = all actionable targets succeeded; partial = some applied but
    # others errored (don't hide the failures); failed = nothing applied.
    status = "excluded" if applied and not errors else "partial" if applied else "failed"
    record_id = _save_record(
        item_id=item_id, market=market.upper(), shop=shop,
        category=plan["resolution"].get("category"), cl0=plan["resolution"].get("cl0"),
        campaign_filter=campaign_filter,
        status=status,
        plp_url=plp_url,
        targets=applied,
        last_result={"applied": len(applied), "errors": errors, "warnings": plan["warnings"]},
        source=source,
    )
    return {
        "id": record_id,
        "item_id": item_id,
        "market": market.upper(),
        "applied": len(applied),
        "skipped": sum(1 for r in results if r.get("result") == "skipped"),
        "errors": errors,
        "warnings": plan["warnings"],
        "targets": results,
    }


def enable(record_id: int) -> Dict[str, Any]:
    """Re-enable a previously-excluded product: remove negatives and prune."""
    rec = _get_record(record_id)
    if rec is None:
        raise ValueError(f"Exclusion #{record_id} not found")
    if rec["status"] == "enabled":
        return {"id": record_id, "status": "already_enabled", "reverted": 0}

    client = _get_client()
    customer_id = _customer_id(rec["market"])
    agc = client.get_service("AdGroupCriterionService")
    item_id = rec["item_id"]

    reverted: List[dict] = []
    errors: List[dict] = []
    for t in rec.get("targets") or []:
        ad_group_id = t["ad_group_id"]
        try:
            nodes = _read_tree(client, customer_id, ad_group_id)
            # locate the subdivision the item lives under
            if t.get("created_subdivision"):
                subdiv = nodes.get(t.get("subdivision_resource")) or _relocate_subdiv(nodes, t)
            else:
                subdiv = nodes.get(t["leaf"]["resource"]) or _relocate_subdiv(nodes, t)
            if subdiv is None:
                errors.append({"campaign_name": t["campaign_name"], "error": "subdivision not found (already changed?)"})
                continue

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
            reverted.append({"campaign_name": t["campaign_name"], "ad_group_id": ad_group_id})
        except Exception as e:  # noqa: BLE001
            logger.exception("enable failed for %s", t.get("campaign_name"))
            errors.append({"campaign_name": t["campaign_name"], "error": str(e)})

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
                 status, targets, last_result, source="manual", plp_url=None) -> int:
    _ensure_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dma_exclusions
                    (item_id, market, shop, category, cl0, campaign_filter,
                     status, targets, last_result, source, plp_url, applied_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
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
                    applied_at = now(),
                    enabled_at = NULL
                RETURNING id
            """, (item_id, market, shop, category, cl0, campaign_filter, status,
                  json.dumps(targets), json.dumps(last_result), source, plp_url))
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
                       status, source, plp_url, created_at, applied_at, enabled_at,
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


def exclusion_targets(record_id: int) -> List[dict]:
    """The campaigns/ad-groups a saved exclusion added the negative to."""
    _ensure_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT item_id, targets FROM dma_exclusions WHERE id = %s",
                        (record_id,))
            row = cur.fetchone()
        if not row:
            raise ValueError(f"exclusion {record_id} not found")
        return row.get("targets") or []
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
# let them exclude a selected set, and re-enable EANs that have genuinely
# recovered. Recovery is read from the monitor's explicit `recovered` state
# (back in stock per Google, lingers 7 days) — NOT inferred from an EAN simply
# leaving the active list, which can't distinguish a recovery from an offer we
# pulled off the site ourselves. See oos_recovered() for the precedence rule.
# ---------------------------------------------------------------------------

def _oos_eans(market: str, state: str = "active") -> List[str]:
    """Fetch the OOS EAN list for a market from the monitor API."""
    country = (market or "NL").upper()
    qs = urllib.parse.urlencode({"country": country, "state": state})
    url = f"{OOS_BASE}/oos-eans?{qs}"
    with urllib.request.urlopen(url, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return list(data.get("eans") or [])


# Per-EAN OOS-monitor enrichment tuning. Each lookup is one HTTP round-trip and
# is the scan's dominant cost after GA was parallelized. NB: raising the pool
# above 16 does NOT help — measured 0.255s/EAN at 32 vs 0.241s/EAN at 16, i.e.
# the monitor is server-bound (its own rate cap), not client-concurrency-bound,
# and more threads only risk tipping the flaky server into timeouts. The retry
# (not the concurrency) is the win here: a transient stall no longer drops the
# row to a stale ES fallback. Real speedup needs a bulk endpoint from the monitor.
_OOS_LOOKUP_CONCURRENCY = 16
_OOS_LOOKUP_ATTEMPTS = 2


def _clean_shop(name: Optional[str]) -> Optional[str]:
    """Override rows sometimes suffix the shop with the country (`Foo.nl|NL`)."""
    return name.split("|", 1)[0] if name else name


def _oos_offer(market: str, ean: str) -> List[dict]:
    """OOS override rows for one EAN, carrying the monitor's headline + freshness
    signals:
      - is_cheapest_offer  is this the cheapest (== served headline) offer?
      - ean_offer_count    how many comparable offers exist for the EAN
      - beslist_served     is the offer still served on beslist (False => gone)?
      - feed_stock         the shop feed's own stock count (>0 contradicts OOS)

    Fetched via the `q=` search on /api/v1/overrides — NOT /oos-products. The
    /oos-products worklist is a served-only top-2000 list, so EANs that are
    capped out OR whose offer has left beslist (beslist_served False) return
    nothing there and used to fall through to a stale ES bestOffer guess (which
    e.g. matched an in-stock offer from a different shop). The /api/v1/overrides
    q= search is uncapped, returns the row regardless of served-state, and still
    computes is_cheapest_offer. Returns [] when the EAN isn't flagged at all.
    """
    country = (market or "NL").upper()
    qs = urllib.parse.urlencode({"country": country, "state": "active",
                                 "q": ean, "page_size": 50, "latest_batch": "true"})
    url = f"{OOS_BASE}?{qs}"
    # The monitor occasionally hangs/times out (esp. under our own concurrency).
    # Retry with a tight per-attempt timeout so a stall fails fast and re-tries
    # rather than dropping the row to a stale ES fallback. Give up -> [] (no signal).
    data = None
    for n in range(1, _OOS_LOOKUP_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(url, timeout=12) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            break
        except Exception as e:  # noqa: BLE001 - transient; retry then treat as "no signal"
            if n == _OOS_LOOKUP_ATTEMPTS:
                logger.warning("OOS offer lookup failed for %s after %d attempts: %s",
                               ean, _OOS_LOOKUP_ATTEMPTS, e)
                return []
            time.sleep(0.4 * n)
    rows = data if isinstance(data, list) else (data.get("products") or [])
    n = _norm_ean(ean)
    return [{"is_cheapest_offer": p.get("is_cheapest_offer"),
             "ean_offer_count": p.get("ean_offer_count"),
             "shop_name": _clean_shop(p.get("shop_name")),
             "beslist_served": p.get("beslist_served"),
             "feed_stock": p.get("feed_stock"),
             "google_last_update": p.get("google_last_update")}
            for p in rows
            if _norm_ean(p.get("ean")) == n]


def _crawl_age_days(iso: Optional[str]) -> Optional[float]:
    """Days since Google last refreshed the crawl-OOS verdict (None if unknown)."""
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return None
    return round((datetime.now(timezone.utc) - dt).total_seconds() / 86400, 1)


def _oos_offers(market: str, eans: List[str]) -> Dict[str, List[dict]]:
    """_oos_offer() for many EANs concurrently, keyed by _norm_ean so lookups
    line up with the (zero-padded) DMA EANs."""
    eans = list(dict.fromkeys(eans))  # dedupe, order-preserving
    if not eans:
        return {}
    with ThreadPoolExecutor(max_workers=min(_OOS_LOOKUP_CONCURRENCY, len(eans))) as ex:
        rows = ex.map(lambda e: _oos_offer(market, e), eans)
    return {_norm_ean(e): r for e, r in zip(eans, rows)}


def _oos_cheapest_row(offers: List[dict]) -> Optional[dict]:
    """The served-headline (cheapest) OOS offer row for an EAN, if one is
    confirmed: is_cheapest_offer True, or the sole offer (null + count 1)."""
    for o in offers:
        if o.get("is_cheapest_offer") is True:
            return o
    for o in offers:
        if o.get("is_cheapest_offer") is None and o.get("ean_offer_count") == 1:
            return o
    return None


def _row_contradicted(o: dict) -> bool:
    """The monitor's own fresher signals contradict the (Google-crawl) OOS flag:
    the offer has left beslist, or the shop feed itself reports stock. Excluding
    on a stale crawl override would kill a buyable ad, so we keep these."""
    if o.get("beslist_served") is False:
        return True
    fs = o.get("feed_stock")
    return isinstance(fs, int) and fs > 0


def _oos_headline_status(offers: List[dict]) -> Optional[str]:
    """Derive the headline status for an EAN from its OOS offer rows.

    Per the monitor, is_cheapest_offer == the served headline offer (independent
    of stock), so:
      cheapest row, signals agree   -> 'match'   OOS offer IS the headline -> exclude
      cheapest row, contradicted    -> 'stale'   crawl OOS flag stale (feed in stock /
                                                  no longer served) -> keep
      else any explicit False       -> 'differs' a cheaper offer is the headline -> keep
      otherwise (no rows / null)     -> None      fall back to the ES check
    """
    if not offers:
        return None
    win = _oos_cheapest_row(offers)
    if win:
        return "stale" if _row_contradicted(win) else "match"
    if any(o.get("is_cheapest_offer") is False for o in offers):
        return "differs"
    return None


def _es_shop_instock(info: dict, shop_name: Optional[str]) -> bool:
    """beslist's product index shows this shop's offer in stock (>0). Used to
    veto a 'match' whose Google crawl-OOS flag is contradicted by live stock
    on the *same* shop (a different in-stock shop must NOT veto — the monitor
    says the flagged shop is the served headline)."""
    if not shop_name:
        return False
    st = (info.get("shop_stock") or {}).get(_norm_shop(shop_name))
    return isinstance(st, int) and st > 0


def _oos_verdict(offers: List[dict], info: dict) -> tuple:
    """Final headline verdict: the monitor's OOS signal, with an ES same-shop
    live-stock veto layered on top. Returns (status, win_row) where status is
    match | stale | differs | None (None => caller falls back to ES status)."""
    st = _oos_headline_status(offers)
    win = _oos_cheapest_row(offers)
    if st == "match" and _es_shop_instock(info, (win or {}).get("shop_name")):
        st = "stale"  # beslist index shows the flagged shop in stock
    return st, win


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


# How many GA batch queries to run concurrently. shopping_performance_view is
# slow (~25s/batch), so this is the scan's dominant cost; a handful of parallel
# searches cuts the GA phase ~N× without tripping GA's concurrency limits.
_GA_BATCH_CONCURRENCY = 6
_OOS_BATCH = 200  # EANs per GA query (product_item_id IN (...))


def _ga_batch_agg(ga, customer_id: str, batch: List[str]) -> Dict[str, dict]:
    """Run one GA serving-metrics query for a batch of EANs and return
    item_id -> aggregated 30d metrics + campaign/cl0/shop sets. Each EAN lives in
    exactly one batch, so the per-batch agg fully aggregates it."""
    ids = [DMA_ITEM_PREFIX + e for e in batch]
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
    return agg


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


def _enrich_oos_headline(market: str, candidates: List[dict]) -> None:
    """Annotate each candidate in-place with the headline verdict. The gold ad
    rides the product's headline (cheapest) offer; only an OOS EAN that IS that
    headline is a genuine exclusion. The monitor's is_cheapest_offer is the
    authority — match/stale shows the cheapest OOS offer itself (named), differs
    leaves shop/ean blank (a cheaper offer we can't name from the OOS rows);
    'stale' = cheapest but the crawl OOS flag is contradicted (feed/beslist in
    stock, or no longer served) → kept. ES supplies the PLP url and a fallback
    verdict for EANs with no monitor row."""
    if not candidates:
        return
    cand_eans = [c["ean"] for c in candidates]
    oos_offers = _oos_offers(market, cand_eans)
    hl = _headline_offers(cand_eans)                      # ES: PLP url (+ fallback verdict/shop)
    for c in candidates:
        info = hl.get(c["ean"], {})
        oos_rows = oos_offers.get(_norm_ean(c["ean"]), [])
        oos_st, win = _oos_verdict(oos_rows, info)
        if oos_st:
            status = oos_st
            c["headline_source"] = "oos"
            c["headline_shop"] = win["shop_name"] if win else None
            c["headline_ean"] = c["ean"] if win else None
            c["headline_stock"] = "out_of_stock" if status == "match" else None
        else:
            status = info.get("status")                   # no_headline|not_found|error
            c["headline_source"] = "es"
            c["headline_shop"] = info.get("headline_shop")
            c["headline_ean"] = info.get("headline_ean")
            c["headline_stock"] = info.get("headline_stock")
        c["headline_status"] = status
        c["headline_match"] = status == "match"
        # Flag a match that rests on a stale Google crawl with no contradicting
        # stock signal (the irreducible residual) so the UI can caution + skip it
        # from Select-all while keeping it individually excludable.
        age = _crawl_age_days((win or {}).get("google_last_update"))
        c["crawl_age_days"] = age
        c["stale_crawl"] = bool(status == "match" and age is not None
                                and age >= CRAWL_STALE_DAYS)
        c["plp_url"] = info.get("plp_url")


def oos_scan(market: str, limit: Optional[int] = None) -> Dict[str, Any]:
    """Return OOS EANs that are live in DMA, with 30d clicks/spend/conversions,
    the campaigns they serve in, and their headline verdict. READ-ONLY.

    `limit` caps how many *headline-offer (match)* candidates to collect: OOS
    EANs are scanned in batches and headline-checked per batch, stopping once
    `limit` of them are confirmed headline offers (the excludable ones). The
    differs/stale/unknown rows found along the way are kept for context. Most OOS
    EANs aren't live in DMA, and only ~half of those are headline offers, so the
    prefix scanned is typically many × the limit. None scans the whole list.
    """
    eans = _oos_eans(market, "active")
    oos_total = len(eans)
    client = _get_client()
    customer_id = _customer_id(market)
    ga = client.get_service("GoogleAdsService")

    # which item ids are already excluded (so the UI can disable them)
    excluded = {r["item_id"] for r in list_exclusions()
                if r["market"] == market.upper() and r["status"] in ("excluded", "partial")}

    # Run the slow GA serving queries in concurrent waves (the dominant cost),
    # headline-checking each wave's live-in-DMA EANs, and stop once `limit`
    # confirmed headline offers (status=match) are collected. A wave overshoots
    # the limit by at most ~one wave's matches (trimmed below).
    batches = [eans[i:i + _OOS_BATCH] for i in range(0, len(eans), _OOS_BATCH)]
    candidates: List[dict] = []
    matches = 0
    scanned = 0
    for w in range(0, len(batches), _GA_BATCH_CONCURRENCY):
        wave = batches[w:w + _GA_BATCH_CONCURRENCY]
        scanned += sum(len(b) for b in wave)
        with ThreadPoolExecutor(max_workers=len(wave)) as ex:
            aggs = list(ex.map(lambda b: _ga_batch_agg(ga, customer_id, b), wave))
        wave_cands = [_build_oos_candidate(market, iid, a, excluded)
                      for agg in aggs for iid, a in agg.items()]
        if wave_cands:
            _enrich_oos_headline(market, wave_cands)
            candidates.extend(wave_cands)
            matches += sum(1 for c in wave_cands if c["headline_match"])
        if limit and matches >= limit:
            break

    # Trim surplus matches beyond `limit` (the last batch may overshoot); keep all
    # non-match rows found so far for context.
    if limit and matches > limit:
        kept, m = [], 0
        for c in candidates:
            if c["headline_match"]:
                if m >= limit:
                    continue
                m += 1
            kept.append(c)
        candidates = kept

    candidates.sort(key=lambda c: c["cost_eur"], reverse=True)

    return {
        "market": market.upper(),
        "oos_total": oos_total,
        "scanned": scanned,
        "live_in_dma": len(candidates),
        "headline_counts": {
            "match": sum(1 for c in candidates if c["headline_status"] == "match"),
            "caution": sum(1 for c in candidates if c.get("stale_crawl")),
            "differs": sum(1 for c in candidates if c["headline_status"] == "differs"),
            "stale": sum(1 for c in candidates if c["headline_status"] == "stale"),
            "unknown": sum(1 for c in candidates
                           if c["headline_status"] in ("no_headline", "not_found", "error")),
        },
        "totals": {
            "clicks": sum(c["clicks"] for c in candidates),
            "cost_eur": round(sum(c["cost_eur"] for c in candidates), 2),
            "conversions": round(sum(c["conversions"] for c in candidates), 1),
            "conv_value_eur": round(sum(c["conv_value_eur"] for c in candidates), 2),
        },
        "candidates": candidates,
    }


def oos_exclude(item_ids: List[str], market: str) -> Dict[str, Any]:
    """Exclude a selected list of OOS item ids (source-tagged 'oos').

    Safety net: re-check the headline offer server-side and SKIP any EAN that
    shouldn't be killed, so a stale UI selection can't pull a buyable product.
    The monitor's is_cheapest_offer is the authority; ES is the fallback for EANs
    it has no offer-level row for. Blocked: 'differs' (a cheaper offer is the
    headline) and 'stale' (the crawl OOS flag is contradicted — feed in stock /
    no longer served on beslist). not_found / no_headline / unknown pass through
    (a gone product is a valid exclusion, and we don't fail-closed).
    """
    # Pull the offer-level cheapest signal and the ES fallback, each batched
    # across a worker pool instead of a serial ~20s lookup per EAN.
    eans = [iid[len(DMA_ITEM_PREFIX):] if iid.startswith(DMA_ITEM_PREFIX) else iid
            for iid in item_ids]
    oos_offers = _oos_offers(market, eans)
    headlines = _headline_offers(eans)
    results = []
    for iid in item_ids:
        ean = iid[len(DMA_ITEM_PREFIX):] if iid.startswith(DMA_ITEM_PREFIX) else iid
        info = headlines.get(ean) or headline_offer(ean)
        oos_st, _ = _oos_verdict(oos_offers.get(_norm_ean(ean), []), info)
        status = oos_st or info["status"]
        if status in ("differs", "stale"):
            if status == "stale":
                reason = ("OOS flag contradicted (offer in stock per beslist/feed, "
                          "or no longer served on beslist); not excluded")
            elif oos_st == "differs":
                reason = ("OOS API: a cheaper (in-stock) offer is the headline for "
                          "this EAN; not excluded")
            else:
                reason = (f"headline offer is a different EAN "
                          f"({info['headline_ean']} @ {info['headline_shop']}); not excluded")
            results.append({
                "item_id": iid, "skipped": True, "headline_status": status,
                "headline_source": "oos" if oos_st else "es", "reason": reason,
            })
            continue
        try:
            res = apply(iid, market, source="oos")
            results.append({"item_id": iid, "id": res["id"], "applied": res["applied"],
                            "errors": res["errors"]})
        except Exception as e:  # noqa: BLE001
            logger.exception("oos_exclude failed for %s", iid)
            results.append({"item_id": iid, "error": str(e)})
    skipped = sum(1 for r in results if r.get("skipped"))
    return {"market": market.upper(), "processed": len(results),
            "skipped": skipped, "results": results}


def oos_recovered(market: str) -> List[dict]:
    """OOS-sourced exclusions that are safe to re-enable because the GTIN has
    genuinely come back in stock.

    Recovery is judged at EAN level against the monitor's two states:
      - ``active``    EANs still flagged OOS & live (an ``open`` offer remains).
      - ``recovered`` EANs Google now reports back in stock (lingers 7 days).

    A DMA exclusion is GTIN-level (``nl-nl-gold-<ean>``) while the monitor is
    per shop-offer, so one EAN may map to several offers across shops. Applied
    in precedence order:
      * still in ``active``           -> an offer is still OOS    -> KEEP excluded
      * in ``recovered`` (not active) -> genuinely back in stock  -> re-enable
      * in neither                    -> offer vanished (we unpublished it or it
                                         aged off, NOT a recovery) -> leave
                                         excluded for manual review

    This replaces the old "dropped off the active list => recovered" rule, which
    could not tell a real recovery from an offer we pulled off the site.
    """
    active = set(_oos_eans(market, "active"))
    recovered = set(_oos_eans(market, "recovered"))
    out: List[dict] = []
    vanished = 0
    for r in list_exclusions():
        if not (r["market"] == market.upper() and r.get("source") == "oos"
                and r["status"] in ("excluded", "partial")):
            continue
        iid = r["item_id"]
        ean = iid[len(DMA_ITEM_PREFIX):] if iid.startswith(DMA_ITEM_PREFIX) else iid
        if ean in active:
            continue  # still OOS on some offer -> keep excluded
        if ean in recovered:
            out.append(r)  # genuine recovery
        else:
            vanished += 1  # vanished / aged off -> needs manual review
    if vanished:
        logger.info(
            "oos_recovered[%s]: %d OOS exclusion(s) left the active list without a "
            "recovery signal (unpublished or aged off); left excluded for review",
            market.upper(), vanished)
    return out


def oos_reenable(market: str) -> Dict[str, Any]:
    """Re-enable every recovered OOS exclusion for a market."""
    recovered = oos_recovered(market)
    results = []
    for r in recovered:
        try:
            results.append(enable(r["id"]))
        except Exception as e:  # noqa: BLE001
            logger.exception("oos_reenable failed for %s", r["id"])
            results.append({"id": r["id"], "error": str(e)})
    return {"market": market.upper(), "recovered": len(recovered), "results": results}
