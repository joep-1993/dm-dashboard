"""
Shop-campaigns performance service.

Tracks the per-day performance of every campaign whose name starts with
``SHOP/`` (the branded Shopping/SEARCH campaigns that live across the Beslist
category subaccounts under MCC 3011145605).

Data source is **Search Ads 360** (login customer 9816507046), queried with the
same vendored ``util_searchads360`` client that GSD Budgets uses. Most metrics
come straight off the SA360 ``campaign`` resource; revenue and margin come from
two SA360 custom columns the SEA team maintains:

    Totaal: Revenue  -> custom column id 29314662
    Totaal: Profit   -> custom column id 29126930   (same column GSD Budgets uses for "marge")

Both are manager-level (9816507046) DOUBLE custom columns that resolve fine when
querying the child accounts.
"""
import logging
import os
import random
import re
import sys
import textwrap
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

# The vendored helper imports siblings relatively; placing backend/vendor/ on
# sys.path lets us import it as `util_searchads360`, exactly like gsd_budgets.
_VENDOR_PATH = str(Path(__file__).parent / "vendor")
if _VENDOR_PATH not in sys.path:
    sys.path.insert(0, _VENDOR_PATH)

from google.api_core import exceptions as gax_exceptions  # noqa: E402
from util_searchads360 import SearchAds360Client  # noqa: E402
from google.ads.searchads360.v0.services.types.search_ads360_service import (  # noqa: E402
    SearchSearchAds360Request,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SA360_LOGIN_CUSTOMER_ID = "9816507046"
SA360_PAGE_SIZE = 10_000

# SA360 custom columns (manager-level, shared to child accounts).
CC_TOTAAL_REVENUE = 29314662   # "Totaal: Revenue"
CC_TOTAAL_PROFIT = 29126930    # "Totaal: Profit"

SHOP_PREFIX = "SHOP/"
TZ = pytz.timezone("Europe/Amsterdam")

# Candidate accounts to scan for SHOP/ campaigns. Superset taken from the
# Keyword Planner rotation list (the canonical set of category subaccounts plus
# the country roots). Accounts without any SHOP/ campaign just return nothing.
CANDIDATE_ACCOUNTS: List[str] = [
    "8485842412", "4056770576", "1496704472", "4964513580", "3114657125",
    "5807833423", "3273661472", "7269160392", "9251309631", "3969307564",
    "8273243429", "8696777335", "5930401821", "6213822688", "6379322129",
    "2237802672", "8338942127", "9525057729", "8431844135", "6862783922",
    "6511658729", "4675585929", "5105960927", "4567815835", "1351439239",
    "5122292229", "7346695290", "5550062935", "4761604080", "6044293584",
    "6271552035", "8755979133", "7938980174", "8276523186", "4192567576",
]

# The 8 tracked metrics, in display order. Derived ones are computed from the
# summed base components so day/total ratios stay correct.
METRICS = [
    "clicks", "revenue", "cost", "ctr", "conversions", "conv_rate", "avg_cpc", "margin",
]

# ---------------------------------------------------------------------------
# SA360 client (one shared, lazily-built service; gRPC stub is thread-safe)
# ---------------------------------------------------------------------------

_sa360_client: Optional[SearchAds360Client] = None
_sa360_service = None
_sa360_lock = threading.Lock()
_yaml_path = Path(__file__).parent / "data" / "shop_campaigns_sa360.yaml"


def _write_yaml(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        textwrap.dedent(
            f"""\
            developer_token: "{os.environ.get('GOOGLE_DEVELOPER_TOKEN', '')}"
            client_id: "{os.environ.get('GOOGLE_CLIENT_ID', '')}"
            client_secret: "{os.environ.get('GOOGLE_CLIENT_SECRET', '')}"
            refresh_token: "{os.environ.get('GOOGLE_REFRESH_TOKEN', '')}"
            use_proto_plus: True
            login_customer_id: "{SA360_LOGIN_CUSTOMER_ID}"
            """
        ),
        encoding="utf-8",
    )


def _service():
    global _sa360_client, _sa360_service
    with _sa360_lock:
        if _sa360_service is None:
            _write_yaml(_yaml_path)
            _sa360_client = SearchAds360Client.load_from_file(str(_yaml_path))
            _sa360_client.set_ids(SA360_LOGIN_CUSTOMER_ID, SA360_LOGIN_CUSTOMER_ID)
            _sa360_service = _sa360_client.get_service()
        return _sa360_service


def _search(customer_id: str, query: str):
    """Run a SA360 search and return the materialised list of rows.

    Retries with exponential backoff on the transient errors SA360 throws under
    per-account fan-out (429/Aborted/Unavailable/DeadlineExceeded). Iteration is
    done inside the retry so a failure mid-pagination is retried too.
    """
    service = _service()
    req = SearchSearchAds360Request()
    req.customer_id = customer_id
    req.query = query
    req.page_size = SA360_PAGE_SIZE

    max_retries = 5
    base_delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            pager = service.search(request=req)
            rows = list(pager)
            return rows, pager
        except gax_exceptions.ResourceExhausted as ex:
            m = re.search(r"Retry in\s+(\d+)\s+seconds", str(ex))
            retry_after = int(m.group(1)) if m else 0
            delay = max(retry_after, base_delay * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
            if attempt >= max_retries:
                raise
            time.sleep(delay)
        except (gax_exceptions.Aborted, gax_exceptions.ServiceUnavailable,
                gax_exceptions.DeadlineExceeded) as ex:
            if attempt >= max_retries:
                raise
            time.sleep(base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.5))


def _cc_value(cc) -> float:
    """Extract a numeric value from a SA360 custom-column cell.

    proto-plus raises AttributeError for fields absent from the schema, so use
    getattr's 3-arg form (which swallows it) rather than bare attribute access.
    """
    v = getattr(cc, "double_value", None)
    if v is None:
        v = getattr(cc, "int64_value", None)
    if v is None:
        v = getattr(cc, "long_value", None)
    return float(v) if v is not None else 0.0


def _cc_indices(pager):
    """Map the two Totaal custom columns to their row positions for this pager."""
    rev = prof = None
    for i, h in enumerate(getattr(pager, "custom_column_headers", None) or []):
        if h.id == CC_TOTAAL_REVENUE:
            rev = i
        elif h.id == CC_TOTAAL_PROFIT:
            prof = i
    return rev, prof


def _base_from_row(r, rev_i, prof_i):
    return {
        "clicks": float(r.metrics.clicks),
        "impressions": float(r.metrics.impressions),
        "cost": r.metrics.cost_micros / 1_000_000.0,
        "conversions": float(r.metrics.conversions),
        "revenue": _cc_value(r.custom_columns[rev_i]) if rev_i is not None and r.custom_columns else 0.0,
        "margin": _cc_value(r.custom_columns[prof_i]) if prof_i is not None and r.custom_columns else 0.0,
    }


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------

_INVENTORY_TTL = 600    # 10 min — the campaign set barely changes
_PERF_TTL = 180         # 3 min — date-range results
_cache_lock = threading.Lock()
_inventory_cache: Dict[str, Any] = {}          # {"ts": float, "data": {...}}
_perf_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
_top_cache: Dict[Tuple[str, str, int], Dict[str, Any]] = {}


def _norm_dates(start_date: Optional[str], end_date: Optional[str]) -> Tuple[str, str]:
    """Return (start, end) as YYYY-MM-DD, defaulting to the last 30 days."""
    today = datetime.now(TZ).date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else today
    start = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else (end - timedelta(days=29))
    if start > end:
        start, end = end, start
    return start.isoformat(), end.isoformat()


# ---------------------------------------------------------------------------
# Inventory — which SHOP/ campaigns exist, their status, which accounts hold them
# ---------------------------------------------------------------------------

def _fetch_account_inventory(customer_id: str) -> List[Dict[str, Any]]:
    query = (
        "SELECT campaign.id, campaign.name, campaign.status "
        "FROM campaign "
        f"WHERE campaign.name LIKE '{SHOP_PREFIX}%'"
    )
    rows, _ = _search(customer_id, query)
    out = []
    for r in rows:
        out.append({
            "account": customer_id,
            "campaign_id": str(r.campaign.id),
            "campaign_name": r.campaign.name,
            "status": r.campaign.status.name,
        })
    return out


def get_inventory(force: bool = False) -> Dict[str, Any]:
    """All SHOP/ campaigns across the candidate accounts, with status counts.

    Cached for 10 minutes. Also records which accounts actually hold SHOP/
    campaigns so the performance scan can skip the empty ones.
    """
    with _cache_lock:
        cached = _inventory_cache.get("data")
        if cached and not force and (time.time() - _inventory_cache.get("ts", 0)) < _INVENTORY_TTL:
            return cached

    campaigns: List[Dict[str, Any]] = []
    errors: List[str] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_fetch_account_inventory, cid): cid for cid in CANDIDATE_ACCOUNTS}
        for fut in as_completed(futures):
            cid = futures[fut]
            try:
                campaigns.extend(fut.result())
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Shop-campaigns inventory failed for {cid}: {e}")
                errors.append(cid)

    by_status: Dict[str, int] = {}
    for c in campaigns:
        by_status[c["status"]] = by_status.get(c["status"], 0) + 1
    accounts_with_shop = sorted({c["account"] for c in campaigns})

    data = {
        "campaigns": campaigns,
        "total": len(campaigns),
        "by_status": by_status,
        "accounts_with_shop": accounts_with_shop,
        "accounts_failed": errors,
        "generated_at": datetime.now(TZ).isoformat(),
    }
    with _cache_lock:
        _inventory_cache["data"] = data
        _inventory_cache["ts"] = time.time()
    return data


# ---------------------------------------------------------------------------
# Performance — per-day aggregate across all SHOP/ campaigns
# ---------------------------------------------------------------------------

def _fetch_account_daily(customer_id: str, start: str, end: str) -> Dict[str, Dict[str, float]]:
    """Per-date base-metric sums for one account's SHOP/ campaigns."""
    s = start.replace("-", "")
    e = end.replace("-", "")
    query = (
        "SELECT segments.date, metrics.clicks, metrics.impressions, "
        "metrics.cost_micros, metrics.conversions, "
        f"custom_columns.id[{CC_TOTAAL_REVENUE}], custom_columns.id[{CC_TOTAAL_PROFIT}] "
        "FROM campaign "
        f"WHERE campaign.name LIKE '{SHOP_PREFIX}%' "
        f"AND segments.date BETWEEN {s} AND {e}"
    )
    rows, pager = _search(customer_id, query)

    # Map custom-column ids to their position in each row's custom_columns list.
    rev_idx = prof_idx = None
    headers = getattr(pager, "custom_column_headers", None) or []
    for i, h in enumerate(headers):
        if h.id == CC_TOTAAL_REVENUE:
            rev_idx = i
        elif h.id == CC_TOTAAL_PROFIT:
            prof_idx = i

    daily: Dict[str, Dict[str, float]] = {}
    for r in rows:
        d = r.segments.date
        acc = daily.setdefault(d, {"clicks": 0.0, "impressions": 0.0, "cost": 0.0,
                                   "conversions": 0.0, "revenue": 0.0, "margin": 0.0})
        acc["clicks"] += r.metrics.clicks
        acc["impressions"] += r.metrics.impressions
        acc["cost"] += r.metrics.cost_micros / 1_000_000.0
        acc["conversions"] += r.metrics.conversions
        if rev_idx is not None and r.custom_columns:
            acc["revenue"] += _cc_value(r.custom_columns[rev_idx])
        if prof_idx is not None and r.custom_columns:
            acc["margin"] += _cc_value(r.custom_columns[prof_idx])
    return daily


def _derive(base: Dict[str, float]) -> Dict[str, float]:
    """Add ctr / conv_rate / avg_cpc from summed base components."""
    clicks = base["clicks"]
    impr = base["impressions"]
    return {
        "clicks": round(clicks, 2),
        "impressions": round(impr, 2),
        "cost": round(base["cost"], 2),
        "conversions": round(base["conversions"], 2),
        "revenue": round(base["revenue"], 2),
        "margin": round(base["margin"], 2),
        "ctr": round(clicks / impr * 100, 4) if impr else 0.0,
        "conv_rate": round(base["conversions"] / clicks * 100, 4) if clicks else 0.0,
        "avg_cpc": round(base["cost"] / clicks, 4) if clicks else 0.0,
    }


def get_performance(start_date: Optional[str] = None,
                    end_date: Optional[str] = None,
                    force: bool = False) -> Dict[str, Any]:
    """Per-day aggregated performance of all SHOP/ campaigns over a date range."""
    start, end = _norm_dates(start_date, end_date)
    key = (start, end)
    with _cache_lock:
        cached = _perf_cache.get(key)
        if cached and not force and (time.time() - cached.get("ts", 0)) < _PERF_TTL:
            return cached["data"]

    inv = get_inventory(force=force)
    # Only scan accounts that actually hold SHOP/ campaigns; fall back to the
    # full candidate list if inventory came back empty (e.g. all calls failed).
    accounts = inv.get("accounts_with_shop") or CANDIDATE_ACCOUNTS

    merged: Dict[str, Dict[str, float]] = {}
    errors: List[str] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_fetch_account_daily, cid, start, end): cid for cid in accounts}
        for fut in as_completed(futures):
            cid = futures[fut]
            try:
                for d, vals in fut.result().items():
                    m = merged.setdefault(d, {"clicks": 0.0, "impressions": 0.0, "cost": 0.0,
                                              "conversions": 0.0, "revenue": 0.0, "margin": 0.0})
                    for k, v in vals.items():
                        m[k] += v
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Shop-campaigns performance failed for {cid}: {e}")
                errors.append(cid)

    # Build a continuous per-day series across the full range (zero-fill gaps).
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    daily: List[Dict[str, Any]] = []
    totals_base = {"clicks": 0.0, "impressions": 0.0, "cost": 0.0,
                   "conversions": 0.0, "revenue": 0.0, "margin": 0.0}
    cur = d0
    while cur <= d1:
        key_str = cur.strftime("%Y%m%d")
        base = merged.get(key_str, {"clicks": 0.0, "impressions": 0.0, "cost": 0.0,
                                    "conversions": 0.0, "revenue": 0.0, "margin": 0.0})
        for k in totals_base:
            totals_base[k] += base[k]
        row = _derive(base)
        row["date"] = cur.isoformat()
        daily.append(row)
        cur += timedelta(days=1)

    totals = _derive(totals_base)

    data = {
        "start_date": start,
        "end_date": end,
        "daily": daily,
        "totals": totals,
        "metrics": METRICS,
        "campaign_total": inv.get("total", 0),
        "campaign_by_status": inv.get("by_status", {}),
        "accounts_scanned": len(accounts),
        "accounts_failed": errors,
        "generated_at": datetime.now(TZ).isoformat(),
    }
    with _cache_lock:
        _perf_cache[key] = {"ts": time.time(), "data": data}
    return data


# ---------------------------------------------------------------------------
# Top performers — best campaigns / ad groups over the date range
# ---------------------------------------------------------------------------

def _fetch_top_for_account(customer_id: str, start: str, end: str):
    """Range-aggregated campaign- and ad-group-level rows for one account.

    Omitting segments.date makes SA360 aggregate the metrics (and the Totaal
    custom columns) over the whole range, one row per campaign / ad group.
    """
    s = start.replace("-", "")
    e = end.replace("-", "")
    cc = f"custom_columns.id[{CC_TOTAAL_REVENUE}], custom_columns.id[{CC_TOTAAL_PROFIT}]"
    base_metrics = "metrics.clicks, metrics.impressions, metrics.cost_micros, metrics.conversions"

    camp_q = (
        f"SELECT campaign.name, {base_metrics}, {cc} "
        f"FROM campaign WHERE campaign.name LIKE '{SHOP_PREFIX}%' "
        f"AND segments.date BETWEEN {s} AND {e}"
    )
    crows, cp = _search(customer_id, camp_q)
    crev, cprof = _cc_indices(cp)
    campaigns = []
    for r in crows:
        item = {"account": customer_id, "campaign_name": r.campaign.name}
        item.update(_derive(_base_from_row(r, crev, cprof)))
        campaigns.append(item)

    ag_q = (
        f"SELECT ad_group.name, campaign.name, {base_metrics}, {cc} "
        f"FROM ad_group WHERE campaign.name LIKE '{SHOP_PREFIX}%' "
        f"AND segments.date BETWEEN {s} AND {e}"
    )
    arows, ap = _search(customer_id, ag_q)
    arev, aprof = _cc_indices(ap)
    ad_groups = []
    for r in arows:
        item = {"account": customer_id, "campaign_name": r.campaign.name,
                "ad_group_name": r.ad_group.name}
        item.update(_derive(_base_from_row(r, arev, aprof)))
        ad_groups.append(item)

    return campaigns, ad_groups


def get_top_performers(start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       limit: int = 10000,
                       force: bool = False) -> Dict[str, Any]:
    """SHOP/ campaigns and ad groups over a date range, ranked by revenue (top `limit`)."""
    start, end = _norm_dates(start_date, end_date)
    key = (start, end, limit)
    with _cache_lock:
        cached = _top_cache.get(key)
        if cached and not force and (time.time() - cached.get("ts", 0)) < _PERF_TTL:
            return cached["data"]

    inv = get_inventory(force=force)
    accounts = inv.get("accounts_with_shop") or CANDIDATE_ACCOUNTS

    campaigns: List[Dict[str, Any]] = []
    ad_groups: List[Dict[str, Any]] = []
    errors: List[str] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_fetch_top_for_account, cid, start, end): cid for cid in accounts}
        for fut in as_completed(futures):
            cid = futures[fut]
            try:
                camps, ags = fut.result()
                campaigns.extend(camps)
                ad_groups.extend(ags)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Shop-campaigns top-performers failed for {cid}: {e}")
                errors.append(cid)

    def _rank(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Best performers first: revenue, then clicks, then cost as tie-breakers.
        items.sort(key=lambda x: (x["revenue"], x["clicks"], x["cost"]), reverse=True)
        return items[:limit]

    data = {
        "start_date": start,
        "end_date": end,
        "limit": limit,
        "ranked_by": "revenue",
        "metrics": METRICS,
        "campaigns": _rank(campaigns),
        "ad_groups": _rank(ad_groups),
        "accounts_failed": errors,
        "generated_at": datetime.now(TZ).isoformat(),
    }
    with _cache_lock:
        _top_cache[key] = {"ts": time.time(), "data": data}
    return data
