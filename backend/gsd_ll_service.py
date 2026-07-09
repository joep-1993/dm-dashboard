"""
GSD Low-Linkage Pause/Enable Service
=====================================

Reads the pixel-monitor GSD feed and pauses or re-enables GSD Shopping
campaigns based on each shop's linkage status:

- Feed rows with GSD = 0  -> shop dropped below the linkage threshold.
  If the shop is still a GSD shop (is_gsd_<country>_shop = 1 in
  beslistbi.bt.shop_list for the most recent date), every ENABLED campaign
  in that country's GSD account(s) whose name contains the ShopNaam is
  PAUSED and tagged with the label 'GSD_LL_PAUSED'.

- Feed rows with GSD = 1  -> shop recovered its linkage.
  If the shop is still a GSD shop, every campaign carrying the
  'GSD_LL_PAUSED' label (for that ShopNaam, in that country's account(s))
  is re-ENABLED and the label is removed.

Every pause / enable action is appended to pa.jvs_gsd_ll_campaigns in the
n8n-vector-db PostgreSQL DB so the frontend can show an audit trail.

Country -> account mapping and the Google Ads client are reused from
gsd_campaigns_service so this stays in sync with the rest of GSD Campaigns.
"""
import csv
import io
import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import requests
from google.ads.googleads.errors import GoogleAdsException

from backend.database import get_db_connection, return_db_connection, get_redshift_connection, return_redshift_connection
from backend.gsd_campaigns_service import _get_client, ACCOUNTS

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FEED_URL = "https://pixel-monitor.aks.beslist.nl/api/gsd/feed.csv"

LL_LABEL = "GSD_LL_PAUSED"

ADMIN_TABLE = "pa.jvs_gsd_ll_campaigns"

# Map the shop_list GSD flag columns to a country code.
FLAG_TO_COUNTRY = {
    "is_gsd_nl_shop": "NL",
    "is_gsd_be_shop": "BE",
    "is_gsd_de_shop": "DE",
}


def _country_customer_ids() -> Dict[str, Set[str]]:
    """Build {country: {customer_id, ...}} from the shared ACCOUNTS map.

    NL_CPR and NL_CPC share one customer_id; BE has two distinct accounts;
    DE has one. De-duplicated via a set so each account is touched once.
    """
    mapping: Dict[str, Set[str]] = {}
    for info in ACCOUNTS.values():
        mapping.setdefault(info["country"], set()).add(info["customer_id"])
    return mapping


COUNTRY_CUSTOMER_IDS = _country_customer_ids()


# ---------------------------------------------------------------------------
# Progress state (single in-process run at a time, polled by the frontend)
# ---------------------------------------------------------------------------

_LL_LOCK = threading.Lock()
_LL_PROGRESS: Dict[str, Any] = {
    "running": False, "phase": "idle", "total": 0, "processed": 0,
    "paused": 0, "enabled": 0, "skipped": 0, "errors": 0,
    "dry_run": False, "done": False, "result": None, "error": None,
    "started_at": None, "finished_at": None,
}


def _progress_set(**kw: Any) -> None:
    with _LL_LOCK:
        _LL_PROGRESS.update(kw)


def get_ll_progress() -> Dict[str, Any]:
    """Snapshot of the current/last low-linkage run for the UI to poll."""
    with _LL_LOCK:
        return dict(_LL_PROGRESS)


def start_ll_run(
    dry_run: bool = False,
    date_str: Optional[str] = None,
    shop_names: Optional[List[str]] = None,
    included: bool = False,
) -> Dict[str, Any]:
    """Kick off a low-linkage run in a background thread and return immediately.

    Returns {"started": True} or {"started": False, "busy": True} if a run is
    already in flight (only one at a time).
    """
    with _LL_LOCK:
        if _LL_PROGRESS["running"]:
            return {"started": False, "busy": True}
        _LL_PROGRESS.update({
            "running": True, "phase": "Starting…", "total": 0, "processed": 0,
            "paused": 0, "enabled": 0, "skipped": 0, "errors": 0,
            "dry_run": dry_run, "done": False, "result": None, "error": None,
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "finished_at": None,
        })

    def _worker() -> None:
        try:
            res = run_low_linkage(dry_run, date_str, shop_names, included)
            _progress_set(
                result=res, done=True, phase="Done",
                processed=_LL_PROGRESS.get("total", 0),
                paused=res.get("paused_count", len(res.get("paused", []))),
                enabled=res.get("enabled_count", len(res.get("enabled", []))),
                skipped=len(res.get("skipped", [])),
                errors=len(res.get("errors", [])),
            )
        except Exception as ex:  # pragma: no cover - defensive
            logger.exception("GSD LL run crashed")
            _progress_set(error=str(ex), done=True, phase="Error")
        finally:
            _progress_set(running=False, finished_at=datetime.now().isoformat(timespec="seconds"))

    threading.Thread(target=_worker, daemon=True).start()
    return {"started": True, "dry_run": dry_run}


# ---------------------------------------------------------------------------
# Feed
# ---------------------------------------------------------------------------


def fetch_feed(url: str = FEED_URL) -> List[Dict[str, Any]]:
    """Fetch and parse the GSD feed CSV.

    Returns a list of dicts with keys: shop_id (int), shop_name (str),
    linkage (float|None), gsd (int 0/1). Malformed rows are skipped.
    """
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    # utf-8-sig strips the leading BOM the feed ships with.
    text = resp.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")

    rows: List[Dict[str, Any]] = []
    for raw in reader:
        shop_id_raw = (raw.get("ShopId") or "").strip()
        shop_name = (raw.get("ShopNaam") or "").strip()
        gsd_raw = (raw.get("GSD") or "").strip()
        if not shop_id_raw or not shop_name or gsd_raw not in ("0", "1"):
            continue
        try:
            shop_id = int(shop_id_raw)
        except ValueError:
            continue
        linkage_raw = (raw.get("LinkagePercentage") or "").strip().replace(",", ".")
        try:
            linkage = float(linkage_raw) if linkage_raw else None
        except ValueError:
            linkage = None
        rows.append({
            "shop_id": shop_id,
            "shop_name": shop_name,
            "linkage": linkage,
            "gsd": int(gsd_raw),
        })
    logger.info("GSD LL: fetched %d usable feed rows", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Redshift: GSD shop flags for the most recent date
# ---------------------------------------------------------------------------


def get_shop_flags(shop_ids: List[int], date_str: Optional[str] = None) -> Dict[int, Dict[str, int]]:
    """Return {shop_id: {is_gsd_nl_shop, is_gsd_be_shop, is_gsd_de_shop}}.

    Uses the most recent row per shop in beslistbi.bt.shop_list, matching the
    ROW_NUMBER()-over-dim_date_key-DESC pattern used elsewhere in GSD tooling.
    If date_str (YYYY-MM-DD) is given, evaluates flags as of that date (most
    recent row on or before it) instead of the absolute latest.
    """
    if not shop_ids:
        return {}

    conn = get_redshift_connection()
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(shop_ids))
            params = list(shop_ids)
            date_filter = ""
            if date_str:
                date_filter = " AND dim_date_key <= CAST(TO_CHAR(CAST(%s AS DATE), 'YYYYMMDD') AS BIGINT)"
                params.append(date_str)
            cur.execute(f"""
                WITH latest AS (
                    SELECT shop_id,
                           is_gsd_nl_shop,
                           is_gsd_be_shop,
                           is_gsd_de_shop,
                           ROW_NUMBER() OVER (
                               PARTITION BY shop_id ORDER BY dim_date_key DESC
                           ) AS rn
                    FROM beslistbi.bt.shop_list
                    WHERE deleted_ind = 0
                      AND shop_id IN ({placeholders})
                      {date_filter}
                )
                SELECT shop_id, is_gsd_nl_shop, is_gsd_be_shop, is_gsd_de_shop
                FROM latest
                WHERE rn = 1
            """, params)
            rows = cur.fetchall()

        flags: Dict[int, Dict[str, int]] = {}
        for row in rows:
            flags[int(row["shop_id"])] = {
                "is_gsd_nl_shop": int(row["is_gsd_nl_shop"] or 0),
                "is_gsd_be_shop": int(row["is_gsd_be_shop"] or 0),
                "is_gsd_de_shop": int(row["is_gsd_de_shop"] or 0),
            }
        return flags
    finally:
        return_redshift_connection(conn)


# ---------------------------------------------------------------------------
# Admin table (n8n-vector-db PostgreSQL)
# ---------------------------------------------------------------------------


def ensure_admin_table() -> None:
    """Create the audit table if it does not exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {ADMIN_TABLE} (
                    id            SERIAL PRIMARY KEY,
                    shop_id       BIGINT,
                    shop_name     TEXT,
                    country       VARCHAR(4),
                    action        VARCHAR(16),   -- 'Paused' | 'Enabled'
                    campaign_id   TEXT,
                    campaign_name TEXT,
                    customer_id   TEXT,
                    linkage       NUMERIC,
                    created_at    TIMESTAMPTZ DEFAULT now()
                )
            """)
        conn.commit()
    finally:
        return_db_connection(conn)


def _record_action(
    conn,
    shop_id: int,
    shop_name: str,
    country: str,
    action: str,
    campaign_id: str,
    campaign_name: str,
    customer_id: str,
    linkage: Optional[float],
) -> None:
    """Insert one audit row (caller owns the transaction / commit)."""
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {ADMIN_TABLE}
                (shop_id, shop_name, country, action, campaign_id,
                 campaign_name, customer_id, linkage)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (shop_id, shop_name, country, action, campaign_id,
              campaign_name, customer_id, linkage))


def get_history(limit: int = 500) -> List[Dict[str, Any]]:
    """Return the most recent audit rows for the frontend."""
    ensure_admin_table()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT shop_id, shop_name, country, action, campaign_id,
                       campaign_name, customer_id, linkage, created_at
                FROM {ADMIN_TABLE}
                ORDER BY created_at DESC, id DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        return_db_connection(conn)


# ---------------------------------------------------------------------------
# Google Ads label helpers (shared client)
# ---------------------------------------------------------------------------


def _escape_gaql(value: str) -> str:
    """Escape a string literal for a GAQL query."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _ensure_label(client, customer_id: str, label_name: str) -> str:
    """Return the resource name of label_name in the account, creating it if
    absent."""
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT label.resource_name
        FROM label
        WHERE label.name = '{_escape_gaql(label_name)}'
    """
    try:
        for row in ga_service.search(customer_id=customer_id, query=query):
            return row.label.resource_name
    except GoogleAdsException as ex:
        logger.warning("Label lookup failed for %s in %s: %s", label_name, customer_id, ex)

    label_service = client.get_service("LabelService")
    op = client.get_type("LabelOperation")
    label = op.create
    label.name = label_name
    label.text_label.background_color = "#E0A800"
    label.text_label.description = "Paused by GSD low-linkage automation"
    response = label_service.mutate_labels(customer_id=customer_id, operations=[op])
    return response.results[0].resource_name


def _apply_label(client, customer_id: str, campaign_resource: str, label_resource: str) -> None:
    """Attach a label to a campaign (idempotent-ish; logs if already applied)."""
    service = client.get_service("CampaignLabelService")
    op = client.get_type("CampaignLabelOperation")
    op.create.campaign = campaign_resource
    op.create.label = label_resource
    try:
        service.mutate_campaign_labels(customer_id=customer_id, operations=[op])
    except GoogleAdsException as ex:
        logger.warning("Could not apply label to %s: %s", campaign_resource, ex)


def _remove_campaign_label(client, customer_id: str, campaign_label_resource: str) -> None:
    """Detach a label from a campaign given the campaign_label resource name."""
    service = client.get_service("CampaignLabelService")
    op = client.get_type("CampaignLabelOperation")
    op.remove = campaign_label_resource
    try:
        service.mutate_campaign_labels(customer_id=customer_id, operations=[op])
    except GoogleAdsException as ex:
        logger.warning("Could not remove label %s: %s", campaign_label_resource, ex)


def _set_status(client, customer_id: str, campaign_id: str, status: str) -> None:
    """Set a campaign's status (ENABLED / PAUSED) using the shared client."""
    campaign_service = client.get_service("CampaignService")
    op = client.get_type("CampaignOperation")
    campaign = op.update
    campaign.resource_name = campaign_service.campaign_path(customer_id, campaign_id)
    campaign.status = getattr(client.enums.CampaignStatusEnum, status)
    field_mask = client.get_type("FieldMask")
    field_mask.paths.append("status")
    op.update_mask.CopyFrom(field_mask)
    campaign_service.mutate_campaigns(customer_id=customer_id, operations=[op])


# ---------------------------------------------------------------------------
# Campaign lookups
# ---------------------------------------------------------------------------


def _find_enabled_campaigns(client, customer_id: str, shop_name: str) -> List[Dict[str, str]]:
    """ENABLED campaigns in the account whose name contains the shop name."""
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT campaign.id, campaign.name, campaign.resource_name
        FROM campaign
        WHERE campaign.status = 'ENABLED'
          AND campaign.name LIKE '%{_escape_gaql(shop_name)}%'
    """
    out: List[Dict[str, str]] = []
    try:
        for row in ga_service.search(customer_id=customer_id, query=query):
            out.append({
                "campaign_id": str(row.campaign.id),
                "campaign_name": row.campaign.name,
                "resource_name": row.campaign.resource_name,
            })
    except GoogleAdsException as ex:
        logger.error("Enabled-campaign lookup failed (%s, %s): %s", customer_id, shop_name, ex)
        raise
    return out


def _find_labeled_campaigns(client, customer_id: str, shop_name: str) -> List[Dict[str, str]]:
    """Non-removed campaigns carrying the GSD_LL_PAUSED label whose name
    contains the shop name. Returns campaign + campaign_label resource names."""
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT campaign.id, campaign.name, campaign.resource_name,
               campaign.status, campaign_label.resource_name
        FROM campaign_label
        WHERE label.name = '{LL_LABEL}'
          AND campaign.status != 'REMOVED'
          AND campaign.name LIKE '%{_escape_gaql(shop_name)}%'
    """
    out: List[Dict[str, str]] = []
    try:
        for row in ga_service.search(customer_id=customer_id, query=query):
            out.append({
                "campaign_id": str(row.campaign.id),
                "campaign_name": row.campaign.name,
                "resource_name": row.campaign.resource_name,
                "campaign_label_resource": row.campaign_label.resource_name,
            })
    except GoogleAdsException as ex:
        logger.error("Labeled-campaign lookup failed (%s, %s): %s", customer_id, shop_name, ex)
        raise
    return out


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------


def _countries_for_shop(flags: Dict[str, int]) -> List[str]:
    """Countries where the shop is flagged as a GSD shop (flag == 1)."""
    return [country for col, country in FLAG_TO_COUNTRY.items() if flags.get(col) == 1]


def run_low_linkage(
    dry_run: bool = False,
    date_str: Optional[str] = None,
    shop_names: Optional[List[str]] = None,
    included: bool = False,
) -> Dict[str, Any]:
    """Fetch the feed and pause / re-enable low-linkage GSD campaigns.

    Parameters
    ----------
    dry_run : if True, no Google Ads mutations or DB writes happen; the return
        value lists exactly what *would* be paused / enabled.
    date_str : optional YYYY-MM-DD; evaluate the shop_list GSD flags as of this
        date (most recent row on or before it). Defaults to the absolute latest.
    shop_names : optional list of feed shop names to scope the run to.
    included : with shop_names, True = process ONLY those shops, False = process
        all EXCEPT those. Ignored when shop_names is empty.
    """
    started = datetime.now()
    result: Dict[str, Any] = {
        "started_at": started.isoformat(timespec="seconds"),
        "dry_run": dry_run,
        "date": date_str,
        "feed_rows": 0,
        "paused": [],
        "enabled": [],
        "skipped": [],
        "errors": [],
    }

    # 1. Feed
    _progress_set(phase="Fetching linkage feed…")
    try:
        feed = fetch_feed()
    except Exception as ex:
        logger.error("GSD LL: failed to fetch feed: %s", ex)
        result["errors"].append({"step": "fetch_feed", "error": str(ex)})
        return result

    # Optional shop-name include/exclude filter (case-insensitive on ShopNaam).
    if shop_names:
        wanted = {s.strip().lower() for s in shop_names if s.strip()}
        if wanted:
            feed = [r for r in feed
                    if (r["shop_name"].lower() in wanted) == bool(included)]

    result["feed_rows"] = len(feed)
    _progress_set(total=len(feed), phase="Reading shop GSD flags…")
    if not feed:
        return result

    # 2. Flags as of the requested date (batch)
    try:
        flags_by_shop = get_shop_flags([r["shop_id"] for r in feed], date_str)
    except Exception as ex:
        logger.error("GSD LL: failed to fetch shop flags: %s", ex)
        result["errors"].append({"step": "shop_flags", "error": str(ex)})
        return result

    _progress_set(phase="Processing shops…")

    # 3. Shared Google Ads client + per-account label cache
    client = _get_client()
    label_cache: Dict[str, str] = {}

    def label_resource(customer_id: str) -> str:
        if customer_id not in label_cache:
            label_cache[customer_id] = _ensure_label(client, customer_id, LL_LABEL)
        return label_cache[customer_id]

    if not dry_run:
        ensure_admin_table()
    db_conn = None if dry_run else get_db_connection()

    try:
        for idx, row in enumerate(feed):
            _progress_set(
                processed=idx,
                paused=len(result["paused"]), enabled=len(result["enabled"]),
                skipped=len(result["skipped"]), errors=len(result["errors"]),
            )
            shop_id = row["shop_id"]
            shop_name = row["shop_name"]
            gsd = row["gsd"]
            linkage = row["linkage"]

            flags = flags_by_shop.get(shop_id)
            if flags is None:
                result["skipped"].append({
                    "shop_id": shop_id, "shop_name": shop_name,
                    "reason": "not_found_in_shop_list",
                })
                continue

            countries = _countries_for_shop(flags)
            if not countries:
                result["skipped"].append({
                    "shop_id": shop_id, "shop_name": shop_name,
                    "reason": "not_a_gsd_shop",
                })
                continue

            action = "Paused" if gsd == 0 else "Enabled"

            for country in countries:
                for customer_id in sorted(COUNTRY_CUSTOMER_IDS.get(country, set())):
                    try:
                        if gsd == 0:
                            campaigns = _find_enabled_campaigns(client, customer_id, shop_name)
                        else:
                            campaigns = _find_labeled_campaigns(client, customer_id, shop_name)
                    except Exception as ex:
                        result["errors"].append({
                            "shop_id": shop_id, "shop_name": shop_name,
                            "country": country, "customer_id": customer_id,
                            "step": "lookup", "error": str(ex),
                        })
                        continue

                    for camp in campaigns:
                        entry = {
                            "shop_id": shop_id,
                            "shop_name": shop_name,
                            "country": country,
                            "customer_id": customer_id,
                            "campaign_id": camp["campaign_id"],
                            "campaign_name": camp["campaign_name"],
                            "linkage": linkage,
                        }

                        if dry_run:
                            (result["paused"] if gsd == 0 else result["enabled"]).append(entry)
                            continue

                        try:
                            if gsd == 0:
                                _set_status(client, customer_id, camp["campaign_id"], "PAUSED")
                                _apply_label(client, customer_id, camp["resource_name"],
                                             label_resource(customer_id))
                            else:
                                _set_status(client, customer_id, camp["campaign_id"], "ENABLED")
                                _remove_campaign_label(client, customer_id,
                                                       camp["campaign_label_resource"])

                            _record_action(
                                db_conn, shop_id, shop_name, country, action,
                                camp["campaign_id"], camp["campaign_name"],
                                customer_id, linkage,
                            )
                            db_conn.commit()
                            (result["paused"] if gsd == 0 else result["enabled"]).append(entry)
                        except Exception as ex:
                            db_conn.rollback()
                            logger.error("GSD LL: %s failed for %s / %s: %s",
                                         action, shop_name, camp["campaign_id"], ex)
                            result["errors"].append({**entry, "step": action.lower(),
                                                     "error": str(ex)})
    finally:
        if db_conn is not None:
            return_db_connection(db_conn)

    result["finished_at"] = datetime.now().isoformat(timespec="seconds")
    result["paused_count"] = len(result["paused"])
    result["enabled_count"] = len(result["enabled"])
    logger.info("GSD LL done (dry_run=%s): %d paused, %d enabled, %d skipped, %d errors",
                dry_run, result["paused_count"], result["enabled_count"],
                len(result["skipped"]), len(result["errors"]))
    return result
