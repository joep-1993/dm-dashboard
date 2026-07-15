"""
GSD Campaigns Service

Manages Google Shopping Direct (GSD) campaigns across multiple Google Ads accounts.
Handles campaign creation, pausing, enabling, and removal. Integrates with Merchant
Center for account linking and Redshift for shop change data.
"""
import os
import re
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - stdlib on py3.9+
    ZoneInfo = None

import psycopg2
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf import field_mask_pb2
from googleapiclient.discovery import build
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ACCOUNTS = {
    "NL_CPR": {"customer_id": "7938980174", "mc_id": "5592708765", "country": "NL", "type": "CPR"},
    "BE_CPR": {"customer_id": "2454295509", "mc_id": "5588879919", "country": "BE", "type": "CPR"},
    "DE_CPR": {"customer_id": "4192567576", "mc_id": "5342886105", "country": "DE", "type": "CPR"},
    "NL_CPC": {"customer_id": "7938980174", "mc_id": "5592708765", "country": "NL", "type": "CPC"},
    "BE_CPC": {"customer_id": "7565255758", "mc_id": "5588879919", "country": "BE", "type": "CPC"},
}

MCC_CUSTOMER_ID = "3011145605"

SCRIPT_LABEL = "GSD_SCRIPT"

LABELS_CPR = ["a", "b", "c", "no_data", "no_ean"]
LABELS_CPC = ["a,b", "c,no_data,no_ean"]

# A Redshift shop-change row is per-country: `kolom` is the GSD flag that flipped,
# so it names the ONE country to act on. A shop flagged for NL only must not
# create/pause BE/DE campaigns (e.g. Calcuso.com|NL -> NL only).
KOLOM_COUNTRY = {"is_gsd_nl_shop": "NL", "is_gsd_be_shop": "BE", "is_gsd_de_shop": "DE"}

# --- Google Sheets run-logging ---------------------------------------------
# Mirrors the original create GSD-campaigns.py: each real run appends one row per
# processed shop to the "campaigns_created" tab of the "Data: Direct Shopping"
# sheet. Best-effort — never fails a run.
LOG_SPREADSHEET_ID = os.environ.get(
    "GSD_LOG_SPREADSHEET_ID", "1m4k8kxhfU7oLIAH3DJOyYx_PKSv4luPyX97j45Wa6s4"
)
LOG_WORKSHEET = os.environ.get("GSD_LOG_WORKSHEET", "campaigns_created")
# The sheet is shared with the dedicated sheets service account
# (gsd-campaign-creator@cla-campaign-creation) — NOT the Content-API accounts in
# backend/service_accounts/. Kept as a separate file/env so it doesn't disturb
# the MC service-account auto-detect (_get_content_service).
SHEETS_SA_FILE = os.environ.get(
    "GSD_SHEETS_SERVICE_ACCOUNT_FILE",
    "/mnt/c/Users/JoepvanSchagen/Downloads/Python/gsd-campaign-creation.json",
)
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Cooperative cancel for an in-flight run_gsd_script — checked between shops, so
# a cancel stops further creates/pauses (already-processed shops stay done).
_run_cancel = {"cancel": False}

# Per-shop progress of an in-flight run_gsd_script, polled by the frontend bar.
_run_progress = {"current": 0, "total": 0, "running": False}


def cancel_run() -> None:
    """Request the active GSD run to stop at the next shop boundary."""
    _run_cancel["cancel"] = True


def get_run_progress() -> Dict[str, Any]:
    return dict(_run_progress)


# Last Google Ads error captured by a create/enable helper, so
# _create_campaigns_for_shop can surface the real reason (not just a code) in
# the run result instead of a bare "—".
_last_gads_error = {"msg": None}


def _gads_err(ex) -> str:
    """Concise message from a GoogleAdsException (joins the per-error messages)."""
    try:
        msgs = [e.message for e in ex.failure.errors if e.message]
        joined = "; ".join(msgs)
        return (joined or str(ex))[:400]
    except Exception:
        return str(ex)[:400]


# Merchant Center (Content API) errors are plain HttpErrors, not
# GoogleAdsExceptions. Stash the real reason so a Merchant-Center failure
# surfaces in the run result instead of a bare "failed_to_get_or_create_mc_account".
_last_mc_error = {"msg": None}


def _mc_err(ex) -> str:
    """Concise message from a Content API HttpError (prefers the API reason+message)."""
    try:
        details = ex.error_details  # googleapiclient HttpError, list of dicts
        if details:
            d = details[0]
            reason = d.get("reason", "")
            msg = d.get("message", "")
            return (f"{reason}: {msg}" if reason else msg or str(ex))[:400]
    except Exception:
        pass
    return str(ex)[:400]

TRACKING_TEMPLATES = {
    "NL": (
        "https://www.beslist.nl/outclick/redirect?aff_id=900"
        "&params=productId%3D{product_id}%26marketingChannelId%3D14&url={lpurl}"
    ),
    "BE": (
        "https://www.beslist.be/outclick/redirect?aff_id=901"
        "&params=productId%3D{product_id}%26marketingChannelId%3D14&url={lpurl}"
    ),
    "DE": (
        "https://www.shopcaddy.de/outclick/redirect?aff_id=910"
        "&params=productId%3D{product_id}%26marketingChannelId%3D14&url={lpurl}"
    ),
}

PRICE_BUCKETS = [
    "0-8", "8-13", "13-21", "21-34", "34-55", "55-89",
    "89-144", "144-233", "233-377", "377-610", "610-987",
    "987-1597", "1597-2584", "2584-Onbeperkt",
]

BIDS_AB = [0.12, 0.12, 0.15, 0.17, 0.19, 0.20, 0.23, 0.25, 0.31, 0.35, 0.40, 0.41, 0.35, 0.25]
BIDS_C = [0.08, 0.09, 0.11, 0.12, 0.14, 0.14, 0.17, 0.18, 0.22, 0.26, 0.29, 0.29, 0.26, 0.18]

GEO_TARGETS = {"NL": "2528", "BE": "2056", "DE": "2276"}

CAMPAIGN_NAME_REGEX = re.compile(r"\[shop:([^\]]+)\].*?\[shop_id:(\d+)\].*?\[label:([^\]]+)\]")
COUNTRY_REGEX = re.compile(r"\[domein:(\w+)\]")

# Default daily budget in micros. 10 EUR = 10_000_000 micros, matching the
# original create GSD-campaigns.py (campaign_budget.amount_micros = 10000000).
DEFAULT_BUDGET_MICROS = 10_000_000

# Content API scopes for Merchant Center
MC_SCOPES = ["https://www.googleapis.com/auth/content"]

# ---------------------------------------------------------------------------
# Temporary ID counter for mutate operations
# ---------------------------------------------------------------------------

_next_temp_id = 0


def next_id() -> int:
    """Return the next temporary negative ID for resource creation."""
    global _next_temp_id
    _next_temp_id -= 1
    return _next_temp_id


def reset_temp_ids() -> None:
    """Reset the temporary ID counter (call before each batch of operations)."""
    global _next_temp_id
    _next_temp_id = 0


# ---------------------------------------------------------------------------
# Client helpers
# ---------------------------------------------------------------------------


def _get_client() -> GoogleAdsClient:
    """Initialize Google Ads client from environment variables."""
    config = {
        "developer_token": os.environ.get("GOOGLE_DEVELOPER_TOKEN", ""),
        "refresh_token": os.environ.get("GOOGLE_REFRESH_TOKEN", ""),
        "client_id": os.environ.get("GOOGLE_CLIENT_ID", ""),
        "client_secret": os.environ.get("GOOGLE_CLIENT_SECRET", ""),
        "login_customer_id": os.environ.get("GOOGLE_LOGIN_CUSTOMER_ID", MCC_CUSTOMER_ID),
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config)


def _is_retryable_gads(ex: GoogleAdsException) -> bool:
    """
    True for transient Google Ads failures that are safe to retry — chiefly
    CONCURRENT_MODIFICATION ("Multiple requests were attempting to modify the
    same resource at once"), plus transient internal/quota errors. Detection is
    tolerant of proto-plus vs protobuf enum representations and falls back to the
    rendered message text.
    """
    try:
        for err in ex.failure.errors:
            code = err.error_code
            for family in ("database_error", "internal_error", "quota_error"):
                val = getattr(code, family, 0)
                name = getattr(val, "name", str(val))
                if name in ("CONCURRENT_MODIFICATION", "INTERNAL_ERROR",
                            "TRANSIENT_ERROR", "RESOURCE_EXHAUSTED",
                            "RESOURCE_TEMPORARILY_EXHAUSTED"):
                    return True
    except Exception:
        pass
    msg = str(ex)
    return "CONCURRENT_MODIFICATION" in msg or "modify the same resource" in msg


def _mutate_with_retry(what: str, fn, retries: int = 5, base_delay: float = 0.5):
    """
    Call a Google Ads mutate (a zero-arg callable) and retry transient failures
    with exponential backoff (0.5s, 1s, 2s, 4s, 8s). Non-retryable errors and a
    final exhausted attempt re-raise, so existing per-call error handling still
    sees the real GoogleAdsException.
    """
    for attempt in range(retries):
        try:
            return fn()
        except GoogleAdsException as ex:
            if attempt < retries - 1 and _is_retryable_gads(ex):
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    "Transient Ads error on %s (attempt %d/%d); retrying in %.1fs",
                    what, attempt + 1, retries, delay,
                )
                time.sleep(delay)
                continue
            raise


# A freshly-created Merchant Center -> Google Ads link is eventually consistent:
# the MC-side link exists but Google Ads can briefly still report
# RESOURCE_NOT_FOUND on shopping_setting.merchant_id when the campaign is created.
# Give the campaign create its own patient retry so the link can propagate within
# the same run (~2 min total) instead of failing the shop.
_MERCHANT_LINK_RETRY_DELAYS = (5, 10, 20, 30, 60)  # seconds


def _is_merchant_link_not_ready(ex: GoogleAdsException) -> bool:
    """
    True when a campaign create fails with RESOURCE_NOT_FOUND on the shopping
    merchant_id — i.e. the MC->Ads link hasn't propagated yet. Deliberately
    narrow (merchant_id / shopping_setting only) so we don't swallow other
    genuinely-missing resources.
    """
    msg = str(ex)
    if "RESOURCE_NOT_FOUND" not in msg:
        return False
    return "merchant_id" in msg or "shopping_setting" in msg


def _create_campaign_with_retry(fn):
    """
    Run the campaign-create mutate (a zero-arg callable) with retries for BOTH
    transient CONCURRENT_MODIFICATION (short exponential backoff) and
    merchant-link eventual-consistency RESOURCE_NOT_FOUND (patient backoff, the
    _MERCHANT_LINK_RETRY_DELAYS schedule). Scoped to campaign creation only, so
    RESOURCE_NOT_FOUND is never treated as retryable elsewhere.

    Retrying just this mutate is safe: mutate_campaigns is atomic (nothing is
    created on failure) and the budget created earlier in the flow is reused, so
    there are no duplicates.
    """
    transient_attempt = 0
    link_attempt = 0
    while True:
        try:
            return fn()
        except GoogleAdsException as ex:
            if _is_merchant_link_not_ready(ex) and link_attempt < len(_MERCHANT_LINK_RETRY_DELAYS):
                delay = _MERCHANT_LINK_RETRY_DELAYS[link_attempt]
                link_attempt += 1
                logger.warning(
                    "Campaign create hit RESOURCE_NOT_FOUND on merchant_id "
                    "(MC link still propagating); retry %d/%d in %ds",
                    link_attempt, len(_MERCHANT_LINK_RETRY_DELAYS), delay,
                )
                time.sleep(delay)
                continue
            if _is_retryable_gads(ex) and transient_attempt < 4:
                delay = 0.5 * (2 ** transient_attempt)
                transient_attempt += 1
                logger.warning(
                    "Transient Ads error on create campaign; retrying in %.1fs", delay,
                )
                time.sleep(delay)
                continue
            raise


def _get_redshift_connection():
    """Create a Redshift connection from environment variables."""
    return psycopg2.connect(
        host=os.environ.get("REDSHIFT_HOST", ""),
        port=int(os.environ.get("REDSHIFT_PORT", "5439")),
        dbname=os.environ.get("REDSHIFT_DB", ""),
        user=os.environ.get("REDSHIFT_USER", ""),
        password=os.environ.get("REDSHIFT_PASSWORD", ""),
    )


def _get_mc_service():
    """Build a Merchant Center Content API service using a service account."""
    sa_file = os.environ.get("GSD_SERVICE_ACCOUNT_FILE", "")
    if not sa_file:
        # Auto-detect: use first .json in service_accounts dir
        sa_dir = os.path.join(os.path.dirname(__file__), "service_accounts")
        if os.path.isdir(sa_dir):
            json_files = [f for f in os.listdir(sa_dir) if f.endswith(".json")]
            if json_files:
                sa_file = os.path.join(sa_dir, json_files[0])
    if not sa_file or not os.path.exists(sa_file):
        raise RuntimeError(
            "Service account file not found. Set GSD_SERVICE_ACCOUNT_FILE env var "
            "or place a .json key file in backend/service_accounts/"
        )
    credentials = service_account.Credentials.from_service_account_file(sa_file, scopes=MC_SCOPES)
    return build("content", "v2.1", credentials=credentials, cache_discovery=False)


# ---------------------------------------------------------------------------
# Campaign name helpers
# ---------------------------------------------------------------------------


def _parse_campaign_name(name: str) -> Dict[str, Optional[str]]:
    """Extract shop_name, shop_id, label, and country from a campaign name."""
    result: Dict[str, Optional[str]] = {
        "shop_name": None,
        "shop_id": None,
        "label": None,
        "country": "NL",
    }
    m = CAMPAIGN_NAME_REGEX.search(name)
    if m:
        result["shop_name"] = m.group(1)
        result["shop_id"] = m.group(2)
        result["label"] = m.group(3)
    cm = COUNTRY_REGEX.search(name)
    if cm:
        result["country"] = cm.group(1).upper()
    return result


def _build_campaign_name(
    country: str, shop_name: str, shop_id: int, label: str
) -> str:
    """Build a campaign name following the GSD naming convention."""
    base = f"[shop:{shop_name}] [shop_id:{shop_id}] [channel:directshopping] [label:{label}]"
    if country.upper() != "NL":
        base = f"[domein:{country.upper()}] {base}"
    return base


def _name_contains_regexp(substring: str) -> str:
    """
    Build a GAQL REGEXP_MATCH pattern that matches names CONTAINING `substring`
    literally. Use this instead of ``LIKE '%substring%'`` whenever the substring
    can contain '[' or ']': GAQL's LIKE treats brackets as a character class, so
    ``LIKE '%[shop:X]%'`` collapses to "contains any one of these characters" and
    matches nearly every campaign in the account (it does NOT filter by shop).
    It also treats '_' as a single-char wildcard, which this avoids too.

    Regex metacharacters are escaped, then backslashes are doubled and single
    quotes escaped so the result is safe to embed in a single-quoted GAQL
    string literal.
    """
    pattern = re.escape(substring)          # escape regex specials incl. [ ] . _
    pattern = pattern.replace("\\", "\\\\")  # double backslashes for the GAQL literal
    pattern = pattern.replace("'", "\\'")    # escape any single quote for the GAQL literal
    return f".*{pattern}.*"


# ---------------------------------------------------------------------------
# Negative keywords helper
# ---------------------------------------------------------------------------


# Two-level public suffixes we encounter in shop names (.co.uk etc.)
_SECOND_LEVEL = {"co.uk", "com.au", "co.nz", "com.br", "co.za"}


def _clean_host(raw: str) -> str:
    """Normalise a shop/domain name to a bare host (no |country, scheme, www, path or note)."""
    s = raw.split("|")[0].strip().lower()   # drop |NL country marker
    s = re.sub(r"^https?://", "", s)        # drop scheme
    s = re.sub(r"^www\.", "", s)            # drop leading www.
    s = s.split("/")[0]                     # drop /path
    s = s.split()[0] if s.split() else s    # drop trailing " (note)" / " OUD"
    return s.strip(".")


def get_negatives(shop_name: str) -> List[str]:
    """
    Build negative keywords from a shop name as [full-domain, brand].

    e.g. "Gymbeam.nl" -> ["gymbeam.nl", "gymbeam"];
         "Calcuso.com|NL" -> ["calcuso.com", "calcuso"];
         "Hoopo.eu" -> ["hoopo.eu", "hoopo"].
    Handles any TLD (incl. two-level like .co.uk) and NEVER emits a bare
    TLD/country token (the old split-on-every-non-alphanumeric produced
    harmful "nl"/"com"/"eu" negatives).
    """
    if not shop_name:
        return []
    host = _clean_host(shop_name)
    if not host:
        return []
    if "." not in host:
        return [host]
    # strip the public suffix (two-level suffixes first)
    for suf in _SECOND_LEVEL:
        if host.endswith("." + suf):
            name = host[: -(len(suf) + 1)]
            break
    else:
        name = host.rsplit(".", 1)[0]
    name = name.rsplit(".", 1)[-1]          # core brand label (drop shop./nl. subdomains)
    negatives = [host]
    if name and name != host:
        negatives.append(name)
    return negatives


# ---------------------------------------------------------------------------
# Google Sheets run-logging helper
# ---------------------------------------------------------------------------


def _log_run_to_sheet(rows: List[List[Any]]) -> Dict[str, Any]:
    """
    Append one row per processed shop to the "campaigns_created" tab of the
    "Data: Direct Shopping" sheet, mirroring the original create GSD-campaigns.py.

    Each row (columns A-I): [datum (dd-mm-yyyy), shop_id, shop_name, CPC/CPR,
    Merchant Center ID, domein, op brand?, campagnes aangemaakt?, actie].

    Best-effort: any failure is logged and swallowed so it never breaks a run.
    """
    if not rows:
        return {"logged": 0}
    try:
        creds = service_account.Credentials.from_service_account_file(
            SHEETS_SA_FILE, scopes=SHEETS_SCOPES
        )
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        sheet = svc.spreadsheets()
        # Write right after the last populated LOGGING row (column A), exactly like
        # the original create GSD-campaigns.py. We deliberately key off column A —
        # NOT append() — because this tab has helper columns (J/K/L) with values
        # far below the last log row, which would make append() leave a large gap.
        col_a = sheet.values().get(
            spreadsheetId=LOG_SPREADSHEET_ID, range=f"{LOG_WORKSHEET}!A:A"
        ).execute().get("values", [])
        first_empty = len(col_a) + 1
        end_row = first_empty + len(rows) - 1
        target = f"{LOG_WORKSHEET}!A{first_empty}:I{end_row}"
        sheet.values().update(
            spreadsheetId=LOG_SPREADSHEET_ID,
            range=target,
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()
        logger.info(
            "Logged %d GSD run row(s) to sheet %s!%s from row %d",
            len(rows), LOG_SPREADSHEET_ID, LOG_WORKSHEET, first_empty,
        )
        return {"logged": len(rows), "first_row": first_empty}
    except Exception as ex:
        logger.warning("Failed to log GSD run to sheet: %s", ex)
        return {"logged": 0, "error": str(ex)[:300]}


# ---------------------------------------------------------------------------
# Google Ads query helpers
# ---------------------------------------------------------------------------


def get_gsd_campaigns(customer_id: str, client: Optional[GoogleAdsClient] = None) -> List[Dict[str, Any]]:
    """
    Query all non-REMOVED campaigns with the GSD_SCRIPT label for a given
    customer account. Returns last-30-day metrics.

    Pass a shared ``client`` to avoid rebuilding one per account (get_all_gsd_stats
    queries several accounts in a row).
    """
    client = client or _get_client()
    ga_service = client.get_service("GoogleAdsService")

    # Step 1: Get campaign IDs with the GSD_SCRIPT label
    label_query = f"""
        SELECT campaign.id, campaign.name, campaign.status
        FROM campaign_label
        WHERE label.name = '{SCRIPT_LABEL}'
          AND campaign.status != 'REMOVED'
    """

    campaigns: Dict[str, Dict[str, Any]] = {}

    try:
        response = ga_service.search(customer_id=customer_id, query=label_query)
        for row in response:
            cid = str(row.campaign.id)
            if cid not in campaigns:
                parsed = _parse_campaign_name(row.campaign.name)
                campaigns[cid] = {
                    "campaign_id": cid,
                    "campaign_name": row.campaign.name,
                    "status": row.campaign.status.name,
                    "shop_id": parsed["shop_id"],
                    "shop_name": parsed["shop_name"],
                    "label": parsed["label"],
                    "country": parsed["country"],
                    "customer_id": customer_id,
                    "impressions": 0,
                    "clicks": 0,
                    "cost": 0.0,
                }
    except GoogleAdsException as ex:
        logger.error("Google Ads API error (label query) for customer %s: %s", customer_id, ex)
        raise

    if not campaigns:
        return []

    # Step 2: Get metrics for those campaigns (last 30 days)
    today = datetime.now().strftime("%Y-%m-%d")
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    campaign_ids = ",".join(campaigns.keys())

    metrics_query = f"""
        SELECT
            campaign.id,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros
        FROM campaign
        WHERE campaign.id IN ({campaign_ids})
          AND segments.date BETWEEN '{thirty_days_ago}' AND '{today}'
    """

    try:
        response = ga_service.search(customer_id=customer_id, query=metrics_query)
        for row in response:
            cid = str(row.campaign.id)
            if cid in campaigns:
                campaigns[cid]["impressions"] += row.metrics.impressions
                campaigns[cid]["clicks"] += row.metrics.clicks
                campaigns[cid]["cost"] += row.metrics.cost_micros / 1_000_000
    except GoogleAdsException as ex:
        logger.warning("Could not fetch metrics for customer %s: %s", customer_id, ex)

    return list(campaigns.values())


def get_all_gsd_stats() -> Dict[str, Any]:
    """
    Fetch GSD campaigns across all accounts and return aggregated stats.
    """
    all_campaigns: List[Dict[str, Any]] = []
    errors: List[str] = []

    client = _get_client()  # build once, reuse across every account (#14)

    # NL_CPR and NL_CPC share one customer_id, and get_gsd_campaigns returns
    # ALL GSD_SCRIPT campaigns in an account regardless of type — so querying
    # per ACCOUNTS entry would fetch (and count) the NL account twice. Query
    # each DISTINCT customer_id once (#4).
    seen_customer_ids: set = set()
    for account_key, info in ACCOUNTS.items():
        customer_id = info["customer_id"]
        if customer_id in seen_customer_ids:
            continue
        seen_customer_ids.add(customer_id)
        try:
            camps = get_gsd_campaigns(customer_id, client=client)
            # Enrich with account info (metadata only; not used in the totals)
            for c in camps:
                c["account_key"] = account_key
                c["account_type"] = info["type"]
            all_campaigns.extend(camps)
        except Exception as ex:
            errors.append(f"{account_key}: {ex}")
            logger.error("Error fetching GSD campaigns for %s: %s", account_key, ex)

    total_impressions = sum(c["impressions"] for c in all_campaigns)
    total_clicks = sum(c["clicks"] for c in all_campaigns)
    total_cost = sum(c["cost"] for c in all_campaigns)
    enabled_count = sum(1 for c in all_campaigns if c["status"] == "ENABLED")
    paused_count = sum(1 for c in all_campaigns if c["status"] == "PAUSED")

    # Per-country stats
    accounts = {}
    for country in ["NL", "BE", "DE"]:
        country_camps = [c for c in all_campaigns if (c.get("country") or "").upper() == country]
        accounts[country] = {
            "total": len(country_camps),
            "active": sum(1 for c in country_camps if c["status"] == "ENABLED"),
            "paused": sum(1 for c in country_camps if c["status"] == "PAUSED"),
        }

    return {
        "campaigns": all_campaigns,
        "accounts": accounts,
        "total_campaigns": len(all_campaigns),
        "enabled": enabled_count,
        "paused": paused_count,
        "total_impressions": total_impressions,
        "total_clicks": total_clicks,
        "total_cost": round(total_cost, 2),
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Campaign status mutations
# ---------------------------------------------------------------------------


def _mutate_campaign_status(customer_id: str, campaign_id: str, status: str) -> Dict[str, Any]:
    """Set campaign status (ENABLED, PAUSED) or remove it (REMOVED)."""
    client = _get_client()
    campaign_service = client.get_service("CampaignService")
    resource_name = campaign_service.campaign_path(customer_id, campaign_id)

    campaign_op = client.get_type("CampaignOperation")
    if status == "REMOVED":
        # Removal uses the dedicated REMOVE operation. Setting status=REMOVED via
        # an update is rejected by the API (INVALID_ENUM_VALUE: "Enum value
        # 'REMOVED' cannot be used.").
        campaign_op.remove = resource_name
    else:
        campaign = campaign_op.update
        campaign.resource_name = resource_name
        campaign.status = getattr(client.enums.CampaignStatusEnum, status)
        campaign_op.update_mask = field_mask_pb2.FieldMask(paths=["status"])

    try:
        response = _mutate_with_retry(
            f"set campaign {campaign_id} -> {status}",
            lambda: campaign_service.mutate_campaigns(
                customer_id=customer_id, operations=[campaign_op]
            ),
        )
        result = response.results[0]
        return {"success": True, "resource_name": result.resource_name}
    except GoogleAdsException as ex:
        logger.error("Failed to set campaign %s to %s: %s", campaign_id, status, ex)
        return {"success": False, "error": str(ex)}


def pause_campaign(customer_id: str, campaign_id: str) -> Dict[str, Any]:
    """Set campaign status to PAUSED."""
    return _mutate_campaign_status(customer_id, campaign_id, "PAUSED")


def enable_campaign(customer_id: str, campaign_id: str) -> Dict[str, Any]:
    """Set campaign status to ENABLED."""
    return _mutate_campaign_status(customer_id, campaign_id, "ENABLED")


def remove_campaign(customer_id: str, campaign_id: str) -> Dict[str, Any]:
    """Remove a campaign."""
    return _mutate_campaign_status(customer_id, campaign_id, "REMOVED")


def undo_run(
    created: Optional[List[Dict[str, Any]]] = None,
    paused: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Reverse a GSD run: PAUSE the campaigns it created and re-ENABLE the campaigns
    it paused. Both `created` and `paused` are lists of dicts with at least
    ``customer_id`` and ``campaign_id``. Operations are grouped per account and
    applied with partial failure, so one bad id doesn't sink the batch.

    Pausing (not removing) created campaigns keeps the undo reversible.
    Returns counts and any per-account errors.
    """
    result: Dict[str, Any] = {"paused_created": 0, "enabled_paused": 0, "errors": []}

    # Group campaign ids by (customer_id, target_status). "created" -> PAUSED,
    # "paused" -> ENABLED.
    groups: Dict[str, List[str]] = {}

    def _add(items, status):
        for it in (items or []):
            cid = str(it.get("customer_id") or "").strip()
            camp = str(it.get("campaign_id") or "").strip()
            if cid and camp:
                groups.setdefault(f"{cid}|{status}", []).append(camp)

    _add(created, "PAUSED")
    _add(paused, "ENABLED")

    if not groups:
        return result

    client = _get_client()
    cs = client.get_service("CampaignService")

    for key, camp_ids in groups.items():
        cid, status = key.split("|", 1)
        ops = []
        for camp in camp_ids:
            op = client.get_type("CampaignOperation")
            op.update.resource_name = cs.campaign_path(cid, camp)
            op.update.status = getattr(client.enums.CampaignStatusEnum, status)
            op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
            ops.append(op)
        try:
            req = client.get_type("MutateCampaignsRequest")
            req.customer_id = cid
            req.operations.extend(ops)
            req.partial_failure = True
            resp = _mutate_with_retry(
                f"bulk {status} ({cid})",
                lambda: cs.mutate_campaigns(request=req),
            )
            ok = sum(1 for r in resp.results if r.resource_name)
            if status == "PAUSED":
                result["paused_created"] += ok
            else:
                result["enabled_paused"] += ok
            if resp.partial_failure_error and resp.partial_failure_error.message:
                result["errors"].append({
                    "customer_id": cid, "status": status,
                    "error": resp.partial_failure_error.message[:500],
                })
        except GoogleAdsException as ex:
            logger.error("Undo failed (%s -> %s): %s", cid, status, ex)
            result["errors"].append({"customer_id": cid, "status": status, "error": str(ex)[:500]})

    logger.info("Undo run: paused %d created, enabled %d paused, %d errors",
                result["paused_created"], result["enabled_paused"], len(result["errors"]))
    return result


def reconstruct_run(
    at_iso: str,
    before_minutes: int = 60,
    after_minutes: int = 10,
) -> Dict[str, Any]:
    """
    Reconstruct what a past GSD run changed, from Google Ads change history, in a
    window around a log entry's timestamp. `at_iso` is an ISO-8601 timestamp
    (the browser sends UTC, e.g. "2026-07-14T09:20:14.000Z"). Read-only.

    Returns campaigns to undo:
      - created: campaigns CREATEd in the window                -> undo pauses them
      - paused:  campaigns whose latest status change in the window is PAUSED and
                 that were NOT created in it                    -> undo re-enables

    Only GSD ("[channel:directshopping]") campaigns across the GSD accounts are
    considered. change_event retains ~30 days, so older runs return nothing.
    """
    result: Dict[str, Any] = {"created": [], "paused": [], "errors": [], "window": {}}

    # Build the window in the accounts' timezone (Europe/Amsterdam) — that's how
    # change_event.change_date_time is expressed. The run's changes precede the
    # log timestamp (it's written after the run completes), hence the asymmetric
    # default window (mostly looking backwards).
    try:
        at = datetime.fromisoformat(at_iso.replace("Z", "+00:00"))
    except ValueError as ex:
        result["errors"].append({"step": "parse_time", "error": f"{at_iso}: {ex}"})
        return result
    if at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    tz = ZoneInfo("Europe/Amsterdam") if ZoneInfo else timezone.utc
    start = (at - timedelta(minutes=before_minutes)).astimezone(tz)
    end = (at + timedelta(minutes=after_minutes)).astimezone(tz)
    start_s = start.strftime("%Y-%m-%d %H:%M:%S")
    end_s = end.strftime("%Y-%m-%d %H:%M:%S")
    result["window"] = {"start": start_s, "end": end_s, "tz": "Europe/Amsterdam"}

    client = _get_client()
    ga = client.get_service("GoogleAdsService")
    customer_ids = sorted({info["customer_id"] for info in ACCOUNTS.values()})

    created_ids: Dict[tuple, str] = {}        # (cid, camp_id) -> name
    status_latest: Dict[tuple, tuple] = {}    # (cid, camp_id) -> (new_status, name), newest first

    for cid in customer_ids:
        query = f"""
            SELECT change_event.change_date_time, change_event.resource_change_operation,
                   change_event.changed_fields, change_event.old_resource,
                   change_event.new_resource, campaign.id, campaign.name
            FROM change_event
            WHERE change_event.change_date_time BETWEEN '{start_s}' AND '{end_s}'
              AND change_event.change_resource_type = 'CAMPAIGN'
            ORDER BY change_event.change_date_time DESC
            LIMIT 10000
        """
        try:
            rows = list(ga.search(customer_id=cid, query=query))
        except GoogleAdsException as ex:
            logger.error("Reconstruct: change_event query failed for %s: %s", cid, ex)
            result["errors"].append({"customer_id": cid, "error": str(ex)[:400]})
            continue

        for row in rows:
            ce = row.change_event
            name = row.campaign.name or ""
            if "[channel:directshopping]" not in name:
                continue  # keep to GSD campaigns only
            key = (cid, str(row.campaign.id))
            if ce.resource_change_operation.name == "CREATE":
                created_ids.setdefault(key, name)
            if "status" in list(ce.changed_fields.paths) and key not in status_latest:
                status_latest[key] = (ce.new_resource.campaign.status.name, name)

    for (cid, camp_id), name in created_ids.items():
        result["created"].append({"customer_id": cid, "campaign_id": camp_id, "campaign_name": name})
    for key, (st, name) in status_latest.items():
        if st == "PAUSED" and key not in created_ids:
            cid, camp_id = key
            result["paused"].append({"customer_id": cid, "campaign_id": camp_id, "campaign_name": name})

    logger.info("Reconstruct [%s..%s]: %d created, %d paused, %d errors",
                start_s, end_s, len(result["created"]), len(result["paused"]), len(result["errors"]))
    return result


# ---------------------------------------------------------------------------
# Redshift queries
# ---------------------------------------------------------------------------


def get_redshift_shop_changes(
    date_str: Optional[str] = None,
    shop_names: Optional[List[str]] = None,
    included: bool = False,
) -> List[Dict[str, Any]]:
    """
    Compute GSD shop changes live by diffing bt.shop_list for the chosen date
    vs. the day before.  Emits actie='aan' (flag 0->1) or 'uit' (1->0).
    Joins hda.efficy_shop_catman for branded, and derives model (CPR/CPC)
    from the is_wecantrack_shop / is_pixel_shop flags.

    Parameters
    ----------
    date_str : optional date string (YYYY-MM-DD), defaults to today.
    shop_names : optional list of shop names to filter on.
    included : if True, also include shops that are already included.

    Returns list of dicts with: shop_id, shop_name, kolom, actie, branded, model.
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    # Each UNION ALL leg needs the date parameter once
    _LEG = """
                  SELECT today.shop_id,
                         today.shop_name,
                         '{flag}' AS kolom,
                         CASE WHEN COALESCE(y.{flag},0)=0 AND COALESCE(today.{flag},0)=1 THEN 'aan'
                              WHEN COALESCE(y.{flag},0)=1 AND COALESCE(today.{flag},0)=0 THEN 'uit' END AS actie,
                         c.f_branded AS branded,
                         CASE WHEN COALESCE(today.is_wecantrack_shop,0)=1
                                OR COALESCE(today.is_pixel_shop,0)=1
                              THEN 'CPR' ELSE 'CPC' END AS model
                  FROM bt.shop_list today
                  JOIN bt.shop_list y
                    ON today.shop_id = y.shop_id
                   AND y.date = today.date - 1
                   AND y.deleted_ind = 0
                  LEFT JOIN hda.efficy_shops s
                    ON s.f_shop_id = today.shop_id
                   AND s.actual_ind = 1 AND s.deleted_ind = 0
                  LEFT JOIN hda.efficy_shop_catman c
                    ON c.k_shop = s.k_shop
                   AND c.actual_ind = 1 AND c.deleted_ind = 0
                  WHERE today.deleted_ind = 0
                    AND today.date = %s::date
                    AND COALESCE(today.{flag},0) <> COALESCE(y.{flag},0)"""

    flags = ["is_gsd_nl_shop", "is_gsd_be_shop", "is_gsd_de_shop"]
    legs = "\n\n                  UNION ALL\n".join(_LEG.format(flag=f) for f in flags)

    query = f"""
                WITH changes AS (
{legs}
                )
                SELECT * FROM changes"""

    # date_str once per leg
    params: list = [date_str] * len(flags)

    conditions: list = []
    if not included:
        conditions.append("actie IN ('aan', 'uit')")

    if shop_names:
        placeholders = ",".join(["%s"] * len(shop_names))
        conditions.append(f"shop_name IN ({placeholders})")
        params.extend(shop_names)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY shop_name, kolom"

    conn = _get_redshift_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Label management
# ---------------------------------------------------------------------------


def ensure_campaign_label_exists(client: GoogleAdsClient, customer_id: str) -> str:
    """
    Ensure the GSD_SCRIPT label exists in the account.
    Returns the label resource name.
    """
    ga_service = client.get_service("GoogleAdsService")

    # Check if label already exists
    query = f"""
        SELECT label.id, label.name, label.resource_name
        FROM label
        WHERE label.name = '{SCRIPT_LABEL}'
    """
    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            return row.label.resource_name
    except GoogleAdsException:
        pass

    # Create the label
    label_service = client.get_service("LabelService")
    label_op = client.get_type("LabelOperation")
    label = label_op.create
    label.name = SCRIPT_LABEL
    label.text_label.background_color = "#0000FF"
    label.text_label.description = "GSD Script managed campaigns"

    response = _mutate_with_retry(
        "create label",
        lambda: label_service.mutate_labels(customer_id=customer_id, operations=[label_op]),
    )
    return response.results[0].resource_name


def _apply_label_to_campaign(
    client: GoogleAdsClient, customer_id: str, campaign_resource_name: str, label_resource_name: str
) -> None:
    """Apply a label to a campaign."""
    campaign_label_service = client.get_service("CampaignLabelService")
    op = client.get_type("CampaignLabelOperation")
    op.create.campaign = campaign_resource_name
    op.create.label = label_resource_name

    try:
        _mutate_with_retry(
            "apply label",
            lambda: campaign_label_service.mutate_campaign_labels(
                customer_id=customer_id, operations=[op]
            ),
        )
    except GoogleAdsException as ex:
        # Label may already be applied
        logger.warning("Could not apply label to campaign: %s", ex)


# ---------------------------------------------------------------------------
# Check if campaign already exists
# ---------------------------------------------------------------------------


def check_campaign(client: GoogleAdsClient, customer_id: str, campaign_name: str) -> Optional[str]:
    """
    Check if a campaign with the given name already exists (non-REMOVED).
    Returns campaign resource name if found, else None.
    """
    ga_service = client.get_service("GoogleAdsService")
    escaped_name = campaign_name.replace("'", "\\'")
    query = f"""
        SELECT campaign.id, campaign.name, campaign.resource_name
        FROM campaign
        WHERE campaign.name = '{escaped_name}'
          AND campaign.status != 'REMOVED'
    """
    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            return row.campaign.resource_name
    except GoogleAdsException as ex:
        logger.error("Error checking campaign existence: %s", ex)
    return None


# ---------------------------------------------------------------------------
# Merchant Center helpers
# ---------------------------------------------------------------------------


def get_mc_id(mc_parent_id: str, shop_name: str) -> Optional[str]:
    """
    Look up a Merchant Center sub-account by name.
    Paginates through all sub-accounts (Content API returns them in
    the ``resources`` key, max 250 per page).
    Returns the account ID if found, else None.

    Raises on API error rather than returning None: the caller must be able to
    tell a genuine "shop not found" (safe to create) apart from a transient
    lookup failure (creating would spawn a DUPLICATE sub-account for a shop that
    may already have one).
    """
    service = _get_mc_service()
    target = shop_name.lower()
    page_token = None
    while True:
        kwargs: Dict[str, Any] = {"merchantId": mc_parent_id, "maxResults": 250}
        if page_token:
            kwargs["pageToken"] = page_token
        response = service.accounts().list(**kwargs).execute()
        for account in response.get("resources", []):
            if account.get("name", "").lower() == target:
                return str(account["id"])
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return None


def create_merchant_id(mc_parent_id: str, shop_name: str) -> Optional[str]:
    """
    Create a new Merchant Center sub-account.
    Returns the new account ID.
    """
    service = _get_mc_service()
    body = {
        "name": shop_name,
        "kind": "content#account",
    }
    try:
        response = service.accounts().insert(merchantId=mc_parent_id, body=body).execute()
        return str(response["id"])
    except Exception as ex:
        logger.error("Error creating MC sub-account for '%s': %s", shop_name, ex)
        _last_mc_error["msg"] = _mc_err(ex)
        return None


def link_to_google_ads(mc_parent_id: str, mc_account_id: str, ads_customer_id: str) -> bool:
    """
    Link a Merchant Center account to a Google Ads account.

    Two-step process:
    1. MC side: add an adsLink on the sub-account (creates a pending invitation).
    2. Ads side: accept the pending ProductLinkInvitation so campaigns can
       reference the merchant_id.
    """
    service = _get_mc_service()
    try:
        # Get current account info
        account = service.accounts().get(merchantId=mc_parent_id, accountId=mc_account_id).execute()

        # Add Google Ads link if not already present
        ads_links = account.get("adsLinks", [])
        ads_id_str = str(ads_customer_id)
        already_linked = any(
            str(link.get("adsId", "")) == ads_id_str and link.get("status") == "active"
            for link in ads_links
        )

        if not already_linked:
            # Remove any stale pending link for this Ads account first
            ads_links = [
                link for link in ads_links
                if str(link.get("adsId", "")) != ads_id_str
            ]
            ads_links.append({
                "adsId": ads_id_str,
                "status": "active",
            })
            account["adsLinks"] = ads_links
            service.accounts().update(
                merchantId=mc_parent_id, accountId=mc_account_id, body=account
            ).execute()
            logger.info("MC side: linked MC %s to Google Ads %s", mc_account_id, ads_customer_id)
    except Exception as ex:
        logger.error("Error linking MC %s to Ads %s (MC side): %s", mc_account_id, ads_customer_id, ex)
        return False

    # Step 2: accept the pending invitation from the Google Ads side
    try:
        accepted = _accept_mc_invitation(ads_customer_id, int(mc_account_id))
        if not accepted:
            # Not an error: the invitation often isn't visible on the Ads side yet
            # right after the MC-side link is created (eventual consistency). The
            # campaign create retries RESOURCE_NOT_FOUND to bridge this window.
            logger.warning(
                "No PENDING_APPROVAL ProductLinkInvitation found yet for MC %s in "
                "Ads %s; link may still be propagating (campaign create will retry).",
                mc_account_id, ads_customer_id,
            )
    except Exception as ex:
        logger.error("Error accepting MC invitation for %s in Ads %s: %s", mc_account_id, ads_customer_id, ex)
        return False

    return True


def _accept_mc_invitation(ads_customer_id: str, mc_account_id: int) -> bool:
    """
    Find and accept a pending ProductLinkInvitation for the given MC account.
    Returns True if an invitation was accepted, False if none was found yet
    (so the caller can tell "linked" from "not visible on the Ads side yet").
    """
    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT product_link_invitation.resource_name,
               product_link_invitation.merchant_center.merchant_center_id,
               product_link_invitation.status
        FROM product_link_invitation
        WHERE product_link_invitation.status = 'PENDING_APPROVAL'
    """
    response = ga_service.search(customer_id=ads_customer_id, query=query)

    for row in response:
        inv = row.product_link_invitation
        if inv.merchant_center.merchant_center_id == mc_account_id:
            invitation_service = client.get_service("ProductLinkInvitationService")
            invitation_service.update_product_link_invitation(
                customer_id=ads_customer_id,
                product_link_invitation_status=(
                    client.enums.ProductLinkInvitationStatusEnum.ACCEPTED
                ),
                resource_name=inv.resource_name,
            )
            logger.info(
                "Ads side: accepted MC invitation %s for MC %s",
                inv.resource_name, mc_account_id,
            )
            return True

    return False

    logger.info("No pending MC invitation found for MC %s in Ads %s", mc_account_id, ads_customer_id)


# ---------------------------------------------------------------------------
# Campaign creation helpers
# ---------------------------------------------------------------------------


def create_location_op(client: GoogleAdsClient, campaign_resource_name: str, country: str):
    """Create a campaign criterion operation for geo-targeting."""
    geo_target_id = GEO_TARGETS.get(country.upper(), GEO_TARGETS["NL"])

    op = client.get_type("CampaignCriterionOperation")
    criterion = op.create
    criterion.campaign = campaign_resource_name
    criterion.location.geo_target_constant = (
        f"geoTargetConstants/{geo_target_id}"
    )
    return op


def add_standard_shopping_campaign(
    client: GoogleAdsClient,
    customer_id: str,
    campaign_name: str,
    merchant_id: str,
    country: str,
    tracking_template: str,
    label_resource_name: str,
    budget_micros: int = DEFAULT_BUDGET_MICROS,
) -> Optional[str]:
    """
    Create a standard Shopping campaign with budget, location targeting,
    tracking template, and GSD_SCRIPT label.

    Returns the campaign resource name.
    """
    campaign_budget_service = client.get_service("CampaignBudgetService")
    campaign_service = client.get_service("CampaignService")
    campaign_criterion_service = client.get_service("CampaignCriterionService")

    # Step 1: Create campaign budget
    budget_op = client.get_type("CampaignBudgetOperation")
    budget = budget_op.create
    budget.name = f"GSD Budget - {campaign_name} - {datetime.now().isoformat()}"
    budget.amount_micros = budget_micros
    budget.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD
    budget.explicitly_shared = False

    try:
        budget_response = _mutate_with_retry(
            "create budget",
            lambda: campaign_budget_service.mutate_campaign_budgets(
                customer_id=customer_id, operations=[budget_op]
            ),
        )
        budget_resource = budget_response.results[0].resource_name
    except GoogleAdsException as ex:
        logger.error("Failed to create budget for '%s': %s", campaign_name, ex)
        _last_gads_error["msg"] = _gads_err(ex)
        return None

    # Step 2: Create campaign
    camp_op = client.get_type("CampaignOperation")
    campaign = camp_op.create
    campaign.name = campaign_name
    campaign.campaign_budget = budget_resource
    campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.SHOPPING
    # Create PAUSED; the caller flips it to ENABLED only after the ad group,
    # product ad and listing-group tree have all succeeded, so a failure partway
    # can never leave a live, budgeted campaign with no products / no bid tree.
    campaign.status = client.enums.CampaignStatusEnum.PAUSED
    campaign.manual_cpc.enhanced_cpc_enabled = False

    # Shopping settings
    campaign.shopping_setting.merchant_id = int(merchant_id)
    campaign.shopping_setting.feed_label = country.upper()
    campaign.shopping_setting.campaign_priority = 0
    campaign.shopping_setting.enable_local = False

    # Required in API v24+ for EU campaigns
    campaign.contains_eu_political_advertising = (
        client.enums.EuPoliticalAdvertisingStatusEnum.DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING
    )

    # Tracking template
    campaign.tracking_url_template = tracking_template

    try:
        camp_response = _create_campaign_with_retry(
            lambda: campaign_service.mutate_campaigns(
                customer_id=customer_id, operations=[camp_op]
            ),
        )
        campaign_resource = camp_response.results[0].resource_name
    except GoogleAdsException as ex:
        logger.error("Failed to create campaign '%s': %s", campaign_name, ex)
        _last_gads_error["msg"] = _gads_err(ex)
        return None

    # Step 3: Add location targeting
    location_op = create_location_op(client, campaign_resource, country)
    try:
        _mutate_with_retry(
            "location targeting",
            lambda: campaign_criterion_service.mutate_campaign_criteria(
                customer_id=customer_id, operations=[location_op]
            ),
        )
    except GoogleAdsException as ex:
        logger.warning("Failed to add location targeting: %s", ex)

    # Step 4: Apply GSD_SCRIPT label
    _apply_label_to_campaign(client, customer_id, campaign_resource, label_resource_name)

    logger.info("Created campaign '%s' -> %s", campaign_name, campaign_resource)
    return campaign_resource


# ---------------------------------------------------------------------------
# Ad group and ads
# ---------------------------------------------------------------------------


def add_shopping_ad_group(
    client: GoogleAdsClient,
    customer_id: str,
    campaign_resource_name: str,
    ad_group_name: str,
    cpc_bid_micros: int = 1_000_000,
) -> Optional[str]:
    """Create a shopping ad group. Returns ad group resource name."""
    ad_group_service = client.get_service("AdGroupService")

    op = client.get_type("AdGroupOperation")
    ad_group = op.create
    ad_group.name = ad_group_name
    ad_group.campaign = campaign_resource_name
    ad_group.type_ = client.enums.AdGroupTypeEnum.SHOPPING_PRODUCT_ADS
    ad_group.cpc_bid_micros = cpc_bid_micros
    ad_group.status = client.enums.AdGroupStatusEnum.ENABLED

    try:
        response = _mutate_with_retry(
            "create ad group",
            lambda: ad_group_service.mutate_ad_groups(
                customer_id=customer_id, operations=[op]
            ),
        )
        resource = response.results[0].resource_name
        logger.info("Created ad group '%s' -> %s", ad_group_name, resource)
        return resource
    except GoogleAdsException as ex:
        logger.error("Failed to create ad group '%s': %s", ad_group_name, ex)
        _last_gads_error["msg"] = _gads_err(ex)
        return None


def add_shopping_product_ad_group_ad(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resource_name: str,
) -> Optional[str]:
    """Create a product shopping ad in the ad group. Returns ad resource name."""
    ad_group_ad_service = client.get_service("AdGroupAdService")

    op = client.get_type("AdGroupAdOperation")
    ad_group_ad = op.create
    ad_group_ad.ad_group = ad_group_resource_name
    ad_group_ad.status = client.enums.AdGroupAdStatusEnum.ENABLED
    ad_group_ad.ad.shopping_product_ad = client.get_type("ShoppingProductAdInfo")

    try:
        response = _mutate_with_retry(
            "create product ad",
            lambda: ad_group_ad_service.mutate_ad_group_ads(
                customer_id=customer_id, operations=[op]
            ),
        )
        resource = response.results[0].resource_name
        logger.info("Created shopping product ad -> %s", resource)
        return resource
    except GoogleAdsException as ex:
        logger.error("Failed to create shopping product ad: %s", ex)
        _last_gads_error["msg"] = _gads_err(ex)
        return None


# ---------------------------------------------------------------------------
# Negative keywords
# ---------------------------------------------------------------------------


def add_negative_keywords(
    client: GoogleAdsClient,
    customer_id: str,
    campaign_resource_name: str,
    keywords: List[str],
) -> int:
    """
    Add negative keywords to a campaign as both EXACT and PHRASE match
    (matching the original create GSD-campaigns.py behaviour).
    Returns count of successfully added criteria.
    """
    if not keywords:
        return 0

    campaign_criterion_service = client.get_service("CampaignCriterionService")
    ops = []

    for kw in keywords:
        for match_type in (
            client.enums.KeywordMatchTypeEnum.EXACT,
            client.enums.KeywordMatchTypeEnum.PHRASE,
        ):
            op = client.get_type("CampaignCriterionOperation")
            criterion = op.create
            criterion.campaign = campaign_resource_name
            criterion.negative = True
            criterion.keyword.text = kw
            criterion.keyword.match_type = match_type
            ops.append(op)

    try:
        response = _mutate_with_retry(
            "negative keywords",
            lambda: campaign_criterion_service.mutate_campaign_criteria(
                customer_id=customer_id, operations=ops
            ),
        )
        count = len(response.results)
        logger.info("Added %d negative keywords to %s", count, campaign_resource_name)
        return count
    except GoogleAdsException as ex:
        logger.error("Failed to add negative keywords: %s", ex)
        return 0


# ---------------------------------------------------------------------------
# Listing group tree builders (product partitions)
# ---------------------------------------------------------------------------


def create_listing_group_subdivision(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resource_name: str,
    parent_resource_name: Optional[str],
    dimension: Optional[Any] = None,
    temp_id: Optional[int] = None,
) -> Any:
    """
    Create a listing group SUBDIVISION operation (non-leaf node).
    Returns the operation and the resource name.
    """
    ad_group_criterion_service = client.get_service("AdGroupCriterionService")

    op = client.get_type("AdGroupCriterionOperation")
    criterion = op.create
    criterion.ad_group = ad_group_resource_name
    criterion.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
    criterion.listing_group.type_ = (
        client.enums.ListingGroupTypeEnum.SUBDIVISION
    )

    if temp_id is not None:
        # ad_group_criterion_path needs THREE components
        # (customer_id, ad_group_id, criterion_id); the ad group already exists,
        # so pull its id out of the resource name. temp_id is negative (a temp
        # criterion id) so children can reference this root within the same
        # atomic mutate.
        ad_group_id = ad_group_resource_name.split("/")[-1]
        criterion.resource_name = ad_group_criterion_service.ad_group_criterion_path(
            customer_id, ad_group_id, str(temp_id)
        )

    if parent_resource_name is not None:
        criterion.listing_group.parent_ad_group_criterion = parent_resource_name

    if dimension is not None:
        criterion.listing_group.case_value = dimension

    return op, criterion.resource_name


def create_listing_group_unit_biddable(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resource_name: str,
    parent_resource_name: str,
    dimension: Optional[Any] = None,
    cpc_bid_micros: int = 1_000_000,
    negative: bool = False,
) -> Any:
    """
    Create a listing group UNIT operation (leaf node). Biddable by default;
    with negative=True it's an excluded leaf (no bid), used for the "other"
    catch-all so only the targeted products serve.
    Returns the operation.
    """
    op = client.get_type("AdGroupCriterionOperation")
    criterion = op.create
    criterion.ad_group = ad_group_resource_name
    criterion.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
    criterion.listing_group.type_ = client.enums.ListingGroupTypeEnum.UNIT
    criterion.listing_group.parent_ad_group_criterion = parent_resource_name
    if negative:
        criterion.negative = True
    else:
        criterion.cpc_bid_micros = cpc_bid_micros

    if dimension is not None:
        criterion.listing_group.case_value = dimension

    return op


# The product custom-label (INDEX0) VALUE on the products uses spaces, not the
# underscored campaign-label form (matches the original create GSD-campaigns.py
# `labels = ["a","b","c","no data","no ean"]`).
_CPR_LABEL_VALUE = {"no_data": "no data", "no_ean": "no ean"}


def add_sub_cpr(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resource_name: str,
    label: str,
    cpc_bid_micros: int = 50_000,
) -> bool:
    """
    Create the CPR listing group tree, matching create GSD-campaigns.py `addSub`:
    a SUBDIVISION root, a biddable UNIT for product_custom_attribute[INDEX0] equal
    to this label's value (plus the invld_ean / nd_c / nd_cr nodes for no_data),
    and an excluded ("other") catch-all so only this label's products serve.
    """
    ad_group_criterion_service = client.get_service("AdGroupCriterionService")
    value = _CPR_LABEL_VALUE.get(label, label)

    def _dim(v):
        d = client.get_type("ListingDimensionInfo")
        d.product_custom_attribute.index = client.enums.ProductCustomAttributeIndexEnum.INDEX0
        if v is not None:
            d.product_custom_attribute.value = v
        return d

    reset_temp_ids()
    ops = []
    root_op, root_resource = create_listing_group_subdivision(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=None, dimension=None, temp_id=next_id(),
    )
    ops.append(root_op)

    # Biddable unit for this label's products.
    ops.append(create_listing_group_unit_biddable(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=root_resource, dimension=_dim(value),
        cpc_bid_micros=cpc_bid_micros,
    ))
    # no_data also carries the invld_ean / nd_c / nd_cr custom-label values.
    if label == "no_data":
        for extra in ("invld_ean", "nd_c", "nd_cr"):
            ops.append(create_listing_group_unit_biddable(
                client, customer_id, ad_group_resource_name,
                parent_resource_name=root_resource, dimension=_dim(extra),
                cpc_bid_micros=cpc_bid_micros,
            ))
    # "other" catch-all — excluded so the campaign only serves its own label.
    ops.append(create_listing_group_unit_biddable(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=root_resource, dimension=_dim(None), negative=True,
    ))

    try:
        _mutate_with_retry(
            "listing group tree",
            lambda: ad_group_criterion_service.mutate_ad_group_criteria(
                customer_id=customer_id, operations=ops
            ),
        )
        logger.info("Created CPR listing group tree (label=%s) for %s", label, ad_group_resource_name)
        return True
    except GoogleAdsException as ex:
        logger.error("Failed to create CPR listing group: %s", ex)
        _last_gads_error["msg"] = _gads_err(ex)
        return False


def add_sub_cpc(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resource_name: str,
    label: str,
) -> bool:
    """
    Create the listing group tree with price buckets for CPC campaigns.
    Uses BIDS_AB for a,b labels and BIDS_C for c labels.
    """
    ad_group_criterion_service = client.get_service("AdGroupCriterionService")

    # LABELS_CPC = ["a,b", "c,no_data,no_ean"]. Test the FIRST token, not a bare
    # `"a" in label` substring — the latter is always true for "c,no_data,no_ean"
    # ("no_data" contains an 'a'), so BIDS_C was never selected and the c bucket
    # got the higher AB bids.
    bids = BIDS_AB if label.split(",")[0].strip().lower() == "a" else BIDS_C

    reset_temp_ids()
    ops = []

    # Root subdivision
    root_temp_id = next_id()
    root_op, root_resource = create_listing_group_subdivision(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=None,
        dimension=None,
        temp_id=root_temp_id,
    )
    ops.append(root_op)

    # Price bucket units
    for i, bucket in enumerate(PRICE_BUCKETS):
        bid_micros = int(bids[i] * 1_000_000)

        dimension = client.get_type("ListingDimensionInfo")
        dimension.product_custom_attribute.index = (
            client.enums.ProductCustomAttributeIndexEnum.INDEX0
        )
        dimension.product_custom_attribute.value = bucket

        unit_op = create_listing_group_unit_biddable(
            client, customer_id, ad_group_resource_name,
            parent_resource_name=root_resource,
            dimension=dimension,
            cpc_bid_micros=bid_micros,
        )
        ops.append(unit_op)

    # "Everything else" (OTHERS) unit. The subdivision partitions on
    # product_custom_attribute INDEX0, so the catch-all leaf must carry a
    # ListingDimensionInfo of the SAME dimension type with the index set and no
    # value — passing dimension=None leaves case_value unset and the API rejects
    # the whole atomic mutate.
    other_dimension = client.get_type("ListingDimensionInfo")
    other_dimension.product_custom_attribute.index = (
        client.enums.ProductCustomAttributeIndexEnum.INDEX0
    )
    other_op = create_listing_group_unit_biddable(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=root_resource,
        dimension=other_dimension,
        cpc_bid_micros=int(bids[0] * 1_000_000),
    )
    ops.append(other_op)

    try:
        _mutate_with_retry(
            "listing group tree",
            lambda: ad_group_criterion_service.mutate_ad_group_criteria(
                customer_id=customer_id, operations=ops
            ),
        )
        logger.info("Created CPC listing group tree for %s", ad_group_resource_name)
        return True
    except GoogleAdsException as ex:
        logger.error("Failed to create CPC listing group: %s", ex)
        _last_gads_error["msg"] = _gads_err(ex)
        return False


# ---------------------------------------------------------------------------
# Main GSD script flow
# ---------------------------------------------------------------------------


def _find_account_info(country: str, campaign_type: str) -> Optional[Dict[str, str]]:
    """Find account info by country and type."""
    key = f"{country.upper()}_{campaign_type.upper()}"
    return ACCOUNTS.get(key)


def _get_or_create_mc_account(
    mc_parent_id: str, shop_name: str, ads_customer_id: str
) -> Optional[str]:
    """Find or create a Merchant Center sub-account and link to Google Ads."""
    _last_mc_error["msg"] = None  # cleared per attempt; set on failure below
    try:
        mc_id = get_mc_id(mc_parent_id, shop_name)
    except Exception as ex:
        # Lookup failed — abort rather than create, or we risk a duplicate
        # sub-account for a shop that may already have one.
        logger.error("MC lookup failed for '%s'; skipping create to avoid a duplicate: %s",
                     shop_name, ex)
        _last_mc_error["msg"] = _mc_err(ex)
        return None
    if mc_id is None:
        mc_id = create_merchant_id(mc_parent_id, shop_name)
        if mc_id is None:
            return None
    link_to_google_ads(mc_parent_id, mc_id, ads_customer_id)
    return mc_id


def _set_campaign_status_by_resource(
    client: GoogleAdsClient, customer_id: str, campaign_resource: str, status: str
) -> bool:
    """Set an existing campaign's status via its resource name, reusing the
    shared client (unlike _mutate_campaign_status, which builds a fresh one).
    Returns True on success."""
    campaign_service = client.get_service("CampaignService")
    op = client.get_type("CampaignOperation")
    op.update.resource_name = campaign_resource
    op.update.status = getattr(client.enums.CampaignStatusEnum, status)
    op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
    try:
        _mutate_with_retry(
            f"set status -> {status}",
            lambda: campaign_service.mutate_campaigns(customer_id=customer_id, operations=[op]),
        )
        return True
    except GoogleAdsException as ex:
        logger.error("Failed to set %s to %s: %s", campaign_resource, status, ex)
        _last_gads_error["msg"] = _gads_err(ex)
        return False


def _tree_targets_label(ga, customer_id, campaign_id, campaign_type, label) -> bool:
    """
    True if the campaign's listing tree looks correct for its custom label: a
    SUBDIVISION root, and (for CPR) a biddable UNIT keyed on
    product_custom_attribute[INDEX0] == the label's value. Catches the legacy
    single-root-UNIT tree and trees targeting the wrong label value.
    """
    crits = list(ga.search(customer_id=customer_id, query=(
        "SELECT ad_group_criterion.listing_group.type, "
        "ad_group_criterion.listing_group.parent_ad_group_criterion, "
        "ad_group_criterion.negative, "
        "ad_group_criterion.listing_group.case_value.product_custom_attribute.value "
        f"FROM ad_group_criterion WHERE campaign.id = {campaign_id} "
        "AND ad_group_criterion.type = 'LISTING_GROUP' "
        "AND ad_group_criterion.status != 'REMOVED'")))
    if not crits:
        return False
    # A single biddable root UNIT (the old wrong tree) has no subdivision root.
    has_subdiv_root = any(
        c.ad_group_criterion.listing_group.type_.name == "SUBDIVISION"
        and not c.ad_group_criterion.listing_group.parent_ad_group_criterion
        for c in crits)
    if not has_subdiv_root:
        return False
    if campaign_type != "CPR":
        return True  # CPC uses a price-bucket subdivision tree; root check suffices
    expected = _CPR_LABEL_VALUE.get(label, label)
    return any(
        c.ad_group_criterion.listing_group.type_.name == "UNIT"
        and not c.ad_group_criterion.negative
        and c.ad_group_criterion.listing_group.case_value.product_custom_attribute.value == expected
        for c in crits)


def _remove_listing_tree(client, customer_id, campaign_id) -> None:
    """Remove all (non-removed) LISTING_GROUP criteria for a campaign's ad group."""
    ga = client.get_service("GoogleAdsService")
    svc = client.get_service("AdGroupCriterionService")
    crits = list(ga.search(customer_id=customer_id, query=(
        f"SELECT ad_group_criterion.resource_name FROM ad_group_criterion "
        f"WHERE campaign.id = {campaign_id} AND ad_group_criterion.type = 'LISTING_GROUP' "
        f"AND ad_group_criterion.status != 'REMOVED'")))
    if not crits:
        return
    ops = []
    for c in crits:
        op = client.get_type("AdGroupCriterionOperation")
        op.remove = c.ad_group_criterion.resource_name
        ops.append(op)
    _mutate_with_retry(
        "remove listing tree",
        lambda: svc.mutate_ad_group_criteria(customer_id=customer_id, operations=ops),
    )


def _repair_campaign(client, customer_id, campaign_resource, campaign_name,
                     campaign_type, label) -> Dict[str, Any]:
    """
    An existing campaign was found. Complete/repair it and leave it PAUSED:
    - missing ad group / product ad / listing tree -> create the missing pieces
    - a present but WRONG listing tree (single root UNIT, or not targeting this
      campaign's custom label) -> remove it and rebuild the correct one
    - fully complete AND correctly targeted -> skip unchanged
    """
    ga = client.get_service("GoogleAdsService")
    campaign_id = campaign_resource.rstrip("/").split("/")[-1]

    ags = list(ga.search(customer_id=customer_id, query=(
        f"SELECT ad_group.resource_name FROM ad_group "
        f"WHERE campaign.id = {campaign_id} AND ad_group.status != 'REMOVED'")))
    ad_group_resource = ags[0].ad_group.resource_name if ags else None
    has_ad = has_lg = False
    if ad_group_resource:
        has_ad = bool(list(ga.search(customer_id=customer_id, query=(
            f"SELECT ad_group_ad.ad.id FROM ad_group_ad "
            f"WHERE campaign.id = {campaign_id} AND ad_group_ad.status != 'REMOVED'"))))
        has_lg = bool(list(ga.search(customer_id=customer_id, query=(
            f"SELECT ad_group_criterion.criterion_id FROM ad_group_criterion "
            f"WHERE campaign.id = {campaign_id} AND ad_group_criterion.type = 'LISTING_GROUP'"))))

    # Validate the existing tree targets this campaign's label; drop a wrong one
    # so it gets rebuilt below.
    retree = False
    if has_lg and not _tree_targets_label(ga, customer_id, campaign_id, campaign_type, label):
        try:
            _remove_listing_tree(client, customer_id, campaign_id)
        except GoogleAdsException as ex:
            logger.error("Failed to remove wrong listing tree for '%s': %s", campaign_name, ex)
            _last_gads_error["msg"] = _gads_err(ex)
            return {"campaign_name": campaign_name, "action": "error", "reason": "repair_retree_failed",
                    "error": _last_gads_error["msg"], "campaign_resource": campaign_resource}
        has_lg = False
        retree = True

    if ad_group_resource and has_ad and has_lg:
        return {"campaign_name": campaign_name, "action": "skipped", "reason": "already_exists"}

    # Incomplete/mis-targeted — complete the missing pieces (stays PAUSED).
    logger.info("Repairing campaign '%s' (ad_group=%s ad=%s listing=%s retree=%s)",
                campaign_name, bool(ad_group_resource), has_ad, has_lg, retree)
    _last_gads_error["msg"] = None
    if not ad_group_resource:
        ad_group_resource = add_shopping_ad_group(
            client, customer_id, campaign_resource, label)
        if ad_group_resource is None:
            return {"campaign_name": campaign_name, "action": "error", "reason": "repair_ad_group_failed",
                    "error": _last_gads_error["msg"] or "ad group creation failed", "campaign_resource": campaign_resource}
    if not has_ad and add_shopping_product_ad_group_ad(client, customer_id, ad_group_resource) is None:
        return {"campaign_name": campaign_name, "action": "error", "reason": "repair_product_ad_failed",
                "error": _last_gads_error["msg"] or "product ad creation failed", "campaign_resource": campaign_resource}
    if not has_lg:
        tree_ok = (add_sub_cpr(client, customer_id, ad_group_resource, label) if campaign_type == "CPR"
                   else add_sub_cpc(client, customer_id, ad_group_resource, label))
        if not tree_ok:
            return {"campaign_name": campaign_name, "action": "error", "reason": "repair_listing_group_failed",
                    "error": _last_gads_error["msg"] or "listing group creation failed", "campaign_resource": campaign_resource}
    # Leave PAUSED (GSD campaigns are created paused); repair only completes the
    # missing structure so the shell is valid, it does not go live.
    return {"campaign_name": campaign_name, "action": "created",
            "reason": "retreed" if retree else "repaired", "campaign_resource": campaign_resource}


def _create_campaigns_for_shop(
    client: GoogleAdsClient,
    customer_id: str,
    mc_id: str,
    shop_name: str,
    shop_id: int,
    country: str,
    campaign_type: str,
    label_resource_name: str,
) -> List[Dict[str, Any]]:
    """
    Create all GSD campaigns for a shop (one per label).
    Returns a list of result dicts.
    """
    labels = LABELS_CPR if campaign_type == "CPR" else LABELS_CPC
    tracking_template = TRACKING_TEMPLATES.get(country.upper(), TRACKING_TEMPLATES["NL"])
    results = []

    for label in labels:
        if _run_cancel["cancel"]:
            break  # stop before the next campaign; run_gsd_script marks the run cancelled
        campaign_name = _build_campaign_name(country, shop_name, shop_id, label)
        _last_gads_error["msg"] = None  # cleared per label; helpers set it on failure

        # Existing campaign? Complete an incomplete shell, else skip.
        existing = check_campaign(client, customer_id, campaign_name)
        if existing:
            results.append(_repair_campaign(
                client, customer_id, existing, campaign_name, campaign_type, label))
            continue

        # Create campaign
        campaign_resource = add_standard_shopping_campaign(
            client=client,
            customer_id=customer_id,
            campaign_name=campaign_name,
            merchant_id=mc_id,
            country=country,
            tracking_template=tracking_template,
            label_resource_name=label_resource_name,
        )
        if campaign_resource is None:
            results.append({
                "campaign_name": campaign_name,
                "action": "error",
                "reason": "campaign_creation_failed",
                "error": _last_gads_error["msg"] or "campaign creation failed",
            })
            continue

        # Create ad group
        ad_group_name = label  # ad group is named after the label (a/b/c/no_data/no_ean), matching the original script
        ad_group_resource = add_shopping_ad_group(
            client, customer_id, campaign_resource, ad_group_name
        )
        if ad_group_resource is None:
            results.append({
                "campaign_name": campaign_name,
                "action": "error",
                "reason": "ad_group_creation_failed",
                "error": _last_gads_error["msg"] or "ad group creation failed",
            })
            continue

        # Create product ad. The campaign was created PAUSED, so a failure here
        # leaves a paused (non-spending) shell we report as an error rather than
        # a live campaign with no product ad.
        product_ad = add_shopping_product_ad_group_ad(client, customer_id, ad_group_resource)
        if product_ad is None:
            results.append({
                "campaign_name": campaign_name,
                "action": "error",
                "reason": "product_ad_creation_failed",
                "error": _last_gads_error["msg"] or "product ad creation failed",
                "campaign_resource": campaign_resource,
            })
            continue

        # Create listing group tree
        if campaign_type == "CPR":
            tree_ok = add_sub_cpr(client, customer_id, ad_group_resource, label)
        else:
            tree_ok = add_sub_cpc(client, customer_id, ad_group_resource, label)
        if not tree_ok:
            results.append({
                "campaign_name": campaign_name,
                "action": "error",
                "reason": "listing_group_creation_failed",
                "error": _last_gads_error["msg"] or "listing group creation failed",
                "campaign_resource": campaign_resource,
            })
            continue

        # Add negative keywords (best-effort).
        negatives = get_negatives(shop_name)
        if negatives:
            add_negative_keywords(client, customer_id, campaign_resource, negatives)

        # Leave the campaign PAUSED — the original script creates GSD campaigns
        # paused and never enables them; enabling is done separately/manually.
        results.append({
            "campaign_name": campaign_name,
            "action": "created",
            "campaign_resource": campaign_resource,
        })

    return results


def _pause_campaigns_for_shop(
    client: GoogleAdsClient,
    customer_id: str,
    shop_name: str,
) -> List[Dict[str, Any]]:
    """
    Pause all active GSD campaigns for a shop in a given account.
    Only pauses campaigns that carry the GSD_SCRIPT label.
    Returns a list of result dicts.
    """
    results = []

    # Find this shop's ENABLED campaigns that carry the GSD_SCRIPT label.
    # Match the exact "[shop:NAME]" token via REGEXP_MATCH (LIKE with brackets
    # matches the whole account — see _name_contains_regexp) AND restrict to the
    # GSD_SCRIPT label so only script-managed campaigns are ever paused.
    ga_service = client.get_service("GoogleAdsService")
    name_pattern = _name_contains_regexp(f"[shop:{shop_name}]")
    query = f"""
        SELECT campaign.id, campaign.name, campaign.status, campaign.resource_name
        FROM campaign_label
        WHERE campaign.name REGEXP_MATCH '{name_pattern}'
          AND campaign.status = 'ENABLED'
          AND label.name = '{SCRIPT_LABEL}'
    """

    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            campaign_id = str(row.campaign.id)
            campaign_name = row.campaign.name
            result = pause_campaign(customer_id, campaign_id)
            action = "paused" if result["success"] else "error"
            if action == "paused":
                logger.info(
                    "Paused campaign '%s' (id=%s) in account %s for shop '%s'",
                    campaign_name, campaign_id, customer_id, shop_name,
                )
            else:
                logger.error(
                    "Failed to pause campaign '%s' (id=%s) in account %s: %s",
                    campaign_name, campaign_id, customer_id, result,
                )
            results.append({
                "campaign_name": campaign_name,
                "campaign_id": campaign_id,
                "action": action,
                "detail": result,
            })
    except GoogleAdsException as ex:
        logger.error("Error finding campaigns to pause for '%s': %s", shop_name, ex)
        results.append({
            "shop_name": shop_name,
            "action": "error",
            "reason": str(ex),
        })

    return results


# Progress for the GSD preview, polled by the frontend to drive its progress bar.
# Single-flight is fine here (one preview at a time in practice).
_preview_progress: Dict[str, Any] = {"current": 0, "total": 0, "running": False}


def get_preview_progress() -> Dict[str, Any]:
    return dict(_preview_progress)


def preview_gsd_script(
    date_str: Optional[str] = None,
    shop_names: Optional[List[str]] = None,
    included: bool = False,
) -> Dict[str, Any]:
    """
    Dry-run of run_gsd_script: report how many GSD campaigns WOULD be created
    and how many WOULD be paused for the current shop changes, without changing
    anything.

    Mirrors run_gsd_script's shop -> country -> label expansion but issues only
    read-only queries. No Merchant Center accounts, budgets, campaigns, ad
    groups, or status changes are created or modified.

    Returns a summary dict with totals (to_create, to_pause, already_exists) and
    a per-shop breakdown.
    """
    client = _get_client()
    summary: Dict[str, Any] = {
        "date": date_str or datetime.now().strftime("%Y-%m-%d"),
        "preview": True,
        "to_create": 0,
        "already_exists": 0,
        "to_pause": 0,
        "shops_aan": 0,
        "shops_uit": 0,
        "by_shop": [],
        # Flat list of the affected campaigns for a table view.
        # Each: {campaign_name, action: create|pause|skip, shop_name, country, type}
        "campaigns": [],
        "errors": [],
    }

    _preview_progress.update({"current": 0, "total": 0, "running": True})

    try:
        changes = get_redshift_shop_changes(date_str, shop_names, included)
    except Exception as ex:
        logger.error("Preview: failed to get shop changes from Redshift: %s", ex)
        summary["errors"].append({"step": "redshift_query", "error": str(ex)})
        _preview_progress["running"] = False
        return summary

    if not changes:
        logger.info("Preview: no shop changes found for %s", summary["date"])
        _preview_progress["running"] = False
        return summary

    _preview_progress["total"] = len(changes)
    ga_service = client.get_service("GoogleAdsService")

    for change in changes:
        shop_id = change.get("shop_id")
        shop_name = change.get("shop_name", "")
        actie = change.get("actie", "")
        model = change.get("model", "CPR")

        campaign_type = model.upper() if model else "CPR"
        if campaign_type not in ("CPR", "CPC"):
            campaign_type = "CPR"

        # Same country/label expansion as run_gsd_script: only the country whose
        # GSD flag flipped (from the feed's `kolom`), NOT every model country.
        country = KOLOM_COUNTRY.get(change.get("kolom"))
        countries = [country] if country else []
        labels = LABELS_CPR if campaign_type == "CPR" else LABELS_CPC

        shop_row: Dict[str, Any] = {
            "shop_name": shop_name,
            "shop_id": shop_id,
            "actie": actie,
            "type": campaign_type,
            "to_create": 0,
            "already_exists": 0,
            "to_pause": 0,
        }
        if actie == "aan":
            summary["shops_aan"] += 1
        elif actie == "uit":
            summary["shops_uit"] += 1

        for country in countries:
            account_info = _find_account_info(country, campaign_type)
            if account_info is None:
                summary["errors"].append({
                    "shop_name": shop_name,
                    "country": country,
                    "type": campaign_type,
                    "error": "no_account_config",
                })
                continue
            customer_id = account_info["customer_id"]

            # One read-only lookup of this shop's existing (non-removed) campaigns
            # in this account, reused for both the create and pause counts.
            # REGEXP_MATCH (not LIKE) — see _name_contains_regexp for why.
            name_pattern = _name_contains_regexp(f"[shop:{shop_name}]")
            query = f"""
                SELECT campaign.name, campaign.status
                FROM campaign
                WHERE campaign.name REGEXP_MATCH '{name_pattern}'
                  AND campaign.status != 'REMOVED'
            """
            try:
                rows = list(ga_service.search(customer_id=customer_id, query=query))
            except GoogleAdsException as ex:
                logger.error("Preview: campaign lookup failed for '%s' in %s: %s",
                             shop_name, country, ex)
                summary["errors"].append({
                    "shop_name": shop_name,
                    "country": country,
                    "error": str(ex),
                })
                continue

            if actie == "aan":
                # A campaign is created only when its exact name doesn't exist yet
                # (matches check_campaign in _create_campaigns_for_shop).
                existing_names = {r.campaign.name for r in rows}
                for label in labels:
                    campaign_name = _build_campaign_name(country, shop_name, shop_id, label)
                    exists = campaign_name in existing_names
                    if exists:
                        shop_row["already_exists"] += 1
                    else:
                        shop_row["to_create"] += 1
                    summary["campaigns"].append({
                        "campaign_name": campaign_name,
                        "action": "skip" if exists else "create",
                        "shop_name": shop_name,
                        "country": country,
                        "type": campaign_type,
                    })
            elif actie == "uit":
                # Pause hits every currently-ENABLED campaign for the shop
                # (matches _pause_campaigns_for_shop).
                for r in rows:
                    if r.campaign.status.name == "ENABLED":
                        shop_row["to_pause"] += 1
                        summary["campaigns"].append({
                            "campaign_name": r.campaign.name,
                            "action": "pause",
                            "shop_name": shop_name,
                            "country": country,
                            "type": campaign_type,
                        })

        summary["to_create"] += shop_row["to_create"]
        summary["already_exists"] += shop_row["already_exists"]
        summary["to_pause"] += shop_row["to_pause"]
        summary["by_shop"].append(shop_row)
        _preview_progress["current"] += 1

    _preview_progress["running"] = False
    logger.info(
        "GSD preview: %d to create, %d to pause, %d already exist across %d shops",
        summary["to_create"], summary["to_pause"], summary["already_exists"], len(changes),
    )
    return summary


def run_gsd_script(
    date_str: Optional[str] = None,
    shop_names: Optional[List[str]] = None,
    included: bool = False,
) -> Dict[str, Any]:
    """
    Main GSD campaign creation/pausing flow.

    For each shop change from Redshift:
    - If action='aan': find/create MC account, link to Google Ads,
      create campaigns with labels, ad groups, product groups, negative keywords.
    - If action='uit': pause campaigns.

    Parameters
    ----------
    date_str : date string (YYYY-MM-DD), defaults to today.
    shop_names : optional list of shop names to process (filter).
    included : if True, also process shops already included.

    Returns a results dict summarizing what was done.
    """
    client = _get_client()
    # Labels are per-account and stable across a run — resolve each customer_id
    # once instead of per shop × country (#14).
    label_cache: Dict[str, str] = {}
    overall_results: Dict[str, Any] = {
        "date": date_str or datetime.now().strftime("%Y-%m-%d"),
        "created": [],
        "paused": [],
        "errors": [],
        "skipped": [],
        "cancelled": False,
    }
    sheet_rows: List[List[Any]] = []                        # one row per processed shop, for the log sheet
    run_date = datetime.now().strftime("%d-%m-%Y")          # dd-mm-yyyy, matching the original sheet format
    _run_cancel["cancel"] = False  # fresh run
    _run_progress.update({"current": 0, "total": 0, "running": True})

    # Get shop changes from Redshift
    try:
        changes = get_redshift_shop_changes(date_str, shop_names, included)
    except Exception as ex:
        logger.error("Failed to get shop changes from Redshift: %s", ex)
        overall_results["errors"].append({"step": "redshift_query", "error": str(ex)})
        _run_progress["running"] = False
        return overall_results

    if not changes:
        logger.info("No shop changes found for %s", overall_results["date"])
        _run_progress["running"] = False
        return overall_results

    logger.info("Processing %d shop changes", len(changes))
    _run_progress["total"] = len(changes)

    for idx, change in enumerate(changes):
        _run_progress["current"] = idx
        if _run_cancel["cancel"]:
            overall_results["cancelled"] = True
            logger.info("GSD run cancelled after %d/%d shop changes", idx, len(changes))
            break
        shop_id = change.get("shop_id")
        shop_name = change.get("shop_name", "")
        actie = change.get("actie", "")
        model = change.get("model", "CPR")
        branded_yes = str(change.get("branded", "")).strip().lower() in ("1", "true", "t", "ja", "yes")

        # Determine campaign type
        campaign_type = model.upper() if model else "CPR"
        if campaign_type not in ("CPR", "CPC"):
            campaign_type = "CPR"

        # Act only on the country whose GSD flag flipped (the feed's `kolom`),
        # NOT every model country — a shop flagged for one country must not
        # create/pause campaigns in the others.
        country = KOLOM_COUNTRY.get(change.get("kolom"))
        countries = [country] if country else []

        for country in countries:
            if _run_cancel["cancel"]:
                overall_results["cancelled"] = True
                break
            account_info = _find_account_info(country, campaign_type)
            if account_info is None:
                overall_results["errors"].append({
                    "shop_name": shop_name,
                    "country": country,
                    "type": campaign_type,
                    "error": "no_account_config",
                })
                continue

            customer_id = account_info["customer_id"]
            mc_parent_id = account_info["mc_id"]

            if actie == "aan":
                # Ensure label exists (cached per account)
                try:
                    if customer_id not in label_cache:
                        label_cache[customer_id] = ensure_campaign_label_exists(client, customer_id)
                    label_resource = label_cache[customer_id]
                except Exception as ex:
                    overall_results["errors"].append({
                        "shop_name": shop_name,
                        "step": "ensure_label",
                        "error": str(ex),
                    })
                    continue

                # Get or create MC sub-account and link
                mc_id = _get_or_create_mc_account(mc_parent_id, shop_name, customer_id)
                if mc_id is None:
                    overall_results["errors"].append({
                        "shop_name": shop_name,
                        "country": country,
                        "step": "mc_account",
                        "error": _last_mc_error["msg"] or "failed_to_get_or_create_mc_account",
                    })
                    continue

                # Create campaigns
                campaign_results = _create_campaigns_for_shop(
                    client=client,
                    customer_id=customer_id,
                    mc_id=mc_id,
                    shop_name=shop_name,
                    shop_id=shop_id,
                    country=country,
                    campaign_type=campaign_type,
                    label_resource_name=label_resource,
                )

                for cr in campaign_results:
                    cr["shop_name"] = shop_name
                    cr["country"] = country
                    cr["type"] = campaign_type
                    cr["customer_id"] = customer_id
                    # Expose the numeric id (parsed from the resource name) so a
                    # later "undo" can pause exactly what was created.
                    res = cr.get("campaign_resource") or ""
                    if res:
                        cr["campaign_id"] = res.rstrip("/").split("/")[-1]
                    if cr["action"] == "created":
                        overall_results["created"].append(cr)
                    elif cr["action"] == "skipped":
                        overall_results["skipped"].append(cr)
                    else:
                        overall_results["errors"].append(cr)

                # Log one row for this shop (mirrors the original sheet).
                created_count = sum(1 for cr in campaign_results if cr.get("action") == "created")
                sheet_rows.append([
                    run_date, str(shop_id or ""), shop_name or "", campaign_type,
                    str(mc_id or ""), country or "",
                    ("ja" if branded_yes else "nee"),
                    ("ja" if created_count > 0 else "nee"),
                    "aan",
                ])

            elif actie == "uit":
                # Pause campaigns
                pause_results = _pause_campaigns_for_shop(
                    client=client,
                    customer_id=customer_id,
                    shop_name=shop_name,
                )

                for pr in pause_results:
                    pr["shop_name"] = shop_name
                    pr["country"] = country
                    pr["type"] = campaign_type
                    pr["customer_id"] = customer_id
                    if pr["action"] == "paused":
                        overall_results["paused"].append(pr)
                    else:
                        overall_results["errors"].append(pr)

                # Log one row for this shop (MC ID not looked up on pause; matches
                # the original: op brand? = n.v.t., campagnes aangemaakt? = nee).
                sheet_rows.append([
                    run_date, str(shop_id or ""), shop_name or "", campaign_type,
                    "", country or "", "n.v.t.", "nee", "uit",
                ])

    # Safety net: cancel may fire on the last shop/label, so the loops above
    # end naturally without hitting a cancel check — flag it here regardless.
    if _run_cancel["cancel"]:
        overall_results["cancelled"] = True

    logger.info(
        "GSD script complete: %d created, %d paused, %d skipped, %d errors",
        len(overall_results["created"]),
        len(overall_results["paused"]),
        len(overall_results["skipped"]),
        len(overall_results["errors"]),
    )
    if overall_results["paused"]:
        for p in overall_results["paused"]:
            logger.info(
                "  PAUSED: '%s' (id=%s) shop=%s country=%s type=%s",
                p.get("campaign_name"), p.get("campaign_id"),
                p.get("shop_name"), p.get("country"), p.get("type"),
            )
    if overall_results["created"]:
        for c in overall_results["created"]:
            logger.info(
                "  CREATED: '%s' (id=%s) shop=%s country=%s type=%s",
                c.get("campaign_name"), c.get("campaign_id"),
                c.get("shop_name"), c.get("country"), c.get("type"),
            )

    # Append this run to the log sheet (best-effort; never fails the run).
    overall_results["sheet_log"] = _log_run_to_sheet(sheet_rows)

    _run_progress["running"] = False
    return overall_results
