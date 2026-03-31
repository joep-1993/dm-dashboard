"""
GSD Campaigns Service

Manages Google Shopping Direct (GSD) campaigns across multiple Google Ads accounts.
Handles campaign creation, pausing, enabling, and removal. Integrates with Merchant
Center for account linking and Redshift for shop change data.
"""
import os
import re
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any

import psycopg2
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
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

# Default daily budget in micros (e.g. 100 EUR = 100_000_000 micros)
DEFAULT_BUDGET_MICROS = 100_000_000

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


def _detect_country_for_account(customer_id: str) -> str:
    """Detect country from customer_id by checking ACCOUNTS."""
    for info in ACCOUNTS.values():
        if info["customer_id"] == customer_id:
            return info["country"]
    return "NL"


# ---------------------------------------------------------------------------
# Negative keywords helper
# ---------------------------------------------------------------------------


def get_negatives(shop_name: str) -> List[str]:
    """
    Extract negative keywords from a shop name.
    Splits the shop name into individual words for use as negative keywords.
    """
    if not shop_name:
        return []
    # Remove common suffixes, split on non-alphanumeric
    words = re.split(r"[^a-zA-Z0-9]+", shop_name.lower())
    return [w for w in words if w and len(w) > 1]


# ---------------------------------------------------------------------------
# Google Ads query helpers
# ---------------------------------------------------------------------------


def get_gsd_campaigns(customer_id: str) -> List[Dict[str, Any]]:
    """
    Query all non-REMOVED campaigns with the GSD_SCRIPT label for a given
    customer account. Returns last-30-day metrics.
    """
    client = _get_client()
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

    for account_key, info in ACCOUNTS.items():
        try:
            camps = get_gsd_campaigns(info["customer_id"])
            # Enrich with account info
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
    """Set campaign status (ENABLED, PAUSED, REMOVED)."""
    client = _get_client()
    campaign_service = client.get_service("CampaignService")

    campaign_op = client.get_type("CampaignOperation")
    campaign = campaign_op.update
    campaign.resource_name = campaign_service.campaign_path(customer_id, campaign_id)

    status_enum = client.enums.CampaignStatusEnum
    campaign.status = getattr(status_enum, status)

    field_mask = client.get_type("FieldMask")
    field_mask.paths.append("status")
    campaign_op.update_mask.CopyFrom(field_mask)

    try:
        response = campaign_service.mutate_campaigns(
            customer_id=customer_id, operations=[campaign_op]
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


# ---------------------------------------------------------------------------
# Redshift queries
# ---------------------------------------------------------------------------


def get_redshift_shop_changes(
    date_str: Optional[str] = None,
    shop_names: Optional[List[str]] = None,
    included: bool = False,
) -> List[Dict[str, Any]]:
    """
    Query Redshift for shops that changed GSD status.
    CPR shops: shop_deelt_data = 1
    CPC shops: shop_shares_data = 0

    Parameters
    ----------
    date_str : optional date string (YYYY-MM-DD), defaults to today.
    shop_names : optional list of shop names to filter on.
    included : if True, also include shops that are already included.

    Returns list of dicts with: shop_id, shop_name, kolom, actie, branded, model.
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    conn = _get_redshift_connection()
    try:
        with conn.cursor() as cur:
            # Base query for shops that changed GSD status
            query = """
                SELECT
                    shop_id,
                    shop_name,
                    kolom,
                    actie,
                    branded,
                    model
                FROM pa.gsd_shop_changes
                WHERE datum = %s
            """
            params: list = [date_str]

            if not included:
                query += " AND actie IN ('aan', 'uit')"

            if shop_names:
                placeholders = ",".join(["%s"] * len(shop_names))
                query += f" AND shop_name IN ({placeholders})"
                params.extend(shop_names)

            query += " ORDER BY shop_name, kolom"

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

    response = label_service.mutate_labels(customer_id=customer_id, operations=[label_op])
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
        campaign_label_service.mutate_campaign_labels(
            customer_id=customer_id, operations=[op]
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
    Returns the account ID if found, else None.
    """
    service = _get_mc_service()
    try:
        response = service.accounts().list(merchantId=mc_parent_id).execute()
        for account in response.get("accounts", []):
            if account.get("name", "").lower() == shop_name.lower():
                return str(account["id"])
    except Exception as ex:
        logger.error("Error looking up MC account for '%s': %s", shop_name, ex)
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
        return None


def link_to_google_ads(mc_parent_id: str, mc_account_id: str, ads_customer_id: str) -> bool:
    """
    Link a Merchant Center account to a Google Ads account.
    """
    service = _get_mc_service()
    try:
        # Get current account info
        account = service.accounts().get(merchantId=mc_parent_id, accountId=mc_account_id).execute()

        # Add Google Ads link if not already present
        ads_links = account.get("adsLinks", [])
        ads_id_str = str(ads_customer_id)
        already_linked = any(str(link.get("adsId", "")) == ads_id_str for link in ads_links)

        if not already_linked:
            ads_links.append({
                "adsId": ads_id_str,
                "status": "active",
            })
            account["adsLinks"] = ads_links
            service.accounts().update(
                merchantId=mc_parent_id, accountId=mc_account_id, body=account
            ).execute()
            logger.info("Linked MC %s to Google Ads %s", mc_account_id, ads_customer_id)

        return True
    except Exception as ex:
        logger.error("Error linking MC %s to Ads %s: %s", mc_account_id, ads_customer_id, ex)
        return False


def get_merchant_id_for_campaign(
    client: GoogleAdsClient, customer_id: str, campaign_resource_name: str
) -> Optional[str]:
    """Get the Merchant Center ID from an existing campaign's shopping setting."""
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT campaign.shopping_setting.merchant_id
        FROM campaign
        WHERE campaign.resource_name = '{campaign_resource_name}'
    """
    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            return str(row.campaign.shopping_setting.merchant_id)
    except GoogleAdsException as ex:
        logger.error("Error getting merchant ID for campaign: %s", ex)
    return None


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
        budget_response = campaign_budget_service.mutate_campaign_budgets(
            customer_id=customer_id, operations=[budget_op]
        )
        budget_resource = budget_response.results[0].resource_name
    except GoogleAdsException as ex:
        logger.error("Failed to create budget for '%s': %s", campaign_name, ex)
        return None

    # Step 2: Create campaign
    camp_op = client.get_type("CampaignOperation")
    campaign = camp_op.create
    campaign.name = campaign_name
    campaign.campaign_budget = budget_resource
    campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.SHOPPING
    campaign.status = client.enums.CampaignStatusEnum.ENABLED
    campaign.manual_cpc.enhanced_cpc_enabled = False

    # Shopping settings
    campaign.shopping_setting.merchant_id = int(merchant_id)
    campaign.shopping_setting.sales_country = country.upper()
    campaign.shopping_setting.campaign_priority = 0
    campaign.shopping_setting.enable_local = False

    # Tracking template
    campaign.tracking_url_template = tracking_template

    try:
        camp_response = campaign_service.mutate_campaigns(
            customer_id=customer_id, operations=[camp_op]
        )
        campaign_resource = camp_response.results[0].resource_name
    except GoogleAdsException as ex:
        logger.error("Failed to create campaign '%s': %s", campaign_name, ex)
        return None

    # Step 3: Add location targeting
    location_op = create_location_op(client, campaign_resource, country)
    try:
        campaign_criterion_service.mutate_campaign_criteria(
            customer_id=customer_id, operations=[location_op]
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
        response = ad_group_service.mutate_ad_groups(
            customer_id=customer_id, operations=[op]
        )
        resource = response.results[0].resource_name
        logger.info("Created ad group '%s' -> %s", ad_group_name, resource)
        return resource
    except GoogleAdsException as ex:
        logger.error("Failed to create ad group '%s': %s", ad_group_name, ex)
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
    ad_group_ad.ad.shopping_product_ad.CopyFrom(
        client.get_type("ShoppingProductAdInfo")
    )

    try:
        response = ad_group_ad_service.mutate_ad_group_ads(
            customer_id=customer_id, operations=[op]
        )
        resource = response.results[0].resource_name
        logger.info("Created shopping product ad -> %s", resource)
        return resource
    except GoogleAdsException as ex:
        logger.error("Failed to create shopping product ad: %s", ex)
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
    Add negative broad-match keywords to a campaign.
    Returns count of successfully added keywords.
    """
    if not keywords:
        return 0

    campaign_criterion_service = client.get_service("CampaignCriterionService")
    ops = []

    for kw in keywords:
        op = client.get_type("CampaignCriterionOperation")
        criterion = op.create
        criterion.campaign = campaign_resource_name
        criterion.negative = True
        criterion.keyword.text = kw
        criterion.keyword.match_type = client.enums.KeywordMatchTypeEnum.BROAD
        ops.append(op)

    try:
        response = campaign_criterion_service.mutate_campaign_criteria(
            customer_id=customer_id, operations=ops
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
        criterion.resource_name = ad_group_criterion_service.ad_group_criterion_path(
            customer_id, str(temp_id)
        )

    if parent_resource_name is not None:
        criterion.listing_group.parent_ad_group_criterion = parent_resource_name

    if dimension is not None:
        criterion.listing_group.case_value.CopyFrom(dimension)

    return op, criterion.resource_name


def create_listing_group_unit_biddable(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resource_name: str,
    parent_resource_name: str,
    dimension: Optional[Any] = None,
    cpc_bid_micros: int = 1_000_000,
) -> Any:
    """
    Create a listing group UNIT operation (leaf node) with a bid.
    Returns the operation.
    """
    op = client.get_type("AdGroupCriterionOperation")
    criterion = op.create
    criterion.ad_group = ad_group_resource_name
    criterion.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
    criterion.listing_group.type_ = client.enums.ListingGroupTypeEnum.UNIT
    criterion.listing_group.parent_ad_group_criterion = parent_resource_name
    criterion.cpc_bid_micros = cpc_bid_micros

    if dimension is not None:
        criterion.listing_group.case_value.CopyFrom(dimension)

    return op


def _create_price_dimension(client: GoogleAdsClient, low: int, high: Optional[int], currency: str = "EUR"):
    """Create a ProductCustomAttribute dimension for a price bucket."""
    dimension = client.get_type("ListingDimensionInfo")
    # Use custom_attribute for price buckets
    dimension.product_custom_attribute.index = (
        client.enums.ProductCustomAttributeIndexEnum.INDEX0
    )
    if high is not None:
        dimension.product_custom_attribute.value = f"{low}-{high}"
    else:
        dimension.product_custom_attribute.value = f"{low}-Onbeperkt"
    return dimension


def add_sub_cpr(
    client: GoogleAdsClient,
    customer_id: str,
    ad_group_resource_name: str,
    cpc_bid_micros: int = 1_000_000,
) -> bool:
    """
    Create the listing group tree for a CPR campaign.
    Simple: just a root UNIT that matches everything.
    """
    ad_group_criterion_service = client.get_service("AdGroupCriterionService")

    op = client.get_type("AdGroupCriterionOperation")
    criterion = op.create
    criterion.ad_group = ad_group_resource_name
    criterion.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
    criterion.listing_group.type_ = client.enums.ListingGroupTypeEnum.UNIT
    criterion.cpc_bid_micros = cpc_bid_micros

    try:
        ad_group_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id, operations=[op]
        )
        logger.info("Created CPR listing group tree for %s", ad_group_resource_name)
        return True
    except GoogleAdsException as ex:
        logger.error("Failed to create CPR listing group: %s", ex)
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

    bids = BIDS_AB if "a" in label.lower() else BIDS_C

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

    # "Everything else" unit (no dimension = catch-all)
    other_op = create_listing_group_unit_biddable(
        client, customer_id, ad_group_resource_name,
        parent_resource_name=root_resource,
        dimension=None,
        cpc_bid_micros=int(bids[0] * 1_000_000),
    )
    ops.append(other_op)

    try:
        ad_group_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id, operations=ops
        )
        logger.info("Created CPC listing group tree for %s", ad_group_resource_name)
        return True
    except GoogleAdsException as ex:
        logger.error("Failed to create CPC listing group: %s", ex)
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
    mc_id = get_mc_id(mc_parent_id, shop_name)
    if mc_id is None:
        mc_id = create_merchant_id(mc_parent_id, shop_name)
        if mc_id is None:
            return None
    link_to_google_ads(mc_parent_id, mc_id, ads_customer_id)
    return mc_id


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
        campaign_name = _build_campaign_name(country, shop_name, shop_id, label)

        # Check if campaign already exists
        existing = check_campaign(client, customer_id, campaign_name)
        if existing:
            logger.info("Campaign '%s' already exists, skipping.", campaign_name)
            results.append({
                "campaign_name": campaign_name,
                "action": "skipped",
                "reason": "already_exists",
            })
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
            })
            continue

        # Create ad group
        ad_group_name = f"{campaign_name} - Ad Group"
        ad_group_resource = add_shopping_ad_group(
            client, customer_id, campaign_resource, ad_group_name
        )
        if ad_group_resource is None:
            results.append({
                "campaign_name": campaign_name,
                "action": "error",
                "reason": "ad_group_creation_failed",
            })
            continue

        # Create product ad
        add_shopping_product_ad_group_ad(client, customer_id, ad_group_resource)

        # Create listing group tree
        if campaign_type == "CPR":
            add_sub_cpr(client, customer_id, ad_group_resource)
        else:
            add_sub_cpc(client, customer_id, ad_group_resource, label)

        # Add negative keywords
        negatives = get_negatives(shop_name)
        if negatives:
            add_negative_keywords(client, customer_id, campaign_resource, negatives)

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
    Returns a list of result dicts.
    """
    results = []

    # Find campaigns for this shop
    ga_service = client.get_service("GoogleAdsService")
    escaped_name = shop_name.replace("'", "\\'")
    query = f"""
        SELECT campaign.id, campaign.name, campaign.status, campaign.resource_name
        FROM campaign
        WHERE campaign.name LIKE '%[shop:{escaped_name}]%'
          AND campaign.status = 'ENABLED'
    """

    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            result = pause_campaign(customer_id, str(row.campaign.id))
            results.append({
                "campaign_name": row.campaign.name,
                "campaign_id": str(row.campaign.id),
                "action": "paused" if result["success"] else "error",
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
    overall_results: Dict[str, Any] = {
        "date": date_str or datetime.now().strftime("%Y-%m-%d"),
        "created": [],
        "paused": [],
        "errors": [],
        "skipped": [],
    }

    # Get shop changes from Redshift
    try:
        changes = get_redshift_shop_changes(date_str, shop_names, included)
    except Exception as ex:
        logger.error("Failed to get shop changes from Redshift: %s", ex)
        overall_results["errors"].append({"step": "redshift_query", "error": str(ex)})
        return overall_results

    if not changes:
        logger.info("No shop changes found for %s", overall_results["date"])
        return overall_results

    logger.info("Processing %d shop changes", len(changes))

    for change in changes:
        shop_id = change.get("shop_id")
        shop_name = change.get("shop_name", "")
        actie = change.get("actie", "")
        model = change.get("model", "CPR")

        # Determine campaign type
        campaign_type = model.upper() if model else "CPR"
        if campaign_type not in ("CPR", "CPC"):
            campaign_type = "CPR"

        # Process each country relevant to this change
        # CPR: NL, BE, DE; CPC: NL, BE
        countries = ["NL", "BE", "DE"] if campaign_type == "CPR" else ["NL", "BE"]

        for country in countries:
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
                # Ensure label exists
                try:
                    label_resource = ensure_campaign_label_exists(client, customer_id)
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
                        "error": "failed_to_get_or_create_mc_account",
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
                    if cr["action"] == "created":
                        overall_results["created"].append(cr)
                    elif cr["action"] == "skipped":
                        overall_results["skipped"].append(cr)
                    else:
                        overall_results["errors"].append(cr)

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
                    if pr["action"] == "paused":
                        overall_results["paused"].append(pr)
                    else:
                        overall_results["errors"].append(pr)

    logger.info(
        "GSD script complete: %d created, %d paused, %d skipped, %d errors",
        len(overall_results["created"]),
        len(overall_results["paused"]),
        len(overall_results["skipped"]),
        len(overall_results["errors"]),
    )

    return overall_results
