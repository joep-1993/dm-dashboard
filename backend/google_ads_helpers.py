"""
Google Ads Helper Functions

This file contains helper functions for Google Ads listing tree operations.
"""

import time
from google.ads.googleads.errors import GoogleAdsException

# Global counter for temporary resource names
_temp_id_counter = -1


def next_id():
    """Generate next temporary ID for criterion resource names"""
    global _temp_id_counter
    _temp_id_counter -= 1
    return _temp_id_counter


def list_listing_groups_with_depth(client, customer_id: str, ad_group_id: str):
    """
    List all listing groups in an ad group with their depth.

    Returns:
        tuple: (rows, max_depth)
    """
    ga_service = client.get_service("GoogleAdsService")
    ag_service = client.get_service("AdGroupService")
    ag_path = ag_service.ad_group_path(customer_id, ad_group_id)

    query = f"""
        SELECT
            ad_group_criterion.resource_name,
            ad_group_criterion.listing_group.type,
            ad_group_criterion.listing_group.parent_ad_group_criterion,
            ad_group_criterion.listing_group.case_value.product_custom_attribute.index,
            ad_group_criterion.listing_group.case_value.product_custom_attribute.value,
            ad_group_criterion.negative,
            ad_group_criterion.cpc_bid_micros
        FROM ad_group_criterion
        WHERE ad_group_criterion.ad_group = '{ag_path}'
            AND ad_group_criterion.type = 'LISTING_GROUP'
    """

    try:
        results = list(ga_service.search(customer_id=customer_id, query=query))

        # Calculate depth
        depth_map = {}

        def calculate_depth(row):
            res_name = row.ad_group_criterion.resource_name
            if res_name in depth_map:
                return depth_map[res_name]

            parent = row.ad_group_criterion.listing_group.parent_ad_group_criterion
            if not parent:
                depth = 0
            else:
                # Find parent row
                parent_row = next((r for r in results if r.ad_group_criterion.resource_name == parent), None)
                if parent_row:
                    depth = 1 + calculate_depth(parent_row)
                else:
                    depth = 1

            depth_map[res_name] = depth
            return depth

        for row in results:
            calculate_depth(row)

        max_depth = max(depth_map.values()) if depth_map else 0
        return results, max_depth

    except Exception:
        return [], 0


def safe_remove_entire_listing_tree(client, customer_id: str, ad_group_id: str):
    """
    Optimized version: Query only for the root node instead of all listing groups.
    This reduces API calls by directly finding the root without fetching the entire tree.
    """
    agc = client.get_service("AdGroupCriterionService")
    ga_service = client.get_service("GoogleAdsService")
    ag_service = client.get_service("AdGroupService")
    ag_path = ag_service.ad_group_path(customer_id, ad_group_id)

    # Query ONLY for the root node (no parent) - much faster than querying all nodes
    query = f"""
        SELECT
            ad_group_criterion.resource_name
        FROM ad_group_criterion
        WHERE ad_group_criterion.ad_group = '{ag_path}'
            AND ad_group_criterion.type = 'LISTING_GROUP'
            AND ad_group_criterion.listing_group.parent_ad_group_criterion IS NULL
        LIMIT 1
    """

    try:
        results = list(ga_service.search(customer_id=customer_id, query=query))
        if not results:
            return  # No root node found, tree already empty

        root_resource_name = results[0].ad_group_criterion.resource_name

        # Remove only the root - the API will cascade-delete all children
        op = client.get_type("AdGroupCriterionOperation")
        op.remove = root_resource_name

        agc.mutate_ad_group_criteria(customer_id=customer_id, operations=[op])
    except GoogleAdsException as ex:
        # Ignore if the tree is already gone or resource not found
        if not any(
            (getattr(e.error_code, "criterion_error", None) and
             e.error_code.criterion_error.name == "LISTING_GROUP_DOES_NOT_EXIST") or
            (getattr(e.error_code, "mutate_error", None) and
             e.error_code.mutate_error.name == "RESOURCE_NOT_FOUND")
            for e in ex.failure.errors
        ):
            raise


def create_listing_group_subdivision(
    client,
    customer_id,
    ad_group_id,
    parent_ad_group_criterion_resource_name=None,
    listing_dimension_info=None,
):
    operation = client.get_type("AdGroupCriterionOperation")
    ad_group_criterion = operation.create
    ad_group_criterion.resource_name = client.get_service(
        "AdGroupCriterionService"
    ).ad_group_criterion_path(customer_id, ad_group_id, next_id())
    ad_group_criterion.status = client.enums.AdGroupCriterionStatusEnum.ENABLED

    listing_group_info = ad_group_criterion.listing_group
    listing_group_info.type_ = client.enums.ListingGroupTypeEnum.SUBDIVISION
    if parent_ad_group_criterion_resource_name is not None:
        listing_group_info.parent_ad_group_criterion = parent_ad_group_criterion_resource_name
    if listing_dimension_info is not None:
        client.copy_from(listing_group_info.case_value, listing_dimension_info)
    return operation


def create_listing_group_unit_biddable(
        client,
        customer_id,
        ad_group_id,
        parent_ad_group_criterion_resource_name,
        listing_dimension_info,
        targeting_negative,
        cpc_bid_micros=None
):
    operation = client.get_type("AdGroupCriterionOperation")
    criterion = operation.create
    criterion.resource_name = client.get_service(
        "AdGroupCriterionService"
    ).ad_group_criterion_path(customer_id, ad_group_id, next_id())
    criterion.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
    if cpc_bid_micros and targeting_negative == False:
        criterion.cpc_bid_micros = cpc_bid_micros

    listing_group = criterion.listing_group
    listing_group.type_ = client.enums.ListingGroupTypeEnum.UNIT
    listing_group.parent_ad_group_criterion = parent_ad_group_criterion_resource_name

    # Case values contain the listing dimension used for the node.
    # For OTHERS units, pass a ListingDimensionInfo with index but no value
    if listing_dimension_info is not None:
        client.copy_from(listing_group.case_value, listing_dimension_info)

    if targeting_negative:
        criterion.negative = True
    return operation


def add_standard_shopping_campaign(
    client, customer_id, merchant_center_account_id, campaign_name, budget_name,
    tracking_template, country, shopid, shopname, label, budget, final_url_suffix=None,
    bidding_strategy_resource_name=None
):

    campaign_service = client.get_service("CampaignService")
    google_ads_service = client.get_service("GoogleAdsService")

    # Check if campaign already exists by exact name match
    # Escape single quotes in campaign name for GAQL (replace ' with \')
    escaped_campaign_name = campaign_name.replace("'", "\\'")
    query = f"""
    SELECT campaign.id, campaign.resource_name, campaign.status
    FROM campaign
    WHERE campaign.name = '{escaped_campaign_name}'
    """
    response = google_ads_service.search(customer_id=customer_id, query=query)
    campaign_exists_not_removed = None
    campaign_removed_found = False

    for row in response:
        if row.campaign.status == client.enums.CampaignStatusEnum.REMOVED:
            print(f"   Campaign '{campaign_name}' exists but is REMOVED. Will create a new one...")
            campaign_removed_found = True
        else:
            print(f"   ✅ Campaign '{campaign_name}' already exists (ID: {row.campaign.id}). Using existing campaign.")
            campaign_exists_not_removed = row.campaign.resource_name
            break

    if campaign_exists_not_removed:
        return campaign_exists_not_removed

    # Create a budget that is NOT shared by multiple campaigns
    campaign_budget_service = client.get_service("CampaignBudgetService")
    campaign_budget_operation = client.get_type("CampaignBudgetOperation")
    campaign_budget = campaign_budget_operation.create
    campaign_budget.name = budget_name
    campaign_budget.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD
    campaign_budget.amount_micros = budget
    #campaign_budget.amount_micros = 5000000
    campaign_budget.explicitly_shared = False

    try:
        campaign_budget_response = campaign_budget_service.mutate_campaign_budgets(
            customer_id=customer_id, operations=[campaign_budget_operation]
        )
    except GoogleAdsException as ex:
        #handle_googleads_exception(ex)
        return None

    # Create standard shopping campaign
    campaign_operation = client.get_type("CampaignOperation")
    campaign = campaign_operation.create
    campaign.name = campaign_name
    campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.SHOPPING
    campaign.shopping_setting.merchant_id = merchant_center_account_id
    campaign.shopping_setting.campaign_priority = 0
    campaign.shopping_setting.enable_local = True

    # Only set tracking_url_template if it's provided and not empty
    if tracking_template:
        campaign.tracking_url_template = tracking_template

    campaign.contains_eu_political_advertising = (
        client.enums.EuPoliticalAdvertisingStatusEnum.DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING
    )

    if final_url_suffix:
        campaign.final_url_suffix = final_url_suffix
    campaign.status = client.enums.CampaignStatusEnum.PAUSED

    # Set bidding strategy
    if bidding_strategy_resource_name:
        # Use portfolio bid strategy
        campaign.bidding_strategy = bidding_strategy_resource_name
    else:
        # Use manual CPC
        campaign.manual_cpc.enhanced_cpc_enabled = False

    campaign.campaign_budget = campaign_budget_response.results[0].resource_name
    time.sleep(1)
    try:
        campaign_response = campaign_service.mutate_campaigns(
            customer_id=customer_id, operations=[campaign_operation]
        )
    except GoogleAdsException as ex:
        print(f"Failed to create campaign '{campaign_name}': {ex}")
        response_retry = google_ads_service.search(customer_id=customer_id, query=query)
        for row in response_retry:
            if row.campaign.status != client.enums.CampaignStatusEnum.REMOVED:
                print(f"Campaign '{campaign_name}' gevonden na fout bij aanmaken.")
                return row.campaign.resource_name
        print(f"Kan campagne '{campaign_name}' niet aanmaken en geen actieve campagne gevonden.")
        return None

    campaign_resource_name = campaign_response.results[0].resource_name

    # Add location targeting
    campaign_id = campaign_resource_name.split("/")[-1]
    campaign_criterion_service = client.get_service("CampaignCriterionService")
    operations = [
        create_location_op(client, customer_id, campaign_id, country),
    ]
    try:
        campaign_criterion_service.mutate_campaign_criteria(
            customer_id=customer_id, operations=operations
        )
    except GoogleAdsException as ex:
        #handle_googleads_exception(ex)
        print(f'error: {ex}')

    # Voeg label 'GSD_SCRIPT' toe aan campagne
    campaign_label_service = client.get_service("CampaignLabelService")
    label_resource_name = ensure_campaign_label_exists(client, customer_id, script_label)
    if label_resource_name:
        campaign_label_operation = client.get_type("CampaignLabelOperation")
        campaign_label = campaign_label_operation.create
        campaign_label.campaign = campaign_resource_name
        campaign_label.label = label_resource_name
        time.sleep(2)

        try:
            campaign_label_service.mutate_campaign_labels(
                customer_id=customer_id, operations=[campaign_label_operation]
            )
            #print(f"                Label '{script_label}' toegevoegd aan campagne '{campaign_name}'.")
        except GoogleAdsException as ex:
            print(f'error: {ex}')
            #handle_googleads_exception(ex)
    else:
        print(f"Kon label '{script_label}' niet aanmaken of ophalen.")

    print(f"   ✅ Campaign created: {campaign_name}")
    return campaign_resource_name

def labelCampaign(client, customer_id, campaign_name, campaign_resource_name):

    # Voeg label 'GSD_SCRIPT' toe aan campagne
    campaign_label_service = client.get_service("CampaignLabelService")
    label_resource_name = ensure_campaign_label_exists(client, customer_id, script_label)
    if label_resource_name:
        campaign_label_operation = client.get_type("CampaignLabelOperation")
        campaign_label = campaign_label_operation.create
        campaign_label.campaign = campaign_resource_name
        campaign_label.label = label_resource_name
        time.sleep(2)

        try:
            campaign_label_service.mutate_campaign_labels(
                customer_id=customer_id, operations=[campaign_label_operation]
            )
            print(f"                Label '{script_label}' toegevoegd aan campagne '{campaign_name}'.")
        except GoogleAdsException as ex:
            #handle_googleads_exception(ex)
            print(f' error: {ex}')
    else:
        print(f"Kon label '{script_label}' niet aanmaken of ophalen.")

def create_location_op(client, customer_id, campaign_id, country):
    campaign_service = client.get_service("CampaignService")
    geo_target_constant_service = client.get_service("GeoTargetConstantService")

    if country == "NL":
        location_id = "2528"
    elif country == "BE":
        location_id = "2056"
    elif country == "DE":
        location_id = "2276"

    # Create the campaign criterion.
    campaign_criterion_operation = client.get_type("CampaignCriterionOperation")
    campaign_criterion = campaign_criterion_operation.create
    campaign_criterion.campaign = campaign_service.campaign_path(
        customer_id, campaign_id
    )

    # Besides using location_id, you can also search by location names from
    # GeoTargetConstantService.suggest_geo_target_constants() and directly
    # apply GeoTargetConstant.resource_name here. An example can be found
    # in get_geo_target_constant_by_names.py.
    campaign_criterion.location.geo_target_constant = (
        geo_target_constant_service.geo_target_constant_path(location_id)
    )

    return campaign_criterion_operation

def add_shopping_ad_group(client, customer_id, campaign_resource_name, ad_group_name, campaign_name):

    # Standard bid: 2 cents = 0.02 EUR = 20,000 micros
    adgroup_bid = 20000

    ad_group_service = client.get_service("AdGroupService")
    google_ads_service = client.get_service("GoogleAdsService")

    # Normalize ad group name
    if ad_group_name == "no ean":
        ad_group_name = "no_ean"
    elif ad_group_name == "no data":
        ad_group_name = "no_data"

    # Check if an ad group with this specific name exists in the campaign
    # Escape single quotes in ad group name for GAQL (replace ' with \')
    escaped_ad_group_name = ad_group_name.replace("'", "\\'")
    query = f"""
        SELECT ad_group.id, ad_group.resource_name, ad_group.name
        FROM ad_group
        WHERE ad_group.campaign = '{campaign_resource_name}'
        AND ad_group.name = '{escaped_ad_group_name}'
        AND ad_group.status != 'REMOVED'
        LIMIT 1
    """
    response = google_ads_service.search(customer_id=customer_id, query=query)

    for row in response:
        print(f"      ✅ Ad group '{ad_group_name}' already exists (ID: {row.ad_group.id}). Using existing ad group.")
        return row.ad_group.resource_name, False

    # No (active) ad group exists — create one
    ad_group_operation = client.get_type("AdGroupOperation")
    ad_group = ad_group_operation.create
    ad_group.campaign = campaign_resource_name
    ad_group.name = ad_group_name

    ad_group.cpc_bid_micros = adgroup_bid
    #ad_group.cpc_bid_micros = 200000  # Adjust bid if needed
    ad_group.status = client.enums.AdGroupStatusEnum.ENABLED

    try:
        ad_group_response = ad_group_service.mutate_ad_groups(
            customer_id=customer_id, operations=[ad_group_operation]
        )
    except GoogleAdsException as ex:
        print(f"      ⚠️  Failed to create ad group '{ad_group_name}'. Checking again...")
        return add_shopping_ad_group(client, customer_id, campaign_resource_name, ad_group_name, campaign_name)

    ad_group_resource_name = ad_group_response.results[0].resource_name
    print(f"      ✅ Ad group created: {ad_group_name}")
    return ad_group_resource_name, True


def ensure_campaign_label_exists(client, customer_id, label_name):
    """Zorgt ervoor dat het label 'label_name' bestaat, en retourneert de resource_name."""
    google_ads_service = client.get_service("GoogleAdsService")
    label_service = client.get_service("LabelService")

    query = f"""
    SELECT label.resource_name, label.name
    FROM label
    WHERE label.name = '{label_name}'
    """
    response = google_ads_service.search(customer_id=customer_id, query=query)

    for row in response:
        return row.label.resource_name

    # Label bestaat nog niet, dus aanmaken
    label_operation = client.get_type("LabelOperation")
    label = label_operation.create
    label.name = label_name

    try:
        label_response = label_service.mutate_labels(
            customer_id=customer_id, operations=[label_operation]
        )
        return label_response.results[0].resource_name
    except GoogleAdsException as ex:
        #handle_googleads_exception(ex)
        print(f'error: {ex}')
        return None

script_label = "DMA_SCRIPT_JVS"


def add_shopping_product_ad(client, customer_id, ad_group_resource_name):
    """
    Add a shopping product ad to an ad group.

    Shopping product ads don't need creative assets - they automatically
    pull product data from the Merchant Center feed.

    Args:
        client: GoogleAdsClient instance
        customer_id: Google Ads customer ID
        ad_group_resource_name: Resource name of the ad group

    Returns:
        str: Resource name of the created ad, or None if already exists
    """
    ad_group_ad_service = client.get_service("AdGroupAdService")
    google_ads_service = client.get_service("GoogleAdsService")

    # Check if ad already exists in this ad group
    query = f"""
        SELECT ad_group_ad.ad.id, ad_group_ad.resource_name
        FROM ad_group_ad
        WHERE ad_group_ad.ad_group = '{ad_group_resource_name}'
        AND ad_group_ad.status != 'REMOVED'
        LIMIT 1
    """

    try:
        response = google_ads_service.search(customer_id=customer_id, query=query)
        for row in response:
            print(f"      ℹ️  Shopping ad already exists in ad group (ID: {row.ad_group_ad.ad.id})")
            return row.ad_group_ad.resource_name
    except Exception:
        pass  # No existing ad found, proceed to create

    # Create shopping product ad
    # NOTE: For Shopping campaigns, ads are minimal - no URLs, no creative
    ad_group_ad_operation = client.get_type("AdGroupAdOperation")
    ad_group_ad = ad_group_ad_operation.create

    # Set ad group and status
    ad_group_ad.ad_group = ad_group_resource_name
    ad_group_ad.status = client.enums.AdGroupAdStatusEnum.ENABLED

    # For shopping product ads, we MUST explicitly set the union field
    # The ad_data oneof field requires us to set shopping_product_ad
    # We do this by creating an empty ShoppingProductAdInfo and assigning it
    shopping_product_ad_info = client.get_type("ShoppingProductAdInfo")
    # Assign the empty shopping product ad info to the ad
    # This properly sets the oneof union field
    ad_group_ad.ad._pb.shopping_product_ad.CopyFrom(shopping_product_ad_info._pb)

    try:
        ad_group_ad_response = ad_group_ad_service.mutate_ad_group_ads(
            customer_id=customer_id, operations=[ad_group_ad_operation]
        )
        ad_resource_name = ad_group_ad_response.results[0].resource_name
        print(f"      ✅ Shopping product ad created")
        return ad_resource_name
    except GoogleAdsException as ex:
        print(f"      ⚠️  Failed to create shopping ad: {ex}")
        return None

def enable_negative_list_for_campaign(
    client,
    customer_id: str,
    campaign_resource_name: str,
    negative_list_name: str
):
    """
    Enable a negative keyword list (shared set) for a campaign.

    Looks up the shared set by name and links it to the campaign.

    Args:
        client: GoogleAdsClient instance
        customer_id: Google Ads customer ID
        campaign_resource_name: Resource name of the campaign
        negative_list_name: Name of the negative keyword list (shared set)

    Returns:
        str: Resource name of the campaign shared set link, or None if failed
    """
    ga_service = client.get_service("GoogleAdsService")

    # First, look up the shared set by name
    query = f"""
        SELECT shared_set.resource_name, shared_set.name
        FROM shared_set
        WHERE shared_set.name = '{negative_list_name}'
            AND shared_set.type = 'NEGATIVE_KEYWORDS'
            AND shared_set.status = 'ENABLED'
        LIMIT 1
    """

    shared_set_resource_name = None
    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            shared_set_resource_name = row.shared_set.resource_name
            break
    except GoogleAdsException as ex:
        print(f"      ⚠️  Error looking up negative list '{negative_list_name}': {ex}")
        return None

    if not shared_set_resource_name:
        print(f"      ⚠️  Negative list '{negative_list_name}' not found")
        return None

    # Check if already linked
    query = f"""
        SELECT campaign_shared_set.resource_name
        FROM campaign_shared_set
        WHERE campaign_shared_set.campaign = '{campaign_resource_name}'
            AND campaign_shared_set.shared_set = '{shared_set_resource_name}'
            AND campaign_shared_set.status = 'ENABLED'
        LIMIT 1
    """

    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            print(f"      ℹ️  Negative list '{negative_list_name}' already linked to campaign")
            return row.campaign_shared_set.resource_name
    except GoogleAdsException:
        pass  # Not linked yet, proceed

    # Create the campaign shared set link
    campaign_shared_set_service = client.get_service("CampaignSharedSetService")
    campaign_shared_set_operation = client.get_type("CampaignSharedSetOperation")
    campaign_shared_set = campaign_shared_set_operation.create

    campaign_shared_set.campaign = campaign_resource_name
    campaign_shared_set.shared_set = shared_set_resource_name

    try:
        response = campaign_shared_set_service.mutate_campaign_shared_sets(
            customer_id=customer_id, operations=[campaign_shared_set_operation]
        )
        resource_name = response.results[0].resource_name
        print(f"      ✅ Negative list '{negative_list_name}' linked to campaign")
        return resource_name
    except GoogleAdsException as ex:
        print(f"      ⚠️  Failed to link negative list: {ex}")
        return None
