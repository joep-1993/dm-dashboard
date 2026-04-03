#!/usr/bin/env python3
"""
Check specific ad group 174058099183 for duplicates.
"""
import os
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, '/app/thema_ads_optimized')

from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient

# Load environment variables
env_path = Path('/app/thema_ads_optimized/.env')
load_dotenv(env_path)

# Initialize client
client = GoogleAdsClient.load_from_dict({
    'developer_token': os.getenv('GOOGLE_DEVELOPER_TOKEN'),
    'refresh_token': os.getenv('GOOGLE_REFRESH_TOKEN'),
    'client_id': os.getenv('GOOGLE_CLIENT_ID'),
    'client_secret': os.getenv('GOOGLE_CLIENT_SECRET'),
    'login_customer_id': os.getenv('GOOGLE_LOGIN_CUSTOMER_ID'),
    'token_uri': 'https://oauth2.googleapis.com/token',
    'use_proto_plus': True
})

customer_id = '6213822688'
ad_group_id = '174058099183'
ga_service = client.get_service("GoogleAdsService")

# First check ad group status
print(f"=== Checking Ad Group {ad_group_id} Status ===")
ag_query = f"""
    SELECT
        ad_group.id,
        ad_group.name,
        ad_group.status,
        campaign.id,
        campaign.name,
        campaign.status
    FROM ad_group
    WHERE ad_group.id = {ad_group_id}
"""

response = ga_service.search(customer_id=customer_id, query=ag_query)
for row in response:
    print(f"Ad Group: {row.ad_group.name}")
    print(f"Ad Group Status: {row.ad_group.status.name}")
    print(f"Campaign: {row.campaign.name}")
    print(f"Campaign Status: {row.campaign.status.name}")
    print()

# Now get all ads in this ad group
print(f"=== Fetching All Ads in Ad Group {ad_group_id} ===")
ads_query = f"""
    SELECT
        ad_group_ad.ad.id,
        ad_group_ad.status,
        ad_group_ad.ad.responsive_search_ad.headlines,
        ad_group_ad.ad.responsive_search_ad.descriptions,
        ad_group_ad.ad.final_urls
    FROM ad_group_ad
    WHERE ad_group_ad.ad_group = 'customers/{customer_id}/adGroups/{ad_group_id}'
    AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
    AND ad_group_ad.status IN (ENABLED, PAUSED)
"""

ads = []
response = ga_service.search(customer_id=customer_id, query=ads_query)
for row in response:
    ad = row.ad_group_ad.ad
    rsa = ad.responsive_search_ad

    headlines = tuple(sorted(h.text for h in rsa.headlines))
    descriptions = tuple(sorted(d.text for d in rsa.descriptions))

    ads.append({
        'ad_id': ad.id,
        'status': row.ad_group_ad.status.name,
        'headlines': headlines,
        'descriptions': descriptions,
        'final_urls': list(ad.final_urls)
    })

print(f"Total ads found: {len(ads)}")
print()

# Check for duplicates
content_map = defaultdict(list)
for ad in ads:
    content_key = (ad['headlines'], ad['descriptions'])
    content_map[content_key].append(ad)

duplicates_found = 0
for content_key, ad_list in content_map.items():
    if len(ad_list) > 1:
        duplicates_found += 1
        print(f"=== Duplicate Set {duplicates_found} ({len(ad_list)} ads) ===")
        for i, ad in enumerate(ad_list, 1):
            print(f"  Ad {i}:")
            print(f"    ID: {ad['ad_id']}")
            print(f"    Status: {ad['status']}")
            print(f"    Headlines (first 2): {', '.join(list(ad['headlines'])[:2])}...")
            print(f"    Descriptions (first 1): {list(ad['descriptions'])[0]}...")
        print()

if duplicates_found == 0:
    print("✓ No duplicates found in this ad group")
else:
    print(f"Found {duplicates_found} duplicate sets")
