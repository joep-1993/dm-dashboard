#!/usr/bin/env python3
"""
Count ENABLED ad groups in HS/ campaigns for customer 6213822688.
"""
import os
import sys
from pathlib import Path

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
target_ag_id = '174058099183'
ga_service = client.get_service("GoogleAdsService")

# Get all ENABLED ad groups in HS/ campaigns (same query as remove_duplicates)
query = """
    SELECT
        ad_group.id,
        ad_group.name,
        campaign.name
    FROM ad_group
    WHERE ad_group.status = 'ENABLED'
    AND campaign.status = 'ENABLED'
    AND campaign.name LIKE 'HS/%'
"""

response = ga_service.search(customer_id=customer_id, query=query)
ad_groups = [(str(row.ad_group.id), row.ad_group.name, row.campaign.name) for row in response]

print(f"Total ENABLED ad groups in HS/ campaigns: {len(ad_groups)}")

# Find position of target ad group
target_position = None
for i, (ag_id, ag_name, campaign_name) in enumerate(ad_groups):
    if ag_id == target_ag_id:
        target_position = i + 1  # 1-indexed
        print(f"\nTarget ad group {target_ag_id} found at position {target_position}")
        print(f"  Name: {ag_name}")
        print(f"  Campaign: {campaign_name}")
        break

if target_position is None:
    print(f"\nWARNING: Target ad group {target_ag_id} NOT found in ENABLED ad groups in HS/ campaigns!")
    print("\nThis could mean:")
    print("  1. Ad group is not ENABLED")
    print("  2. Campaign is not ENABLED")
    print("  3. Campaign name doesn't start with 'HS/'")
else:
    if target_position <= 50:
        print(f"\n✓ Ad group IS within the first 50 (position {target_position})")
    else:
        print(f"\n✗ Ad group is BEYOND the first 50 (position {target_position})")
        print(f"  Need limit >= {target_position} to include this ad group")
