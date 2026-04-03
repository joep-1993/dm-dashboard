"""Data models for thema ads optimizer."""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class AdGroupInput:
    """Input data for processing an ad group."""
    customer_id: str
    campaign_name: str
    campaign_id: str
    ad_group_id: str
    ad_group_name: Optional[str] = None  # If provided, use for lookup instead of ID (Excel precision issue)
    theme_name: str = "singles_day"  # Theme to apply to this ad group


@dataclass
class ExistingAd:
    """Existing RSA data."""
    resource_name: str
    status: str
    headlines: List[str]
    descriptions: List[str]
    final_urls: List[str]
    path1: str
    path2: str


@dataclass
class CachedData:
    """Prefetched data for a customer."""
    labels: dict  # label_name -> resource_name
    existing_ads: dict  # ad_group_resource -> ExistingAd
    campaigns: dict  # campaign_name -> resource_name
    ad_group_labels: dict = None  # ad_group_resource -> has_SD_DONE_label (bool)


@dataclass
class ProcessingResult:
    """Result of processing an ad group."""
    customer_id: str
    ad_group_id: str
    success: bool
    new_ad_resource: Optional[str] = None
    error: Optional[str] = None
    operations_count: int = 0
