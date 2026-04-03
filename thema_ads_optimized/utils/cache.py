"""Caching utilities for performance optimization."""

from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class GlobalCache:
    """Thread-safe global cache for API responses."""

    def __init__(self):
        self._labels: Dict[str, Dict[str, str]] = {}  # customer_id -> {label_name: resource_name}
        self._campaigns: Dict[str, Dict[str, str]] = {}  # customer_id -> {campaign_name: resource_name}
        self._ad_groups: Dict[str, Dict[str, Any]] = {}  # customer_id -> {ad_group_id: data}

    def get_label(self, customer_id: str, label_name: str) -> Optional[str]:
        """Get cached label resource name."""
        return self._labels.get(customer_id, {}).get(label_name)

    def set_label(self, customer_id: str, label_name: str, resource_name: str):
        """Cache label resource name."""
        if customer_id not in self._labels:
            self._labels[customer_id] = {}
        self._labels[customer_id][label_name] = resource_name

    def set_labels_bulk(self, customer_id: str, labels: Dict[str, str]):
        """Cache multiple labels at once."""
        if customer_id not in self._labels:
            self._labels[customer_id] = {}
        self._labels[customer_id].update(labels)
        logger.debug(f"Cached {len(labels)} labels for customer {customer_id}")

    def get_campaign(self, customer_id: str, campaign_name: str) -> Optional[str]:
        """Get cached campaign resource name."""
        return self._campaigns.get(customer_id, {}).get(campaign_name)

    def set_campaign(self, customer_id: str, campaign_name: str, resource_name: str):
        """Cache campaign resource name."""
        if customer_id not in self._campaigns:
            self._campaigns[customer_id] = {}
        self._campaigns[customer_id][campaign_name] = resource_name

    def get_ad_group_data(self, customer_id: str, ad_group_id: str) -> Optional[Any]:
        """Get cached ad group data."""
        return self._ad_groups.get(customer_id, {}).get(ad_group_id)

    def set_ad_group_data(self, customer_id: str, ad_group_id: str, data: Any):
        """Cache ad group data."""
        if customer_id not in self._ad_groups:
            self._ad_groups[customer_id] = {}
        self._ad_groups[customer_id][ad_group_id] = data

    def clear_customer(self, customer_id: str):
        """Clear all cached data for a customer."""
        self._labels.pop(customer_id, None)
        self._campaigns.pop(customer_id, None)
        self._ad_groups.pop(customer_id, None)
        logger.debug(f"Cleared cache for customer {customer_id}")

    def clear_all(self):
        """Clear entire cache."""
        self._labels.clear()
        self._campaigns.clear()
        self._ad_groups.clear()
        logger.debug("Cleared entire cache")
