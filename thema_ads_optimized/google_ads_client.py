"""Google Ads client initialization and management."""

import logging
from google.ads.googleads.client import GoogleAdsClient
from config import GoogleAdsConfig

logger = logging.getLogger(__name__)


def initialize_client(config: GoogleAdsConfig) -> GoogleAdsClient:
    """Initialize Google Ads API client from configuration."""

    client_config = {
        "developer_token": config.developer_token,
        "refresh_token": config.refresh_token,
        "client_id": config.client_id,
        "client_secret": config.client_secret,
        "login_customer_id": config.login_customer_id,
        "use_proto_plus": config.use_proto_plus,
    }

    try:
        client = GoogleAdsClient.load_from_dict(client_config)
        logger.info("Google Ads client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Google Ads client: {e}")
        raise
