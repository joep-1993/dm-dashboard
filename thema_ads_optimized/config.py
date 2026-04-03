"""Configuration management for thema ads optimizer."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class GoogleAdsConfig:
    """Google Ads API configuration."""
    developer_token: str
    refresh_token: str
    client_id: str
    client_secret: str
    login_customer_id: str
    use_proto_plus: bool = True


@dataclass
class PerformanceConfig:
    """Performance tuning settings."""
    max_concurrent_customers: int = 10  # BALANCED: Increased from 5 for 2x throughput (combined with other optimizations)
    max_concurrent_operations: int = 50
    batch_size: int = 5000  # Keep at 5000 to avoid 503 errors
    api_retry_attempts: int = 5  # Increased for 503 retry handling
    api_retry_delay: float = 2.0  # Increased from 1.0
    api_batch_delay: float = 2.0  # Increased from 0.5 to avoid rate limits
    customer_delay: float = 5.0  # BALANCED: Reduced from 15.0s for faster customer transitions
    enable_caching: bool = True


@dataclass
class AppConfig:
    """Application configuration."""
    google_ads: GoogleAdsConfig
    performance: PerformanceConfig
    input_file: Path
    log_level: str = "INFO"
    dry_run: bool = False


def load_config_from_env() -> AppConfig:
    """Load configuration from environment variables."""

    # Validate required env vars
    required_vars = [
        "GOOGLE_CLIENT_ID",
        "GOOGLE_CLIENT_SECRET",
        "GOOGLE_DEVELOPER_TOKEN",
        "GOOGLE_REFRESH_TOKEN",
        "GOOGLE_LOGIN_CUSTOMER_ID"
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Please set them in your .env file or environment."
        )

    google_ads_config = GoogleAdsConfig(
        developer_token=os.getenv("GOOGLE_DEVELOPER_TOKEN"),
        refresh_token=os.getenv("GOOGLE_REFRESH_TOKEN"),
        client_id=os.getenv("GOOGLE_CLIENT_ID"),
        client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
        login_customer_id=os.getenv("GOOGLE_LOGIN_CUSTOMER_ID"),
        use_proto_plus=True
    )

    performance_config = PerformanceConfig(
        max_concurrent_customers=int(os.getenv("MAX_CONCURRENT_CUSTOMERS", "5")),
        max_concurrent_operations=int(os.getenv("MAX_CONCURRENT_OPERATIONS", "50")),
        batch_size=int(os.getenv("BATCH_SIZE", "5000")),
        api_retry_attempts=int(os.getenv("API_RETRY_ATTEMPTS", "5")),
        api_retry_delay=float(os.getenv("API_RETRY_DELAY", "2.0")),
        api_batch_delay=float(os.getenv("API_BATCH_DELAY", "2.0")),
        customer_delay=float(os.getenv("CUSTOMER_DELAY", "30.0")),
        enable_caching=os.getenv("ENABLE_CACHING", "true").lower() == "true"
    )

    input_file = Path(os.getenv("INPUT_FILE", "input_data.xlsx"))

    return AppConfig(
        google_ads=google_ads_config,
        performance=performance_config,
        input_file=input_file,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        dry_run=os.getenv("DRY_RUN", "false").lower() == "true"
    )
