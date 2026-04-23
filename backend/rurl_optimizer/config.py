"""
Configuration settings for the R-URL Redirect Optimizer.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent

# Redshift Database configuration.
# dm-tools' .env uses REDSHIFT_DB; the upstream tool used REDSHIFT_DATABASE — accept either.
DB_CONFIG = {
    "host": os.getenv("REDSHIFT_HOST"),
    "port": os.getenv("REDSHIFT_PORT", "5439"),
    "database": os.getenv("REDSHIFT_DATABASE") or os.getenv("REDSHIFT_DB", "beslistbi"),
    "user": os.getenv("REDSHIFT_USER"),
    "password": os.getenv("REDSHIFT_PASSWORD"),
    "sslmode": os.getenv("REDSHIFT_SSL_MODE", "require"),
}

# URL configuration
BASE_URL = "https://www.beslist.nl"
PRODUCTS_PREFIX = "/products"

# Matching thresholds - imported from validation_rules for consistency
# All validation rules are centralized in src/validation_rules.py
from src.validation_rules import FUZZY_THRESHOLD

# Paths
DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
CACHE_DIR = DATA_DIR / "cache"
OUTPUT_DIR = DATA_DIR / "output"
TEST_OUTPUT_DIR = PROJECT_ROOT / "test_output"

# Ensure directories exist
for dir_path in [INPUT_DIR, CACHE_DIR, OUTPUT_DIR, TEST_OUTPUT_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)
