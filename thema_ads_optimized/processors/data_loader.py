"""Data loading from various sources."""

import logging
from pathlib import Path
from typing import List
import pandas as pd
from models import AdGroupInput

logger = logging.getLogger(__name__)


def load_from_excel(file_path: Path, sheet_name: str = "ad_groups") -> List[AdGroupInput]:
    """Load ad group data from Excel file."""

    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    logger.info(f"Loading data from Excel: {file_path}")

    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
        df = df.fillna("")

        # Get columns by position (as in original script)
        cols = list(df.columns)
        if len(cols) < 6:
            raise ValueError(f"Sheet '{sheet_name}' must have at least 6 columns")

        # Map positions to names (B=1, C=2, D=3, F=5)
        mapped = df[[cols[1], cols[2], cols[3], cols[5]]].copy()
        mapped.columns = ["customer_id", "campaign_name", "campaign_id", "ad_group_id"]

        # Clean data
        mapped["customer_id"] = mapped["customer_id"].astype(str).str.replace("-", "").str.strip()
        mapped["campaign_name"] = mapped["campaign_name"].astype(str).str.strip()
        mapped["campaign_id"] = mapped["campaign_id"].astype(str).str.strip()
        mapped["ad_group_id"] = mapped["ad_group_id"].astype(str).str.strip()

        # Filter empty rows
        mapped = mapped[
            (mapped["customer_id"] != "") &
            (mapped["ad_group_id"] != "") &
            (mapped["customer_id"] != "nan") &
            (mapped["ad_group_id"] != "nan")
        ]

        # Convert to models
        inputs = [
            AdGroupInput(
                customer_id=row["customer_id"],
                campaign_name=row["campaign_name"],
                campaign_id=row["campaign_id"],
                ad_group_id=row["ad_group_id"]
            )
            for _, row in mapped.iterrows()
        ]

        logger.info(f"Loaded {len(inputs)} ad groups from Excel")
        return inputs

    except Exception as e:
        logger.error(f"Failed to load Excel file: {e}")
        raise


def load_from_csv(file_path: Path) -> List[AdGroupInput]:
    """Load ad group data from CSV file."""

    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    logger.info(f"Loading data from CSV: {file_path}")

    try:
        df = pd.read_csv(file_path, dtype=str)
        df = df.fillna("")

        # Expect standard column names
        required_cols = ["customer_id", "campaign_name", "campaign_id", "ad_group_id"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")

        # Clean data
        df["customer_id"] = df["customer_id"].str.replace("-", "").str.strip()
        df["campaign_name"] = df["campaign_name"].str.strip()
        df["campaign_id"] = df["campaign_id"].str.strip()
        df["ad_group_id"] = df["ad_group_id"].str.strip()

        # Filter empty rows
        df = df[
            (df["customer_id"] != "") &
            (df["ad_group_id"] != "") &
            (df["customer_id"] != "nan") &
            (df["ad_group_id"] != "nan")
        ]

        # Convert to models
        inputs = [
            AdGroupInput(
                customer_id=row["customer_id"],
                campaign_name=row["campaign_name"],
                campaign_id=row["campaign_id"],
                ad_group_id=row["ad_group_id"]
            )
            for _, row in df.iterrows()
        ]

        logger.info(f"Loaded {len(inputs)} ad groups from CSV")
        return inputs

    except Exception as e:
        logger.error(f"Failed to load CSV file: {e}")
        raise


def load_data(file_path: Path) -> List[AdGroupInput]:
    """Load data from file (auto-detect format)."""

    suffix = file_path.suffix.lower()

    if suffix in [".xlsx", ".xls"]:
        return load_from_excel(file_path)
    elif suffix == ".csv":
        return load_from_csv(file_path)
    else:
        raise ValueError(f"Unsupported file format: {suffix}. Use .xlsx, .xls, or .csv")
