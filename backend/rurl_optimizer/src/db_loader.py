"""
Data loading module for Beslist.nl R-URL Optimizer.
Supports loading from Redshift database or cached CSV files.
"""

import pandas as pd
from pathlib import Path
from typing import Optional
import sys

# Add parent directory to path for config import
sys.path.insert(0, str(Path(__file__).parent.parent))
import config


class DataLoader:
    """Handles loading category and facet data from database or cache."""

    def __init__(self, use_cache: bool = True):
        """
        Initialize the data loader.

        Args:
            use_cache: If True, prefer loading from CSV cache files.
        """
        self.use_cache = use_cache
        self._connection = None

    def _get_connection(self):
        """Create Redshift database connection if not using cache."""
        if self._connection is None:
            try:
                import psycopg2
                self._connection = psycopg2.connect(
                    host=config.DB_CONFIG['host'],
                    port=config.DB_CONFIG['port'],
                    database=config.DB_CONFIG['database'],
                    user=config.DB_CONFIG['user'],
                    password=config.DB_CONFIG['password'],
                    sslmode=config.DB_CONFIG['sslmode']
                )
            except Exception as e:
                raise ConnectionError(f"Failed to connect to Redshift: {e}")
        return self._connection

    def load_main_categories(self) -> pd.DataFrame:
        """
        Load main categories from dim_category.

        Returns:
            DataFrame with columns: cat_id, name, table_name
        """
        cache_path = config.CACHE_DIR / "main_categories.csv"

        if self.use_cache and cache_path.exists():
            return pd.read_csv(cache_path)

        query = """
        SELECT DISTINCT
            main_category_id AS cat_id,
            main_category_name AS name,
            table_name
        FROM beslistbi.datamart.dim_category
        WHERE category_is_live = 1
        """
        df = pd.read_sql(query, self._get_connection())

        # Cache for future use
        df.to_csv(cache_path, index=False)

        return df

    def load_categories(self) -> pd.DataFrame:
        """
        Load category URLs from tblcategories_online.

        Returns:
            DataFrame with columns: cat_id, url_name, display_name

        Note V20: Excludes product-series categories where both cat_id and p3 are filled.
        These are categories like "Tefal Easy Fry Dual XXL" that should not be used
        for subcategory name matching as they are product-specific landing pages.
        """
        cache_path = config.CACHE_DIR / "categories.csv"

        if self.use_cache and cache_path.exists():
            return pd.read_csv(cache_path)

        # V20: Exclude categories where cat_id AND p3 are both filled
        # These are product-series categories (e.g., Tefal Easy Fry Dual XXL)
        # V22: Also exclude INACTIVE categories
        query = """
        SELECT
            cat_id,
            url_name,
            display_name
        FROM beslistbi.hda.tblcategories_online
        WHERE actual_ind = 1
            AND deleted_ind = 0
            AND cat_is_live = 1
            AND (p3 IS NULL OR cat_id IS NULL)
            AND display_name NOT ILIKE 'INACTIVE%'
        """
        df = pd.read_sql(query, self._get_connection())

        df.to_csv(cache_path, index=False)

        return df

    def load_facets(self) -> pd.DataFrame:
        """
        Load facet values from facet_facetvalues.

        Returns:
            DataFrame with facet data for NL.
        """
        cache_path = config.CACHE_DIR / "facets.csv"

        if self.use_cache and cache_path.exists():
            return pd.read_csv(cache_path)

        query = """
        SELECT *
        FROM beslistbi.bt.facet_facetvalues
        WHERE actual_ind = 1
            AND deleted_ind = 0
            AND country = 'nl'
        """
        df = pd.read_sql(query, self._get_connection())

        df.to_csv(cache_path, index=False)

        return df

    def load_r_urls(self, filepath: str) -> pd.DataFrame:
        """
        Load R-URLs to process from CSV file.

        Args:
            filepath: Path to CSV with R-URLs

        Returns:
            DataFrame with R-URL data
        """
        return pd.read_csv(filepath)

    def save_to_cache(self, df: pd.DataFrame, filename: str) -> Path:
        """
        Save a DataFrame to the cache directory.

        Args:
            df: DataFrame to save
            filename: Name of the cache file (e.g., 'facets.csv')

        Returns:
            Path to the saved file
        """
        cache_path = config.CACHE_DIR / filename
        df.to_csv(cache_path, index=False)
        return cache_path

    def close(self):
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None


def create_sample_facets_cache():
    """
    Create a sample facets.csv file for testing.
    Based on the example from CLAUDE.md (Parasols category).
    """
    sample_data = {
        'facet_id': [1, 1, 1, 2, 2, 2, 3, 3, 3],
        'facet_name': [
            'type_parasol', 'type_parasol', 'type_parasol',
            'kleur', 'kleur', 'kleur',
            'merk', 'merk', 'merk'
        ],
        'facet_value_id': [
            3599193, 3599194, 3599195,
            100, 101, 102,
            200, 201, 202
        ],
        'facet_value_name': [
            'Zweefparasols', 'Stokparasols', 'Strandparasols',
            'Grijs', 'Zwart', 'Wit',
            'Madison', 'Platinum', 'Doppler'
        ],
        'url': [
            '/products/tuin_accessoires/tuin_accessoires_504063/c/type_parasol~3599193',
            '/products/tuin_accessoires/tuin_accessoires_504063/c/type_parasol~3599194',
            '/products/tuin_accessoires/tuin_accessoires_504063/c/type_parasol~3599195',
            '/products/tuin_accessoires/tuin_accessoires_504063/c/kleur~100',
            '/products/tuin_accessoires/tuin_accessoires_504063/c/kleur~101',
            '/products/tuin_accessoires/tuin_accessoires_504063/c/kleur~102',
            '/products/tuin_accessoires/tuin_accessoires_504063/c/merk~200',
            '/products/tuin_accessoires/tuin_accessoires_504063/c/merk~201',
            '/products/tuin_accessoires/tuin_accessoires_504063/c/merk~202',
        ]
    }

    df = pd.DataFrame(sample_data)
    cache_path = config.CACHE_DIR / "facets.csv"
    df.to_csv(cache_path, index=False)
    print(f"Sample facets cache created at: {cache_path}")
    return df


if __name__ == "__main__":
    # Create sample data for testing
    create_sample_facets_cache()

    # Test loading
    loader = DataLoader(use_cache=True)
    facets = loader.load_facets()
    print(f"\nLoaded {len(facets)} facet records")
    print(facets.head())
