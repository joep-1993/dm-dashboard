"""
Facet filtering module.
Filters facets based on subcategory to narrow down matching candidates.
"""

import pandas as pd
from dataclasses import dataclass
from typing import Optional


@dataclass
class FacetValue:
    """Represents a single facet value."""
    facet_id: int
    facet_name: str
    facet_value_id: int
    facet_value_name: str
    url: str

    @property
    def url_fragment(self) -> str:
        """Get the URL fragment for this facet (e.g., 'type_parasol~3599193')."""
        return f"{self.facet_name}~{self.facet_value_id}"


class FacetFilter:
    """Filters facets based on subcategory."""

    def __init__(self, facets_df: pd.DataFrame):
        """
        Initialize with facets DataFrame.

        Args:
            facets_df: DataFrame from facet_facetvalues table.
                       Expected columns: facet_id, facet_name, facet_value_id,
                       facet_value_name, url (or similar)
        """
        self.facets_df = facets_df
        self._detect_columns()

    def _detect_columns(self):
        """Detect column names with flexibility for naming variations."""
        self.col_mapping = {
            'facet_id': self._find_column(['facet_id', 'FacetId', 'facetid']),
            'facet_name': self._find_column(['facet_name', 'FacetName', 'facetname', 'name']),
            'facet_value_id': self._find_column(['facet_value_id', 'FacetValueId', 'facetvalueid', 'value_id']),
            'facet_value_name': self._find_column(['facet_value', 'facet_value_name', 'FacetValueName', 'facetvaluename', 'value_name', 'display_name']),
            'url': self._find_column(['url', 'URL', 'Url', 'category_url', 'facet_url']),
            # v5: Added main_category columns for cross-category lookup
            'main_category_id': self._find_column(['main_category_id', 'MainCategoryId']),
            'main_category_name': self._find_column(['main_category_name', 'MainCategoryName']),
        }

    def _find_column(self, candidates: list[str]) -> Optional[str]:
        """Find first matching column name from candidates."""
        for col in candidates:
            if col in self.facets_df.columns:
                return col
        return None

    def filter_by_subcategory(self, subcategory_id: str) -> pd.DataFrame:
        """
        Filter facets to only those valid for a subcategory.

        Uses URL LIKE '%subcategory_id%' logic to find relevant facets.

        Args:
            subcategory_id: The subcategory ID (e.g., "504063")

        Returns:
            Filtered DataFrame with facets for this subcategory
        """
        url_col = self.col_mapping.get('url')

        if url_col is None:
            # Fallback: try to match on category_id column if available
            if 'category_id' in self.facets_df.columns:
                return self.facets_df[
                    self.facets_df['category_id'].astype(str) == str(subcategory_id)
                ].copy()
            # Return all facets if no filtering possible
            return self.facets_df.copy()

        # Filter where URL contains the subcategory ID
        mask = self.facets_df[url_col].astype(str).str.contains(
            str(subcategory_id),
            case=False,
            na=False
        )
        return self.facets_df[mask].copy()

    def filter_by_subcategory_name(self, subcategory_name: str) -> pd.DataFrame:
        """
        Alternative filter using subcategory name instead of ID.

        Args:
            subcategory_name: The subcategory name (e.g., "tuin_accessoires_504063")

        Returns:
            Filtered DataFrame with facets for this subcategory
        """
        url_col = self.col_mapping.get('url')

        if url_col is None:
            return self.facets_df.copy()

        mask = self.facets_df[url_col].astype(str).str.contains(
            subcategory_name,
            case=False,
            na=False
        )
        return self.facets_df[mask].copy()

    def extract_parent_subcategory_id(self, subcategory_name: str) -> Optional[str]:
        """
        v8: Extract parent subcategory ID from a subcategory name.

        Example:
            'huis_tuin_505313_505230' -> '505313' (parent)
            'huis_tuin_505313' -> None (no parent, direct main cat child)

        Args:
            subcategory_name: Full subcategory name (e.g., 'huis_tuin_505313_505230')

        Returns:
            Parent subcategory ID or None if no parent exists
        """
        # Split by underscore and find numeric IDs
        parts = subcategory_name.split('_')
        numeric_ids = [p for p in parts if p.isdigit()]

        # If there are 2+ numeric IDs, the first is the parent
        if len(numeric_ids) >= 2:
            return numeric_ids[0]
        return None

    def filter_by_parent_subcategory(self, subcategory_name: str) -> pd.DataFrame:
        """
        v8: Filter facets by parent subcategory for hierarchical fallback.

        Args:
            subcategory_name: Full subcategory name (e.g., 'huis_tuin_505313_505230')

        Returns:
            Filtered DataFrame with facets from parent subcategory
        """
        parent_id = self.extract_parent_subcategory_id(subcategory_name)
        if parent_id:
            return self.filter_by_subcategory(parent_id)
        return pd.DataFrame()

    def filter_by_main_category(self, main_category_name: str) -> pd.DataFrame:
        """
        v5/v16: Filter facets by main category name for cross-category type lookup.

        V16: URL matching is tried FIRST because the URL path (e.g., /mode/) is more
        reliable than partial name matching (which can match "mode" in "Mode accessoires").

        Args:
            main_category_name: The main category name (e.g., "tuin_accessoires", "huis_tuin", "mode")

        Returns:
            Filtered DataFrame with all facets in this main category
        """
        url_col = self.col_mapping.get('url')
        main_cat_col = self.col_mapping.get('main_category_name')

        # V16: Try URL matching FIRST - more reliable than name matching
        # This ensures /products/mode/ matches Kleding, not Mode accessoires
        if url_col:
            mask = self.facets_df[url_col].astype(str).str.contains(
                f"/products/{main_category_name}/",
                case=False,
                na=False
            )
            if mask.any():
                return self.facets_df[mask].copy()

        # Fallback to main_category_name column
        if main_cat_col and main_cat_col in self.facets_df.columns:
            mask = self.facets_df[main_cat_col].astype(str).str.lower().str.contains(
                main_category_name.lower().replace('_', ' '),
                case=False,
                na=False
            )
            if mask.any():
                return self.facets_df[mask].copy()

        return pd.DataFrame()

    def get_type_facets_only(self, filtered_df: pd.DataFrame) -> list[FacetValue]:
        """
        v5: Get only type facets (type_*, kleur, materiaal, etc.) from filtered DataFrame.

        Args:
            filtered_df: Filtered facets DataFrame

        Returns:
            List of FacetValue objects for type facets only
        """
        facet_name_col = self.col_mapping.get('facet_name')
        if not facet_name_col:
            return []

        # Filter for type facets
        type_prefixes = ('type_', 'kleur', 'materiaal', 'maat', 'vorm')
        mask = self.facets_df[facet_name_col].astype(str).str.lower().str.startswith(type_prefixes)
        type_df = filtered_df[filtered_df[facet_name_col].astype(str).str.lower().str.startswith(type_prefixes)]

        return self.get_facet_values(type_df)

    def get_all_type_facets(self) -> list[FacetValue]:
        """
        v5: Get ALL type facets across all categories.
        Used for cross-category type matching when subcategory has no type match.

        Returns:
            List of all FacetValue objects that are type facets
        """
        facet_name_col = self.col_mapping.get('facet_name')
        if not facet_name_col:
            return []

        type_prefixes = ('type_', )  # Only type_ for cross-category to be more specific
        mask = self.facets_df[facet_name_col].astype(str).str.lower().str.startswith(type_prefixes)
        type_df = self.facets_df[mask]

        return self.get_facet_values(type_df)

    def get_facet_values(self, filtered_df: pd.DataFrame, deduplicate_to_highest_level: bool = True) -> list[FacetValue]:
        """
        Convert filtered DataFrame to list of FacetValue objects.

        V16: When deduplicate_to_highest_level=True (default), if the same facet_value_id
        exists at multiple category levels, keep only the one at the highest (least specific)
        level. This ensures redirects go to the broadest applicable category.

        Args:
            filtered_df: Filtered facets DataFrame
            deduplicate_to_highest_level: If True, deduplicate by facet_value_id keeping highest level

        Returns:
            List of FacetValue objects
        """
        facet_values = []

        for _, row in filtered_df.iterrows():
            try:
                fv = FacetValue(
                    facet_id=int(row.get(self.col_mapping['facet_id'], 0)),
                    facet_name=str(row.get(self.col_mapping['facet_name'], '')),
                    facet_value_id=int(row.get(self.col_mapping['facet_value_id'], 0)),
                    facet_value_name=str(row.get(self.col_mapping['facet_value_name'], '')),
                    url=str(row.get(self.col_mapping['url'], ''))
                )
                facet_values.append(fv)
            except (ValueError, TypeError):
                continue

        # V16: Deduplicate to keep only highest level (shortest URL path) per facet_value_id
        if deduplicate_to_highest_level and facet_values:
            facet_values = self._deduplicate_to_highest_level(facet_values)

        return facet_values

    def _deduplicate_to_highest_level(self, facet_values: list[FacetValue]) -> list[FacetValue]:
        """
        V16: Deduplicate facet values by facet_value_id, keeping the one at the highest
        (least specific) category level.

        The "highest level" is determined by the number of underscores in the subcategory
        part of the URL. Fewer underscores = higher/broader level.

        Example:
            /products/gezond_mooi/gezond_mooi_560760/c/...           -> 1 underscore (highest)
            /products/gezond_mooi/gezond_mooi_560760_570196/c/...    -> 2 underscores
            /products/gezond_mooi/gezond_mooi_560760_6911749/c/...   -> 2 underscores

        Args:
            facet_values: List of FacetValue objects (may contain duplicates)

        Returns:
            Deduplicated list with highest level URLs preserved
        """
        # Group by facet_value_id
        by_value_id = {}
        for fv in facet_values:
            key = fv.facet_value_id
            if key not in by_value_id:
                by_value_id[key] = []
            by_value_id[key].append(fv)

        # For each group, keep the one with the shortest subcategory path
        result = []
        for value_id, fvs in by_value_id.items():
            if len(fvs) == 1:
                result.append(fvs[0])
            else:
                # Find the one with fewest underscores in subcategory (= highest level)
                best = min(fvs, key=lambda fv: self._count_subcategory_depth(fv.url))
                result.append(best)

        return result

    def _count_subcategory_depth(self, url: str) -> int:
        """
        V16: Count the depth of a subcategory URL.

        Depth is determined by counting underscores after the main category in the
        subcategory name. More underscores = deeper/more specific level.

        Example:
            /products/gezond_mooi/gezond_mooi_560760/c/...           -> depth 1
            /products/gezond_mooi/gezond_mooi_560760_570196/c/...    -> depth 2
            /products/gezond_mooi/gezond_mooi_560760_6911749/c/...   -> depth 2

        Args:
            url: Facet URL

        Returns:
            Depth count (lower = higher level)
        """
        if not url:
            return 999  # Unknown URLs go to the end

        # Extract subcategory part from URL
        # /products/gezond_mooi/gezond_mooi_560760_6911749/c/... -> gezond_mooi_560760_6911749
        try:
            if '/c/' in url:
                path = url.split('/c/')[0]
            else:
                path = url

            parts = path.strip('/').split('/')
            if len(parts) >= 3:  # products/main_cat/subcat
                subcat = parts[2]
                # Count numeric IDs (underscores followed by digits)
                # gezond_mooi_560760 has 1 ID, gezond_mooi_560760_6911749 has 2 IDs
                import re
                numeric_ids = re.findall(r'_(\d+)', subcat)
                return len(numeric_ids)
        except:
            pass

        return 999

    def get_unique_facet_names(self, filtered_df: pd.DataFrame) -> list[str]:
        """Get list of unique facet names in the filtered set."""
        facet_name_col = self.col_mapping.get('facet_name')
        if facet_name_col:
            return filtered_df[facet_name_col].unique().tolist()
        return []

    def get_facet_summary(self, filtered_df: pd.DataFrame) -> dict:
        """
        Get summary of facets grouped by facet name.

        Returns:
            Dict with facet names as keys and list of values as values
        """
        facet_name_col = self.col_mapping.get('facet_name')
        facet_value_name_col = self.col_mapping.get('facet_value_name')

        if not facet_name_col or not facet_value_name_col:
            return {}

        summary = {}
        for facet_name in filtered_df[facet_name_col].unique():
            values = filtered_df[
                filtered_df[facet_name_col] == facet_name
            ][facet_value_name_col].tolist()
            summary[facet_name] = values

        return summary


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from src.db_loader import DataLoader

    # Load facets from cache
    loader = DataLoader(use_cache=True)
    facets_df = loader.load_facets()

    print("Facet Filter Test")
    print("=" * 60)

    # Create filter
    facet_filter = FacetFilter(facets_df)

    # Test filtering by subcategory ID
    subcategory_id = "504063"  # Parasols
    filtered = facet_filter.filter_by_subcategory(subcategory_id)

    print(f"\nFiltering facets for subcategory: {subcategory_id}")
    print(f"Found {len(filtered)} facets")

    # Show summary
    summary = facet_filter.get_facet_summary(filtered)
    print("\nFacet summary:")
    for facet_name, values in summary.items():
        print(f"  {facet_name}: {values}")

    # Convert to FacetValue objects
    facet_values = facet_filter.get_facet_values(filtered)
    print(f"\nConverted to {len(facet_values)} FacetValue objects")

    # Show some examples
    print("\nExample FacetValues:")
    for fv in facet_values[:3]:
        print(f"  {fv.facet_name}: {fv.facet_value_name} -> {fv.url_fragment}")
