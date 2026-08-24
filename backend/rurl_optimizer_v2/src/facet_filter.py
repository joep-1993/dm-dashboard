"""
Facet filtering module.
Filters facets based on subcategory to narrow down matching candidates.
"""

import re
import pandas as pd
from dataclasses import dataclass
from typing import Optional


def _subcat_slug_from_url(url: str) -> str:
    """V43: the subcategory slug (segment after the main category) of a facet
    value URL, e.g. '/products/horloge/horloge_6918306/c/merk~423317' ->
    'horloge_6918306'. '' when the URL has no subcat segment."""
    if not url:
        return ""
    path = url.split("/c/", 1)[0].rstrip("/")
    if "/products/" in path:
        path = path.split("/products/", 1)[-1]
    parts = path.split("/")
    return parts[1] if len(parts) >= 2 else ""


@dataclass
class FacetValue:
    """Represents a single facet value."""
    facet_id: int
    facet_name: str
    facet_value_id: int
    facet_value_name: str
    url: str
    count: int = 0  # Number of products on this facet page (from Search API)

    @property
    def url_fragment(self) -> str:
        """Get the URL fragment for this facet (e.g., 'type_parasol~3599193')."""
        return f"{self.facet_name}~{self.facet_value_id}"


class FacetFilter:
    """Filters facets based on subcategory."""

    # V31: When the same facet value appears at multiple category depths
    # (e.g. brand "Ferrero Rocher" present in parent "Snoep" and in its
    # children "Bonbons" + "Chocolade"), prefer the deepest descendant if
    # it concentrates at least this share of the parent's product count.
    # Bonbons=10 / Snoep=11 → 91% → pick Bonbons.
    # Bonbons=11 / Snoep=22 with Chocolade=10 also under Snoep → 50% →
    # stay at Snoep (split is too even to confidently go deeper).
    CHILD_DOMINANCE_THRESHOLD = 0.7

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
        # V61 memo. A worker filters the same ~3.5k subcategories and ~30 main
        # categories over and over (73 ms per filter_by_subcategory, 46-318 ms
        # per get_facet_values, several times per URL), while the frame itself
        # never changes for the process's lifetime. Cache both halves keyed on
        # what the caller asked for; frames handed out by these filters carry a
        # marker in .attrs so get_facet_values knows it may memoize on them.
        self._df_cache: dict = {}
        self._values_cache: dict = {}

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
            'count': self._find_column(['count', 'product_count', 'Count']),
        }

    def _find_column(self, candidates: list[str]) -> Optional[str]:
        """Find first matching column name from candidates."""
        for col in candidates:
            if col in self.facets_df.columns:
                return col
        return None

    def facet_url_set(self) -> frozenset:
        """V32: All facet URLs as a frozenset for O(1) existence checks.

        Built once and cached. Used by UrlBuilder.build_multi_facet to verify
        a brand/shop facet exists under a sibling leaf subcategory before
        rescuing it across depths. Returns an empty set if no URL column.
        """
        cached = getattr(self, '_url_set_cache', None)
        if cached is not None:
            return cached
        url_col = self.col_mapping.get('url')
        if url_col is None:
            self._url_set_cache = frozenset()
        else:
            self._url_set_cache = frozenset(
                self.facets_df[url_col].astype(str)
            )
        return self._url_set_cache

    def _url_lower(self) -> pd.Series:
        """Lowercased URL column, built once and cached. The per-subcategory
        filters used to `.astype(str)` + casefold (case=False) the full 459k-row
        column on EVERY call (per URL, 5-10x per fallback cascade); precomputing
        it once turns each filter into a single vectorized substring scan against
        already-lowercased data — behaviour-identical to the old case=False regex."""
        cached = getattr(self, '_url_lower_cache', None)
        if cached is not None:
            return cached
        url_col = self.col_mapping.get('url')
        if url_col is None:
            self._url_lower_cache = pd.Series([], dtype=str)
        else:
            self._url_lower_cache = self.facets_df[url_col].astype(str).str.lower()
        return self._url_lower_cache

    def _memo_df(self, key: tuple, df: pd.DataFrame) -> pd.DataFrame:
        """Store a filtered frame under `key` and stamp it so get_facet_values
        can memoize on it. Boolean masking already returns an independent frame;
        only the degenerate 'whole frame' fallbacks are copied, so a caller can
        never reach self.facets_df through one of these."""
        if df is self.facets_df:
            df = df.copy()
        try:
            df.attrs['rurl_cache_key'] = key
        except Exception:
            pass
        self._df_cache[key] = df
        return df

    def filter_by_subcategory(self, subcategory_id: str) -> pd.DataFrame:
        """
        Filter facets to only those valid for a subcategory.

        Uses URL LIKE '%subcategory_id%' logic to find relevant facets.

        Args:
            subcategory_id: The subcategory ID (e.g., "504063")

        Returns:
            Filtered DataFrame with facets for this subcategory
        """
        key = ('subcat', str(subcategory_id))
        cached = self._df_cache.get(key)
        if cached is not None:
            return cached

        url_col = self.col_mapping.get('url')

        if url_col is None:
            # Fallback: try to match on category_id column if available
            if 'category_id' in self.facets_df.columns:
                out = self.facets_df[
                    self.facets_df['category_id'].astype(str) == str(subcategory_id)
                ]
            else:
                # Return all facets if no filtering possible
                out = self.facets_df
        else:
            # Filter where URL contains the subcategory ID (lowercased col cached).
            # regex=False: the needle is a bare numeric id, so the regex engine
            # bought nothing and cost ~2x on a 625k-row scan.
            mask = self._url_lower().str.contains(
                str(subcategory_id).lower(),
                na=False,
                regex=False,
            )
            out = self.facets_df[mask]

        return self._memo_df(key, out)

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
        key = ('maincat', main_category_name)
        cached = self._df_cache.get(key)
        if cached is not None:
            return cached

        url_col = self.col_mapping.get('url')
        main_cat_col = self.col_mapping.get('main_category_name')

        # V16: Try URL matching FIRST - more reliable than name matching
        # This ensures /products/mode/ matches Kleding, not Mode accessoires
        if url_col:
            mask = self._url_lower().str.contains(
                f"/products/{main_category_name}/".lower(),
                na=False,
                regex=False,
            )
            if mask.any():
                return self._memo_df(key, self.facets_df[mask])

        # Fallback to main_category_name column
        if main_cat_col and main_cat_col in self.facets_df.columns:
            mask = self.facets_df[main_cat_col].astype(str).str.lower().str.contains(
                main_category_name.lower().replace('_', ' '),
                case=False,
                na=False
            )
            if mask.any():
                return self._memo_df(key, self.facets_df[mask])

        return self._memo_df(key, pd.DataFrame())

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

    def get_facet_values(self, filtered_df: pd.DataFrame, deduplicate_to_highest_level: bool = True,
                         exclude_subcat_slugs: Optional[frozenset] = None) -> list[FacetValue]:
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
        # V61 memo — only for frames this filter handed out (they carry a marker
        # in .attrs). An arbitrary caller-built frame is not memoized: id() gets
        # recycled after garbage collection, which would serve another frame's
        # values. The stored list is copied out so a caller can never mutate the
        # cache; the FacetValue objects themselves are read-only by convention
        # (nothing in the tree assigns to their fields).
        memo_key = None
        try:
            _ck = filtered_df.attrs.get('rurl_cache_key')
        except Exception:
            _ck = None
        if _ck is not None:
            memo_key = (_ck, bool(deduplicate_to_highest_level), exclude_subcat_slugs)
            hit = self._values_cache.get(memo_key)
            if hit is not None:
                return list(hit)

        facet_values = []

        # Column lookups and the count column are loop-invariant; zip over the
        # raw column arrays instead of materialising a dict per row
        # (to_dict("records") was 145 ms of a 306 ms call on an 82k-row frame).
        cm = self.col_mapping
        count_col = cm.get('count')
        n = len(filtered_df)
        blanks = [None] * n

        def _col(name, default=None):
            c = cm.get(name)
            if c is None or c not in filtered_df.columns:
                return [default] * n
            return filtered_df[c].values

        for _fid, _fname, _vid, _vname, _url, _cnt in zip(
            _col('facet_id', 0), _col('facet_name', ''), _col('facet_value_id', 0),
            _col('facet_value_name', ''), _col('url', ''),
            (filtered_df[count_col].values if count_col and count_col in filtered_df.columns
             else blanks),
        ):
            try:
                count_val = 0
                if count_col:
                    try:
                        count_val = (int(_cnt) if _cnt is not None and str(_cnt) != 'nan'
                                     else 0)
                    except (ValueError, TypeError):
                        count_val = 0
                fv = FacetValue(
                    facet_id=int(_fid if _fid is not None else 0),
                    facet_name=str(_fname if _fname is not None else ''),
                    facet_value_id=int(_vid if _vid is not None else 0),
                    facet_value_name=str(_vname if _vname is not None else ''),
                    url=str(_url if _url is not None else ''),
                    count=count_val,
                )
                facet_values.append(fv)
            except (ValueError, TypeError):
                continue

        # V43: drop gated subcategories BEFORE dedup, so the count-leader dedup
        # picks a real product subcat instead of the gated accessory one (the
        # caller decides which slugs are gated for this URL — see
        # GATED_SUBCATEGORIES and _gated_excluded_slugs).
        if exclude_subcat_slugs:
            facet_values = [fv for fv in facet_values
                            if _subcat_slug_from_url(fv.url) not in exclude_subcat_slugs]

        # V16: Deduplicate to keep only highest level (shortest URL path) per facet_value_id
        if deduplicate_to_highest_level and facet_values:
            facet_values = self._deduplicate_to_highest_level(facet_values)

        if memo_key is not None:
            self._values_cache[memo_key] = facet_values
            return list(facet_values)
        return facet_values

    def _deduplicate_to_highest_level(self, facet_values: list[FacetValue]) -> list[FacetValue]:
        """
        V16: Deduplicate facet values by facet_value_id.

        V31 rewrite (after V31-rev1 failed in production for Ferrero Rocher):
        The Search API doesn't always emit a row for the parent category — for
        Ferrero Rocher under Eten & drinken there's NO Snoep row, only the
        children Bonbons (count=14) and Chocolade (count=10), plus the
        unrelated Brood (count=3) at depth 1. The earlier "pick shallowest
        then check descendants" heuristic locked onto Brood (count-leader
        among depth-1 entries) and never reached Bonbons.

        New algorithm:
          1. Pick the entry with the highest product count globally
             (ties broken by shallower depth). This naturally lands on
             Bonbons (14) for the Ferrero Rocher case.
          2. If the leader has an *ancestor* in the data and the leader's
             count is below ``CHILD_DOMINANCE_THRESHOLD`` of that
             ancestor's count, fall back to the ancestor. This preserves
             the original "prefer broader when no clear winner" intent
             for the case where the parent IS in the data: Snoep=22 with
             Bonbons=11 + Chocolade=10 → leader Snoep wins outright;
             Snoep=11 with Bonbons=10 + Chocolade=1 → leader Snoep, but
             Bonbons would have promoted (10/11=91%) under the old
             descendant-promotion direction — handled by step 1b below.

          1b. After picking the count-leader, ALSO scan its descendants:
              if any descendant of the leader has count >=
              threshold * leader.count, promote to that descendant
              (handles the lopsided "Bonbons concentrates most of Snoep"
              case where Snoep itself was the count-leader).

        Args:
            facet_values: List of FacetValue objects (may contain duplicates)

        Returns:
            Deduplicated list with one representative per facet_value_id
        """
        # Group by facet_value_id
        by_value_id = {}
        for fv in facet_values:
            key = fv.facet_value_id
            if key not in by_value_id:
                by_value_id[key] = []
            by_value_id[key].append(fv)

        result = []
        for value_id, fvs in by_value_id.items():
            if len(fvs) == 1:
                result.append(fvs[0])
                continue

            # Step 1: highest count wins, ties broken by shallower depth.
            #         Sort key: (count desc, depth asc).
            # V61: `fv.url` is the tie-break of last resort at all three
            # picks below. Without it the winner was whichever row `facets.csv`
            # happened to list first, so rebuilding the facet cache silently
            # moved redirects to a different category depth with no code change
            # — measured at ~6% of duplicate-value groups, every one of them
            # pointing at a different destination.
            def _rank(fv):
                c = getattr(fv, 'count', 0) or 0
                d = self._count_subcategory_depth(fv.url)
                return (-c, d, fv.url)
            leader = min(fvs, key=_rank)
            leader_count = getattr(leader, 'count', 0) or 0

            # Step 1b: if a descendant of the leader concentrates most of
            # the leader's products, promote to that descendant.
            descendants = [fv for fv in fvs
                           if fv is not leader
                           and self._is_strict_descendant(fv.url, leader.url)]
            if descendants and leader_count > 0:
                best_desc = min(descendants,
                                key=lambda fv: (-(getattr(fv, 'count', 0) or 0),
                                                fv.url))
                bd_count = getattr(best_desc, 'count', 0) or 0
                if bd_count >= self.CHILD_DOMINANCE_THRESHOLD * leader_count:
                    result.append(best_desc)
                    continue

            # Step 2: if the leader has an ancestor in the data and the leader
            # is NOT dominant enough on its own, fall back to that ancestor.
            ancestors = [fv for fv in fvs
                         if fv is not leader
                         and self._is_strict_descendant(leader.url, fv.url)]
            if ancestors:
                # Closest ancestor = deepest ancestor (most specific shared parent)
                closest = min(ancestors,
                              key=lambda fv: (-self._count_subcategory_depth(fv.url),
                                              fv.url))
                anc_count = getattr(closest, 'count', 0) or 0
                if anc_count > 0 and leader_count < self.CHILD_DOMINANCE_THRESHOLD * anc_count:
                    result.append(closest)
                    continue

            result.append(leader)

        return result

    def _is_strict_descendant(self, child_url: str, parent_url: str) -> bool:
        """V31: True if child_url is a deeper category beneath parent_url.

        Uses the URL stem (everything before the optional ``/c/`` facet
        suffix) and requires the parent stem followed by ``_`` to avoid
        false positives where one numeric ID is a prefix of another
        (e.g. ``574519`` vs ``5745190``).
        """
        if not child_url or not parent_url:
            return False
        parent_stem = parent_url.split('/c/')[0].rstrip('/')
        child_stem = child_url.split('/c/')[0].rstrip('/')
        return child_stem != parent_stem and child_stem.startswith(parent_stem + '_')

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
                numeric_ids = re.findall(r'_(\d+)', subcat)
                return len(numeric_ids)
        except Exception:
            pass

        return 999

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
