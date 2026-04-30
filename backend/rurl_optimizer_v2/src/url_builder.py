"""
URL builder module.
Constructs redirect URLs from category paths and matched facets.
"""

from dataclasses import dataclass
from typing import Optional
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import config
from src.parser import ParsedRUrl
from src.matcher import MatchResult


@dataclass
class RedirectResult:
    """Result of redirect URL generation."""
    original_url: str
    redirect_url: Optional[str]
    facet_fragment: str         # e.g., "type_parasol~3599193"
    match_score: int
    match_type: str
    success: bool
    reason: str                 # Explanation of result
    keyword: str = ""           # Extracted keyword from R-URL
    facet_count: int = 0        # Number of facets matched
    main_category: str = ""     # e.g., "tuin_accessoires"
    subcategory_id: str = ""    # e.g., "504063" (for database lookup)
    facet_names: str = ""       # e.g., "type_parasol" or "type_parasol, kleur"
    facet_value_names: str = "" # e.g., "Zweefparasols" or "Zweefparasols, Grijs"
    # V12: Keyword coverage tracking
    matched_keywords: str = ""      # Keywords that were matched (comma-separated)
    unmatched_keywords: str = ""    # Keywords that were NOT matched
    match_coverage: float = 0.0     # Percentage of keywords matched (0-100)
    has_stopwords: bool = False     # Whether stopwords were found in original keyword
    stopwords_found: str = ""       # Which stopwords were found (comma-separated)
    # V16: Cross-category merk detection
    merk_of_shop_missing: str = ""  # e.g., "Merk 'PSV' bestaat in 'Mode accessoires' maar niet in 'mode'"


class UrlBuilder:
    """Builds redirect URLs from matched components."""

    def __init__(self, base_url: str = None):
        """
        Initialize URL builder.

        Args:
            base_url: Base URL for beslist.nl (default from config)
        """
        self.base_url = base_url or config.BASE_URL

    def build(self, parsed_url: ParsedRUrl, match_result: MatchResult) -> RedirectResult:
        """
        Build a redirect URL from parsed R-URL and match result.

        URL format: {base}/{category_path}/c/{facet_name}~{facet_value_id}

        v9: If facet comes from a different subcategory (hierarchical fallback),
        use the facet's own category path to ensure valid URL.

        Args:
            parsed_url: Parsed R-URL components
            match_result: Keyword match result

        Returns:
            RedirectResult with redirect URL or failure reason
        """
        if not parsed_url.is_valid:
            return RedirectResult(
                original_url=parsed_url.original_url,
                redirect_url=None,
                facet_fragment='',
                match_score=0,
                match_type='none',
                success=False,
                reason=f"Invalid R-URL: {parsed_url.error_message}",
                keyword='',
                facet_count=0,
                main_category='',
                subcategory_id='',
                facet_names='',
                facet_value_names=''
            )

        if not match_result.is_match:
            return RedirectResult(
                original_url=parsed_url.original_url,
                redirect_url=None,
                facet_fragment='',
                match_score=match_result.score,
                match_type=match_result.match_type,
                success=False,
                reason=f"No matching facet found for keyword '{parsed_url.keyword}' (best score: {match_result.score})",
                keyword=parsed_url.keyword,
                facet_count=0,
                main_category=parsed_url.main_category,
                subcategory_id=parsed_url.subcategory_id,
                facet_names='',
                facet_value_names=''
            )

        # Build the redirect URL
        facet_fragment = match_result.facet_value.url_fragment

        # v9: Check if facet belongs to the original category
        # If not, use the facet's own category path
        if match_result.facet_value.url and not self._facet_matches_category(match_result.facet_value.url, parsed_url):
            # V16: Check if this is a merk/winkel facet from a different main category
            facet_main_cat = self._extract_main_category_from_facet_url(match_result.facet_value.url)
            is_merk_or_winkel = match_result.facet_value.facet_name.lower() in ('merk', 'winkel')
            is_different_main_cat = facet_main_cat and facet_main_cat != parsed_url.main_category

            if is_merk_or_winkel and is_different_main_cat:
                # V16: Merk/winkel exists in different main category - don't redirect, flag for creation
                merk_missing_msg = f"Merk '{match_result.matched_text}' bestaat in '{facet_main_cat}' maar niet in '{parsed_url.main_category}'"

                return RedirectResult(
                    original_url=parsed_url.original_url,
                    redirect_url=None,
                    facet_fragment='',
                    match_score=match_result.score,
                    match_type='merk_missing',
                    success=False,
                    reason=f"[V16] {merk_missing_msg}",
                    keyword=parsed_url.keyword,
                    facet_count=0,
                    main_category=parsed_url.main_category,
                    subcategory_id=parsed_url.subcategory_id,
                    facet_names=match_result.facet_value.facet_name,
                    facet_value_names=match_result.matched_text,
                    merk_of_shop_missing=merk_missing_msg
                )

            # V26: An R-URL that already carries a maincat may only redirect to
            # another deepest_cat within the same maincat. Cross-maincat moves
            # are almost always a worse landing page than no redirect.
            if is_different_main_cat and parsed_url.main_category:
                return RedirectResult(
                    original_url=parsed_url.original_url,
                    redirect_url=None,
                    facet_fragment='',
                    match_score=match_result.score,
                    match_type='cross_maincat_blocked',
                    success=False,
                    reason=f"[V26] Cross-maincat redirect blocked: facet '{match_result.facet_value.facet_name}' lives in '{facet_main_cat}', R-URL is in '{parsed_url.main_category}'",
                    keyword=parsed_url.keyword,
                    facet_count=0,
                    main_category=parsed_url.main_category,
                    subcategory_id=parsed_url.subcategory_id,
                    facet_names=match_result.facet_value.facet_name,
                    facet_value_names=match_result.matched_text,
                )

            # Facet comes from different category - use its own path
            category_path = self._extract_category_path_from_facet_url(match_result.facet_value.url)
            if category_path:
                redirect_url = f"{self.base_url}{category_path}/c/{facet_fragment}"

                return RedirectResult(
                    original_url=parsed_url.original_url,
                    redirect_url=redirect_url,
                    facet_fragment=facet_fragment,
                    match_score=match_result.score,
                    match_type=match_result.match_type,
                    success=True,
                    reason=f"Matched '{parsed_url.keyword}' to '{match_result.matched_text}' (redirected to valid category)",
                    keyword=parsed_url.keyword,
                    facet_count=1,
                    main_category=parsed_url.main_category,
                    subcategory_id=parsed_url.subcategory_id,
                    facet_names=match_result.facet_value.facet_name,
                    facet_value_names=match_result.matched_text
                )

        # Facet is valid for original category - use original path.
        # V28: Beslist URLs only support one value per facet name. If the
        # new facet name already exists in the original URL's facet
        # fragment, the new value cannot be added — keep only the original
        # facet and mark the match for a heavy score penalty downstream.
        existing_names = self._existing_facet_names(parsed_url.existing_facet)
        new_facet_name = match_result.facet_value.facet_name
        duplicate = bool(parsed_url.existing_facet) and new_facet_name in existing_names

        if duplicate:
            combined_fragment = parsed_url.existing_facet
            final_match_type = 'duplicate_facet_dropped'
            final_reason = (
                f"[V28] '{new_facet_name}' value already present in original URL; "
                f"new value '{match_result.matched_text}' dropped, kept "
                f"'{parsed_url.existing_facet}'"
            )
        elif parsed_url.existing_facet:
            combined_fragment = f"{parsed_url.existing_facet}~~{facet_fragment}"
            final_match_type = match_result.match_type
            final_reason = f"Matched '{parsed_url.keyword}' to '{match_result.matched_text}' ({match_result.match_type}, score: {match_result.score})"
        else:
            combined_fragment = facet_fragment
            final_match_type = match_result.match_type
            final_reason = f"Matched '{parsed_url.keyword}' to '{match_result.matched_text}' ({match_result.match_type}, score: {match_result.score})"

        redirect_url = (
            f"{self.base_url}"
            f"{parsed_url.full_category_path}"
            f"/c/{combined_fragment}"
        )

        return RedirectResult(
            original_url=parsed_url.original_url,
            redirect_url=redirect_url,
            facet_fragment=combined_fragment,
            match_score=match_result.score,
            match_type=final_match_type,
            success=True,
            reason=final_reason,
            keyword=parsed_url.keyword,
            facet_count=1 + (1 if parsed_url.existing_facet else 0),
            main_category=parsed_url.main_category,
            subcategory_id=parsed_url.subcategory_id,
            facet_names=match_result.facet_value.facet_name,
            facet_value_names=match_result.matched_text
        )

    def _extract_category_path_from_facet_url(self, facet_url: str) -> Optional[str]:
        """
        v9: Extract the category path from a facet URL.

        Example:
            Input: "/products/huis_tuin/huis_tuin_505313_505230/c/type_opberger~23807768"
            Output: "/products/huis_tuin/huis_tuin_505313_505230"
        """
        if not facet_url:
            return None
        if '/c/' in facet_url:
            return facet_url.split('/c/')[0]
        return None

    def _extract_main_category_from_facet_url(self, facet_url: str) -> Optional[str]:
        """
        V16: Extract the main category from a facet URL.

        Example:
            Input: "/products/mode_accessoires/mode_accessoires_457573_457622/c/merk~2323227"
            Output: "mode_accessoires"
        """
        if not facet_url:
            return None
        # Remove /c/... part first
        if '/c/' in facet_url:
            path = facet_url.split('/c/')[0]
        else:
            path = facet_url
        # Extract main category from path like "/products/mode_accessoires/..."
        parts = path.strip('/').split('/')
        if len(parts) >= 2 and parts[0] == 'products':
            return parts[1]
        elif len(parts) >= 1:
            return parts[0]
        return None

    def _extract_subcategory_from_facet_url(self, facet_url: str) -> Optional[str]:
        """
        V18: Extract the subcategory_name from a facet URL.

        Example:
            Input: "/products/huis_tuin/huis_tuin_505062_505149/c/t_dekbed~6993695"
            Output: "huis_tuin_505062_505149"
        """
        if not facet_url:
            return None
        # Remove /c/... part first
        if '/c/' in facet_url:
            path = facet_url.split('/c/')[0]
        else:
            path = facet_url
        # Extract subcategory (last part of path)
        parts = path.strip('/').split('/')
        if len(parts) >= 3 and parts[0] == 'products':
            return parts[2]  # e.g., "huis_tuin_505062_505149"
        return None

    def _existing_facet_names(self, existing_facet: str) -> set:
        """V28: Extract the facet NAMES (not values) already present in the
        original URL's /c/ fragment.

        Beslist URLs only allow one value per facet name. A fragment like
        'merk~250064~~kleur~12345' has names {'merk', 'kleur'}; trying to
        add another 'merk~...' would produce an invalid double-merk URL.
        """
        if not existing_facet:
            return set()
        names: set = set()
        for piece in existing_facet.split("~~"):
            if "~" in piece:
                name, _, _ = piece.partition("~")
                if name:
                    names.add(name)
        return names

    def _facet_matches_category(self, facet_url: str, parsed_url: ParsedRUrl) -> bool:
        """
        v9/v11/V18: Check if a facet URL belongs to the same subcategory as the parsed URL.

        V18 CHANGE: A facet is only valid if it's EXACTLY in the same subcategory.
        Parent category facets are NO LONGER considered valid because they may not
        actually exist in the child subcategory (e.g., stijl_woonaccessoires exists
        in Beddengoed but not in Dekbedden).

        A facet is valid ONLY if:
        1. The facet's subcategory is EXACTLY the same as the R-URL's subcategory

        A facet is INVALID if:
        - The facet is in a PARENT category (may not exist in child)
        - The facet is in a CHILD (deeper) subcategory than the R-URL

        Args:
            facet_url: The facet's URL (e.g., "/products/meubilair/meubilair_389371_395590/c/...")
            parsed_url: The parsed R-URL

        Returns:
            True if facet is valid for the original category
        """
        if not facet_url:
            return False

        # Extract the subcategory from facet URL
        # e.g., "/products/meubilair/meubilair_389371_395590/c/..." -> "meubilair_389371_395590"
        facet_category_path = self._extract_category_path_from_facet_url(facet_url)
        if not facet_category_path:
            return False

        # Get the subcategory_name from the facet path (last part)
        facet_subcat = facet_category_path.split('/')[-1] if '/' in facet_category_path else facet_category_path

        # The R-URL's subcategory_name (e.g., "meubilair_389371")
        rurl_subcat = parsed_url.subcategory_name

        # V18: Only exact match is valid
        # Parent category facets may not exist in child subcategory
        if facet_subcat == rurl_subcat:
            return True

        # V18: Facet from parent is now treated as "different category"
        # It will be redirected to the parent category where it actually exists
        if rurl_subcat.startswith(facet_subcat + '_'):
            return False  # Changed from True to False in V18

        # Case 3: Facet is in CHILD category (R-URL subcat is prefix of facet subcat)
        # e.g., facet in "meubilair_389371_395590", R-URL in "meubilair_389371"
        # This is INVALID - the facet only applies to the deeper category
        if facet_subcat.startswith(rurl_subcat + '_'):
            return False

        # Case 4: Different branches - not related
        return False

    def build_multi_facet(self, parsed_url: ParsedRUrl, match_results: list[MatchResult]) -> RedirectResult:
        """
        Build a redirect URL with multiple facet filters.

        URL format: {base}/{category_path}/c/{facet1}~{id1}~~{facet2}~{id2}
        Note: Multiple facets are separated by ~~ (double tilde)

        v5: If a cross-category match is found, redirect to the facet's valid subcategory
        instead of the original R-URL subcategory.

        v9: Also handles hierarchical fallback matches where facet comes from
        parent/maincat but needs to redirect to facet's own category.

        Args:
            parsed_url: Parsed R-URL components
            match_results: List of keyword match results

        Returns:
            RedirectResult with multi-facet redirect URL
        """
        if not parsed_url.is_valid:
            return RedirectResult(
                original_url=parsed_url.original_url,
                redirect_url=None,
                facet_fragment='',
                match_score=0,
                match_type='none',
                success=False,
                reason=f"Invalid R-URL: {parsed_url.error_message}",
                keyword='',
                facet_count=0,
                main_category='',
                subcategory_id='',
                facet_names='',
                facet_value_names=''
            )

        valid_matches = [r for r in match_results if r.is_match]

        if not valid_matches:
            return RedirectResult(
                original_url=parsed_url.original_url,
                redirect_url=None,
                facet_fragment='',
                match_score=0,
                match_type='none',
                success=False,
                reason="No matching facets found",
                keyword=parsed_url.keyword,
                facet_count=0,
                main_category=parsed_url.main_category,
                subcategory_id=parsed_url.subcategory_id,
                facet_names='',
                facet_value_names=''
            )

        # v5: Check for cross-category match - use the facet's category path
        cross_cat_match = next((r for r in valid_matches if r.is_cross_category), None)
        if cross_cat_match and cross_cat_match.cross_category_path:
            # Use the cross-category path for redirect
            category_path = cross_cat_match.cross_category_path
            # Only use the cross-category facet (don't mix with original category facets)
            facet_fragment = cross_cat_match.facet_value.url_fragment

            # V26: Block cross-maincat redirects when the R-URL already has a
            # maincat — landing in a different maincat is almost never the
            # right answer for a categorised R-URL.
            cross_main_cat = self._extract_main_category_from_facet_url(category_path)
            if (parsed_url.main_category and cross_main_cat
                    and cross_main_cat != parsed_url.main_category):
                return RedirectResult(
                    original_url=parsed_url.original_url,
                    redirect_url=None,
                    facet_fragment='',
                    match_score=cross_cat_match.score,
                    match_type='cross_maincat_blocked',
                    success=False,
                    reason=f"[V26] Cross-maincat redirect blocked: cross-category match in '{cross_main_cat}', R-URL is in '{parsed_url.main_category}'",
                    keyword=parsed_url.keyword,
                    facet_count=0,
                    main_category=parsed_url.main_category,
                    subcategory_id=parsed_url.subcategory_id,
                    facet_names=cross_cat_match.facet_value.facet_name,
                    facet_value_names=cross_cat_match.matched_text,
                )

            redirect_url = f"{self.base_url}{category_path}/c/{facet_fragment}"

            return RedirectResult(
                original_url=parsed_url.original_url,
                redirect_url=redirect_url,
                facet_fragment=facet_fragment,
                match_score=cross_cat_match.score,
                match_type='cross_category_type',
                success=True,
                reason=f"Cross-category match: '{cross_cat_match.matched_text}' in {category_path}",
                keyword=parsed_url.keyword,
                facet_count=1,
                main_category=parsed_url.main_category,
                subcategory_id=parsed_url.subcategory_id,
                facet_names=cross_cat_match.facet_value.facet_name,
                facet_value_names=cross_cat_match.matched_text
            )

        # v9/V18: Check if ALL facets belong to the original category
        # If any facet comes from a different subcategory, we need to use its category path
        # V18: Also check if facets from parent category actually exist in the target subcategory
        facets_from_different_category = []
        facets_from_same_category = []

        for match in valid_matches:
            if match.facet_value and match.facet_value.url:
                # V18: Check both URL match AND that facet actually exists in target subcat
                facet_subcat = self._extract_subcategory_from_facet_url(match.facet_value.url)
                rurl_subcat = parsed_url.subcategory_name

                # Facet is only valid for same category if it's EXACTLY the same subcategory
                # or if it's from a parent AND we're using the parent category as target
                if facet_subcat == rurl_subcat:
                    facets_from_same_category.append(match)
                else:
                    facets_from_different_category.append(match)
            else:
                facets_from_same_category.append(match)

        # v9: If we have facets from different categories, use the first one's category
        # and only include facets valid for that category
        if facets_from_different_category:
            # Use the first different-category facet's path
            primary_match = facets_from_different_category[0]
            category_path = self._extract_category_path_from_facet_url(primary_match.facet_value.url)

            # V16: Check if this is a merk/winkel facet from a different main category
            facet_main_cat = self._extract_main_category_from_facet_url(primary_match.facet_value.url)
            is_merk_or_winkel = primary_match.facet_value.facet_name.lower() in ('merk', 'winkel')
            is_different_main_cat = facet_main_cat and facet_main_cat != parsed_url.main_category

            if is_merk_or_winkel and is_different_main_cat:
                # V16: Merk/winkel exists in different main category - don't redirect, flag for creation
                merk_missing_msg = f"Merk '{primary_match.matched_text}' bestaat in '{facet_main_cat}' maar niet in '{parsed_url.main_category}'"

                return RedirectResult(
                    original_url=parsed_url.original_url,
                    redirect_url=None,
                    facet_fragment='',
                    match_score=primary_match.score,
                    match_type='merk_missing',
                    success=False,
                    reason=f"[V16] {merk_missing_msg}",
                    keyword=parsed_url.keyword,
                    facet_count=0,
                    main_category=parsed_url.main_category,
                    subcategory_id=parsed_url.subcategory_id,
                    facet_names=primary_match.facet_value.facet_name,
                    facet_value_names=primary_match.matched_text,
                    merk_of_shop_missing=merk_missing_msg
                )

            # V26: Block any non-merk/winkel cross-maincat redirect too —
            # categorised R-URLs must stay in their own maincat.
            if is_different_main_cat and parsed_url.main_category:
                return RedirectResult(
                    original_url=parsed_url.original_url,
                    redirect_url=None,
                    facet_fragment='',
                    match_score=primary_match.score,
                    match_type='cross_maincat_blocked',
                    success=False,
                    reason=f"[V26] Cross-maincat redirect blocked: facet '{primary_match.facet_value.facet_name}' lives in '{facet_main_cat}', R-URL is in '{parsed_url.main_category}'",
                    keyword=parsed_url.keyword,
                    facet_count=0,
                    main_category=parsed_url.main_category,
                    subcategory_id=parsed_url.subcategory_id,
                    facet_names=primary_match.facet_value.facet_name,
                    facet_value_names=primary_match.matched_text,
                )

            if category_path:
                # Only use facets that are valid for this category
                # For simplicity, just use the primary facet to ensure validity
                facet_fragment = primary_match.facet_value.url_fragment

                redirect_url = f"{self.base_url}{category_path}/c/{facet_fragment}"

                return RedirectResult(
                    original_url=parsed_url.original_url,
                    redirect_url=redirect_url,
                    facet_fragment=facet_fragment,
                    match_score=primary_match.score,
                    match_type='multi',
                    success=True,
                    reason=f"Matched '{primary_match.matched_text}' (redirected to valid category)",
                    keyword=parsed_url.keyword,
                    facet_count=1,
                    main_category=parsed_url.main_category,
                    subcategory_id=parsed_url.subcategory_id,
                    facet_names=primary_match.facet_value.facet_name,
                    facet_value_names=primary_match.matched_text
                )

        # V18: Only use facets that are valid for the R-URL's exact subcategory
        # This prevents combining facets from parent categories that don't exist in the target subcat
        if facets_from_same_category:
            # V28: Drop new facets whose name already exists in the original
            # URL's /c/ fragment — Beslist URLs only allow one value per
            # facet name. The original value wins; the new ones are
            # discarded and the row is flagged for a heavy score penalty.
            existing_names = self._existing_facet_names(parsed_url.existing_facet)
            dropped_for_dup = [r for r in facets_from_same_category
                               if r.facet_value.facet_name in existing_names]
            kept_new_facets = [r for r in facets_from_same_category
                               if r.facet_value.facet_name not in existing_names]

            # V25: Sorteer facets alfabetisch op facet_name voor consistente URLs
            sorted_facets = sorted(kept_new_facets, key=lambda r: r.facet_value.facet_name)
            facet_fragments = [r.facet_value.url_fragment for r in sorted_facets]
            combined_fragment = '~~'.join(facet_fragments)

            # Add existing facet from original URL if present.
            if parsed_url.existing_facet:
                if combined_fragment:
                    combined_fragment = f"{parsed_url.existing_facet}~~{combined_fragment}"
                else:
                    combined_fragment = parsed_url.existing_facet

            # Score: average of the kept facets if any survived, else fall
            # back to the dropped-duplicates' score so the output isn't 0.
            if sorted_facets:
                avg_score = sum(r.score for r in sorted_facets) // len(sorted_facets)
            else:
                avg_score = sum(r.score for r in dropped_for_dup) // max(1, len(dropped_for_dup))

            redirect_url = (
                f"{self.base_url}"
                f"{parsed_url.full_category_path}"
                f"/c/{combined_fragment}"
            )

            # V25: Gebruik sorted_facets voor consistente volgorde in output
            display_facets = sorted_facets if sorted_facets else dropped_for_dup
            matched_terms = [r.matched_text for r in display_facets]
            facet_names = [r.facet_value.facet_name for r in display_facets]

            # V28: When a duplicate was dropped, flag the match_type so the
            # reliability scorer can apply a heavy cap.
            if dropped_for_dup:
                single_match_type = 'duplicate_facet_dropped'
                dropped_names = ", ".join(sorted({r.facet_value.facet_name for r in dropped_for_dup}))
                reason_text = (
                    f"[V28] Dropped duplicate facet name(s) '{dropped_names}' — "
                    f"already present in original URL fragment '{parsed_url.existing_facet}'"
                )
            elif len(sorted_facets) == 1:
                # V29: preserve semantic match_types (synonym, token_coverage)
                # so the downstream coverage logic can trust them. The old
                # behaviour collapsed everything to 'fuzzy'/'exact' on score,
                # which made V27's literal-substring unmatched-token check
                # second-guess matches the matcher had already validated.
                inner_type = sorted_facets[0].match_type
                if inner_type in ('synonym', 'token_coverage'):
                    single_match_type = inner_type
                else:
                    single_match_type = 'exact' if avg_score == 100 else 'fuzzy'
                reason_text = f"Matched {len(sorted_facets)} facet: {', '.join(matched_terms)}"
            else:
                single_match_type = 'multi'
                reason_text = f"Matched {len(sorted_facets)} facets: {', '.join(matched_terms)}"

            return RedirectResult(
                original_url=parsed_url.original_url,
                redirect_url=redirect_url,
                facet_fragment=combined_fragment,
                match_score=avg_score,
                match_type=single_match_type,
                success=True,
                reason=reason_text,
                keyword=parsed_url.keyword,
                facet_count=len(sorted_facets) + (1 if parsed_url.existing_facet else 0),
                main_category=parsed_url.main_category,
                subcategory_id=parsed_url.subcategory_id,
                facet_names=', '.join(facet_names),
                facet_value_names=', '.join(matched_terms)
            )

        # V18: No facets from same category - this shouldn't happen but handle gracefully
        return RedirectResult(
            original_url=parsed_url.original_url,
            redirect_url=None,
            facet_fragment='',
            match_score=0,
            match_type='none',
            success=False,
            reason="[V18] No facets valid for target subcategory",
            keyword=parsed_url.keyword,
            facet_count=0,
            main_category=parsed_url.main_category,
            subcategory_id=parsed_url.subcategory_id,
            facet_names='',
            facet_value_names=''
        )

    def build_category_only(self, parsed_url: ParsedRUrl) -> RedirectResult:
        """
        Build a redirect URL to category page (no facet filter).

        Fallback when no facet match is found. If original URL had a facet,
        preserve it in the redirect.

        Args:
            parsed_url: Parsed R-URL components

        Returns:
            RedirectResult with category-only URL
        """
        if not parsed_url.is_valid:
            return RedirectResult(
                original_url=parsed_url.original_url,
                redirect_url=None,
                facet_fragment='',
                match_score=0,
                match_type='none',
                success=False,
                reason=f"Invalid R-URL: {parsed_url.error_message}",
                keyword='',
                facet_count=0,
                main_category='',
                subcategory_id='',
                facet_names='',
                facet_value_names=''
            )

        # If original URL had a facet, preserve it
        if parsed_url.existing_facet:
            redirect_url = f"{self.base_url}{parsed_url.full_category_path}/c/{parsed_url.existing_facet}"
            facet_count = 1
        else:
            redirect_url = f"{self.base_url}{parsed_url.full_category_path}/"
            facet_count = 0

        return RedirectResult(
            original_url=parsed_url.original_url,
            redirect_url=redirect_url,
            facet_fragment=parsed_url.existing_facet,
            match_score=50,  # Lower confidence for category-only redirect
            match_type='category_fallback',
            success=True,
            reason="No facet match, redirecting to category page",
            keyword=parsed_url.keyword,
            facet_count=facet_count,
            main_category=parsed_url.main_category,
            subcategory_id=parsed_url.subcategory_id,
            facet_names='',
            facet_value_names=''
        )

    def build_subcategory_redirect(
        self,
        original_url: str,
        keyword: str,
        subcategory_match: dict,
        main_category: str = '',
        existing_facet: str = ''
    ) -> RedirectResult:
        """
        V14: Build a redirect URL to a matched subcategory.

        When keyword matches a subcategory name (e.g., "scharnieren" -> "Deurscharnieren"),
        redirect to that subcategory page without a facet filter.

        V19: If original URL had a facet (existing_facet), preserve it in the redirect.

        Args:
            original_url: Original R-URL
            keyword: Search keyword that was matched
            subcategory_match: Dict from KeywordMatcher.match_subcategory_name() with:
                - matched_category: Display name (e.g., "Deurscharnieren")
                - url_name: URL name (e.g., "klussen_486170_6356938")
                - category_path: Full path (e.g., "/products/klussen/klussen_486170_6356938")
                - score: Match score (0-100)
                - match_type: "subcategory_name"
            main_category: Main category for the redirect
            existing_facet: V19 - Existing facet from original URL (e.g., "merk~250064")

        Returns:
            RedirectResult with subcategory redirect URL
        """
        if not subcategory_match:
            return RedirectResult(
                original_url=original_url,
                redirect_url=None,
                facet_fragment='',
                match_score=0,
                match_type='none',
                success=False,
                reason="No subcategory match provided",
                keyword=keyword,
                facet_count=0,
                main_category=main_category,
                subcategory_id='',
                facet_names='',
                facet_value_names=''
            )

        category_path = subcategory_match.get('category_path', '')
        matched_category = subcategory_match.get('matched_category', '')
        score = subcategory_match.get('score', 0)

        # V19: If original URL had a facet, preserve it in the redirect
        if existing_facet:
            redirect_url = f"{self.base_url}{category_path}/c/{existing_facet}"
            facet_fragment = existing_facet
            facet_count = 1
        else:
            redirect_url = f"{self.base_url}{category_path}/"
            facet_fragment = ''
            facet_count = 0

        # Extract subcategory_id from url_name
        url_name = subcategory_match.get('url_name', '')
        subcategory_id = ''
        if url_name:
            parts = url_name.split('_')
            for part in reversed(parts):
                if part.isdigit():
                    subcategory_id = part
                    break

        return RedirectResult(
            original_url=original_url,
            redirect_url=redirect_url,
            facet_fragment=facet_fragment,
            match_score=score,
            match_type='subcategory_name',
            success=True,
            reason=f"[V14 subcategory_match] Keyword '{keyword}' matched subcategory '{matched_category}'",
            keyword=keyword,
            facet_count=facet_count,
            main_category=main_category,
            subcategory_id=subcategory_id,
            facet_names='',
            facet_value_names=''  # V14.1 fix: geen facet gematcht, alleen subcategorie naam
        )


if __name__ == "__main__":
    from src.parser import RUrlParser
    from src.db_loader import DataLoader
    from src.facet_filter import FacetFilter
    from src.matcher import KeywordMatcher

    print("URL Builder Test")
    print("=" * 80)

    # Setup
    parser = RUrlParser()
    loader = DataLoader(use_cache=True)
    facets_df = loader.load_facets()
    facet_filter = FacetFilter(facets_df)
    matcher = KeywordMatcher()
    builder = UrlBuilder()

    # Test URLs
    test_urls = [
        "/products/tuin_accessoires/tuin_accessoires_504063/r/zweefparasol/",
        "/products/tuin_accessoires/tuin_accessoires_504063/r/zweefparasol+grijs/",
        "/products/tuin_accessoires/tuin_accessoires_504063/r/laptop/",  # No match
    ]

    for url in test_urls:
        print(f"\n{'='*80}")
        print(f"Input: {url}")

        # Parse
        parsed = parser.parse(url)
        print(f"Keyword: {parsed.keyword}")

        # Filter facets
        filtered = facet_filter.filter_by_subcategory(parsed.subcategory_id)
        facet_values = facet_filter.get_facet_values(filtered)

        # Check for multi-word keyword
        if ' ' in parsed.keyword:
            # Try multi-facet match
            match_results = matcher.match_multi_word(parsed.keyword, facet_values)
            if match_results:
                result = builder.build_multi_facet(parsed, match_results)
            else:
                result = builder.build_category_only(parsed)
        else:
            # Single facet match
            match_result = matcher.match_with_partial(parsed.keyword, facet_values)
            if match_result.is_match:
                result = builder.build(parsed, match_result)
            else:
                result = builder.build_category_only(parsed)

        # Output
        print(f"\nResult:")
        print(f"  Success:  {result.success}")
        print(f"  Type:     {result.match_type}")
        print(f"  Score:    {result.match_score}")
        print(f"  Reason:   {result.reason}")
        print(f"  Redirect: {result.redirect_url}")
