"""
R-URL Parser for Beslist.nl URLs.
Extracts category path, subcategory ID, and search keyword from R-URLs.
"""

import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import unquote


@dataclass
class ParsedRUrl:
    """Parsed components of an R-URL."""
    original_url: str
    category_path: str          # e.g., "tuin_accessoires/tuin_accessoires_504063"
    full_category_path: str     # e.g., "/products/tuin_accessoires/tuin_accessoires_504063"
    main_category: str          # e.g., "tuin_accessoires"
    subcategory_id: str         # e.g., "504063"
    subcategory_name: str       # e.g., "tuin_accessoires_504063"
    keyword: str                # e.g., "zweefparasol"
    existing_facet: str = ""    # e.g., "t_zonnebril~6158071" (from /c/ in URL)
    is_valid: bool = True
    error_message: Optional[str] = None


class RUrlParser:
    """Parser for Beslist.nl R-URLs."""

    # Pattern for R-URLs: /products/{main_cat}/{subcat_id}/r/{keyword}/
    # Example: /products/tuin_accessoires/tuin_accessoires_504063/r/zweefparasol/
    # Can optionally have /c/{facet} at the end
    # v5: Updated to handle multi-part keywords (e.g., /r/balkon_bloembakken/reling/)
    R_URL_PATTERN = re.compile(
        r'^/?(?:products/)?'              # Optional /products/ prefix
        r'(([^/]+)/[^/]+_(\d+))'          # category_path with main_cat and subcategory_id
        r'/r/'                             # /r/ separator
        r'(.+?)'                           # keyword (non-greedy, can include slashes)
        r'(?:/c/([^/]+))?'                # Optional /c/{facet}
        r'/?$'                             # Optional trailing slash
    )

    # Alternative pattern for full URLs with domain
    # v5: Updated to handle multi-part keywords
    FULL_URL_PATTERN = re.compile(
        r'(?:https?://)?(?:www\.)?beslist\.nl'
        r'(/products/(([^/]+)/[^/]+_(\d+))/r/(.+?)(?:/c/([^/]+))?/?$)'
    )

    # V14: Pattern for main-category-only R-URLs (no subcategory ID)
    # Example: /products/klussen/r/scharnieren/
    # These URLs have only main category, no subcategory with numeric ID
    MAIN_CAT_ONLY_PATTERN = re.compile(
        r'^/?(?:products/)?'              # Optional /products/ prefix
        r'([^/]+)'                         # main_category only (e.g., "klussen")
        r'/r/'                             # /r/ separator
        r'(.+?)'                           # keyword
        r'(?:/c/([^/]+))?'                # Optional /c/{facet}
        r'/?$'                             # Optional trailing slash
    )

    # V14: Full URL pattern for main-category-only
    FULL_URL_MAIN_CAT_ONLY_PATTERN = re.compile(
        r'(?:https?://)?(?:www\.)?beslist\.nl'
        r'/products/([^/]+)/r/(.+?)(?:/c/([^/]+))?/?$'
    )

    def parse(self, url: str) -> ParsedRUrl:
        """
        Parse an R-URL and extract its components.

        Args:
            url: The R-URL to parse (can be relative or absolute)

        Returns:
            ParsedRUrl with extracted components

        Example:
            >>> parser = RUrlParser()
            >>> result = parser.parse("/products/tuin_accessoires/tuin_accessoires_504063/r/zweefparasol/")
            >>> result.keyword
            'zweefparasol'
            >>> result.subcategory_id
            '504063'
        """
        url = unquote(url.strip())

        # Try full URL pattern first (with domain and subcategory)
        match = self.FULL_URL_PATTERN.match(url)
        if match:
            return self._extract_from_full_match(url, match)

        # Try relative URL pattern (with subcategory)
        # First, normalize the URL
        normalized_url = url.lstrip('/')
        if not normalized_url.startswith('products/'):
            normalized_url = url  # Keep original for pattern matching

        match = self.R_URL_PATTERN.match(normalized_url)
        if match:
            return self._extract_from_relative_match(url, match)

        # V14: Try main-category-only patterns (no subcategory ID)
        # Full URL with domain
        match = self.FULL_URL_MAIN_CAT_ONLY_PATTERN.match(url)
        if match:
            return self._extract_from_main_cat_only_match(url, match)

        # Relative URL
        match = self.MAIN_CAT_ONLY_PATTERN.match(normalized_url)
        if match:
            return self._extract_from_main_cat_only_relative_match(url, match)

        # No match found
        return ParsedRUrl(
            original_url=url,
            category_path="",
            full_category_path="",
            main_category="",
            subcategory_id="",
            subcategory_name="",
            keyword="",
            existing_facet="",
            is_valid=False,
            error_message=f"URL does not match expected R-URL pattern: {url}"
        )

    def _extract_from_full_match(self, url: str, match: re.Match) -> ParsedRUrl:
        """Extract components from a full URL match."""
        full_path = match.group(1)      # /products/tuin.../r/keyword
        category_path = match.group(2)  # tuin.../tuin..._504063
        main_category_from_url = match.group(3)  # Could be wrong (e.g., "tuin" instead of "tuin_accessoires")
        subcategory_id = match.group(4) # 504063
        keyword = match.group(5)        # zweefparasol
        existing_facet = match.group(6) or ""  # t_zonnebril~6158071 (optional)

        subcategory_name = category_path.split('/')[-1]

        # V15: Derive main_category from subcategory_name, not from URL path
        # subcategory_name is like "tuin_accessoires_504071_23755313"
        # main_category should be "tuin_accessoires" (everything before first numeric ID)
        main_category = self._extract_main_category_from_subcategory_name(subcategory_name)

        # V15: Fix category_path to use correct main_category
        # If URL had wrong main_cat (e.g., "tuin" instead of "tuin_accessoires"),
        # rebuild the category_path with correct main_category
        corrected_category_path = f"{main_category}/{subcategory_name}"

        return ParsedRUrl(
            original_url=url,
            category_path=corrected_category_path,
            full_category_path=f"/products/{corrected_category_path}",
            main_category=main_category,
            subcategory_id=subcategory_id,
            subcategory_name=subcategory_name,
            keyword=self._normalize_keyword(keyword),
            existing_facet=existing_facet
        )

    def _extract_from_relative_match(self, url: str, match: re.Match) -> ParsedRUrl:
        """Extract components from a relative URL match."""
        category_path = match.group(1)  # tuin.../tuin..._504063
        main_category_from_url = match.group(2)  # Could be wrong
        subcategory_id = match.group(3) # 504063
        keyword = match.group(4)        # zweefparasol
        existing_facet = match.group(5) or ""  # t_zonnebril~6158071 (optional)

        subcategory_name = category_path.split('/')[-1]

        # V15: Derive main_category from subcategory_name
        main_category = self._extract_main_category_from_subcategory_name(subcategory_name)

        # V15: Fix category_path to use correct main_category
        corrected_category_path = f"{main_category}/{subcategory_name}"

        return ParsedRUrl(
            original_url=url,
            category_path=corrected_category_path,
            full_category_path=f"/products/{corrected_category_path}",
            main_category=main_category,
            subcategory_id=subcategory_id,
            subcategory_name=subcategory_name,
            keyword=self._normalize_keyword(keyword),
            existing_facet=existing_facet
        )

    def _extract_from_main_cat_only_match(self, url: str, match: re.Match) -> ParsedRUrl:
        """
        V14: Extract components from a main-category-only full URL match.

        Example: https://www.beslist.nl/products/klussen/r/scharnieren/
        These URLs have no subcategory ID, only main category.
        """
        main_category = match.group(1)  # klussen
        keyword = match.group(2)        # scharnieren
        existing_facet = match.group(3) or ""

        # For main-cat-only URLs, category_path is just the main category
        category_path = main_category

        return ParsedRUrl(
            original_url=url,
            category_path=category_path,
            full_category_path=f"/products/{main_category}",
            main_category=main_category,
            subcategory_id="",  # No subcategory ID
            subcategory_name=main_category,  # Use main_category as fallback
            keyword=self._normalize_keyword(keyword),
            existing_facet=existing_facet,
            is_valid=True  # Mark as valid - V14 will handle subcategory matching
        )

    def _extract_from_main_cat_only_relative_match(self, url: str, match: re.Match) -> ParsedRUrl:
        """
        V14: Extract components from a main-category-only relative URL match.

        Example: /products/klussen/r/scharnieren/
        """
        main_category = match.group(1)  # klussen
        keyword = match.group(2)        # scharnieren
        existing_facet = match.group(3) or ""

        category_path = main_category

        return ParsedRUrl(
            original_url=url,
            category_path=category_path,
            full_category_path=f"/products/{main_category}",
            main_category=main_category,
            subcategory_id="",
            subcategory_name=main_category,
            keyword=self._normalize_keyword(keyword),
            existing_facet=existing_facet,
            is_valid=True
        )

    def _extract_main_category_from_subcategory_name(self, subcategory_name: str) -> str:
        """
        V15: Extract the main category from a subcategory_name.

        The subcategory_name format is: {main_category}_{id1}_{id2}_...
        We need to extract the main_category part (everything before the first numeric ID).

        Examples:
            "tuin_accessoires_504071_23755313" -> "tuin_accessoires"
            "electronica_12345" -> "electronica"
            "huis_tuin_505313" -> "huis_tuin"
            "klussen_486170_6356938" -> "klussen"

        Args:
            subcategory_name: Full subcategory name like "tuin_accessoires_504071"

        Returns:
            Main category name like "tuin_accessoires"
        """
        if not subcategory_name:
            return ""

        parts = subcategory_name.split('_')

        # Find the first numeric part - everything before it is the main_category
        main_cat_parts = []
        for part in parts:
            if part.isdigit():
                break
            main_cat_parts.append(part)

        if main_cat_parts:
            return '_'.join(main_cat_parts)

        # Fallback: return the whole thing if no numeric parts found
        return subcategory_name

    def _normalize_keyword(self, keyword: str) -> str:
        """
        Normalize a keyword for matching.

        - Decode URL encoding
        - Lowercase
        - Replace hyphens/underscores with spaces
        - Replace + with spaces
        - Replace slashes with spaces (v5: for multi-part keywords like "balkon_bloembakken/reling")
        - Strip whitespace
        """
        keyword = unquote(keyword)
        keyword = keyword.lower()
        # v5: Also replace / with space for multi-part keywords
        keyword = keyword.replace('-', ' ').replace('_', ' ').replace('+', ' ').replace('/', ' ')
        keyword = ' '.join(keyword.split())  # Normalize whitespace
        return keyword

    def parse_batch(self, urls: list[str]) -> list[ParsedRUrl]:
        """Parse multiple R-URLs."""
        return [self.parse(url) for url in urls]


if __name__ == "__main__":
    # Test the parser
    parser = RUrlParser()

    test_urls = [
        "/products/tuin_accessoires/tuin_accessoires_504063/r/zweefparasol/",
        "https://www.beslist.nl/products/tuin_accessoires/tuin_accessoires_504063/r/zweefparasol/",
        "/products/tuin_accessoires/tuin_accessoires_504063/r/parasol+grijs/",
        "/products/electronica/tv_123456/r/samsung-tv/",
        "tuin_accessoires/tuin_accessoires_504063/r/stokparasol/",
        "/invalid/url/format",
    ]

    print("R-URL Parser Test Results")
    print("=" * 80)

    for url in test_urls:
        result = parser.parse(url)
        print(f"\nInput:    {url}")
        print(f"Valid:    {result.is_valid}")
        if result.is_valid:
            print(f"Keyword:  {result.keyword}")
            print(f"Cat ID:   {result.subcategory_id}")
            print(f"Cat Path: {result.full_category_path}")
        else:
            print(f"Error:    {result.error_message}")
        print("-" * 40)
