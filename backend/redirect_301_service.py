"""
301 Redirect Generator Service

Generates 301 redirects for:
1. URLs where facets are not in alphabetical order
2. URLs with facet transformations (FACET-FACET rules)

Example sorting:
- Wrong: /products/fietsen/fietsen_123/c/merk~456~~materiaal~789
- Correct: /products/fietsen/fietsen_123/c/materiaal~789~~merk~456

Example transformation with ID:
- merk~4412606 -> materiaal~484491 (specific facet replacement)

Example transformation without ID:
- merk -> materiaal (all 'merk' facets become 'materiaal' in a category)
"""

import re
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from backend.database import get_redshift_connection, return_redshift_connection


@dataclass
class FacetRule:
    """Facet transformation rule"""
    old_facet: str  # e.g., "merk~4412606" or "merk" (without ID)
    new_facet: str  # e.g., "materiaal~484491" or "materiaal"
    category: Optional[str] = None  # Optional: only apply in this category


@dataclass
class CategoryRule:
    """Category URL replacement rule"""
    old_cat: str  # e.g., "fietsen_484521_484542" or "/fietsen_484521_484542/"
    new_cat: str  # e.g., "fietsen_484521" or "/fietsen_484521/"
    new_maincat: Optional[str] = None  # Optional: also replace the maincat


def check_facets_sorted(url: str) -> Tuple[bool, str]:
    """
    Check if facets in URL are alphabetically sorted.

    Returns:
        Tuple of (is_sorted, corrected_url)
    """
    if "/c/" not in url:
        return True, url

    parts = url.split("/c/")
    if len(parts) < 2:
        return True, url

    base = parts[0]
    facet_str = parts[1].rstrip("/")

    if "~~" not in facet_str:
        return True, url

    facets = facet_str.split("~~")
    facets = [f.lower() for f in facets if f]

    def get_facet_name(facet: str) -> str:
        return facet.split("~")[0] if "~" in facet else facet

    facet_names = [get_facet_name(f) for f in facets]
    sorted_facet_names = sorted(facet_names)

    is_sorted = facet_names == sorted_facet_names

    if is_sorted:
        return True, url

    sorted_facets = sorted(facets, key=get_facet_name)
    corrected_url = f"{base}/c/{'~~'.join(sorted_facets)}"

    return False, corrected_url


def apply_category_rules(url: str, rules: List[CategoryRule]) -> str:
    """
    Apply category URL replacement rules.
    """
    for rule in rules:
        # Normalize category slugs (ensure they have slashes)
        old_cat = rule.old_cat.strip("/")
        new_cat = rule.new_cat.strip("/")

        old_pattern = f"/{old_cat}/"
        new_pattern = f"/{new_cat}/"

        if old_pattern in url:
            url = url.replace(old_pattern, new_pattern)

            # Also replace maincat if specified
            if rule.new_maincat:
                # Extract current maincat from URL
                maincat_match = re.search(r'/products/([^/]+)/', url)
                if maincat_match:
                    old_maincat = maincat_match.group(1)
                    new_maincat = rule.new_maincat.strip("/")
                    if old_maincat != new_maincat:
                        url = url.replace(f"/products/{old_maincat}/", f"/products/{new_maincat}/")

    return url


def apply_facet_rules(url: str, rules: List[FacetRule]) -> str:
    """
    Apply facet transformation rules to a URL.
    """
    if "/c/" not in url:
        return url

    for rule in rules:
        # Check category filter if specified
        if rule.category and rule.category not in url:
            continue

        old_facet = rule.old_facet.lower()
        new_facet = rule.new_facet.lower()

        # Check if old_facet has an ID (contains ~)
        if "~" in old_facet:
            # Exact match with ID - replace entire facet
            # Match patterns: old_facet~~, ~~old_facet~~, ~~old_facet (end), /c/old_facet
            patterns = [
                (f"{old_facet}~~", f"{new_facet}~~"),
                (f"~~{old_facet}~~", f"~~{new_facet}~~"),
                (f"~~{old_facet}", f"~~{new_facet}"),
                (f"/c/{old_facet}", f"/c/{new_facet}"),
            ]
            for pattern, replacement in patterns:
                if pattern in url.lower():
                    url = re.sub(re.escape(pattern), replacement, url, flags=re.IGNORECASE)
                    break
        else:
            # No ID - replace facet name only, keeping the ID
            # Match pattern: old_facet~ID (where ID is digits)
            pattern = rf'\b{re.escape(old_facet)}(~\d+)'
            replacement = f'{new_facet}\\1'
            url = re.sub(pattern, replacement, url, flags=re.IGNORECASE)

    return url


def transform_and_sort_url(
    url: str,
    facet_rules: List[FacetRule] = None,
    category_rules: List[CategoryRule] = None
) -> Tuple[str, bool]:
    """
    Apply category rules, facet rules, and sort facets alphabetically.

    Returns:
        Tuple of (transformed_url, was_changed)
    """
    original = url.lower()

    # Step 1: Apply category transformation rules
    if category_rules:
        url = apply_category_rules(url, category_rules)

    # Step 2: Apply facet transformation rules
    if facet_rules:
        url = apply_facet_rules(url, facet_rules)

    # Step 3: Sort facets alphabetically
    _, url = check_facets_sorted(url)

    # Normalize
    url = url.lower()
    url = re.sub(r'(?<!:)//+', '/', url)  # Remove double slashes except in http://

    was_changed = original != url
    return url, was_changed


def fetch_urls_with_facets(
    contains: str = None,
    start_date: str = "20240101",
    end_date: str = "20261231",
    limit: int = 10000
) -> List[Dict]:
    """
    Fetch URLs from Redshift that have facets (/c/ with ~~).
    """
    conn = None
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()

        query = """
            SELECT
                SPLIT_PART(dv.url, '?', 1) as url,
                COUNT(*) as visits
            FROM datamart.fct_visits fcv
            JOIN datamart.dim_visit dv
                ON fcv.dim_visit_key = dv.dim_visit_key
            WHERE dv.is_real_visit = 1
              AND fcv.dim_date_key BETWEEN %s AND %s
              AND dv.url LIKE '%%beslist.nl%%'
              AND dv.url LIKE '%%/products/%%'
              AND dv.url LIKE '%%/c/%%'
              AND dv.url NOT LIKE '%%/r/%%'
              AND dv.url NOT LIKE '%%/l/%%'
              AND dv.url NOT LIKE '%%/page_%%'
              AND dv.url NOT LIKE '%%#%%'
              AND dv.url NOT LIKE '%%device=%%'
              AND dv.url NOT LIKE '%%sortby=%%'
              AND dv.url NOT LIKE '%%shop_id=%%'
              AND dv.url NOT LIKE '%%/sitemap/%%'
              AND dv.url NOT LIKE '%%/filters/%%'
        """

        params = [int(start_date), int(end_date)]

        if contains:
            query += " AND dv.url LIKE %s"
            params.append(f"%{contains}%")

        query += """
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT %s
        """
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()

        results = []
        for row in rows:
            if isinstance(row, dict):
                results.append({"url": row.get("url"), "visits": row.get("visits")})
            else:
                results.append({"url": row[0], "visits": row[1]})
        return results

    except Exception as e:
        print(f"[ERROR] Failed to fetch URLs from Redshift: {e}")
        raise
    finally:
        if conn:
            return_redshift_connection(conn)


def generate_301_redirects(
    urls: List[str],
    facet_rules: List[FacetRule] = None,
    category_rules: List[CategoryRule] = None,
    sort_only: bool = False
) -> List[Dict[str, str]]:
    """
    Generate 301 redirects for URLs.

    Args:
        urls: List of URLs to check
        facet_rules: Optional facet transformation rules
        category_rules: Optional category transformation rules
        sort_only: If True, only check for sorting issues (no transformations)

    Returns:
        List of dicts with 'original' and 'redirect' keys (only for URLs that need redirects)
    """
    results = []

    for url in urls:
        if sort_only:
            is_sorted, corrected_url = check_facets_sorted(url)
            if not is_sorted:
                results.append({
                    "original": url,
                    "redirect": corrected_url
                })
        else:
            corrected_url, was_changed = transform_and_sort_url(url, facet_rules, category_rules)
            if was_changed:
                results.append({
                    "original": url,
                    "redirect": corrected_url
                })

    return results


def parse_facet_rules(rules_data: List[dict]) -> List[FacetRule]:
    """
    Parse facet rules from JSON input.

    Expected format:
    [
        {"old_facet": "merk~4412606", "new_facet": "materiaal~484491"},
        {"old_facet": "merk", "new_facet": "materiaal", "category": "/fietsen/"}
    ]
    """
    rules = []
    for r in rules_data:
        if r.get("old_facet") and r.get("new_facet"):
            rules.append(FacetRule(
                old_facet=r["old_facet"],
                new_facet=r["new_facet"],
                category=r.get("category")
            ))
    return rules


def parse_category_rules(rules_data: List[dict]) -> List[CategoryRule]:
    """
    Parse category rules from JSON input.

    Expected format:
    [
        {"old_cat": "fietsen_484521_484542", "new_cat": "fietsen_484521"},
        {"old_cat": "mode_123", "new_cat": "kleding_456", "new_maincat": "kleding"}
    ]
    """
    rules = []
    for r in rules_data:
        if r.get("old_cat") and r.get("new_cat"):
            rules.append(CategoryRule(
                old_cat=r["old_cat"],
                new_cat=r["new_cat"],
                new_maincat=r.get("new_maincat")
            ))
    return rules
