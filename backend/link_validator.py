import csv
import os
import re
import requests
from typing import List, Dict, Tuple, Optional
from bs4 import BeautifulSoup
from pathlib import Path

from backend.database import get_db_connection, return_db_connection

# Elasticsearch configuration
ES_URL = "https://elasticsearch-job-cluster-eck.beslist.nl"
INDEX_PREFIX = "product_search_v4_nl-nl_"

# Maincat mapping file path (relative to this file)
MAINCAT_MAPPING_FILE = Path(__file__).parent / "maincat_mapping.csv"

# Cache for maincat mapping (loaded once)
_maincat_mapping_cache: Optional[Dict[str, str]] = None


def load_maincat_mapping(filepath: Path = MAINCAT_MAPPING_FILE) -> Dict[str, str]:
    """Load maincat URL to maincat_id mapping from CSV file."""
    global _maincat_mapping_cache

    if _maincat_mapping_cache is not None:
        return _maincat_mapping_cache

    mapping = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            # maincat_url contains values like /gezond_mooi/
            url_part = row['maincat_url'].strip('/')  # Remove leading/trailing slashes
            maincat_id = row['maincat_id']
            mapping[url_part] = maincat_id

    _maincat_mapping_cache = mapping
    return mapping


def extract_from_url(url: str, maincat_mapping: Dict[str, str]) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Extract maincat_id and lookup value from URL.

    Supports three formats:
    1. Old: /p/gezond_mooi/nl-nl-gold-6150802976981/ -> maincat from mapping, pimId with prefix
    2. New numeric: /p/product-name/286/6150802976981/ -> maincat_id and pimId directly from URL
    3. V4 UUID: /p/product-name/137/V4_xxx/ -> maincat_id and plpUrl path (for plpUrl-based lookup)

    Returns: (maincat_id, lookup_value, is_v4_url) tuple
    - For V4 URLs: lookup_value is the relative plpUrl path (search by plpUrl in ES)
    - For other URLs: lookup_value is the pimId (search by pimId in ES)
    """
    url = url.rstrip('/')
    parts = url.split('/')

    # Try old format first: check if any part matches maincat_url mapping
    for url_part, maincat_id in maincat_mapping.items():
        if f"/{url_part}/" in url:
            # Old format - pimId is the last part (already has nl-nl-gold- prefix)
            pim_id = parts[-1] if parts else None
            return maincat_id, pim_id, False

    # Try new formats: /p/product-name/maincat_id/pimId/
    # Pattern: the second-to-last part should be a number (maincat_id)
    if len(parts) >= 2:
        potential_maincat = parts[-2]
        potential_pim_id = parts[-1]

        if potential_maincat.isdigit():
            # Check for V4 UUID format: V4_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
            if potential_pim_id.startswith('V4_'):
                # V4 UUID format - return the plpUrl path for plpUrl-based lookup
                # The pimId in ES is different from the V4 UUID in the URL
                # Find the /p/ part of the URL to get the plpUrl path
                p_index = url.find('/p/')
                if p_index != -1:
                    plp_path = url[p_index:] + '/'
                    return potential_maincat, plp_path, True
                return None, None, False
            # Check for numeric pimId
            elif potential_pim_id.isdigit():
                # New numeric format - add nl-nl-gold- prefix to pimId
                pim_id = f"nl-nl-gold-{potential_pim_id}"
                return potential_maincat, pim_id, False

    return None, None, False


def query_elasticsearch(index: str, pim_ids: List[str], min_offers: int = 2) -> Dict[str, str]:
    """
    Query Elasticsearch for plpUrls given a list of pimIds.

    Args:
        index: Elasticsearch index name
        pim_ids: List of pimIds to look up
        min_offers: Minimum number of offers required (default: 2).
                    Products with fewer offers are treated as "gone".

    Returns:
        Dict mapping pimId to plpUrl. Products with < min_offers return None.
    """
    if not pim_ids:
        return {}

    query = {
        "_source": ["plpUrl", "pimId", "shopCount"],
        "size": len(pim_ids),
        "query": {
            "terms": {
                "pimId": pim_ids
            }
        }
    }

    url = f"{ES_URL}/{index}/_search"
    response = requests.post(url, json=query, timeout=60)
    response.raise_for_status()

    data = response.json()

    # Map pimId to plpUrl (only if shopCount >= min_offers)
    result = {}
    for hit in data.get('hits', {}).get('hits', []):
        source = hit.get('_source', {})
        pim_id = source.get('pimId')
        plp_url = source.get('plpUrl')
        shop_count = source.get('shopCount', 0) or 0

        if pim_id:
            # Only return plpUrl if product has enough offers
            if shop_count >= min_offers and plp_url:
                result[pim_id] = plp_url
            else:
                # Treat as "gone" if not enough offers
                result[pim_id] = None

    return result


def query_elasticsearch_by_plpurl(index: str, plp_urls: List[str], min_offers: int = 2) -> Dict[str, str]:
    """
    Query Elasticsearch for products by their plpUrl paths.
    Used for V4 UUID URLs where the pimId in ES differs from the URL.

    Args:
        index: Elasticsearch index name
        plp_urls: List of plpUrl paths to look up (e.g., '/p/product-name/137/V4_xxx/')
        min_offers: Minimum number of offers required (default: 2).

    Returns:
        Dict mapping plpUrl to itself if valid, None if not found or < min_offers.
    """
    if not plp_urls:
        return {}

    query = {
        "_source": ["plpUrl", "shopCount"],
        "size": len(plp_urls),
        "query": {
            "terms": {
                "plpUrl": plp_urls
            }
        }
    }

    url = f"{ES_URL}/{index}/_search"
    response = requests.post(url, json=query, timeout=60)
    response.raise_for_status()

    data = response.json()

    # Map plpUrl to itself (only if shopCount >= min_offers)
    result = {}
    for hit in data.get('hits', {}).get('hits', []):
        source = hit.get('_source', {})
        plp_url = source.get('plpUrl')
        shop_count = source.get('shopCount', 0) or 0

        if plp_url:
            if shop_count >= min_offers:
                result[plp_url] = plp_url
            else:
                result[plp_url] = None

    return result


def extract_hyperlinks_from_content(content: str) -> List[str]:
    """
    Extract all href URLs from HTML content.
    Returns list of relative URLs found in <a href="..."> tags.
    Handles both relative (/p/...) and absolute (https://www.beslist.nl/p/...) URLs.
    """
    soup = BeautifulSoup(content, 'html.parser')
    links = []

    for link in soup.find_all('a', href=True):
        href = link['href']
        # Include both relative /p/ and absolute beslist.nl/p/ URLs
        if '/p/' in href:
            # Convert absolute URLs to relative for consistent processing
            if href.startswith('https://www.beslist.nl'):
                href = href.replace('https://www.beslist.nl', '')
            links.append(href)

    return links


def lookup_plp_urls_for_content(content: str) -> Dict[str, Optional[str]]:
    """
    Look up correct plpUrls for all product links in content.

    Returns dict mapping original URLs to their correct plpUrls:
    - If plpUrl found: original_url -> correct_plpUrl
    - If product gone: original_url -> None
    """
    maincat_mapping = load_maincat_mapping()
    links = extract_hyperlinks_from_content(content)

    if not links:
        return {}

    # Group links by maincat_id for batch ES queries
    # Separate V4 URLs (query by plpUrl) from regular URLs (query by pimId)
    # Structure: {maincat_id: {lookup_value: original_url}}
    maincat_pimid_groups: Dict[str, Dict[str, str]] = {}  # For pimId lookups
    maincat_plpurl_groups: Dict[str, Dict[str, str]] = {}  # For V4 plpUrl lookups
    url_to_lookup: Dict[str, Tuple[str, bool]] = {}  # url -> (lookup_value, is_v4)

    for link in set(links):  # deduplicate
        maincat_id, lookup_value, is_v4 = extract_from_url(link, maincat_mapping)

        if maincat_id and lookup_value:
            url_to_lookup[link] = (lookup_value, is_v4)
            if is_v4:
                # V4 URL - group for plpUrl lookup
                if maincat_id not in maincat_plpurl_groups:
                    maincat_plpurl_groups[maincat_id] = {}
                maincat_plpurl_groups[maincat_id][lookup_value] = link
            else:
                # Regular URL - group for pimId lookup
                if maincat_id not in maincat_pimid_groups:
                    maincat_pimid_groups[maincat_id] = {}
                maincat_pimid_groups[maincat_id][lookup_value] = link

    # Results dict: lookup_value -> plpUrl (or None)
    lookup_to_plp_url: Dict[str, Optional[str]] = {}

    # Query by pimId for regular URLs
    for maincat_id, pim_id_map in maincat_pimid_groups.items():
        index = f"{INDEX_PREFIX}{maincat_id}"
        pim_ids = list(pim_id_map.keys())

        try:
            result = query_elasticsearch(index, pim_ids)
            for pim_id in pim_ids:
                lookup_to_plp_url[pim_id] = result.get(pim_id)
        except Exception as e:
            print(f"[LINK_VALIDATOR] Error querying ES index {index} by pimId: {e} - skipping batch (not marking as gone)")

    # Query by plpUrl for V4 URLs
    for maincat_id, plp_url_map in maincat_plpurl_groups.items():
        index = f"{INDEX_PREFIX}{maincat_id}"
        plp_urls = list(plp_url_map.keys())

        try:
            result = query_elasticsearch_by_plpurl(index, plp_urls)
            for plp_url in plp_urls:
                lookup_to_plp_url[plp_url] = result.get(plp_url)
        except Exception as e:
            print(f"[LINK_VALIDATOR] Error querying ES index {index} by plpUrl: {e} - skipping batch (not marking as gone)")

    # Build result: original_url -> correct_plpUrl (or None if GONE)
    result = {}
    for link in set(links):
        lookup_info = url_to_lookup.get(link)
        if lookup_info:
            lookup_value, _ = lookup_info
            if lookup_value in lookup_to_plp_url:
                result[link] = lookup_to_plp_url[lookup_value]
            # else: ES query failed for this link - skip it entirely (don't mark as gone)
        else:
            # Could not extract lookup value from URL - treat as GONE
            result[link] = None

    return result


def replace_url_in_content(content: str, old_url: str, new_url: str) -> str:
    """Replace all occurrences of old_url with new_url in HTML content."""
    # Use BeautifulSoup to properly handle HTML
    soup = BeautifulSoup(content, 'html.parser')

    for link in soup.find_all('a', href=old_url):
        link['href'] = new_url

    return str(soup)


def validate_and_fix_content_links(content: str, content_url: str) -> Dict:
    """
    Validate all product hyperlinks in content and fix them if needed.

    Returns dict with:
    {
        'content_url': str,  # The URL this content belongs to
        'original_content': str,  # Original content
        'corrected_content': str,  # Content with corrected URLs (or original if no changes)
        'has_changes': bool,  # Whether content was modified
        'replaced_urls': List[Dict],  # List of {old_url, new_url}
        'gone_urls': List[str],  # URLs where product is gone (need reprocessing)
        'valid_urls': List[str],  # URLs that were already correct
    }
    """
    result = {
        'content_url': content_url,
        'original_content': content,
        'corrected_content': content,
        'has_changes': False,
        'replaced_urls': [],
        'gone_urls': [],
        'valid_urls': []
    }

    if not content:
        return result

    # Look up correct plpUrls for all links
    url_lookup = lookup_plp_urls_for_content(content)

    if not url_lookup:
        return result

    corrected_content = content

    for original_url, correct_plp_url in url_lookup.items():
        if correct_plp_url is None:
            # Product is GONE - mark for reprocessing
            result['gone_urls'].append(original_url)
        elif correct_plp_url != original_url:
            # URL needs to be replaced
            corrected_content = replace_url_in_content(corrected_content, original_url, correct_plp_url)
            result['replaced_urls'].append({
                'old_url': original_url,
                'new_url': correct_plp_url
            })
            result['has_changes'] = True
        else:
            # URL is already correct
            result['valid_urls'].append(original_url)

    result['corrected_content'] = corrected_content
    return result


def update_content_in_redshift(content_url: str, new_content: str) -> bool:
    """
    Update content in local PostgreSQL pa.content_urls_joep table.
    Returns True if successful.
    Note: Function name kept for backwards compatibility.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            UPDATE pa.content_urls_joep
            SET content = %s
            WHERE url = %s
        """, (new_content, content_url))

        conn.commit()
        print(f"[LINK_VALIDATOR] Updated content for URL: {content_url}")
        return True
    except Exception as e:
        print(f"[LINK_VALIDATOR] Error updating content in PostgreSQL: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_db_connection(conn)


def add_urls_to_werkvoorraad(urls: List[str]) -> int:
    """
    Add URLs to werkvoorraad table for reprocessing.
    Inserts into pa.jvs_seo_werkvoorraad with kopteksten=0.
    Returns number of URLs added.
    """
    if not urls:
        return 0

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        added_count = 0
        for url in urls:
            try:
                # PostgreSQL supports ON CONFLICT
                cur.execute("""
                    INSERT INTO pa.jvs_seo_werkvoorraad (url, kopteksten)
                    VALUES (%s, 0)
                    ON CONFLICT (url) DO UPDATE SET kopteksten = 0
                """, (url,))
                added_count += cur.rowcount
            except Exception as e:
                print(f"[LINK_VALIDATOR] Error adding URL {url} to werkvoorraad: {e}")

        conn.commit()
        print(f"[LINK_VALIDATOR] Added {added_count} URLs to werkvoorraad for reprocessing")
        return added_count
    except Exception as e:
        print(f"[LINK_VALIDATOR] Error adding URLs to werkvoorraad: {e}")
        if conn:
            conn.rollback()
        return 0
    finally:
        if conn:
            return_db_connection(conn)


def validate_and_fix_content_batch(contents: List[Tuple[str, str]],
                                    auto_update_db: bool = False,
                                    auto_add_to_werkvoorraad: bool = False) -> Dict:
    """
    Validate and fix hyperlinks for multiple content items.

    Args:
        contents: List of tuples (content_url, content)
        auto_update_db: If True, automatically update corrected content in PostgreSQL
        auto_add_to_werkvoorraad: If True, automatically add gone URLs to werkvoorraad

    Returns:
        Dict with summary and detailed results
    """
    results = []
    all_gone_urls = []
    total_replaced = 0
    total_updated = 0

    for content_url, content in contents:
        validation = validate_and_fix_content_links(content, content_url)
        results.append(validation)

        # Collect gone URLs
        all_gone_urls.extend(validation['gone_urls'])

        # Update PostgreSQL if content changed and auto_update is enabled
        if validation['has_changes'] and auto_update_db:
            if update_content_in_redshift(content_url, validation['corrected_content']):
                total_updated += 1

        total_replaced += len(validation['replaced_urls'])

    # Add gone URLs to werkvoorraad if enabled
    urls_added_to_werkvoorraad = 0
    if auto_add_to_werkvoorraad and all_gone_urls:
        urls_added_to_werkvoorraad = add_urls_to_werkvoorraad(all_gone_urls)

    return {
        'total_content_items': len(contents),
        'total_urls_replaced': total_replaced,
        'total_content_updated_in_db': total_updated,
        'total_gone_urls': len(all_gone_urls),
        'urls_added_to_werkvoorraad': urls_added_to_werkvoorraad,
        'gone_urls': list(set(all_gone_urls)),  # deduplicated
        'details': results
    }


# --- FAQ Link Validation ---

def extract_hyperlinks_from_faq_json(faq_json: str) -> List[str]:
    """
    Extract all product href URLs from FAQ JSON content.
    FAQ JSON is a list of {question, answer} objects where answers may contain HTML links.
    Handles both relative (/p/...) and absolute (https://www.beslist.nl/p/...) URLs.
    """
    import json

    links = []
    try:
        faqs = json.loads(faq_json) if faq_json else []
        for faq in faqs:
            answer = faq.get('answer', '')
            # Extract links from each answer using BeautifulSoup
            soup = BeautifulSoup(answer, 'html.parser')
            for link in soup.find_all('a', href=True):
                href = link['href']
                # Include both relative /p/ and absolute beslist.nl/p/ URLs
                if '/p/' in href:
                    links.append(href)
    except json.JSONDecodeError:
        pass

    return links


def validate_faq_links(faq_json: str) -> Dict:
    """
    Validate all product hyperlinks in FAQ JSON content.

    Returns dict with:
    {
        'total_links': int,
        'valid_links': int,
        'gone_links': List[str],  # URLs where product is gone
        'has_gone_links': bool,
    }
    """
    links = extract_hyperlinks_from_faq_json(faq_json)

    if not links:
        return {
            'total_links': 0,
            'valid_links': 0,
            'gone_links': [],
            'has_gone_links': False
        }

    # Look up all links
    maincat_mapping = load_maincat_mapping()

    # Group links by maincat_id for batch ES queries
    # Separate V4 URLs (query by plpUrl) from regular URLs (query by pimId)
    maincat_pimid_groups: Dict[str, Dict[str, str]] = {}
    maincat_plpurl_groups: Dict[str, Dict[str, str]] = {}
    url_to_lookup: Dict[str, Tuple[str, bool]] = {}  # url -> (lookup_value, is_v4)

    unique_links = list(set(links))

    for link in unique_links:
        # Handle both relative and absolute URLs
        relative_link = link
        if link.startswith('https://www.beslist.nl'):
            relative_link = link.replace('https://www.beslist.nl', '')

        maincat_id, lookup_value, is_v4 = extract_from_url(relative_link, maincat_mapping)

        if maincat_id and lookup_value:
            url_to_lookup[link] = (lookup_value, is_v4)
            if is_v4:
                if maincat_id not in maincat_plpurl_groups:
                    maincat_plpurl_groups[maincat_id] = {}
                maincat_plpurl_groups[maincat_id][lookup_value] = link
            else:
                if maincat_id not in maincat_pimid_groups:
                    maincat_pimid_groups[maincat_id] = {}
                maincat_pimid_groups[maincat_id][lookup_value] = link

    # Results dict
    lookup_to_plp_url: Dict[str, Optional[str]] = {}

    # Query by pimId for regular URLs
    for maincat_id, pim_id_map in maincat_pimid_groups.items():
        index = f"{INDEX_PREFIX}{maincat_id}"
        pim_ids = list(pim_id_map.keys())

        try:
            result = query_elasticsearch(index, pim_ids)
            for pim_id in pim_ids:
                lookup_to_plp_url[pim_id] = result.get(pim_id)
        except Exception as e:
            print(f"[FAQ_VALIDATOR] Error querying ES index {index} by pimId: {e} - skipping batch (not marking as gone)")

    # Query by plpUrl for V4 URLs
    for maincat_id, plp_url_map in maincat_plpurl_groups.items():
        index = f"{INDEX_PREFIX}{maincat_id}"
        plp_urls = list(plp_url_map.keys())

        try:
            result = query_elasticsearch_by_plpurl(index, plp_urls)
            for plp_url in plp_urls:
                lookup_to_plp_url[plp_url] = result.get(plp_url)
        except Exception as e:
            print(f"[FAQ_VALIDATOR] Error querying ES index {index} by plpUrl: {e} - skipping batch (not marking as gone)")

    # Determine which links are gone
    gone_links = []
    valid_count = 0
    skipped_count = 0

    for link in unique_links:
        lookup_info = url_to_lookup.get(link)
        if lookup_info:
            lookup_value, _ = lookup_info
            if lookup_value not in lookup_to_plp_url:
                skipped_count += 1
                continue
            plp_url = lookup_to_plp_url[lookup_value]
            if plp_url is None:
                gone_links.append(link)
            else:
                valid_count += 1
        else:
            # Could not extract pim_id - treat as gone
            gone_links.append(link)

    return {
        'total_links': len(unique_links),
        'valid_links': valid_count,
        'gone_links': gone_links,
        'has_gone_links': len(gone_links) > 0
    }


def validate_faq_batch(faqs: List[Tuple[str, str]]) -> Dict:
    """
    Validate links for multiple FAQ entries.

    Args:
        faqs: List of tuples (url, faq_json)

    Returns:
        Dict with validation summary and list of URLs with gone products
    """
    urls_with_gone_products = []
    total_links = 0
    total_valid = 0
    total_gone = 0

    for url, faq_json in faqs:
        result = validate_faq_links(faq_json)
        total_links += result['total_links']
        total_valid += result['valid_links']
        total_gone += len(result['gone_links'])

        if result['has_gone_links']:
            urls_with_gone_products.append({
                'url': url,
                'gone_links': result['gone_links']
            })

    return {
        'total_faqs_checked': len(faqs),
        'total_links_checked': total_links,
        'total_valid_links': total_valid,
        'total_gone_links': total_gone,
        'faqs_with_gone_products': len(urls_with_gone_products),
        'urls_with_gone_products': urls_with_gone_products
    }


def reset_faq_to_pending(urls: List[str]) -> int:
    """
    Reset FAQ URLs to pending status for regeneration.
    - Updates pa.faq_tracking to 'pending'
    - Deletes from pa.faq_content

    Returns number of URLs reset.
    """
    if not urls:
        return 0

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        reset_count = 0
        for url in urls:
            # Update tracking to pending
            cur.execute("""
                UPDATE pa.faq_tracking
                SET status = 'pending', skip_reason = NULL
                WHERE url = %s
            """, (url,))

            # Delete existing FAQ content
            cur.execute("""
                DELETE FROM pa.faq_content
                WHERE url = %s
            """, (url,))

            reset_count += 1

        conn.commit()
        print(f"[FAQ_VALIDATOR] Reset {reset_count} FAQ URLs to pending")
        return reset_count
    except Exception as e:
        print(f"[FAQ_VALIDATOR] Error resetting FAQ URLs: {e}")
        if conn:
            conn.rollback()
        return 0
    finally:
        if conn:
            return_db_connection(conn)


# Legacy function for backward compatibility
def validate_content_links(content: str, conservative_mode: bool = False) -> Dict:
    """
    Legacy function - now uses Elasticsearch lookup instead of HTTP requests.
    The conservative_mode parameter is kept for backward compatibility but ignored.

    Returns dict with validation results in the old format for compatibility.
    """
    result = validate_and_fix_content_links(content, "")

    # Convert to legacy format
    broken_links = []
    for gone_url in result['gone_urls']:
        broken_links.append({
            'url': gone_url,
            'full_url': f"https://www.beslist.nl{gone_url}",
            'status_code': 'GONE',
            'status_text': 'Product not found in Elasticsearch'
        })

    return {
        'total_links': len(result['valid_urls']) + len(result['gone_urls']) + len(result['replaced_urls']),
        'broken_links': broken_links,
        'valid_links': len(result['valid_urls']),
        'has_broken_links': len(broken_links) > 0,
        'replaced_urls': result['replaced_urls'],
        'corrected_content': result['corrected_content']
    }


# Test function
if __name__ == "__main__":
    # Test with sample content
    test_content = '''
    <p>Check out these products:</p>
    <a href="/p/gezond_mooi/nl-nl-gold-6150802976981/">Product 1</a>
    <a href="/p/product-name/286/6150802976981/">Product 2</a>
    '''

    print("Testing link validator with Elasticsearch lookup...")
    result = validate_and_fix_content_links(test_content, "test-url")
    print(f"Result: {result}")
